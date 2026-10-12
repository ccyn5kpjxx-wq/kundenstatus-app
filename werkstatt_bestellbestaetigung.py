"""Explicit, immutable order links to dated supplier confirmations, never invoices."""
from decimal import Decimal, ROUND_HALF_UP
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from werkstatt_artikel_import import _price_evidence
from werkstatt_einkaufseingang import IntakeConflict, _hash, _id, _json, _norm, _text
from werkstatt_rechnungsfreigabe import normalize_supplier
from werkstatt_rechnungsquelle import invoice_date


TABLE = 'assistent_bestellpreis_bestaetigungen'
_REQUEST_PROPOSALS = ContextVar('order_confirmation_proposals',default=None)


def init_confirmation_schema(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS assistent_bestellpreis_bestaetigungen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,order_key TEXT NOT NULL,order_fingerprint TEXT NOT NULL,
        source_key TEXT NOT NULL UNIQUE,payload_hash TEXT NOT NULL,payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,created_by TEXT NOT NULL);''')


def _number(value, label, positive=False):
    if not isinstance(value,str) or not re.fullmatch(r'\d+(?:\.\d+)?',value):
        raise ValueError(label + ' ist nicht eindeutig belegt.')
    number = Decimal(value)
    if not number.is_finite() or number < 0 or positive and number == 0 or number > 10000000:
        raise ValueError(label + ' ist nicht eindeutig belegt.')
    return number


class OrderConfirmationLinks:
    def __init__(self, portal, comparison=None):
        self.p = portal
        self.comparison = comparison or portal.order_price_comparison

    def _proposals(self):
        """One scoped snapshot and one original hash per document, no OCR/import."""
        cached = _REQUEST_PROPOSALS.get()
        if cached is not None and cached['service'] is self:
            return cached['proposals']
        return self._fresh_proposals()

    @contextmanager
    def request_snapshot(self):
        # Only this optional read is recoverable. Do not hide failures in the
        # caller's invoice/delivery logic, and never reuse an older quote.
        try:
            proposals = self._fresh_proposals()
        except Exception as error:
            logging.getLogger(__name__).warning('Optional confirmation source unavailable (%s)',type(error).__name__)
            proposals = {}
        token = _REQUEST_PROPOSALS.set({'service':self,'proposals':proposals})
        try:
            yield
        finally:
            _REQUEST_PROPOSALS.reset(token)

    def _fresh_proposals(self):
        catalog = self.comparison._catalog()
        snapshot = catalog.knowledge_rows(limit=5000)
        if snapshot.get('truncated'):
            raise ValueError('Der Artikelbestand ist begrenzt; Auftragsbestätigung zuerst eindeutig eingrenzen.')
        candidates = [row for row in snapshot['items']
                      if row.get('document_type') == 'auftragsbestaetigung'
                      and (row.get('quelle') or {}).get('beleg_typ') == 'auftragsbestaetigung']
        if not candidates:
            return {}
        originals = {}
        imports = {}
        with self.comparison.db() as db:
            for row in db.execute("SELECT id,beleg_typ,lieferant,original_name,stored_name,status FROM einkauf_belege WHERE beleg_typ='auftragsbestaetigung'").fetchall():
                originals[row['id']] = dict(row)
            for row in db.execute('''SELECT a.id,i.id AS import_id,i.state,i.source_kind,i.source_id,i.supplier
                FROM assistent_rechnungsartikel a JOIN assistent_rechnungsimporte i ON i.id=a.import_id WHERE a.active=1''').fetchall():
                imports[row['id']] = dict(row)
        root = Path(self.p.UPLOAD_DIR).resolve()
        original_hashes = {}
        result = {}
        for row in candidates:
            source = row.get('quelle') or {}
            try:
                original = originals.get(_id(source.get('beleg_id')))
            except ValueError:
                continue
            imported = imports.get(row.get('vorschlag_id'))
            if (source.get('art') != 'einkauf' or not original
                    or not imported or imported['state'] not in {'ausgelesen','pruefen'}
                    or imported['source_kind'] != 'einkauf' or str(original['id']) != str(imported['source_id'])
                    or normalize_supplier(imported['supplier']) != normalize_supplier(row.get('lieferant'))
                    or str(original.get('status') or '').casefold() in {'deleted','void','voided','cancelled','canceled','geloescht','gelöscht','storniert','quarantaene','quarantäne','quarantine','gesperrt'}
                    or normalize_supplier(original['lieferant']) != normalize_supplier(row.get('lieferant'))
                    or catalog.source_rule(original).get('allowed') is not True):
                continue
            stored = original.get('stored_name') or ''
            if not stored or '/' in stored or '\\' in stored:
                continue
            path = (root / stored).resolve()
            if path.parent != root or not path.is_file() or path.stat().st_size > 20*1024*1024:
                continue
            if original['id'] not in original_hashes:
                original_hashes[original['id']] = hashlib.sha256(path.read_bytes()).hexdigest()
            digest = original_hashes[original['id']]
            if digest != source.get('datei_sha256'):
                continue
            evidence = _price_evidence(row.get('price_evidence'))
            metadata = evidence.get('metadata_source') or {}
            if (evidence.get('document_type') != 'auftragsbestaetigung'
                    or evidence.get('basis') != 'auftragsbestaetigung_netto_positionswert'
                    or evidence.get('currency') != 'EUR' or evidence.get('tax_basis') != 'net'
                    or metadata.get('tax_summary_reconciled') is not True
                    or not invoice_date(source.get('datum'))
                    or not isinstance(row.get('artikelnummer'),str) or not row['artikelnummer'].strip()
                    or not row.get('ve') or not row.get('gebinde')):
                continue
            try:
                quantity = _number(evidence.get('quantity'),'Belegmenge',True)
                total = _number(evidence.get('quoted_line_total_net'),'Gedruckter Netto-Positionspreis')
                quote = _number(evidence.get('quoted_unit_net'),'Netto-Gebindepreis')
                rate = _number(evidence.get('tax_rate'),'Steuersatz')
                if rate > 100 or abs(quote*quantity-total) > Decimal('.000001'):
                    continue
                page,position = _id(source.get('seite')),_id(source.get('position'))
                proposal_id = _id(row.get('vorschlag_id'))
            except ValueError:
                continue
            canonical = dict(row,original=dict(original,sha256=digest),imported=imported)
            result[proposal_id] = {
                'proposal_id':proposal_id,'proposal_fingerprint':_hash(canonical),'import_id':imported['import_id'],
                'supplier':row['lieferant'],'sku':row['artikelnummer'],'product':row['produkt_name'],
                'unit':row['ve'],'package':row['gebinde'],'variant':row.get('groesse') or row.get('farbe') or '',
                'amount':format(quote.quantize(Decimal('.01'),rounding=ROUND_HALF_UP),'f'),
                'quoted_unit_net':str(quote),'quantity':str(quantity),'line_amount':str(total),
                'currency':'EUR','tax_basis':'net','tax_rate':str(rate),'date':invoice_date(source['datum']),
                'source':dict(source,page=page,position=position,sha256=digest,reference=original['original_name']),
                'source_key':f'original:{digest}:{page}:{position}',
                'source_url':f"/admin/einkauf/beleg/{original['id']}/datei",
                'formula_warning':evidence.get('quote_formula_reconciled') is not True,
                'evidence':evidence,'source_notes':row.get('auslese_hinweise') or [],
            }
            result[proposal_id].update(id=proposal_id,packaging=row['gebinde'],position=position,
                date_display=datetime.fromisoformat(invoice_date(source['datum'])).strftime('%d.%m.%Y'),
                source_reference=original['original_name'],source_url=f"/admin/einkauf/beleg/datei/{original['id']}",
                label=f"Pos. {position}: {row['produkt_name']} · {row['artikelnummer']} · {result[proposal_id]['amount']} € netto/{row['ve']} · {row['gebinde']}")
        return result

    @staticmethod
    def _review(order, quote, reason):
        notes = []
        sku = order.get('sku') or ''
        if not sku:
            notes.append('Artikelnummer der ursprünglichen Bestellung nicht belegt; keine automatische Artikelgleichsetzung.')
        elif _norm(sku) != _norm(quote['sku']) and not re.search(r'(?<![\w/])'+re.escape(sku)+r'(?![\w/])',quote['product'],re.I):
            notes.append('Artikelnummer/Artikel der Bestätigung unterscheidet sich von der ursprünglichen Bestellung.')
        order_unit,quote_unit = _norm(order['unit']),_norm(quote['unit'])
        single = {'stuck','stueck','stück','gebinde','dose','flasche','rolle'}
        if order_unit != quote_unit and not (order_unit in single and quote_unit in single):
            notes.append('Bestelleinheit/Kartoninhalt stimmt noch nicht eindeutig mit dem bestätigten Gebinde überein.')
        if order['quantity'] != quote['quantity'] and Decimal(order['quantity']) != Decimal(quote['quantity']):
            notes.append('Menge der Bestätigung unterscheidet sich von der ursprünglichen Bestellmenge.')
        from werkstatt_bestellvergleich import _variant_conflict
        if _variant_conflict(order,{'produkt_name':quote['product'],'groesse':quote['variant'],'gebinde':quote['package']}):
            notes.append('Variante oder Gebinde der Bestätigung weicht von der ursprünglichen Bestellung ab.')
        if quote['formula_warning']:
            notes.append('Gedruckter Quote-Preis bleibt erhalten; Formelberechnung am Original prüfen.')
        if reason:
            notes.append(reason)
        return notes

    def record_batch(self, assignments, actor='admin'):
        self.comparison._actor(actor)
        if not isinstance(assignments,(list,tuple)) or not 1 <= len(assignments) <= 100:
            raise ValueError('Eine bis 100 ausdrücklich zugeordnete AB-Positionen angeben.')
        required = {'order_key','order_fingerprint','proposal_id','proposal_fingerprint','review_reason','reviewed'}
        with self.p.portal_originals_operation_lock():
            proposals = self._fresh_proposals()
            prepared = []
            with self.comparison.db() as db:
                # Retry/quarantine and source edits use these same database
                # rows. Lock them before validating a fresh candidate set so
                # they cannot revoke or rewrite evidence between check/commit.
                requested = sorted({_id(item.get('proposal_id')) for item in assignments if isinstance(item,dict)})
                # Catalog completion locks import rows before article rows.
                # Follow that order (sorted across a batch) on PostgreSQL too.
                for import_id in sorted({proposals[ident]['import_id'] for ident in requested if ident in proposals}):
                    db.execute('UPDATE assistent_rechnungsimporte SET state=state WHERE id=?',(import_id,))
                for proposal_id in requested:
                    db.execute('UPDATE assistent_rechnungsartikel SET active=active WHERE id=?',(proposal_id,))
                for original_id in sorted({proposals[ident]['source']['beleg_id'] for ident in requested if ident in proposals}):
                    db.execute('UPDATE einkauf_belege SET status=status WHERE id=?',(original_id,))
                proposals = self._fresh_proposals()
                for item in assignments:
                    if not isinstance(item,dict) or set(item) != required or item.get('reviewed') is not True:
                        raise ValueError('Die Zuordnung zum Bestellvorgang am Original ausdrücklich prüfen.')
                    order = self.comparison._order(db,item['order_key'])
                    if not order['dispatch_known']:
                        raise ValueError('Nur bereits versandte Bestellungen mit einer Auftragsbestätigung verknüpfen.')
                    if order['fingerprint'] != item['order_fingerprint']:
                        raise IntakeConflict('Die Bestellung wurde geändert. Bitte neu laden.')
                    quote = proposals.get(_id(item['proposal_id']))
                    if not quote or quote['proposal_fingerprint'] != item['proposal_fingerprint']:
                        raise IntakeConflict('Die Auftragsbestätigungsquelle wurde geändert oder gesperrt. Bitte neu laden.')
                    if normalize_supplier(order['supplier']) != normalize_supplier(quote['supplier']):
                        raise ValueError('Auftragsbestätigung gehört einem anderen Lieferanten.')
                    order_day = datetime.fromisoformat(order['created_at']).astimezone(ZoneInfo('Europe/Berlin')).date().isoformat() if order['created_at'] else None
                    if order_day and quote['date'] < order_day:
                        raise ValueError('Auftragsbestätigung liegt vor dieser Bestellung.')
                    reason = _text(item['review_reason'],'Prüfhinweis',500,optional=True)
                    notes = self._review(order,quote,'')
                    if notes and not reason:
                        raise ValueError('Abweichende oder unklare Artikel-/Gebindeangaben mit einem Prüfhinweis zuordnen.')
                    record = dict(quote,order_key=order['key'],order_fingerprint=order['fingerprint'],
                                  original_order={name:order[name] for name in ('sku','product','variant','quantity','unit')},
                                  review_required=bool(notes),review_notes=notes,review_reason=reason,
                                  document_type='auftragsbestaetigung',can_dispatch=False,price_approved=False)
                    prepared.append((order,record))
                if (len({order['key'] for order,_ in prepared}) != len(prepared)
                        or len({row['source_key'] for _,row in prepared}) != len(prepared)):
                    raise ValueError('Jede Bestellung und jede AB-Belegposition in dieser Auswahl genau einmal zuordnen.')
                for order,record in prepared:
                    digest = _hash(record)
                    old = db.execute(f'SELECT * FROM {TABLE} WHERE source_key=?',(record['source_key'],)).fetchone()
                    if old and (old['order_key'] != order['key'] or old['payload_hash'] != digest):
                        raise IntakeConflict('Diese AB-Belegposition ist bereits anders zugeordnet; keine Doppelzuordnung.')
                    if not old:
                        db.execute(f'''INSERT INTO {TABLE}(order_key,order_fingerprint,source_key,payload_hash,payload_json,created_at,created_by)
                            VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING''',
                            (order['key'],order['fingerprint'],record['source_key'],digest,_json(record),self.comparison._now(),'admin'))
                    saved = db.execute(f'SELECT order_key,payload_hash FROM {TABLE} WHERE source_key=?',(record['source_key'],)).fetchone()
                    if saved['order_key'] != order['key'] or saved['payload_hash'] != digest:
                        raise IntakeConflict('Gleichzeitige abweichende AB-Zuordnung; gespeicherter Nachweis bleibt erhalten.')
                # A source can be quarantined or replaced during an upload or
                # restore. Recheck the same original-backed candidate set before
                # publishing any links; the whole transaction then rolls back.
                fresh = self._fresh_proposals()
                if any(not fresh.get(record['proposal_id'])
                       or fresh[record['proposal_id']]['proposal_fingerprint'] != record['proposal_fingerprint']
                       for _,record in prepared):
                    raise IntakeConflict('Auftragsbestätigungsquelle wurde während der Zuordnung geändert.')
            return {'count':len(prepared),'order_keys':[order['key'] for order,_ in prepared]}

    def _view(self, row, order, proposals):
        record = json.loads(row['payload_json'])
        proposal = proposals.get(record.get('proposal_id'))
        if (row['order_fingerprint'] != order['fingerprint'] or _hash(record) != row['payload_hash']
                or not proposal or proposal['proposal_fingerprint'] != record.get('proposal_fingerprint')):
            return {'available':False,'review_required':True,'reason':'AB-Preisquelle ist geändert, gesperrt oder passt nicht zur unveränderten Bestellung.'}
        return dict(record,available=True,created_at=row['created_at'],created_by=row['created_by'],
                    label='Preis laut Auftragsbestätigung vom '+record['date'],
                    can_dispatch=False,price_approved=False)

    def summaries(self, entries, *, proposals=None):
        proposals = self._proposals() if proposals is None else proposals
        results = {}
        with self.comparison.db() as db:
            for entry in entries:
                key = entry if isinstance(entry,str) else entry.get('source') or entry.get('order_key')
                if not key or not key.startswith(('material:','order:','dispatch:')):
                    continue
                try:
                    order = self.comparison._order(db,key)
                except (ValueError,LookupError,PermissionError):
                    continue
                rows = db.execute(f'SELECT * FROM {TABLE} WHERE order_key=? ORDER BY id',(order['key'],)).fetchall()
                if rows:
                    results[key] = self._view(rows[-1],order,proposals)
        return results

    def batch_form(self, entries):
        proposals = self._proposals()
        results = []
        with self.comparison.db() as db:
            for entry in entries:
                key = entry if isinstance(entry,str) else entry.get('source') or entry.get('order_key')
                if not key:
                    continue
                try:
                    order = self.comparison._order(db,key)
                except (ValueError,LookupError,PermissionError):
                    continue
                if not order['dispatch_known']:
                    continue
                candidates = [dict(quote,review_notes=self._review(order,quote,'')) for quote in proposals.values()
                              if normalize_supplier(quote['supplier']) == normalize_supplier(order['supplier'])]
                candidates.sort(key=lambda quote:(quote['date'],quote['source']['position']),reverse=True)
                results.append(dict(source=key,order_key=order['key'],order_fingerprint=order['fingerprint'],order=order,
                                    proposals=candidates))
        return {'orders':results,'summaries':self.summaries(entries,proposals=proposals)}


def get_order_confirmations(portal, comparison=None):
    service = getattr(portal,'order_confirmation_links',None)
    if service is None:
        service = portal.order_confirmation_links = OrderConfirmationLinks(portal,comparison)
    return service
