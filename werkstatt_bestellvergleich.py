"""Frozen historical estimates and explicit invoice matching, never dispatch.

The estimate is a separate appendix to an immutable order. It is not a current
price approval, a payment, or a modification of the supplier's purchase order.
Originals and catalog permissions belong to the existing intake/catalog services.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import re
from zoneinfo import ZoneInfo

from werkstatt_artikel_identity import parse_unit_price
from werkstatt_artikel_import import _price_evidence
from werkstatt_einkaufseingang import MaterialIntake, IntakeConflict, _decimal, _hash, _id, _json, _norm, _text
from werkstatt_rechnungsquelle import invoice_date
from werkstatt_rechnungsfreigabe import normalize_supplier
from werkstatt_materialwissen import _measurements, _colors, tokens


TABLES = ('assistent_bestellpreis_basis', 'assistent_bestellpreis_rechnungen')


def _stamp(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return None


def _identity(supplier, sku, variant):
    return {'supplier': normalize_supplier(supplier), 'sku': _norm(sku), 'variant': _norm(variant)}


def _variant_conflict(order, row):
    """Reject known contradictions; confirmation cannot erase source facts."""
    def facts(value):
        value = re.sub(r'\b[Ll]iter\b', 'l', value)
        dimensions = {}
        for amount, unit in _measurements(value):
            # This only detects contradictions (500 ml vs 1 l), never converts
            # an order quantity or its price unit for purchasing/comparison.
            amount = Decimal(amount)
            if unit in {'l', 'kg'}:
                amount *= 1000
                unit = {'l': 'ml', 'kg': 'g'}[unit]
            dimensions.setdefault(unit, set()).add(amount)
        normalized = ' '.join(tokens(value)).replace(',', '.')
        for amount, unit in re.findall(r'\b(\d+(?:\.\d+)?)\s+(rolle|stueck|pack|karton|dose|set)\b', normalized):
            dimensions.setdefault(unit, set()).add(Decimal(amount))
        return dimensions, _colors(value)
    expected, colors = facts(' '.join(str(order.get(key) or '') for key in ('product', 'variant')))
    actual, actual_colors = facts(' '.join(str(row.get(key) or '') for key in
        ('produkt_name', 'groesse', 'farbe', 'gebinde', 've')))
    return (any(expected[unit] != actual[unit] for unit in expected.keys() & actual.keys())
            or bool(colors and actual_colors and colors != actual_colors))


class OrderPriceComparison:
    def __init__(self, portal):
        self.p = portal
        self.init_schema()

    @contextmanager
    def db(self):
        db = self.p.get_db()
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def init_schema(self):
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS assistent_bestellpreis_basis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, order_key TEXT NOT NULL UNIQUE, order_fingerprint TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
                    dispatch_state_at_capture TEXT NOT NULL,
                    created_at TEXT NOT NULL, created_by TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS assistent_bestellpreis_rechnungen (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, order_key TEXT NOT NULL,
                    order_fingerprint TEXT NOT NULL, source_key TEXT NOT NULL UNIQUE,
                    payload_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
                    dispatch_state_at_capture TEXT NOT NULL,
                    created_at TEXT NOT NULL, created_by TEXT NOT NULL);
            ''')

    @staticmethod
    def _actor(actor):
        return MaterialIntake._actor(actor)

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat()

    def _order(self, db, key):
        if not isinstance(key, str) or len(key) > 180:
            raise ValueError('Bestellung eindeutig angeben.')
        if key.startswith('dispatch:'):
            key = 'order:' + key[9:]
        material = re.fullmatch(r'material:([1-9][0-9]*)', key)
        if material:
            draft = db.execute('SELECT * FROM einkauf_material_dialoge WHERE id=?', (int(material[1]),)).fetchone()
            if draft and draft['state'] in {'external_pending', 'external_sent'}:
                claim = json.loads(draft['snapshot_json'])
                if claim.get('kind') != 'manual_external_order' or _hash(claim) != draft['snapshot_hash']:
                    raise ValueError('Gespeicherte externe Bestellung ist nicht unverändert belegt.')
                if (claim.get('draft_id') != int(material[1]) or not re.fullmatch(r'[a-f0-9]{32}', str(claim.get('reservation_id', '')))
                        or draft['state'] == 'external_sent' and (not _stamp(claim.get('sent_at'))
                        or not isinstance(claim.get('send_evidence'), str) or not claim['send_evidence'].strip())):
                    raise ValueError('Externer Reservierungs- oder Versandnachweis ist unvollständig.')
                stable = {name: claim.get(name) for name in ('reservation_id', 'draft_id', 'supplier_id',
                    'supplier_name', 'recipient', 'article_number', 'product_name', 'variant', 'quantity',
                    'unit', 'urgent', 'max_total_cents', 'reserved_at')}
                return self._order_view(key, stable, claim['supplier_name'], claim,
                    claim.get('reserved_at'), claim.get('sent_at'), 'external', draft['state'])
            rows = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE request_id=? OR id=?',
                (key, dict(draft).get('dispatch_id', '') if draft else '')).fetchall()
            if len(rows) != 1:
                raise ValueError('Noch keine eindeutige dauerhafte Bestellung für diesen Materialvorgang vorhanden.')
            row = rows[0]
        else:
            if not re.fullmatch(r'order:[A-Za-z0-9_-]{1,160}', key):
                raise ValueError('Bestellschlüssel ist ungültig.')
            row = db.execute('SELECT * FROM assistent_bestellanforderungen WHERE id=?', (key[6:],)).fetchone()
            if not row:
                raise ValueError('Gespeicherte Bestellung nicht gefunden.')
            if re.fullmatch(r'material:[1-9][0-9]*', row['request_id']):
                return self._order(db, row['request_id'])
            linked = db.execute("SELECT id FROM einkauf_material_dialoge WHERE dispatch_id=? AND dispatch_id<>''", (row['id'],)).fetchall()
            if len(linked) > 1:
                raise ValueError('Bestellung ist mehreren Materialvorgängen zugeordnet; zuerst klären.')
            if linked:
                return self._order(db, 'material:' + str(linked[0]['id']))
        saved = json.loads(row['snapshot_json'])
        intent = saved.get('order')
        if not isinstance(intent, dict) or _hash(intent) != row['request_fingerprint'] or saved.get('actor_id') != row['actor_id']:
            raise ValueError('Gespeicherte Bestellung stimmt nicht mit ihrem Freigabenachweis überein.')
        canonical = key if material else 'order:' + row['id']
        batch = db.execute('SELECT state,result_json FROM assistent_bestellpakete WHERE id=?', (row['batch_id'],)).fetchone() if row['batch_id'] else None
        state = batch['state'] if batch else 'queued'
        # A batch result may not contain a reliable send timestamp. Keep this
        # uncertainty visible instead of substituting the order creation time.
        result = json.loads(batch['result_json']) if batch else {}
        sent_at = _stamp(result.get('sent_at')) if state in {'sent', 'copy_pending', 'partial'} else None
        return self._order_view(canonical, {'id': row['id'], 'fingerprint': row['request_fingerprint']},
            saved.get('supplier_name', ''), intent, row['created_at'], sent_at, 'dispatch', state)

    @staticmethod
    def _order_view(key, stable, supplier, order, created, sent, kind, state):
        if not supplier or not order.get('article_number') or not order.get('unit'):
            raise ValueError('Lieferant, Artikelnummer oder Bestelleinheit der Bestellung fehlen.')
        return {'key': key, 'fingerprint': _hash(stable), 'kind': kind, 'state': state,
            'supplier': supplier, 'sku': order['article_number'], 'product': order.get('product_name', ''),
            'variant': order.get('variant', ''), 'unit': order['unit'], 'quantity': str(order.get('quantity', '')),
            'identity': _identity(supplier, order['article_number'], order.get('variant', '')),
            'created_at': _stamp(created), 'sent_at': _stamp(sent),
            'dispatch_known': state in {'external_sent', 'sent', 'copy_pending', 'partial'}}

    def _catalog(self):
        service = getattr(getattr(self.p, 'cockpit_data', None), 'catalog', None)
        if service is None:
            raise PermissionError('Freigegebener Rechnungsartikelkatalog ist nicht verfügbar.')
        return service

    def _allowed(self, source):
        try:
            metadata = {'supplier': source['supplier'], 'beleg_typ': 'rechnung', 'reference': source.get('reference', '')}
            if source['kind'] == 'catalog':
                metadata.update(source_kind=source['source_kind'], source_id=source['source_id'])
            else:
                with self.p.workshop_intake.db() as db:
                    group = self.p.workshop_intake._group(db, source['group_id'])
                    file = self.p.workshop_intake._file(db, source['group_id'], source['file_id'])
                # Inspect the actual source, never classify an arbitrary file
                # using the allowed supplier copied from the destination order.
                if normalize_supplier(group['supplier']) != normalize_supplier(source['supplier']):
                    return False
                metadata.update(supplier=group['supplier'], beleg_typ=file['kind'], original_name=file['original_name'],
                    reference=' '.join(str(value or '') for value in (group['external_ref'], source.get('reference'))))
                if source.get('sha256') and source['sha256'] != file['sha256']:
                    return False
            return self._catalog().source_rule(metadata)['allowed'] is True
        except (KeyError, ValueError, TypeError, PermissionError):
            return False

    def _original_price(self, order, payload):
        keys = {'group_id', 'file_id', 'page', 'position', 'source_date', 'amount', 'currency',
                'tax_basis', 'tax_rate', 'discount_basis', 'unit', 'pack', 'identity', 'reviewed', 'quantity'}
        if set(payload) - keys or payload.get('reviewed') is not True:
            raise ValueError('Rechnungsposition und Preisbasis am Original ausdrücklich prüfen.')
        incoming = payload.get('identity')
        if not isinstance(incoming, dict) or set(incoming) != {'supplier', 'sku', 'variant'}:
            raise ValueError('Lieferant, Artikelnummer und Variante am Original bestätigen.')
        if _identity(**incoming) != order['identity']:
            raise ValueError('Rechnungsposition passt nicht zu Lieferant, Artikelnummer und Variante der Bestellung.')
        source = {'kind': 'intake', 'supplier': order['supplier'], 'group_id': _id(payload.get('group_id')),
                  'file_id': _id(payload.get('file_id')), 'page': _id(payload.get('page')),
                  'position': _id(payload.get('position'))}
        if not self._allowed(source):
            raise PermissionError('Diese Rechnungsquelle ist nicht für Materialpreise freigegeben.')
        intake = self.p.workshop_intake
        with intake.db() as db:
            file = intake._file(db, source['group_id'], source['file_id'])
        if file['kind'] != 'rechnung':
            raise ValueError('Ein Lieferschein oder Produktfoto belegt keinen Rechnungspreis.')
        # Validate the existing original, without creating another attachment.
        intake.original(source['group_id'], source['file_id'])
        source.update(sha256=file['sha256'], reference=file['original_name'])
        date = invoice_date(payload.get('source_date'))
        if not date:
            raise ValueError('Belegdatum am Rechnungsoriginal angeben.')
        if payload.get('currency') != 'EUR' or payload.get('tax_basis') not in {'net', 'gross'}:
            raise ValueError('Belegten EUR-Preis mit Netto-/Bruttobasis angeben.')
        unit = _text(payload.get('unit'), 'Preiseinheit', 100)
        if _norm(unit) != _norm(order['unit']):
            raise ValueError('Preis muss pro bestellter Einheit belegt sein; Liter und Gebinde nicht gleichsetzen.')
        return {'source': source, 'date': date, 'amount': _decimal(payload.get('amount'), 'Einheitspreis', zero=True, maximum='10000000'),
                'currency': 'EUR', 'tax_basis': payload['tax_basis'],
                'tax_rate': _decimal(payload.get('tax_rate'), 'Steuersatz', optional=True, zero=True, maximum='100'),
                'discount_basis': _text(payload.get('discount_basis'), 'Rabatt- und Nebenkostenbasis', 300),
                'unit': order['unit'], 'pack': _text(payload.get('pack'), 'Gebindeinhalt', 150),
                'identity': order['identity'], 'verified': True,
                'quantity': _decimal(payload.get('quantity'), 'Rechnungsmenge', optional=True)}

    def _catalog_estimate(self, order, payload):
        allowed = {'proposal_id', 'identity_confirmed', 'unit_matches_order'}
        if set(payload) != allowed or payload.get('identity_confirmed') is not True or payload.get('unit_matches_order') is not True:
            raise ValueError('Identische Variante und genau ein Rechnungsgebinde pro Bestelleinheit ausdrücklich bestätigen.')
        proposal = _id(payload['proposal_id'])
        items = self._catalog().knowledge_rows(limit=5000)['items']
        row = next((row for row in items if row.get('vorschlag_id') == proposal), None)
        if not row or normalize_supplier(row.get('lieferant')) != order['identity']['supplier'] or _norm(row.get('artikelnummer')) != order['identity']['sku']:
            raise ValueError('Kein freigegebener Rechnungsartikel mit genau diesem Lieferanten und dieser Artikelnummer.')
        if _variant_conflict(order, row):
            raise ValueError('Belegte Größe, Farbe oder Gebindevariante widerspricht der Bestellung; Quelle zuerst klären.')
        evidence = _price_evidence(row.get('price_evidence'))
        if evidence['basis'] != 'gebindepreis_netto_abgeleitet' or evidence.get('reconciled') is not True:
            raise ValueError('Katalogpreis ist nicht als Preis pro Gebinde belegt. Preis am Original zuordnen.')
        amount = parse_unit_price(evidence.get('value'))
        if amount is None or amount < 0 or amount > Decimal('10000000'):
            raise ValueError('Kein gültiger historischer Preis vorhanden.')
        provenance = row.get('quelle') or {}
        source = {'kind': 'catalog', 'proposal_id': proposal, 'supplier': row['lieferant'],
            'source_kind': provenance.get('art'), 'source_id': provenance.get('beleg_id'),
            'reference': provenance.get('beleg'), 'sha256': provenance.get('datei_sha256'),
            'page': provenance.get('seite'), 'position': provenance.get('position'),
            'source_unit': row.get('ve'), 'source_variant': row.get('groesse'), 'price_evidence': evidence}
        if source['source_kind'] not in {'einkauf', 'lexware'} or not self._allowed(source):
            raise PermissionError('Historische Rechnungsquelle ist nicht mehr freigegeben.')
        pack = _text(row.get('gebinde') or '', 'Gebindeinhalt', 150, optional=True)
        return {'source': source, 'date': invoice_date(provenance.get('datum')), 'amount': format(amount, 'f'),
            'currency': evidence.get('currency', 'unknown'), 'tax_basis': 'net', 'tax_rate': evidence.get('tax_rate'),
            'discount_basis': evidence.get('calculation') or 'unknown', 'unit': order['unit'], 'pack': pack,
            'identity': order['identity'], 'verified': False, 'quantity': None,
            'one_package_per_order_unit_confirmed': True}

    def _record(self, order_key, payload, actor, role):
        actor = self._actor(actor)
        if not isinstance(payload, dict):
            raise ValueError('Vollständige Preisquelle angeben.')
        with self.p.portal_originals_operation_lock():
            with self.db() as db:
                order = self._order(db, order_key)
            record = self._catalog_estimate(order, payload) if role == 'estimate' and 'proposal_id' in payload else self._original_price(order, payload)
            order_day = datetime.fromisoformat(order['created_at']).astimezone(ZoneInfo('Europe/Berlin')).date().isoformat() if order['created_at'] else None
            if role == 'estimate' and record['date'] and order_day and record['date'] > order_day:
                raise ValueError('Eine spätere Rechnung darf nicht zur ursprünglichen historischen Preisbasis werden.')
            if role == 'invoice' and order_day and record['date'] < order_day:
                raise ValueError('Diese Rechnung liegt vor der Bestellung und ist keine neue Rechnung zu diesem Auftrag.')
            record.update(order_fingerprint=order['fingerprint'], role=role)
            digest = _hash(record)
            source = record['source']
            source_key = ('original:' + source['sha256'] + ':' + str(source['page']) + ':' + str(source['position'])) if role == 'invoice' else None
            with self.db() as db:
                current = self._order(db, order['key'])
                if current['fingerprint'] != order['fingerprint']:
                    raise IntakeConflict('Bestellidentität wurde während der Zuordnung geändert.')
                if not self._allowed(record['source']):
                    raise PermissionError('Rechnungsfreigabe wurde während der Zuordnung geändert.')
                if role == 'estimate':
                    old = db.execute('SELECT payload_hash FROM assistent_bestellpreis_basis WHERE order_key=?', (order['key'],)).fetchone()
                    if old and old['payload_hash'] != digest:
                        raise IntakeConflict('Die ursprüngliche Preisbasis ist bereits festgehalten und wird nicht überschrieben.')
                    if not old:
                        db.execute('''INSERT INTO assistent_bestellpreis_basis
                            (order_key,order_fingerprint,payload_hash,payload_json,dispatch_state_at_capture,created_at,created_by)
                            VALUES(?,?,?,?,?,?,?) ON CONFLICT(order_key) DO NOTHING''',
                            (order['key'], order['fingerprint'], digest, _json(record), current['state'], self._now(), actor))
                    stored = db.execute('SELECT payload_hash FROM assistent_bestellpreis_basis WHERE order_key=?', (order['key'],)).fetchone()
                else:
                    old = db.execute('SELECT order_key,payload_hash FROM assistent_bestellpreis_rechnungen WHERE source_key=?', (source_key,)).fetchone()
                    if old and (old['order_key'] != order['key'] or old['payload_hash'] != digest):
                        raise IntakeConflict('Diese Rechnungsposition wurde bereits anders zugeordnet; keine Doppelzuordnung.')
                    if not old:
                        db.execute('''INSERT INTO assistent_bestellpreis_rechnungen
                            (order_key,order_fingerprint,source_key,payload_hash,payload_json,dispatch_state_at_capture,created_at,created_by)
                            VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING''',
                            (order['key'], order['fingerprint'], source_key, digest, _json(record), current['state'], self._now(), actor))
                    stored = db.execute('SELECT order_key,payload_hash FROM assistent_bestellpreis_rechnungen WHERE source_key=?', (source_key,)).fetchone()
                    if stored['order_key'] != order['key']:
                        raise IntakeConflict('Diese Rechnungsposition gehört bereits zu einer anderen Bestellung.')
                if stored['payload_hash'] != digest:
                    raise IntakeConflict('Gleichzeitige Preiszuordnung mit anderen Angaben; gespeicherte Quelle bleibt erhalten.')
        return self.detail(order['key'], actor=actor)

    def record_estimate(self, order_key, payload, actor='admin'):
        return self._record(order_key, payload, actor, 'estimate')

    def record_invoice(self, order_key, payload, actor='admin'):
        return self._record(order_key, payload, actor, 'invoice')

    def _view_record(self, row, order):
        if not row:
            return None
        record = json.loads(row['payload_json'])
        if row['order_fingerprint'] != order['fingerprint'] or _hash(record) != row['payload_hash']:
            return {'available': False, 'reason': 'Preisnachweis passt nicht zur unveränderten Bestellung.'}
        if not self._allowed(record['source']):
            return {'available': False, 'reason': 'Rechnungsquelle ist nicht mehr freigegeben.'}
        notes = []
        if not record.get('date'):
            notes.append('Belegdatum fehlt; letzter Preis nicht vollständig bestimmbar.')
        if not record['source'].get('page') or not record['source'].get('position'):
            notes.append('Seite oder Belegposition sind noch nicht vollständig belegt.')
        if not record.get('verified'):
            notes.append('Ungeprüfte Rechnungsauslese; geschätzter Preis, keine aktuelle Preisfreigabe.')
        if not record.get('pack'):
            notes.append('Gebindeinhalt ist noch nicht belegt.')
        if record.get('currency') != 'EUR':
            notes.append('EUR als Preiswährung ist noch nicht belegt.')
        after_dispatch = row['dispatch_state_at_capture'] in {'external_sent', 'sent', 'copy_pending', 'partial'}
        if record['role'] == 'estimate' and after_dispatch:
            notes.append('Preisnachtrag zu einer bereits versandten Bestellung; keine damalige Preiszusage.')
        total = None
        quantity = order['quantity'] if record['role'] == 'estimate' else record.get('quantity')
        if record.get('currency') == 'EUR' and record.get('amount') is not None and isinstance(quantity, str) and re.fullmatch(r'\d+(?:\.\d+)?', quantity):
            total = format((Decimal(record['amount']) * Decimal(quantity)).quantize(Decimal('.01'), rounding=ROUND_HALF_UP), '.2f')
        return dict(record, available=True, created_at=row['created_at'], created_by=row['created_by'],
            after_dispatch=after_dispatch, article_total=total,
            estimated_article_total=total if record['role'] == 'estimate' else None,
            notes=notes, label='Geschätzter Preis laut früherer Rechnung' if record['role'] == 'estimate' else 'Zugeordneter Rechnungspreis')

    @staticmethod
    def compare(estimate, invoice):
        if not estimate or not estimate.get('available') or not invoice or not invoice.get('available'):
            return {'comparable': False, 'reason': 'Zugängliche historische Preisbasis oder neue Rechnung fehlt.'}
        for key, reason in (('currency', 'Preiswährung ist unterschiedlich oder nicht als EUR belegt.'),
            ('identity', 'Lieferant, Artikelnummer oder Variante unterscheiden sich.'),
            ('unit', 'Preiseinheit unterscheidet sich.'), ('pack', 'Gebindeinhalt unterscheidet sich oder fehlt.'),
            ('tax_basis', 'Netto- und Bruttopreise sind nicht direkt vergleichbar.'),
            ('tax_rate', 'Steuersatz fehlt oder ist unterschiedlich belegt.'),
            ('discount_basis', 'Rabatt- und Nebenkostenbasis unterscheidet sich oder fehlt.')):
            if estimate.get(key) != invoice.get(key) or estimate.get(key) in (None, '', 'unknown') or key == 'currency' and estimate[key] != 'EUR':
                return {'comparable': False, 'reason': reason}
        result = MaterialIntake.compare_prices(estimate, invoice)
        if result['comparable']:
            result.update(provisional=not estimate.get('verified'), label='Abweichung gegenüber der gespeicherten Schätzung',
                scope='Artikelpreis je Bestelleinheit; kein Zahlungs- oder Gesamtbelegabgleich.')
        return result

    def detail(self, order_key, actor='admin'):
        self._actor(actor)
        with self.db() as db:
            order = self._order(db, order_key)
            baseline = db.execute('SELECT * FROM assistent_bestellpreis_basis WHERE order_key=?', (order['key'],)).fetchone()
            rows = db.execute('SELECT * FROM assistent_bestellpreis_rechnungen WHERE order_key=? ORDER BY id', (order['key'],)).fetchall()
        estimate = self._view_record(baseline, order)
        invoices = []
        for row in rows:
            value = self._view_record(row, order)
            invoices.append(dict(value, id=row['id'], comparison=self.compare(estimate, value)))
        if not estimate:
            status = 'no_estimate'
        elif not estimate.get('available'):
            status = 'source_blocked'
        elif not invoices:
            status = 'awaiting_invoice'
        elif any(not invoice['comparison']['comparable'] for invoice in invoices):
            status = 'not_comparable'
        elif any(Decimal(invoice['comparison']['difference']) != 0 for invoice in invoices):
            status = 'difference'
        else:
            status = 'matched'
        return {'order': order, 'estimate': estimate, 'invoices': invoices, 'status': status,
            'can_dispatch': False, 'notice': 'Historischer Vergleich ohne Preisfreigabe, Bestellversand oder Zahlungsbuchung.'}

    def summaries(self, order_keys, actor='admin'):
        self._actor(actor)
        if not isinstance(order_keys, (list, tuple)) or len(order_keys) > 100:
            raise ValueError('Höchstens 100 eindeutige Bestellungen abfragen.')
        return {key: self.detail(key, actor=actor) for key in dict.fromkeys(order_keys)}
