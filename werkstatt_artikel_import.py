"""Resumable, review-only supplier invoice catalog. Never places an order."""
import hashlib
import json
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone

from flask import jsonify, render_template, request, flash, redirect, url_for

from werkstatt_artikel_identity import catalog_identity
from werkstatt_rechnungsquelle import read_source
from werkstatt_rechnungsfreigabe import classify_invoice_source


def _text(value, limit=1000):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return ''
    return str(value).strip()[:limit]


def _first(mapping, *keys):
    for key in keys:
        if mapping.get(key) is not None and mapping.get(key) != '':
            return mapping[key]
    return None


def _product_text(value):
    text = _text(value, 4000).casefold().translate(str.maketrans({'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss'}))
    return ' '.join(unicodedata.normalize('NFKC', text).split())


_PAYMENT_TEXT = re.compile(
    r'\b(?:iban|bic|swift|bankverbindung|kontoverbindung|kontoinhaber|kontonummer|kontoauszug|kontobelastung|'
    r'zahlungsziel|zahlungsbedingung(?:en)?|zahlungstermin|zahlungseingang|zahlungshinweis|zahlungsart|zahlweise|'
    r'lastschrift|sepa(?:lastschrift)?|glaeubiger(?:id|identifikationsnummer)?|mandatsreferenz|'
    r'abbuchung|abgebucht|ueberweisung|zahlung|zahlungen|einzugsermaechtigung|skonto)\b|'
    r'\b(?:konto|kontos|konten)\b.{0,160}\b(?:belastet|belasten|belastung|abbuchen|eingezogen)\b|'
    r'\b(?:belastet|belasten|belastung|abbuchen|eingezogen)\b.{0,160}\b(?:konto|kontos|konten)\b|'
    r'\bbetrag\b.{0,160}\b(?:belastet|abgebucht|eingezogen|ueberweisen|zahlbar|faellig)\b|'
    r'\b(?:zahlbar|faellig)\b.{0,80}\b(?:tage|tagen|erhalt|rechnung|netto)\b|'
    r'\b[A-Z]{2}\s*\d{2}(?:[ ]?[A-Z0-9]){11,30}\b', re.I)
_SUMMARY_TEXT = re.compile(
    r'\b(?:summe|zwischensumme|gesamtsumme|gesamtbetrag|rechnungsbetrag|rechnungssumme|'
    r'nettobetrag|bruttobetrag|nettosumme|bruttosumme|uebertrag|vortrag|mwst|mehrwertsteuer|umsatzsteuer|ust)\b')
_FEE_TEXT = re.compile(
    r'\b(?:[a-z]*gebuehr(?:en)?|[a-z]*pauschale(?:n)?|[a-z]*kostenpauschale(?:n)?|'
    r'[a-z]*kostenzuschla(?:g|ege)|[a-z]*zuschlag|[a-z]*zuschlaege|versandkosten|frachtkosten|portokosten|porto|'
    r'energiekosten|logistik(?:kosten)?|transportkosten|verpackungskosten|entsorgungskosten)\b')


def is_product_candidate(candidate, supplier=''):
    """Reject invoice finance/footer/service-fee rows, including old proposals.

    This is separate from supplier access: a legitimate supplier's invoice still
    contains bank instructions and totals. Match explicit words/phrases, never
    broad ``bank``/``steuer`` fragments that would reject Werkbank/Steuergerät.
    """
    if not isinstance(candidate, dict):
        return False
    product = _product_text(_first(candidate, 'produkt_name', 'product_name', 'description'))
    if not product:
        return False
    description = _product_text(_first(candidate, 'produkt_beschreibung', 'description'))
    evidence = _product_text(_first(candidate, 'preis', 'historischer_preishinweis'))
    combined = ' '.join((product, description, evidence))
    if _PAYMENT_TEXT.search(combined) or _SUMMARY_TEXT.search(product + ' ' + description):
        return False
    generic_title = re.fullmatch(r'(?:artikel|position|posten)(?:\s+[a-z0-9./-]+)?', product)
    if _FEE_TEXT.search(product) or (generic_title and _FEE_TEXT.search(description)):
        return False
    # Confirmed fee SKU: do not confuse this supplier's bookkeeping line with a
    # physical material just because the OCR returned a plausible article id.
    number = re.sub(r'[^a-z0-9]', '', _product_text(_first(candidate, 'artikelnummer', 'article_number')))
    actual_supplier = supplier or _text(_first(candidate, 'lieferant', 'supplier'))
    if number == '00000071' and classify_invoice_source(actual_supplier)['rule'] == 'known_supplier:topcolor':
        return False
    return True


def _visible_article_payload(row):
    try:
        payload = json.loads(row['payload_json'])
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or not is_product_candidate(payload, row['supplier']):
        return None
    stored = dict(payload, produkt_name=row.get('product_name') or payload.get('produkt_name'),
                  artikelnummer=row.get('article_number') or payload.get('artikelnummer'))
    return payload if is_product_candidate(stored, row['supplier']) else None


def _marker(value):
    # Page/line references are metadata, never arbitrary document text.
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and value.strip().isdigit() and len(value.strip()) <= 9:
        return int(value.strip())
    return None


def _price_evidence(value):
    if not isinstance(value, dict):
        return {'verified': False, 'basis': 'unknown'}
    result = {'verified': False, 'basis': value.get('basis') if value.get('basis') in ('unknown', 'gebindepreis_netto_abgeleitet') else 'unknown'}
    for key in ('value', 'unrounded_value', 'quantity', 'content', 'base_price_per_measure', 'discount_percent', 'line_total_net'):
        raw = _text(value.get(key), 80)
        result[key] = raw if re.fullmatch(r'[+-]?\d+(?:[.,]\d+)?', raw) else ''
    unit = _text(value.get('measure_unit'), 30)
    result['measure_unit'] = unit if _product_text(unit) in ('ltr/kg', 'ltr', 'kg', 'l', 'ml', 'g', 'm', 'mtr', 'cm', 'mm', 'stk', 'st', 'stueck', 'rol', 'rolle', 'pack', 'set', 'gebinde', 'karton', 'stck', 'dose') else ''
    result['reconciled'] = value.get('reconciled') is True and bool(result['value'])
    if result['basis'] == 'gebindepreis_netto_abgeleitet':
        result['calculation'] = 'Inhalt × Grundpreis je Maßeinheit × (1 + Rabatt / 100)'
    return result


def _source_pages(value):
    if not isinstance(value, (list, tuple)):
        return []
    pages = []
    for raw in value[:100]:
        page = _marker(raw)
        if page and page > 0 and page not in pages:
            pages.append(page)
    return pages


def _coverage(value):
    if not isinstance(value, dict):
        return {'complete': False}
    result = {'complete': value.get('complete') is True,
              'extraction_verified': False}
    for key in ('files_total', 'files_read', 'pages_total', 'pages_read', 'pages_attempted'):
        result[key] = _marker(value.get(key))
    result['files'] = []
    details = value.get('files')
    for detail in details[:100] if isinstance(details, list) else []:
        if isinstance(detail, dict):
            result['files'].append({
                'file_id': _text(detail.get('file_id'), 100),
                'pages_total': _marker(detail.get('pages_total')),
                'pages_read': _marker(detail.get('pages_read')),
                'pages_attempted': _marker(detail.get('pages_attempted')),
                'complete': detail.get('complete') is True,
            })
    return result


class InvoiceCatalog:
    def __init__(self, portal):
        self.p = portal
        db = portal.get_db()
        try:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS assistent_rechnungsimporte (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_key TEXT UNIQUE NOT NULL, source_kind TEXT NOT NULL,
                    source_id TEXT NOT NULL, supplier TEXT NOT NULL DEFAULT '',
                    reference TEXT NOT NULL DEFAULT '', state TEXT NOT NULL DEFAULT 'offen',
                    lease TEXT NOT NULL DEFAULT '', started_at TEXT NOT NULL DEFAULT '',
                    result_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS assistent_rechnungsartikel (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT UNIQUE NOT NULL, identity_key TEXT NOT NULL,
                    import_id INTEGER NOT NULL, supplier TEXT NOT NULL,
                    product_name TEXT NOT NULL, article_number TEXT NOT NULL,
                    payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1
                );
            """)
            ensure_column = getattr(portal, 'ensure_column', None)
            if callable(ensure_column):
                ensure_column(db, 'assistent_rechnungsartikel', 'active', 'INTEGER NOT NULL DEFAULT 1')
            elif 'active' not in {row[1] for row in db.execute('PRAGMA table_info(assistent_rechnungsartikel)').fetchall()}:
                db.execute('ALTER TABLE assistent_rechnungsartikel ADD COLUMN active INTEGER NOT NULL DEFAULT 1')
            db.commit()
        finally:
            db.close()

    def rows(self, sql, args=()):
        db = self.p.get_db()
        try:
            return [dict(r) for r in db.execute(sql, args).fetchall()]
        finally:
            db.close()

    def allowed_suppliers(self):
        getter = getattr(self.p, 'get_app_setting', None)
        try:
            values = json.loads(getter('ASSISTANT_MATERIAL_SUPPLIERS', '[]') or '[]') if callable(getter) else []
        except (ValueError, TypeError):
            return []
        return values if isinstance(values, list) else []

    def source_rule(self, source, allowed_suppliers=None):
        allowed = self.allowed_suppliers() if allowed_suppliers is None else allowed_suppliers
        return classify_invoice_source(source, allowed_suppliers=allowed)

    @staticmethod
    def _scope_report(rule):
        return {'status': 'ausgeschlossen' if rule['decision'] == 'block' else 'zuordnen',
                'positionen': 0, 'hinweise': [rule['reason']], 'scope_rule': rule['rule']}

    def _apply_source_rules(self, db):
        """Recheck the complete existing queue; revoke a denied worker's lease."""
        allowed = self.allowed_suppliers()
        rows = db.execute('SELECT id,supplier,reference,state FROM assistent_rechnungsimporte').fetchall()
        for row in rows:
            source = dict(row)
            rule = self.source_rule(source, allowed)
            if not rule['allowed']:
                state = 'ausgeschlossen' if rule['decision'] == 'block' else 'zuordnen'
                db.execute("UPDATE assistent_rechnungsimporte SET state=?,lease='',result_json=? WHERE id=? AND state<>?",
                           (state, json.dumps(self._scope_report(rule), ensure_ascii=False), source['id'], state))
            elif source['state'] in ('ausgeschlossen', 'zuordnen'):
                db.execute("UPDATE assistent_rechnungsimporte SET state='offen',lease='',result_json='{}' WHERE id=? AND state=?",
                           (source['id'], source['state']))

    def _stop_source(self, source, rule, lease):
        db = self.p.get_db()
        try:
            state = 'ausgeschlossen' if rule['decision'] == 'block' else 'zuordnen'
            db.execute("UPDATE assistent_rechnungsimporte SET state=?,lease='',result_json=? WHERE id=? AND lease=?",
                       (state, json.dumps(self._scope_report(rule), ensure_ascii=False), source['id'], lease))
            db.commit()
        finally:
            db.close()

    def prepare(self, inventory):
        sources = [('einkauf', str(r['id']), r.get('lieferant') or '', r.get('original_name') or '')
                   for r in inventory['einkaufsbelege']]
        sources += [('lexware', r['voucher_id'], r.get('contact_name') or '', r.get('voucher_number') or '')
                    for r in inventory['lieferantenrechnungen']]
        allowed = self.allowed_suppliers()
        def priority(source):
            rule = self.source_rule({'supplier': source[2], 'reference': source[3]}, allowed)
            return {'known_supplier:topcolor': 0, 'known_supplier:carparts': 1,
                    'known_supplier:techmasters': 2}.get(rule['rule'], 3)
        # Execution priority only: supplier identity is never changed or merged.
        sources.sort(key=priority)
        db = self.p.get_db()
        try:
            self._reclaim_expired(db)
            for kind, sid, supplier, reference in sources:
                db.execute("INSERT INTO assistent_rechnungsimporte(source_key,source_kind,source_id,supplier,reference) VALUES(?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET supplier=excluded.supplier,reference=excluded.reference",
                           (kind+':'+sid, kind, sid, supplier, reference))
            self._apply_source_rules(db)
            db.commit()
        finally:
            db.close()
        return self.status()

    @staticmethod
    def _reclaim_expired(db, now=None):
        now = now or datetime.now(timezone.utc)
        db.execute("UPDATE assistent_rechnungsimporte SET state='offen',lease='' WHERE state='laeuft' AND started_at<?",
                   ((now-timedelta(minutes=15)).isoformat(),))

    def status(self):
        sources = self.rows('SELECT id,supplier,reference,state,result_json FROM assistent_rechnungsimporte ORDER BY id')
        allowed = self.allowed_suppliers()
        allowed_ids = set()
        for row in sources:
            rule = self.source_rule(row, allowed)
            if not rule['allowed']:
                row.pop('result_json', None)
                row['state'] = 'ausgeschlossen' if rule['decision'] == 'block' else 'zuordnen'
                row['result'] = self._scope_report(rule)
                row['reference'] = ''
                if rule['decision'] == 'block':
                    row['supplier'] = ''
                continue
            allowed_ids.add(row['id'])
            try:
                result = json.loads(row.pop('result_json'))
                row['result'] = result if isinstance(result, dict) else {}
            except (ValueError, TypeError):
                row['result'] = {'hinweise': ['Verarbeitungsergebnis muss geprüft werden.']}
        counts = {}
        for article in self.rows('SELECT import_id,supplier,product_name,article_number,payload_json FROM assistent_rechnungsartikel WHERE active=1'):
            if article['import_id'] in allowed_ids and self.source_rule(article, allowed)['allowed'] and _visible_article_payload(article) is not None:
                counts[article['import_id']] = counts.get(article['import_id'], 0) + 1
        for row in sources:
            if row['id'] in allowed_ids and isinstance(row['result'], dict) and 'positionen' in row['result']:
                row['result']['positionen'] = counts.get(row['id'], 0)
        return {'quellen': sources,
                'offen': sum(r['state'] == 'offen' for r in sources),
                'laeuft': sum(r['state'] == 'laeuft' for r in sources),
                'ausgeschlossen': sum(r['state'] == 'ausgeschlossen' for r in sources),
                'zuordnen': sum(r['state'] == 'zuordnen' for r in sources),
                'vorschlaege': sum(counts.values())}

    def retry_source(self, source_id):
        sources = self.rows('SELECT id,supplier,reference,state FROM assistent_rechnungsimporte WHERE id=?', (source_id,))
        if not sources or not self.source_rule(sources[0])['allowed']:
            return False
        db = self.p.get_db()
        try:
            changed = db.execute("UPDATE assistent_rechnungsimporte SET state='offen',lease='',started_at='' WHERE id=? AND state IN ('ausgelesen','pruefen')", (source_id,))
            db.commit()
            return bool(changed.rowcount)
        finally:
            db.close()

    def process_next(self):
        lease = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        db = self.p.get_db()
        source = None
        try:
            # A failed worker can be resumed. Its expired lease cannot publish results.
            self._reclaim_expired(db, now)
            self._apply_source_rules(db)
            row = db.execute("SELECT * FROM assistent_rechnungsimporte WHERE state='offen' ORDER BY id LIMIT 1").fetchone()
            if row:
                source = dict(row)
                changed = db.execute("UPDATE assistent_rechnungsimporte SET state='laeuft',lease=?,started_at=? WHERE id=? AND state='offen'",
                                     (lease, now.isoformat(), source['id']))
                if not changed.rowcount:
                    source = None
            db.commit()
        finally:
            db.close()
        if not source:
            return self.status()
        # Settings may have changed since the queue was claimed. Do not open a
        # receipt merely to find out whether its supplier belongs to our scope.
        rule = self.source_rule(source)
        if not rule['allowed']:
            self._stop_source(source, rule, lease)
            return self.status()
        try:
            result = read_source(self.p, source['source_kind'], source['source_id'])
            if not isinstance(result, dict) or not isinstance(result.get('candidates'), list):
                raise ValueError('Invalid reader result')
        except Exception:
            result = {'status': 'error', 'candidates': [], 'coverage': {'complete': False},
                      'warnings': ['Beleg konnte nicht ausgewertet werden. Original und Zugang prüfen.']}
        records = []
        malformed = 0
        missing_provenance = 0
        for index, candidate in enumerate(result['candidates'], 1):
            try:
                record, known_source = self._candidate_record(source, candidate, index, now)
                records.append(record)
                missing_provenance += not known_source
            except (ValueError, TypeError, AttributeError, OverflowError):
                malformed += 1
        coverage = _coverage(result.get('coverage'))
        raw_warnings = result.get('warnings')
        warnings = [_text(warning, 500) for warning in raw_warnings[:30]
                    if isinstance(warning, str)] if isinstance(raw_warnings, list) else []
        if malformed:
            warnings.append(f'{malformed} ungültige Produktposition(en) nicht übernommen; Original prüfen.')
        if missing_provenance:
            warnings.append(f'Bei {missing_provenance} Position(en) fehlt ein genauer Seiten-/Positionsnachweis.')
        if not records:
            warnings.append('Keine Produktpositionen erkannt. Originalrechnung muss geprüft werden.')
        if malformed or missing_provenance:
            coverage['complete'] = False
        state = 'ausgelesen' if records and result.get('status') == 'ok' and coverage.get('complete') is True else 'pruefen'
        report = {'positionen': len(records), 'abdeckung': coverage,
                  'hinweise': warnings, 'status': _text(result.get('status'), 40) or 'error'}
        rule = self.source_rule(source)
        if not rule['allowed']:
            self._stop_source(source, rule, lease)
            return self.status()
        db = self.p.get_db()
        try:
            owned = db.execute("UPDATE assistent_rechnungsimporte SET state=?,lease='',result_json=? WHERE id=? AND lease=? AND state='laeuft'",
                               (state, json.dumps(report, ensure_ascii=False), source['id'], lease))
            if owned.rowcount:
                # Replace this receipt's visible extraction atomically, retaining
                # all older records for review. An expired worker cannot do this.
                db.execute('UPDATE assistent_rechnungsartikel SET active=0 WHERE import_id=?', (source['id'],))
                for record in records:
                    db.execute('INSERT INTO assistent_rechnungsartikel(fingerprint,identity_key,import_id,supplier,product_name,article_number,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO UPDATE SET active=1', record)
            db.commit()
        finally:
            db.close()
        return self.status()

    @staticmethod
    def _candidate_record(source, candidate, index, now):
        if not is_product_candidate(candidate, source.get('supplier', '')):
            raise ValueError('Not a product candidate')
        product = _text(_first(candidate, 'produkt_name', 'description'), 300)
        if not product:
            raise ValueError('Missing product')
        supplier = _text(source['supplier'], 300)
        number = _text(_first(candidate, 'artikelnummer', 'article_number'), 100)
        unit = _text(_first(candidate, 've', 'unit'), 60)
        description = _text(candidate.get('produkt_beschreibung') or product)
        packaging = _text(_first(candidate, 'gebinde', 'packaging'), 200)
        size = _text(_first(candidate, 'groesse', 'size'), 100)
        color = _text(_first(candidate, 'farbe', 'color'), 100)
        quantity = _text(_first(candidate, 'menge', 'quantity', 'stueckzahl'), 60)
        # Keep exact variant evidence separate from quantity and price evidence.
        variants = json.dumps([packaging, size, color, description], ensure_ascii=False, separators=(',', ':'))
        identity = catalog_identity(supplier, number, product, variants, unit)
        evidence = _text(_first(candidate, 'preis', 'price_evidence'), 100)
        price_evidence = _price_evidence(candidate.get('price_evidence'))
        evidence = evidence or price_evidence.get('value', '')
        raw_source = candidate.get('source')
        raw_source = raw_source if isinstance(raw_source, dict) else {}
        file_id = _text(raw_source.get('file_id'), 100) or None
        digest = _text(raw_source.get('sha256'), 64)
        digest = digest if re.fullmatch('[0-9a-fA-F]{64}', digest) else None
        page = _marker(raw_source.get('page'))
        page = page if page and page > 0 else None
        pages = _source_pages(raw_source.get('pages'))
        if page and page not in pages:
            pages.insert(0, page)
        if not page and pages:
            page = pages[0]
        method = _text(raw_source.get('method'), 80)
        method = method if method in ('topcolor_word_columns_v1',) else None
        line = _marker(_first(raw_source, 'line', 'line_index'))
        position = _marker(_first(raw_source, 'position', 'position_index'))
        known_source = page is not None or line is not None or position is not None
        # Whitelist product data. Do not persist full invoice, payment or bank fields.
        payload = {'produkt_name': product, 'lieferant': supplier, 'artikelnummer': number,
                   'produkt_beschreibung': description, 've': unit, 'gebinde': packaging,
                   'groesse': size, 'farbe': color, 'menge': quantity,
                   'historischer_preishinweis': evidence, 'preis_geprueft': False,
                   'price_evidence': price_evidence,
                   'preis_basis': 'Ungeprüft: Einzel-/Gesamtpreis und netto/brutto am Original prüfen.',
                   'status': 'vorschlag', 'bestellbar': False,
                   'quelle': {'art': source['source_kind'], 'beleg': source['reference'],
                              'beleg_id': source['source_id'], 'seite': page, 'seiten': pages, 'zeile': line, 'methode': method,
                              'datei_id': file_id, 'datei_sha256': digest,
                              'position': position, 'extraktionsindex': index,
                              'positionsnachweis': 'vorhanden_ungeprueft' if known_source else 'ungeklaert'},
                   'hinweis': 'Aus Rechnung vorgeschlagen. Größe, Farbe, Lieferant und Preis am Original bestätigen.'}
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        fingerprint = hashlib.sha256((source['source_key']+serialized).encode()).hexdigest()
        return ((fingerprint, identity or '', source['id'], supplier, product, number, serialized, now.isoformat()), known_source)

    def search(self, query, limit=30):
        limit = max(1, min(int(limit), 100))
        allowed = self.allowed_suppliers()
        sources = self.rows('SELECT id,supplier,reference FROM assistent_rechnungsimporte')
        ids = [row['id'] for row in sources if self.source_rule(row, allowed)['allowed']]
        if not ids:
            return []
        terms = query.lower().split()
        conditions = []
        args = []
        for term in terms[:12]:
            conditions.append('(LOWER(product_name) LIKE ? OR LOWER(article_number) LIKE ? OR LOWER(supplier) LIKE ?)')
            args += ['%'+term+'%']*3
        results = []
        # Apply source permissions before LIMIT, otherwise blocked historical
        # proposals could hide valid workshop results on later pages.
        for offset in range(0, len(ids), 400):
            batch = ids[offset:offset+400]
            base = 'active=1 AND import_id IN ('+','.join('?' for _ in batch)+')'
            where = ' AND '+ ' AND '.join(conditions) if conditions else ''
            after_id = None
            batch_results = []
            while len(batch_results) < limit:
                cursor_clause = ' AND id<?' if after_id is not None else ''
                values = (*batch, *args, after_id, 200) if after_id is not None else (*batch, *args, 200)
                rows = self.rows('SELECT id,supplier,product_name,article_number,payload_json FROM assistent_rechnungsartikel WHERE '+base+where+cursor_clause+' ORDER BY id DESC LIMIT ?', values)
                if not rows:
                    break
                for row in rows:
                    if not self.source_rule(row, allowed)['allowed']:
                        continue
                    payload = _visible_article_payload(row)
                    if payload is not None:
                        batch_results.append(dict(payload, vorschlag_id=row['id']))
                        if len(batch_results) == limit:
                            break
                after_id = rows[-1]['id']
            results.extend(batch_results)
        return sorted(results, key=lambda row: row['vorschlag_id'], reverse=True)[:limit]


def register_invoice_catalog(p, service):
    catalog = InvoiceCatalog(p)

    @p.app.get('/admin/assistent-artikel')
    @p.admin_required
    def assistant_invoice_catalog():
        return render_template('assistent_artikel.html', report=catalog.status(), articles=catalog.search('', 100))

    @p.app.post('/admin/assistent-artikel/upload')
    @p.admin_required
    def assistant_invoice_catalog_upload():
        supplier = str(request.form.get('lieferant') or '').strip()
        rule = catalog.source_rule({'supplier': supplier})
        if not rule['allowed']:
            flash(rule['reason'], 'warning')
            return redirect(url_for('assistant_invoice_catalog'))
        files = [f for f in request.files.getlist('rechnungen') if f.filename]
        if not files or len(files) > 20:
            flash('Bitte 1 bis 20 Lieferantenrechnungen auswählen.', 'warning')
            return redirect(url_for('assistant_invoice_catalog'))
        saved = 0
        seen = set()
        for file in files:
            rule = catalog.source_rule({'supplier': supplier, 'original_name': file.filename})
            if not rule['allowed']:
                flash(rule['reason'], 'warning')
                continue
            try:
                # Deduplicate identical files within the upload without saving
                # or OCR first. The source reader enforces the same 20 MiB cap.
                digest = hashlib.sha256()
                size = 0
                stream = file.stream
                while chunk := stream.read(65536):
                    size += len(chunk)
                    if size > 20 * 1024 * 1024:
                        raise ValueError('Invoice exceeds limit')
                    digest.update(chunk)
                stream.seek(0)
                if not size or digest.hexdigest() in seen:
                    continue
                seen.add(digest.hexdigest())
                # Save receipt only: legacy import may collapse different variants.
                if p.save_einkauf_beleg_upload(file, lieferant=supplier, beleg_typ='rechnung'):
                    saved += 1
            except ValueError:
                flash('Eine Datei konnte nicht als Lieferantenrechnung gespeichert werden.', 'warning')
        catalog.prepare(service.invoice_sources(include_held=True))
        flash(f'{saved} Rechnungsdateien gespeichert. Einlesen setzt die Verarbeitung fort.', 'success')
        return redirect(url_for('assistant_invoice_catalog'))

    @p.app.post('/admin/assistent-artikel/start')
    @p.admin_required
    def assistant_invoice_catalog_start():
        return jsonify(catalog.prepare(service.invoice_sources(include_held=True)))

    @p.app.post('/admin/assistent-artikel/lieferant/<int:source_id>/freigeben')
    @p.admin_required
    def assistant_invoice_supplier_approve(source_id):
        # Standard app POST/CSRF protection also covers this explicit admin action.
        sources = catalog.rows('SELECT id,supplier,reference FROM assistent_rechnungsimporte WHERE id=?', (source_id,))
        if not sources:
            flash('Die Rechnungsquelle ist nicht vorhanden.', 'warning')
            return redirect(url_for('assistant_invoice_catalog'))
        source = sources[0]
        rule = catalog.source_rule(source)
        if rule['decision'] != 'review' or rule['rule'] != 'unclassified_supplier':
            flash('Diese Quelle kann hier nicht zusätzlich freigegeben werden.', 'warning')
            return redirect(url_for('assistant_invoice_catalog'))
        allowed = catalog.allowed_suppliers()
        allowed.append(source['supplier'])
        # No client-supplied supplier name, wildcards or financial-scope override.
        if not catalog.source_rule(source, allowed)['allowed']:
            flash('Der Lieferant konnte nicht für Materialrechnungen freigegeben werden.', 'warning')
            return redirect(url_for('assistant_invoice_catalog'))
        p.set_app_setting('ASSISTANT_MATERIAL_SUPPLIERS', json.dumps(allowed, ensure_ascii=False))
        catalog.prepare(service.invoice_sources(include_held=True))
        flash('Der Lieferant ist für Materialrechnungen freigegeben. Einlesen kann jetzt fortgesetzt werden.', 'success')
        return redirect(url_for('assistant_invoice_catalog'))

    @p.app.post('/admin/assistent-artikel/weiter')
    @p.admin_required
    def assistant_invoice_catalog_next():
        return jsonify(catalog.process_next())

    @p.app.post('/admin/assistent-artikel/quelle/<int:source_id>/wiederholen')
    @p.admin_required
    def assistant_invoice_source_retry(source_id):
        if catalog.retry_source(source_id):
            flash('Dieser Beleg ist zur erneuten Auslese vorgemerkt. Einlesen setzt die Verarbeitung fort.', 'success')
        else:
            flash('Der Beleg ist nicht freigegeben, bereits offen oder wird gerade gelesen.', 'warning')
        return redirect(url_for('assistant_invoice_catalog'))

    @p.app.get('/admin/assistent-artikel/status')
    @p.admin_required
    def assistant_invoice_catalog_status():
        return jsonify(catalog.status())

    return catalog
