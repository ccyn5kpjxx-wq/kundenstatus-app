"""Resumable, review-only supplier invoice catalog. Never places an order."""
import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone

from flask import jsonify, render_template

from werkstatt_artikel_identity import catalog_identity
from werkstatt_rechnungsquelle import read_source


def _text(value, limit=1000):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return ''
    return str(value).strip()[:limit]


def _first(mapping, *keys):
    for key in keys:
        if mapping.get(key) is not None and mapping.get(key) != '':
            return mapping[key]
    return None


def _marker(value):
    # Page/line references are metadata, never arbitrary document text.
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and value.strip().isdigit() and len(value.strip()) <= 9:
        return int(value.strip())
    return None


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
                    payload_json TEXT NOT NULL, created_at TEXT NOT NULL
                );
            """)
            db.commit()
        finally:
            db.close()

    def rows(self, sql, args=()):
        db = self.p.get_db()
        try:
            return [dict(r) for r in db.execute(sql, args).fetchall()]
        finally:
            db.close()

    def prepare(self, inventory):
        sources = [('einkauf', str(r['id']), r.get('lieferant') or '', r.get('original_name') or '')
                   for r in inventory['einkaufsbelege']]
        sources += [('lexware', r['voucher_id'], r.get('contact_name') or '', r.get('voucher_number') or '')
                    for r in inventory['lieferantenrechnungen']]
        def priority(source):
            supplier = ''.join(c for c in _text(source[2]).casefold() if c.isalnum())
            if 'topcolor' in supplier or 'topcolour' in supplier or 'autocolor' in supplier:
                return 0
            if 'carparts' in supplier:
                return 1
            if 'tech' in supplier or 'master' in supplier:
                return 2
            return 3
        # Execution priority only: supplier identity is never changed or merged.
        sources.sort(key=priority)
        db = self.p.get_db()
        try:
            self._reclaim_expired(db)
            for kind, sid, supplier, reference in sources:
                db.execute("INSERT INTO assistent_rechnungsimporte(source_key,source_kind,source_id,supplier,reference) VALUES(?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING",
                           (kind+':'+sid, kind, sid, supplier, reference))
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
        for row in sources:
            try:
                result = json.loads(row.pop('result_json'))
                row['result'] = result if isinstance(result, dict) else {}
            except (ValueError, TypeError):
                row['result'] = {'hinweise': ['Verarbeitungsergebnis muss geprüft werden.']}
        return {'quellen': sources,
                'offen': sum(r['state'] == 'offen' for r in sources),
                'laeuft': sum(r['state'] == 'laeuft' for r in sources),
                'vorschlaege': self.rows('SELECT COUNT(*) AS n FROM assistent_rechnungsartikel')[0]['n']}

    def process_next(self):
        lease = uuid.uuid4().hex
        now = datetime.now(timezone.utc)
        db = self.p.get_db()
        source = None
        try:
            # A failed worker can be resumed. Its expired lease cannot publish results.
            self._reclaim_expired(db, now)
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
        db = self.p.get_db()
        try:
            owned = db.execute("UPDATE assistent_rechnungsimporte SET state=?,lease='',result_json=? WHERE id=? AND lease=? AND state='laeuft'",
                               (state, json.dumps(report, ensure_ascii=False), source['id'], lease))
            if owned.rowcount:
                for record in records:
                    db.execute('INSERT INTO assistent_rechnungsartikel(fingerprint,identity_key,import_id,supplier,product_name,article_number,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(fingerprint) DO NOTHING', record)
            db.commit()
        finally:
            db.close()
        return self.status()

    @staticmethod
    def _candidate_record(source, candidate, index, now):
        if not isinstance(candidate, dict):
            raise ValueError('Invalid candidate')
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
        raw_source = candidate.get('source')
        raw_source = raw_source if isinstance(raw_source, dict) else {}
        file_id = _text(raw_source.get('file_id'), 100) or None
        digest = _text(raw_source.get('sha256'), 64)
        digest = digest if re.fullmatch('[0-9a-fA-F]{64}', digest) else None
        page = _marker(raw_source.get('page'))
        page = page if page and page > 0 else None
        line = _marker(_first(raw_source, 'line', 'line_index'))
        position = _marker(_first(raw_source, 'position', 'position_index'))
        known_source = page is not None or line is not None or position is not None
        # Whitelist product data. Do not persist full invoice, payment or bank fields.
        payload = {'produkt_name': product, 'lieferant': supplier, 'artikelnummer': number,
                   'produkt_beschreibung': description, 've': unit, 'gebinde': packaging,
                   'groesse': size, 'farbe': color, 'menge': quantity,
                   'historischer_preishinweis': evidence, 'preis_geprueft': False,
                   'preis_basis': 'Ungeprüft: Einzel-/Gesamtpreis und netto/brutto am Original prüfen.',
                   'status': 'vorschlag', 'bestellbar': False,
                   'quelle': {'art': source['source_kind'], 'beleg': source['reference'],
                              'beleg_id': source['source_id'], 'seite': page, 'zeile': line,
                              'datei_id': file_id, 'datei_sha256': digest,
                              'position': position, 'extraktionsindex': index,
                              'positionsnachweis': 'vorhanden_ungeprueft' if known_source else 'ungeklaert'},
                   'hinweis': 'Aus Rechnung vorgeschlagen. Größe, Farbe, Lieferant und Preis am Original bestätigen.'}
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        fingerprint = hashlib.sha256((source['source_key']+serialized).encode()).hexdigest()
        return ((fingerprint, identity or '', source['id'], supplier, product, number, serialized, now.isoformat()), known_source)

    def search(self, query, limit=30):
        limit = max(1, min(int(limit), 100))
        terms = query.lower().split()
        conditions = []
        args = []
        for term in terms[:12]:
            conditions.append('(LOWER(product_name) LIKE ? OR LOWER(article_number) LIKE ? OR LOWER(supplier) LIKE ?)')
            args += ['%'+term+'%']*3
        rows = self.rows('SELECT id,payload_json FROM assistent_rechnungsartikel'+
                         (' WHERE '+' AND '.join(conditions) if conditions else '')+' ORDER BY id DESC LIMIT ?', (*args, limit))
        return [dict(json.loads(r['payload_json']), vorschlag_id=r['id']) for r in rows]


def register_invoice_catalog(p, service):
    catalog = InvoiceCatalog(p)

    @p.app.get('/admin/assistent-artikel')
    @p.admin_required
    def assistant_invoice_catalog():
        return render_template('assistent_artikel.html', report=catalog.status(), articles=catalog.search('', 100))

    @p.app.post('/admin/assistent-artikel/start')
    @p.admin_required
    def assistant_invoice_catalog_start():
        return jsonify(catalog.prepare(service.invoice_sources()))

    @p.app.post('/admin/assistent-artikel/weiter')
    @p.admin_required
    def assistant_invoice_catalog_next():
        return jsonify(catalog.process_next())

    @p.app.get('/admin/assistent-artikel/status')
    @p.admin_required
    def assistant_invoice_catalog_status():
        return jsonify(catalog.status())

    return catalog
