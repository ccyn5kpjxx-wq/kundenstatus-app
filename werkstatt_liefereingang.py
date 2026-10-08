"""Delivery evidence attached to frozen orders, with conservative upload automation."""
from datetime import datetime, timezone
from decimal import Decimal
import json
import re
import threading
from statistics import median

from flask import abort, flash, redirect, request, url_for

from werkstatt_einkaufseingang import IntakeConflict, _decimal, _hash, _id, _json, _norm, _text
from werkstatt_rechnungsfreigabe import normalize_supplier
from werkstatt_bestellplan import BERLIN


def document_position(value):
    if type(value) is int and 0 <= value <= 999999:
        return value
    if isinstance(value, str) and re.fullmatch(r'\d{1,6}', value.strip()):
        return int(value)
    raise ValueError('Belegposition muss eine ganze Zahl ab 0 sein.')


def receipt_payload(order):
    # Keep byte-for-byte semantic compatibility with existing invoice collections.
    return {'source_key': 'order-receipts:' + order['key'], 'source_at': order['created_at'],
        'supplier': order['supplier'], 'external_ref': 'Belegsammlung zur Bestellung ' + order['key'],
        'already_ordered': False, 'original_author': None,
        'lines': [{'product': order['product'], 'sku': order['sku'], 'variant': order['variant'],
            'quantity': None, 'unit': order['unit'], 'pack': '', 'urgent': None, 'category': 'material'}]}


def delivery_ocr_rows(result):
    """Preserve table rows from local OCR boxes, without inventing column values."""
    words, slopes = [], []
    for item in result or []:
        if len(item) < 3 or float(item[2]) < .35:
            continue
        box = item[0]
        width = box[1][0] - box[0][0]
        if width > 200:
            slope = (box[1][1] - box[0][1]) / width
            if abs(slope) < .15:
                slopes.append(slope)
    slope = median(slopes) if slopes else 0
    for item in result or []:
        if len(item) < 3 or float(item[2]) < .35:
            continue
        box, text = item[0], str(item[1]).strip()
        top, bottom = min(point[1] for point in box), max(point[1] for point in box)
        center_x = sum(point[0] for point in box) / len(box)
        words.append({'x': min(point[0] for point in box), 'y': (top + bottom) / 2 - slope * center_x,
                      'height': bottom - top, 'text': text})
    rows = []
    for word in sorted(words, key=lambda word: word['y']):
        if rows and abs(word['y'] - rows[-1][0]['y']) <= max(word['height'], rows[-1][0]['height']) * .45:
            rows[-1].append(word)
        else:
            rows.append([word])
    return '\n'.join(' '.join(word['text'] for word in sorted(row, key=lambda word: word['x'])) for row in rows)


def analyze_delivery_text(text):
    """Conservative Top-Color table hints, with raw OCR retained as the fallback."""
    result = {'number': '', 'date': '', 'supplier': '', 'items': [], 'warnings': [
        'Auslese ist ungeprüft. Artikel, Variante, Gebinde und Liefermenge am Original kontrollieren.',
        'Geliefert zählt Gebinde; Inhalt und Gesamtmenge in Litern sind getrennte Angaben.']}
    if not isinstance(text, str):
        return result
    if re.search(r'top[\s\-]*color', text, re.I):
        result['supplier'] = 'Top-Color'
    number = re.search(r'\b[A-Z0-9]+-LS\d+\b', text, re.I)
    if number:
        result['number'] = number[0]
    date = re.search(r'(?:Beleg[\s-]*Datum|Lieferdat\.?)[^\d]{0,20}(\d{2}\.\d{2}\.\d{4})', text, re.I)
    if date:
        result['date'] = date[1]
    # Require the actual delivery table. Do not reuse the invoice-price parser.
    if not (re.search(r'lieferschein', text, re.I) and re.search(r'geliefert', text, re.I)
            and re.search(r'bestellt', text, re.I) and re.search(r'inhalt', text, re.I)):
        result['warnings'].append('Keine sichere Lieferscheintabelle erkannt; Werte manuell am Original prüfen.')
        return result
    start = re.compile(r'^\s*(\d{1,6})[.)]?\s+([A-Za-z0-9][A-Za-z0-9._/-]{3,30})\s*(.*)$')
    compact_topcolor = re.compile(r'^\s*(\d{1,6})[.)]\s*(\d{8})\s*(.*)$')
    columns = re.compile(r'^(.*?)\s+(\d+[,.]\d+)\s+(\d+[,.]\d+)\s+(\d+[,.]\d+)\s*(Ltr\s*/\s*KG|St[üu]ck|Stuck|Liter|kg|l)\s+(\d+[,.]\d+)(?:\s+.*)?$', re.I)
    lines = text.splitlines()
    page = 1
    for index, line in enumerate(lines):
        page_marker = re.search(r'^\s*(?:Seite|Page)\s*:?\s*(\d+)\s*$', line, re.I)
        if page_marker:
            page = int(page_marker[1]) or 1
        match = compact_topcolor.match(line) or start.match(line)
        if not match:
            continue
        tail = columns.match(match[3])
        if not tail:
            continue
        description = tail[1]
        for next_line in lines[index + 1:index + 3]:
            continuation = next_line.strip()
            if start.match(continuation) or compact_topcolor.match(continuation):
                break
            if re.match(r'^(?:WF\w+|CRYSTAL|ZZZ\w+|/Energie)', continuation, re.I):
                description += ' ' + continuation
        fee = bool(re.search(r'logistik|energie|pauschale|fracht|versand', description, re.I))
        result['items'].append({'page': page, 'position': int(match[1]), 'sku': match[2],
            'description': description.strip(), 'quantity': tail[2].replace(',', '.'),
            'ordered': tail[3].replace(',', '.'), 'content': tail[4] + ' ' + tail[5],
            'total_content': tail[6], 'fee': fee})
    if not result['items']:
        result['warnings'].append('Tabellenspalten nicht eindeutig erkannt; keine Liefermenge vorgeschlagen.')
    return result


class OrderDelivery:
    def __init__(self, portal):
        self.p = portal
        self._analysis_lock = threading.Lock()
        self.init_schema()

    def init_schema(self):
        with self.p.order_price_comparison.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS assistent_bestelllieferungen (
                id INTEGER PRIMARY KEY AUTOINCREMENT, order_key TEXT NOT NULL,
                order_fingerprint TEXT NOT NULL, source_key TEXT NOT NULL UNIQUE,
                payload_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL, created_by TEXT NOT NULL);''')

    def order(self, key):
        with self.p.order_price_comparison.db() as db:
            return self.p.order_price_comparison._order(db, key)

    def choices(self):
        """Only offer saved, verifiable orders; aliases share one choice."""
        comparison = self.p.order_price_comparison
        orders = {}
        with comparison.db() as db:
            keys = ['material:' + str(row['id']) for row in db.execute(
                "SELECT id FROM einkauf_material_dialoge WHERE state IN ('external_pending','external_sent') ORDER BY id DESC").fetchall()]
            keys += ['order:' + row['id'] for row in db.execute(
                'SELECT id FROM assistent_bestellanforderungen ORDER BY created_at DESC,id DESC').fetchall()]
            for key in keys:
                try:
                    order = comparison._order(db, key)
                    _decimal(order['quantity'], 'Bestellmenge')
                except (ValueError, LookupError, PermissionError):
                    continue
                order['created_display'] = (datetime.fromisoformat(order['created_at']).astimezone(BERLIN)
                    .strftime('%d.%m.%Y %H:%M') if order['created_at'] else '')
                orders.setdefault(order['key'], order)
        return list(orders.values())

    def files(self, order):
        intake = self.p.workshop_intake
        with intake.db() as db:
            group = db.execute('SELECT id FROM einkauf_eingang WHERE source_key=?',
                ('order-receipts:' + order['key'],)).fetchone()
        if not group:
            return []
        files = [dict(file, group_id=group['id'], analysis=analyze_delivery_text(file['draft_text']))
                 for file in intake.detail(group['id'])['files'] if file['kind'] == 'lieferschein']
        if not files:
            return []
        with self.p.order_price_comparison.db() as db:
            rows = db.execute('SELECT DISTINCT order_key FROM assistent_bestelllieferungen').fetchall()
        events = []
        for row in rows:
            try:
                detail = self.detail(row['order_key'])
                events.extend(dict(event, matching_order=detail['order']) for event in detail['events'])
            except (ValueError, LookupError, PermissionError):
                continue
        from werkstatt_lieferautomatik import document_identity, matching_item
        for file in files:
            items = [item for item in file['analysis']['items'] if not item['fee']]
            covered = [item for item in items if any(event['page'] == item['page'] and event['position'] == item['position'] and
                event['sku'].casefold() == item['sku'].casefold() and Decimal(event['quantity']) == Decimal(item['quantity']) and
                matching_item(event['matching_order'], file['draft_text'], item) and
                (event['file_sha256'] == file['sha256'] or
                 event.get('document_identity') == document_identity(file['analysis'], item)) for event in events)]
            file['assignment_complete'] = bool(items) and len(covered) == len(items)
            file['assignment_partial'] = bool(covered) and not file['assignment_complete']
        return files

    def detail(self, key):
        order = self.order(key)
        with self.p.order_price_comparison.db() as db:
            rows = [dict(row) for row in db.execute('SELECT * FROM assistent_bestelllieferungen WHERE order_key=? ORDER BY id',
                (order['key'],)).fetchall()]
        valid, stale = [], False
        for row in rows:
            data = json.loads(row['payload_json'])
            if row['order_fingerprint'] != order['fingerprint'] or _hash(data) != row['payload_hash']:
                stale = True
                continue
            try:
                _, _, original = self.source(key, data['group_id'], data['file_id'])
                if original['sha256'] != data['file_sha256']:
                    raise ValueError('Original stimmt nicht mit Liefernachweis überein.')
            except (ValueError, LookupError, KeyError):
                stale = True
                continue
            valid.append(dict(data, id=row['id']))
        received = sum((Decimal(row['quantity']) for row in valid), Decimal(0))
        ordered = Decimal(_decimal(order['quantity'], 'Bestellmenge'))
        state = ('pruefen' if stale else 'offen' if not valid else 'teillieferung' if received < ordered
                 else 'geliefert' if received == ordered else 'mehrlieferung')
        labels = {'offen': 'Lieferung noch offen', 'teillieferung': 'Teillieferung', 'geliefert': 'Vollständig geliefert',
                  'mehrlieferung': 'Mehr geliefert – prüfen', 'pruefen': 'Liefernachweis prüfen'}
        return {'order': order, 'events': valid, 'quantity': format(received.normalize(), 'f'),
                'ordered': format(ordered.normalize(), 'f'), 'unit': order['unit'], 'state': state, 'label': labels[state]}

    def attach(self, key, upload):
        with self.p.portal_originals_operation_lock():
            order = self.order(key)
            if upload is None or not upload.filename:
                raise ValueError('Lieferschein als Foto oder PDF auswählen.')
            group = self.p.workshop_intake.create(receipt_payload(order))
            file = self.p.workshop_intake.attach(group['id'], upload, 'lieferschein')
        return order, group, file

    def source(self, key, group_id, file_id):
        order = self.order(key)
        with self.p.workshop_intake.db() as db:
            group = self.p.workshop_intake._group(db, group_id)
            file = self.p.workshop_intake._file_metadata(db, group_id, file_id)
        if group['source_key'] != 'order-receipts:' + order['key'] or file['kind'] != 'lieferschein':
            raise ValueError('Lieferschein gehört nicht zur Belegsammlung dieser Bestellung.')
        return order, group, file

    def analyze(self, key, group_id, file_id):
        # Avoid accumulating requests and simultaneous native OCR processes.
        if not self._analysis_lock.acquire(blocking=False):
            raise ValueError('Eine Beleganalyse läuft bereits. Bitte gleich erneut versuchen.')
        try:
            with self.p.portal_originals_operation_lock():
                self.source(key, group_id, file_id)
                return self.p.workshop_intake.analyze_file(group_id, file_id,
                    reader=self.read_text, reuse_success=True)
        finally:
            self._analysis_lock.release()

    def read_text(self, path, name):
        if getattr(self.p, 'RUNNING_ON_RENDER', False):
            from werkstatt_belegauslese import read_receipt
            return read_receipt(path)
        if path.suffix.lower() in {'.jpg', '.png', '.webp'}:
            factory = getattr(self.p, 'get_rapid_ocr', None)
            if callable(factory):
                try:
                    engine = factory()
                    if engine is not None:
                        result, _ = engine(str(path))
                        text = delivery_ocr_rows(result)
                        if text.strip():
                            return text
                except Exception:
                    pass
        reader = getattr(self.p, 'extract_document_text_local', None)
        if not callable(reader):
            raise ValueError('Lokale Belegauslese ist nicht verfügbar.')
        return reader(path, name)

    def record(self, key, payload, *, automatic=False):
        if not automatic and payload.get('reviewed') is not True:
            raise ValueError('Lieferant, Artikel, Variante, Gebinde und Liefermenge am Original bestätigen.')
        group_id, file_id = _id(payload.get('group_id')), _id(payload.get('file_id'))
        page, position = _id(payload.get('page'), 'Belegseite'), document_position(payload.get('position'))
        quantity = _decimal(payload.get('quantity'), 'Gelieferte Gebinde')
        identity = {name: _text(payload.get(name), name, 500, name == 'variant') for name in ('supplier', 'sku', 'variant', 'unit')}
        with self.p.portal_originals_operation_lock():
            order, group, file = self.source(key, group_id, file_id)
            automatic_fields = {}
            if automatic:
                from werkstatt_lieferautomatik import matching_item, previous_delivery, document_identity
                analysis = analyze_delivery_text(file['draft_text'])
                analysis['_text'] = file['draft_text']
                items = [item for item in analysis['items'] if item['page'] == page and item['position'] == position]
                if (file['extraction_status'] != 'pruefen' or len(items) != 1 or
                        not matching_item(order, file['draft_text'], items[0]) or
                        Decimal(quantity) != Decimal(items[0]['quantity'])):
                    raise ValueError('Lieferscheinposition ist nicht eindeutig automatisch belegbar.')
                previous = previous_delivery(self, analysis, items[0], file['sha256'])
                if previous:
                    if previous['order']['key'] != order['key']:
                        raise IntakeConflict('Lieferscheinposition ist bereits einer anderen Bestellung zugeordnet.')
                    return previous
                current = self.detail(order['key'])
                if (current['state'] in {'pruefen', 'mehrlieferung'} or
                        Decimal(quantity) > Decimal(current['ordered']) - Decimal(current['quantity'])):
                    raise ValueError('Liefermenge passt nicht zur offenen Bestellmenge.')
                automatic_fields = {'assignment_method': 'automatic-v1', 'document_identity': document_identity(analysis, items[0]),
                    'document_number': analysis['number'], 'document_date': analysis['date'],
                    'content': items[0]['content'], 'total_content': items[0]['total_content']}
            if (normalize_supplier(identity['supplier']) != normalize_supplier(order['supplier'])
                    or any(_norm(identity[name]) != _norm(order[name]) for name in ('sku', 'variant', 'unit'))):
                raise ValueError('Lieferant, Artikel, Variante oder Bestelleinheit passt nicht zur Bestellung.')
            raw, mime, _ = self.p.workshop_intake.original(group_id, file_id)
            if mime == 'application/pdf':
                import fitz
                with fitz.open(stream=raw, filetype='pdf') as document:
                    pages = document.page_count
            else:
                pages = 1
            if page > pages:
                raise ValueError('Belegseite ist im Original nicht vorhanden.')
            hints = [item for item in analyze_delivery_text(file['draft_text'])['items']
                     if item['page'] == page and item['position'] == position]
            correction = ''
            if len(hints) == 1 and (hints[0]['fee'] or _norm(hints[0]['sku']) != _norm(order['sku'])):
                if payload.get('correction_confirmed') is not True:
                    raise ValueError('Auslese zeigt hier Nebenkosten oder einen anderen Artikel. Original prüfen; eine fehlerhafte Auslese mit Begründung korrigieren.')
                correction = _text(payload.get('correction_reason'), 'Begründung der Auslesekorrektur', 500)
            record = {'order_key': order['key'], 'order_fingerprint': order['fingerprint'],
                'file_sha256': file['sha256'], 'group_id': group_id, 'file_id': file_id, 'page': page,
                'position': position, 'quantity': quantity, 'unit': order['unit'],
                'supplier': order['supplier'], 'sku': order['sku'], 'variant': order['variant'],
                'ocr_correction': correction, **automatic_fields}
            # Physical file IDs can differ after re-upload; the original bytes and
            # printed page/position establish the global delivery identity.
            stable = {name: value for name, value in record.items() if name not in {'group_id', 'file_id'}}
            digest = _hash(stable)
            source = 'original:' + file['sha256'] + ':' + str(page) + ':' + str(position)
            with self.p.order_price_comparison.db() as db:
                db.execute('INSERT INTO assistent_bestelllieferungen '
                    '(order_key,order_fingerprint,source_key,payload_hash,payload_json,created_at,created_by) '
                    'VALUES(?,?,?,?,?,?,?) ON CONFLICT(source_key) DO NOTHING',
                    (order['key'], order['fingerprint'], source, _hash(record), _json(record),
                     datetime.now(timezone.utc).isoformat(), 'admin:auto' if automatic else 'admin'))
                existing = dict(db.execute('SELECT * FROM assistent_bestelllieferungen WHERE source_key=?', (source,)).fetchone())
                old = json.loads(existing['payload_json'])
                old_stable = {name: value for name, value in old.items() if name not in {'group_id', 'file_id'}}
                if _hash(old_stable) != digest:
                    raise IntakeConflict('Diese Lieferscheinposition ist bereits anders zugeordnet. Keine doppelte Lieferbuchung.')
        return self.detail(order['key'])


def get_delivery(portal):
    service = getattr(portal, 'order_delivery', None)
    if service is None:
        service = portal.order_delivery = OrderDelivery(portal)
        portal.order_delivery_init_schema = service.init_schema
    return service


def delivery_context(portal, overview):
    context = {'delivery_summaries': {}, 'order_delivery': None, 'delivery_files': []}
    if not getattr(portal, 'order_price_comparison', None) or not getattr(portal, 'workshop_intake', None):
        return context
    service = getattr(portal, 'order_delivery', None)
    if service is None:
        return context
    selected = overview.get('selected')
    for entry in list(overview.get('items', [])) + ([selected] if selected else []):
        source = entry.get('source', '')
        if not source.startswith(('material:', 'dispatch:')):
            continue
        try:
            detail = service.detail(source)
        except (ValueError, LookupError, PermissionError):
            continue
        context['delivery_summaries'][source] = detail
        if entry is selected:
            context['order_delivery'] = detail
            context['delivery_files'] = service.files(detail['order'])
    return context


def register_delivery_forms(bp, portal):
    @bp.route('/lieferung/<action>', methods=['GET', 'POST'])
    def order_delivery_form(action):
        if action not in {'beleg', 'analyse', 'zuordnen', 'automatisch'}:
            abort(404)
        if request.method != 'POST':
            if action not in {'beleg', 'automatisch'}:
                abort(405)
            flash('Bitte den Lieferschein und die passende Bestellung erneut auswählen.' if action == 'beleg'
                  else 'Bitte den Lieferschein erneut auswählen. Die passende Bestellung wird automatisch erkannt.', 'warning')
            return redirect(url_for('werkstatt_orders.intake_index', _anchor='lieferschein-upload'), 303)
        key = request.form.get('order_key', '')
        from_intake = action in {'beleg', 'automatisch'} and request.form.get('return_to') == 'eingang'
        try:
            service = get_delivery(portal)
            if action == 'automatisch':
                from werkstatt_lieferautomatik import process_delivery
                report = process_delivery(service, key, request.files.get('file'),
                    request.form.get('group_id'), request.form.get('file_id'))
                orders = {item['order']['key']: item for item in report['orders']}
                if orders:
                    key = next(iter(orders))
                    message = 'Lieferschein automatisch zugeordnet. ' + ' · '.join(item['label'] for item in orders.values()) + '.'
                    if report['review']:
                        message += ' Weitere Positionen brauchen Prüfung: ' + ' '.join(report['review'])
                    flash(message, 'warning' if report['review'] else 'success')
                    return redirect(url_for('werkstatt_orders.index', bestellung=key[6:] if key.startswith('order:') else key,
                        _anchor='liefereingang'), 303)
                flash('Original gespeichert. ' + ' '.join(report['review']), 'warning')
                if not key:
                    return redirect(url_for('werkstatt_orders.intake_index', id=report['group_id']), 303)
                return redirect(url_for('werkstatt_orders.index', bestellung=key[6:] if key.startswith('order:') else key,
                    _anchor='liefereingang'), 303)
            elif action == 'beleg':
                if not key:
                    raise ValueError('Bitte die passende Bestellung auswählen und den Lieferschein erneut auswählen.')
                order, _, _ = service.attach(key, request.files.get('file'))
                key = order['key']
                message = 'Lieferschein gespeichert. Jetzt analysieren und die Lieferung prüfen.'
            elif action == 'analyse':
                result = service.analyze(key, request.form.get('group_id'), request.form.get('file_id'))
                message = {'pruefen': 'Beleg analysiert. Auslese am Original prüfen; noch keine Lieferung gebucht.',
                    'keine_auslese': 'Keine lesbare Auslese. Original öffnen und Lieferdaten manuell prüfen.',
                    'zeitlimit': 'Auslese nach 20 Sekunden beendet. Original erhalten; Lieferdaten manuell prüfen oder erneut analysieren.',
                    'fehler': 'Auslese fehlgeschlagen. Original erhalten; manuelle Prüfung möglich.'}.get(result['extraction_status'], 'Auslese prüfen.')
            else:
                result = service.record(key, {name: request.form.get(name) for name in
                    ('group_id', 'file_id', 'page', 'position', 'quantity', 'supplier', 'sku', 'variant', 'unit')} |
                    {'reviewed': request.form.get('reviewed') == 'ja',
                     'correction_reason': request.form.get('correction_reason', ''),
                     'correction_confirmed': request.form.get('correction_confirmed') == 'ja'})
                key = result['order']['key']
                message = 'Geprüfte Lieferung zugeordnet. ' + result['label'] + '.'
            flash(message, 'success')
        except (ValueError, LookupError, PermissionError) as exc:
            flash(str(exc), 'error')
            if from_intake:
                return redirect(url_for('werkstatt_orders.intake_index', _anchor='lieferschein-upload'), 303)
        return redirect(url_for('werkstatt_orders.index', bestellung=key[6:] if key.startswith('order:') else key,
            _anchor='liefereingang'), 303)
