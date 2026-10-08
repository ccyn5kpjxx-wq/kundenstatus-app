"""Positive delivery matching; document text never supplies actions or order data."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from io import BytesIO
import json
import re

from werkzeug.datastructures import FileStorage
from werkstatt_einkaufseingang import MAX_FILE_BYTES, _image_or_pdf
from werkstatt_rechnungsfreigabe import normalize_supplier


def compact(value):
    return re.sub(r'[^a-z0-9/.,]+', '', str(value).casefold()).replace(',', '.')


def supplier_key(value):
    value = normalize_supplier(value).replace(' ', '')
    return 'topcolor' if value in {'topcolor', 'topcolorgmbh'} else value


def document_identity(analysis, item):
    if not analysis['number'] or not analysis['supplier']:
        return ''
    return 'lieferschein:' + sha256((supplier_key(analysis['supplier']) + ':' +
        analysis['number'].upper()).encode()).hexdigest() + ':' + str(item['page']) + ':' + str(item['position'])


def matching_item(order, text, item):
    """Supported Top-Color table only; exact SKU, positive variant and pack proof."""
    from werkstatt_liefereingang import analyze_delivery_text
    analysis = analyze_delivery_text(text)
    header = re.split(r'lieferschein', text, maxsplit=1, flags=re.I)[0]
    if (not re.search(r'\bTOP[\s.-]*COLOR\b', header, re.I) or
            supplier_key(order['supplier']) != 'topcolor' or not analysis['number'] or not analysis['date'] or
            not order['dispatch_known'] or item['fee'] or item['sku'].casefold() != order['sku'].casefold() or
            normalize_supplier(order['unit']) not in {'stueck', 'stuck', 'gebinde'}):
        return False
    if len(set(re.findall(r'\b[A-Z0-9]+-LS\d+\b', text, re.I))) != 1:
        return False
    try:
        date = datetime.strptime(analysis['date'], '%d.%m.%Y').date()
        if date < datetime.fromisoformat(order['created_at']).date():
            return False
    except (ValueError, TypeError):
        return False
    # Only the frozen PPG code/pack family is currently positively understood.
    codes = re.findall(r'T\d{3,5}/E\d+(?:[.,]\d+)?', order['variant'] + ' ' + order['product'], re.I)
    codes = set(map(compact, codes))
    found = set(map(compact, re.findall(r'T\d{3,5}/E\d+(?:[.,]\d+)?', item['description'], re.I)))
    if len(codes) != 1 or codes != found:
        return False
    code = next(iter(codes))
    try:
        pack = Decimal(code.split('/e')[1])
        content = re.fullmatch(r'(\d+[,.]\d+)\s*(Ltr\s*/\s*KG|Liter|l)', item['content'], re.I)
        quantity, ordered = Decimal(item['quantity']), Decimal(item['ordered'])
        if (not content or Decimal(content[1].replace(',', '.')) != pack or quantity <= 0 or
                quantity != quantity.to_integral_value() or quantity > ordered or
                quantity * pack != Decimal(item['total_content'].replace(',', '.'))):
            return False
    except (InvalidOperation, ValueError):
        return False
    # Check explicit pack declarations too, including contradictory variants.
    frozen = order['variant'] + ' ' + order['product']
    packs = re.findall(r'(\d+(?:[.,]\d+)?)\s*(?:Liter|Ltr|L)\b', frozen + ' ' + item['description'], re.I)
    if any(Decimal(value.replace(',', '.')) != pack for value in packs):
        return False
    if any(Decimal(value.replace(',', '.')) != 1 for value in
           re.findall(r'(\d+(?:[.,]\d+)?)\s*Gebinde\b', frozen, re.I)):
        return False
    extra = re.sub(r'T\d{3,5}/E\d+(?:[.,]\d+)?', '', frozen, flags=re.I)
    extra = re.sub(r'\d+(?:[.,]\d+)?\s*(?:Liter|Ltr|L|Gebinde|Stück)', '', extra, flags=re.I)
    extra = re.sub(r'\b(?:à|a|je|PPG|Envirobase|High|Performance)\b', '', extra, flags=re.I)
    extra = re.sub(r'crystal\s*(?:silver|silber)', 'crystalsilber', extra, flags=re.I)
    description = re.sub(r'crystal\s*(?:silver|silber)', 'crystalsilber', item['description'], flags=re.I)
    if any(compact(token) not in compact(description)
           for token in re.findall(r'[A-Za-zÄÖÜäöüß]+|\d+(?:[.,]\d+)?', extra)):
        return False
    description, product = compact(description), compact(order['product'])
    if 'crystalsilver' in product or 'crystalsilber' in product:
        if not ('crystalsilver' in description or 'crystalsilber' in description):
            return False
    elif 'envirobase' not in description or 'envirobase' not in product:
        return False
    return True


def previous_delivery(service, analysis, item, digest):
    """Original hash AND printed document identity dedupe, including older manual rows."""
    from werkstatt_liefereingang import analyze_delivery_text
    identity = document_identity(analysis, item)
    with service.p.order_price_comparison.db() as db:
        rows = db.execute('SELECT * FROM assistent_bestelllieferungen ORDER BY id').fetchall()
    for row in rows:
        data = json.loads(row['payload_json'])
        if data['page'] != item['page'] or data['position'] != item['position']:
            continue
        same = data['file_sha256'] == digest or data.get('document_identity') == identity
        if not same:
            with service.p.workshop_intake.db() as db:
                try:
                    old_file = service.p.workshop_intake._file_metadata(db, data['group_id'], data['file_id'])
                    old_analysis = analyze_delivery_text(old_file['draft_text'])
                    old_identity = document_identity(old_analysis, data)
                    same = bool(identity) and old_identity == identity
                    if (not old_identity and supplier_key(data['supplier']) == supplier_key(analysis['supplier']) and
                            data['sku'].casefold() == item['sku'].casefold()):
                        raise LookupError('Älterer Liefernachweis ohne erkennbare Belegnummer vorhanden. Vor weiterer Zuordnung Original prüfen.')
                except (ValueError, KeyError):
                    pass
        if same:
            detail = service.detail(data['order_key'])
            if (detail['state'] == 'pruefen' or not any(event['id'] == row['id'] for event in detail['events']) or
                    not matching_item(detail['order'], analysis.get('_text', ''), item) or
                    data['sku'].casefold() != item['sku'].casefold() or
                    Decimal(data['quantity']) != Decimal(item['quantity'])):
                raise ValueError('Dieser Lieferschein ist bereits mit abweichenden Angaben zugeordnet. Original prüfen.')
            return detail
    return None


def inbox_upload(service, upload):
    if upload is None or not upload.filename:
        raise ValueError('Lieferschein als Foto oder PDF auswählen.')
    raw = upload.read(MAX_FILE_BYTES + 1)
    if not raw or len(raw) > MAX_FILE_BYTES:
        raise ValueError('Originaldatei leer oder größer als 8 MB.')
    _image_or_pdf(raw)
    digest = sha256(raw).hexdigest()
    intake = service.p.workshop_intake
    with service.p.portal_originals_operation_lock():
        with intake.db() as db:
            existing = db.execute('SELECT id FROM einkauf_eingang WHERE source_key=?', ('delivery-inbox:' + digest,)).fetchone()
        group = intake.detail(existing['id']) if existing else intake.create({
            'source_key': 'delivery-inbox:' + digest, 'source_at': datetime.now(timezone.utc).isoformat(),
            'supplier': 'Lieferant wird aus dem Lieferschein erkannt', 'external_ref': 'Lieferschein-Upload',
            'already_ordered': False, 'lines': [{'product': 'Lieferschein zur automatischen Zuordnung', 'category': 'ungeklaert'}]})
        file = intake.attach(group['id'], FileStorage(stream=BytesIO(raw), filename=upload.filename), 'lieferschein')
    return group, file


def process_delivery(service, key='', upload=None, group_id=None, file_id=None):
    """One upload action: preserve, analyze, match, book, render delivery status."""
    from werkstatt_liefereingang import analyze_delivery_text, receipt_payload
    if upload is not None:
        if key:
            order, group, file = service.attach(key, upload)
            key = order['key']
        else:
            group, file = inbox_upload(service, upload)
        group_id, file_id = group['id'], file['id']
    if key:
        file = service.analyze(key, group_id, file_id)
    else:
        if not service._analysis_lock.acquire(blocking=False):
            raise ValueError('Eine Beleganalyse läuft bereits. Der gespeicherte Beleg kann gleich erneut analysiert werden.')
        try:
            with service.p.portal_originals_operation_lock():
                with service.p.workshop_intake.db() as db:
                    group = service.p.workshop_intake._group(db, group_id)
                    file = service.p.workshop_intake._file_metadata(db, group_id, file_id)
                if not group['source_key'].startswith('delivery-inbox:') or file['kind'] != 'lieferschein':
                    raise ValueError('Dieser Eingang ist kein Lieferschein zur automatischen Zuordnung.')
                file = service.p.workshop_intake.analyze_file(group_id, file_id, reader=service.read_text, reuse_success=True)
        finally:
            service._analysis_lock.release()
    report = {'group_id': int(group_id), 'orders': [], 'review': [], 'booked': 0}
    if file['extraction_status'] != 'pruefen':
        report['review'].append({'zeitlimit': 'Die Auslese hat das Zeitlimit erreicht.',
            'keine_auslese': 'Der Lieferschein ist nicht ausreichend lesbar.'}.get(file['extraction_status'], 'Die Auslese ist fehlgeschlagen.'))
        return report
    analysis = analyze_delivery_text(file['draft_text'])
    analysis['_text'] = file['draft_text']
    if not analysis['items']:
        report['review'].append('Keine eindeutig lesbare Lieferscheintabelle erkannt.')
    with service.p.portal_originals_operation_lock():
        orders = [service.order(key)] if key else service.choices()
        for item in analysis['items']:
            if item['fee']:
                continue
            try:
                if sum(other['page'] == item['page'] and other['position'] == item['position']
                       for other in analysis['items']) != 1:
                    raise ValueError('Belegseite und Position sind mehrfach erkennbar. Original prüfen.')
                old = previous_delivery(service, analysis, item, file['sha256'])
                if old:
                    report['orders'].append(old)
                    continue
                candidates = []
                for order in orders:
                    if matching_item(order, file['draft_text'], item):
                        state = service.detail(order['key'])
                        if state['state'] in {'offen', 'teillieferung'} and Decimal(state['quantity']) < Decimal(state['ordered']):
                            candidates.append(order)
                if len(candidates) != 1:
                    raise ValueError('Artikel ' + item['sku'] + ': keine eindeutige passende Bestellung gefunden.')
                order = candidates[0]
                detail = service.detail(order['key'])
                remaining = Decimal(detail['ordered']) - Decimal(detail['quantity'])
                if detail['state'] in {'pruefen', 'mehrlieferung'} or Decimal(item['quantity']) > remaining:
                    raise ValueError('Artikel ' + item['sku'] + ': Liefermenge passt nicht zur offenen Bestellmenge.')
                if key:
                    target_group, target_file = int(group_id), file
                else:
                    raw, _, name = service.p.workshop_intake.original(group_id, file_id)
                    target = service.p.workshop_intake.create(receipt_payload(order))
                    target_group = target['id']
                    target_file = service.p.workshop_intake.attach(target_group,
                        FileStorage(stream=BytesIO(raw), filename=name), 'lieferschein')
                    with service.p.workshop_intake.db() as db:
                        db.execute('UPDATE einkauf_eingang_dateien SET draft_text=?,extraction_status=? WHERE id=? AND eingang_id=?',
                            (file['draft_text'], 'pruefen', target_file['id'], target_group))
                result = service.record(order['key'], {'group_id': target_group, 'file_id': target_file['id'],
                    'page': item['page'], 'position': item['position'], 'quantity': item['quantity'],
                    **{name: order[name] for name in ('supplier', 'sku', 'variant', 'unit')}}, automatic=True)
                report['orders'].append(result)
                report['booked'] += 1
            except (ValueError, LookupError, PermissionError) as exc:
                report['review'].append(str(exc))
    return report
