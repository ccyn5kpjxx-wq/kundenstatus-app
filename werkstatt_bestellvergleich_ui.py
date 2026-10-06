"""Admin presentation for immutable order-price evidence; no dispatch actions."""
from flask import abort, flash, redirect, request, url_for

from werkstatt_rechnungsfreigabe import normalize_supplier


def comparison_key(entry):
    source = entry.get('source', '')
    if source.startswith('material:'):
        return source
    if source.startswith('dispatch:'):
        return 'order:' + source[9:]
    return None


def comparison_context(portal, overview):
    service = getattr(portal, 'order_price_comparison', None)
    context = dict(price_summaries={}, order_comparison=None, comparison_files=[], comparison_candidates=[])
    if service is None:
        return context
    selected = overview.get('selected')
    entries = list(overview.get('items', [])) + ([selected] if selected else [])
    for entry in entries:
        key = comparison_key(entry)
        if not key:
            continue
        try:
            detail = service.detail(key)
        except (ValueError, LookupError, PermissionError):
            continue
        context['price_summaries'][entry['source']] = detail
        if entry is selected:
            context['order_comparison'] = detail
    detail = context['order_comparison']
    if detail is None:
        return context
    order = detail['order']
    intake = getattr(portal, 'workshop_intake', None)
    if intake:
        with intake.db() as db:
            files = db.execute('''SELECT f.id,f.eingang_id AS group_id,f.original_name,g.supplier
                FROM einkauf_eingang_dateien f JOIN einkauf_eingang g ON g.id=f.eingang_id
                WHERE f.kind='rechnung' ORDER BY f.id DESC LIMIT 500''').fetchall()
        for row in files:
            source = dict(row)
            source.update(kind='intake', file_id=source['id'])
            if (normalize_supplier(source['supplier']) == normalize_supplier(order['supplier'])
                    and service._allowed(source)):
                context['comparison_files'].append(source)
    if not detail['estimate']:
        try:
            rows = service._catalog().knowledge_rows(limit=5000)['items']
            for row in rows:
                if (normalize_supplier(row.get('lieferant')) != normalize_supplier(order['supplier'])
                        or (row.get('artikelnummer') or '').strip().casefold() != order['sku'].strip().casefold()):
                    continue
                try:
                    price = service._catalog_estimate(order, {'proposal_id': row.get('vorschlag_id'),
                        'identity_confirmed': True, 'unit_matches_order': True})
                except (ValueError, LookupError, PermissionError):
                    continue
                context['comparison_candidates'].append(dict(row, comparison_price=price))
        except (ValueError, LookupError, PermissionError):
            pass
    return context


def register_comparison_forms(bp, portal):
    @bp.post('/preisvergleich/<action>')
    def price_comparison_form(action):
        service = getattr(portal, 'order_price_comparison', None)
        if service is None or action not in {'schaetzung', 'rechnung', 'katalog', 'beleg'}:
            abort(404)
        key = request.form.get('order_key', '')
        # The blueprint already enforces admin authentication and CSRF.
        try:
            if action == 'beleg':
                with portal.portal_originals_operation_lock():
                    order = service.detail(key)['order']
                    upload = request.files.get('file')
                    if upload is None or not upload.filename:
                        raise ValueError('Rechnungsoriginal auswählen.')
                    rule = service._catalog().source_rule({'supplier': order['supplier'],
                        'beleg_typ': 'rechnung', 'original_name': upload.filename})
                    if rule.get('allowed') is not True:
                        raise PermissionError('Diese Rechnungsquelle ist nicht für Materialpreise freigegeben.')
                    # A separate receipt collection keeps the original WhatsApp
                    # intake and the immutable supplier order untouched.
                    group = portal.workshop_intake.create({
                        'source_key': 'order-receipts:' + order['key'],
                        'source_at': order['created_at'],
                        'supplier': order['supplier'], 'external_ref': 'Belegsammlung zur Bestellung ' + order['key'],
                        'already_ordered': False, 'original_author': None,
                        'lines': [{'product': order['product'], 'sku': order['sku'], 'variant': order['variant'],
                            'quantity': None, 'unit': order['unit'], 'pack': '', 'urgent': None, 'category': 'material'}]})
                    portal.workshop_intake.attach(group['id'], upload, 'rechnung')
                    flash('Rechnungsoriginal beim Bestelllieferanten gespeichert. Jetzt die Position für den Preisvergleich auswählen.', 'success')
                    return redirect(url_for('werkstatt_orders.index',
                        bestellung=key[6:] if key.startswith('order:') else key, _anchor='bestelldetail'), 303)
            elif action == 'katalog':
                payload = {'proposal_id': request.form.get('proposal_id'),
                    'identity_confirmed': request.form.get('identity_confirmed') == 'ja',
                    'unit_matches_order': request.form.get('unit_matches_order') == 'ja'}
            else:
                original = request.form.get('original', '').split(':')
                if len(original) != 2:
                    raise ValueError('Rechnungsoriginal auswählen.')
                payload = {name: request.form.get(name, '') for name in
                    ('source_date', 'page', 'position', 'amount', 'tax_basis', 'tax_rate',
                     'discount_basis', 'unit', 'pack')}
                payload.update(group_id=original[0], file_id=original[1], currency='EUR',
                    identity={name: request.form.get(name, '') for name in ('supplier','sku','variant')},
                    reviewed=request.form.get('reviewed') == 'ja')
                if action == 'rechnung':
                    payload['quantity'] = request.form.get('quantity', '')
            if action == 'rechnung':
                result = service.record_invoice(key, payload)
                message = 'Rechnungspreis zugeordnet. Der Vergleich steht bei der Bestellung.'
            else:
                result = service.record_estimate(key, payload)
                message = 'Geschätzter Preis mit seiner Quelle fest bei dieser Bestellung gespeichert.'
            key = result['order']['key']
            flash(message, 'success')
        except (ValueError, LookupError, PermissionError) as exc:
            flash(str(exc), 'error')
        selected = key[6:] if key.startswith('order:') else key
        return redirect(url_for('werkstatt_orders.index', bestellung=selected, _anchor='bestelldetail'), 303)
