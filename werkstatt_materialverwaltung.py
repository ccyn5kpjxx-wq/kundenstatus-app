"""Admin review of material requests; no impersonated employee or implicit send."""
from decimal import Decimal, InvalidOperation
from datetime import datetime
import hmac
import re
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, flash, redirect, request, session, url_for


def euro_cents(value):
    """Parse an explicitly entered gross EUR amount, including an explicit zero."""
    if not isinstance(value, str):
        raise ValueError('Alle Bruttopreise und Nebenkosten ausdrücklich in Euro angeben.')
    value = value.strip().replace(',', '.')
    if not re.fullmatch(r'[0-9]{1,7}(?:\.[0-9]{1,2})?', value):
        raise ValueError('Beträge ohne Tausendertrennzeichen mit höchstens zwei Nachkommastellen angeben; bei keinen Nebenkosten 0 eintragen.')
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError('Betrag ist ungültig.') from exc
    if not amount.is_finite() or amount < 0:
        raise ValueError('Betrag ist ungültig.')
    return int(amount * 100)


def register_material_admin(p):
    if 'werkstatt_materialverwaltung' in p.app.blueprints:
        return
    bp = Blueprint('werkstatt_materialverwaltung', __name__,
                   url_prefix='/admin/assistent-bestellungen/eingang/material')

    def display_time(value):
        try:
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                return 'Zeitpunkt prüfen'
            return parsed.astimezone(ZoneInfo('Europe/Berlin')).strftime('%d.%m.%Y, %H:%M Uhr')
        except (TypeError, ValueError):
            return 'Zeitpunkt unbekannt'

    p.app.jinja_env.globals['material_time'] = display_time

    def unresolved_texts():
        if not callable(getattr(p, 'get_db', None)):
            return []
        db = p.get_db()
        try:
            return [dict(row) for row in db.execute('''SELECT t.body,t.source_at,t.error_code,m.name
                FROM einkauf_material_texte t LEFT JOIN mitarbeiter m ON m.id=t.employee_id
                WHERE t.state='review' ORDER BY t.id DESC LIMIT 20''').fetchall()]
        finally:
            db.close()

    p.app.jinja_env.globals['material_unresolved_texts'] = unresolved_texts

    @bp.before_request
    def authorize():
        if not session.get('admin'):
            abort(403)
        expected = session.get('csrf_token')
        supplied = request.form.get('csrf_token')
        if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
            abort(400, description='Die Seite ist veraltet. Bitte neu öffnen.')

    @bp.after_request
    def private(response):
        response.headers['Cache-Control'] = 'no-store'
        return response

    @bp.post('/<int:draft_id>/pruefen')
    def review(draft_id):
        form = request.form
        try:
            if form.get('reviewed') != 'ja':
                raise ValueError('Artikel, Variante und aktuelle Preisbedingungen zuerst ausdrücklich prüfen.')
            revision = form.get('revision', type=int)
            if not revision or revision < 1:
                raise ValueError('Materialvorgang wurde geändert. Bitte neu laden.')
            payload = {key: form.get(key, '').strip() for key in (
                'supplier_id', 'article_number', 'product_name', 'variant', 'unit', 'price_source', 'verified_until')}
            for key in ('unit_price', 'shipping', 'extra_costs'):
                payload[key + '_cents'] = euro_cents(form.get(key))
            payload['reviewed'] = True
            p.material_dialog.apply_admin_review(draft_id, revision, payload, actor='admin')
        except (ValueError, PermissionError, LookupError) as exc:
            flash(str(exc), 'error')
        else:
            flash('Artikel und Preisbedingungen gespeichert. Eine vollständige Mitarbeiterbestellung wird vom aktiven Bestelldienst gemäß Dringlichkeit übergeben.', 'success')
        return redirect(url_for('werkstatt_orders.intake_index', material=draft_id, _anchor='materialdialog'), code=303)

    @bp.post('/<int:draft_id>/auslesen')
    def analyze(draft_id):
        try:
            current = p.material_dialog.status(draft_id)
            if current['revision'] != request.form.get('revision', type=int):
                raise ValueError('Materialvorgang wurde geändert. Bitte neu laden.')
            result = p.material_dialog.analyze(draft_id)
            if result['analysis_state'] == 'done':
                flash('Foto ausgelesen. Die Artikeltreffer sind Vorschläge zur Prüfung.', 'success')
            else:
                flash('Die Fotoauslese ist noch nicht abgeschlossen. Bitte den Status prüfen.', 'error')
        except (ValueError, PermissionError, LookupError) as exc:
            flash(str(exc), 'error')
        return redirect(url_for('werkstatt_orders.intake_index', material=draft_id, _anchor='materialdialog'), code=303)

    @bp.post('/<int:draft_id>/uebergabe-pruefen')
    def recheck(draft_id):
        try:
            revision = request.form.get('revision', type=int)
            if not revision or revision < 1:
                raise ValueError('Materialvorgang wurde geändert. Bitte neu laden.')
            result = p.material_dialog.recheck(draft_id, revision)
            if result['state'] == 'approved':
                flash('Die unveränderte Anforderung ist wieder zur Übergabe vorgemerkt. Der aktive Bestelldienst prüft vor dem Versand erneut Rechte, Kosten und Kontakt.', 'success')
            elif result.get('internal_review_pending') is True and result.get('employee_reply_required') is False:
                flash('Die Mitarbeiterangaben sind erfasst. Lieferantenzuordnung und Einkaufskonditionen werden intern geprüft; keine erneute Artikelfrage nötig. Noch nicht bestellt.', 'info')
            else:
                flash('Anforderung erneut geprüft. Offene Angaben sind weiterhin zu klären.', 'error')
        except (ValueError, PermissionError, LookupError) as exc:
            flash(str(exc), 'error')
        return redirect(url_for('werkstatt_orders.intake_index', material=draft_id, _anchor='materialdialog'), code=303)

    @bp.post('/<int:draft_id>/extern-reservieren')
    def reserve_external(draft_id):
        try:
            revision = request.form.get('revision', type=int)
            if not revision or revision<1 or request.form.get('confirmed')!='ja':
                raise ValueError('Aktuellen Vorgang und die einzelne externe Bestellung ausdrücklich bestätigen.')
            payload = {key:request.form.get(key,'').strip() for key in (
                'supplier_id','recipient','product_name','article_number','variant','subject',
                'recipient_source','authorization_note')}
            payload.update(max_total_cents=euro_cents(request.form.get('max_total')),confirmed=True)
            p.material_dialog.reserve_external(draft_id,revision,payload,actor='admin')
            flash('Einzelbestellung fest reserviert. Die Automatik bleibt für diesen Vorgang gesperrt. Es wurde keine E-Mail versandt; Versand anschließend am selben Vorgang nachweisen.', 'success')
        except (ValueError,PermissionError,LookupError) as exc:
            flash(str(exc),'error')
        return redirect(url_for('werkstatt_orders.intake_index', material=draft_id, _anchor='materialdialog'),code=303)

    @bp.post('/<int:draft_id>/extern-versand-nachweisen')
    def record_external_sent(draft_id):
        try:
            revision = request.form.get('revision', type=int)
            if not revision or revision<1 or request.form.get('confirmed')!='ja':
                raise ValueError('Aktuelle Reservierung und tatsächlich erfolgten Versand ausdrücklich bestätigen.')
            payload = {key:request.form.get(key,'').strip() for key in (
                'reservation_id','recipient','subject','sent_at','send_evidence')}
            payload['confirmed'] = True
            p.material_dialog.record_external_sent(draft_id,revision,payload,actor='admin')
            flash('Externer Mailversand mit Nachweis gespeichert. Dieser Vorgang bleibt dauerhaft gegen eine zweite automatische Bestellung gesperrt.', 'success')
        except (ValueError,PermissionError,LookupError) as exc:
            flash(str(exc),'error')
        return redirect(url_for('werkstatt_orders.intake_index', material=draft_id, _anchor='materialdialog'),code=303)

    p.app.register_blueprint(bp)
