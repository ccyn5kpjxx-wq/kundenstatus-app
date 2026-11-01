"""Small office cockpit over personal, explicitly granted employee access."""
from flask import Blueprint, abort, g, redirect, render_template, request, session, url_for

from werkstatt_buero_zugang import CATEGORY_LABELS, OFFICE_ALLOWED_ENDPOINTS
from werkstatt_buero_posteingang import InboxBlocked, confirmed_ceo_mailbox_user


NAVIGATION = (
    ('termine', 'Heute', '/werkstatt/buero', 'Tagesübersicht und offene Büroaufgaben'),
    ('hol_bring', 'Fahrzeuge holen / bringen', '/werkstatt/buero/planung', 'Fahrzeug, Uhrzeit, Adresse und Kontakt'),
    ('termine', 'Kalender', '/werkstatt/buero/kalender', 'Bürotermine eintragen und bearbeiten'),
    ('arbeitszeiten', 'Arbeitszeiten', '/werkstatt/buero/arbeitszeiten', 'Tägliche Kontrolle der erfassten Zeiten'),
    ('urlaub', 'Urlaub & Abwesenheit', '/werkstatt/buero/abwesenheit', 'Abwesenheiten im Blick behalten'),
    ('belege', 'Belege & Freigaben', '/werkstatt/buero/belege', 'Erfassen, zur Freigabe vorlegen und Zahlung bearbeiten'),
    ('loehne', 'Löhne', '/werkstatt/buero/loehne', 'Vom Chef freigegebene Lohnläufe bearbeiten'),
    ('preise', 'Preise', '/werkstatt/buero/preise', 'Partner- und Privatpreise direkt nachschlagen'),
    ('mahnungen', 'Mahnungen', '/werkstatt/buero/mahnungen', 'Offene und fällige Mahnungen ansehen'),
    ('news', 'News', '/werkstatt/buero/news', 'Mitteilungen aus dem Betrieb'),
    ('bewerbungen', 'Bewerbungen', '/werkstatt/buero/bewerbungen', 'Bewerbungen ansehen und Einladungen vorbereiten'),
    ('lieferanten', 'Lieferanten', '/werkstatt/buero/lieferanten', 'Ansprechpartner und Kontaktdaten'),
    ('mailbox', 'E-Mails', '/admin/mail', 'Geschäftliches Postfach lesen'),
    ('mailbox', 'Postfach', '/werkstatt/buero/postfach', 'Nachrichten aus den Autohausportalen'),
    ('partnerzugaenge', 'Zugänge', '/werkstatt/buero/zugaenge', 'Freigegebene Lieferanten- und Autohauszugänge'),
    ('autohausportale', 'Autohausportale', '/werkstatt/buero/autohausportale', 'Bestehende Partnerportale öffnen'),
)


def categories(who):
    if who and who.get('actor') == 'admin':
        return set(CATEGORY_LABELS)
    return set((who or {}).get('office_categories', ()))


def navigation(who):
    allowed = categories(who)
    result = []
    for key, title, path, hint in NAVIGATION:
        if key in allowed or (path == '/werkstatt/buero' and allowed):
            if who and who.get('actor') == 'admin':
                path = {'/werkstatt/buero/belege': '/admin/buero/belege',
                        '/werkstatt/buero/loehne': '/admin/buero/loehne'}.get(path, path)
            result.append(dict(key=key, title=title, path=path, hint=hint))
    return result


def configure_office_endpoints():
    """No implicit administrative routes: every method is named explicitly."""
    read = frozenset(('GET', 'HEAD'))
    for key in OFFICE_ALLOWED_ENDPOINTS:
        OFFICE_ALLOWED_ENDPOINTS[key]['office_cockpit.index'] = read
    OFFICE_ALLOWED_ENDPOINTS['termine'].update({
        'office_planning.calendar': read,
        'office_planning.save_note': frozenset(('POST',)),
    })
    OFFICE_ALLOWED_ENDPOINTS['hol_bring']['office_planning.planning'] = read
    OFFICE_ALLOWED_ENDPOINTS['arbeitszeiten']['office_sections.arbeitszeit_pruefen'] = frozenset(('POST',))
    OFFICE_ALLOWED_ENDPOINTS['arbeitszeiten']['office_sections.arbeitszeit_korrigieren'] = frozenset(('POST',))
    OFFICE_ALLOWED_ENDPOINTS['arbeitszeiten']['assistent.personal_time'] = read
    OFFICE_ALLOWED_ENDPOINTS['urlaub'].update({
        'assistent.vacation_page': read,
        'assistent.vacation_summary': read,
        'assistent.vacation_apply': frozenset(('POST',)),
        'assistent.vacation_withdraw': frozenset(('POST',)),
    })
    for key, endpoint in (
        ('arbeitszeiten', 'arbeitszeiten'), ('urlaub', 'abwesenheit'),
        ('mahnungen', 'mahnungen'), ('news', 'news'), ('bewerbungen', 'bewerbungen'),
        ('lieferanten', 'lieferanten'), ('mailbox', 'postfach'),
        ('partnerzugaenge', 'zugaenge'), ('autohausportale', 'autohausportale'),
    ):
        OFFICE_ALLOWED_ENDPOINTS[key]['office_sections.' + endpoint] = (
            frozenset(('GET', 'HEAD', 'POST')) if endpoint == 'bewerbungen' else read)
    for endpoint in ('index', 'listing', 'message', 'attachment'):
        OFFICE_ALLOWED_ENDPOINTS['mailbox']['mailbox.' + endpoint] = read
    for endpoint in ('index', 'detail', 'original'):
        OFFICE_ALLOWED_ENDPOINTS['belege']['buero_belege.' + endpoint] = read
    for endpoint in ('upload', 'update', 'submit', 'start_payment', 'paid'):
        OFFICE_ALLOWED_ENDPOINTS['belege']['buero_belege.' + endpoint] = frozenset(('POST',))
    for endpoint in ('analyse', 'transfer', 'filing', 'status'):
        OFFICE_ALLOWED_ENDPOINTS['belege']['office_lexware.' + endpoint] = frozenset(('POST',))
    OFFICE_ALLOWED_ENDPOINTS['loehne'].update({
        'office_payroll.office': frozenset(('GET', 'HEAD', 'POST')),
        'office_payroll.office_original': read,
    })
    OFFICE_ALLOWED_ENDPOINTS['preise'].update({
        'office_prices.index': read,
        'office_prices.pdf': read,
        'office_prices.tyre_pdf': read,
    })


def register_office_cockpit(p):
    configure_office_endpoints()
    bp = Blueprint('office_cockpit', __name__)

    def current():
        if session.get('admin'):
            return dict(actor='admin', mitarbeiter_name='Werkstattleitung')
        if not session.get('assistent_mid'):
            return None
        return p.office_access.identity()

    @bp.get('/werkstatt/buero/login')
    def login():
        if current():
            return redirect('/werkstatt/buero')
        return render_template('buero_login.html')

    @bp.get('/werkstatt/buero')
    def index():
        who = current()
        if not who:
            return redirect(url_for('office_cockpit.login'))
        allowed = categories(who)
        day = dict(gruppen=[], termine=[], offene_hinweise=[], anzahl_fahrten=0, anzahl_termine=0)
        if allowed & {'termine', 'hol_bring'}:
            source = p.office_planning.today()
            day.update(datum_text=source['datum_text'])
            if 'hol_bring' in allowed:
                day.update(gruppen=source['gruppen'], offene_hinweise=source['offene_hinweise'],
                           anzahl_fahrten=source['anzahl_fahrten'])
            if 'termine' in allowed:
                day.update(termine=source['termine'], anzahl_termine=source['anzahl_termine'])
        documents = p.office_documents.counters(admin=bool(session.get('admin'))) if 'belege' in allowed else None
        time_count = (p.office_sections.daily_time_review_count() if 'arbeitszeiten' in allowed
                      and hasattr(p.office_sections, 'daily_time_review_count') else None)
        payroll_count = p.office_payroll.pending_payment_count(admin=bool(session.get('admin'))) if 'loehne' in allowed else None
        return render_template('buero_cockpit.html', office_identity=who, day=day,
                               office_cards=navigation(who), document_counts=documents,
                               daily_time_count=time_count, payroll_payment_count=payroll_count, active_nav='heute')

    @p.app.before_request
    def office_boundary():
        if session.get('admin') or not session.get('assistent_mid'):
            return None
        who = p.office_access.identity()
        if not who:
            return None
        g.office_identity = who
        if request.blueprint == 'mailbox':
            # The CEO inbox also contains private material. A general office
            # mailbox grant must never expose that account before approval.
            configured_user = str(p.get_werkstatt_imap_config().get('user', '')).strip().casefold()
            ceo_user = str(p.get_app_setting('OFFICE_CEO_MAIL_ADDRESS', '') or
                           p.app.config.get('OFFICE_CEO_MAIL_ADDRESS', '')).strip().casefold()
            try:
                ceo_mailbox_user = confirmed_ceo_mailbox_user(p, ceo_user).casefold()
            except InboxBlocked:
                ceo_mailbox_user = ''
            if not ceo_user or not ceo_mailbox_user or not configured_user or configured_user in (ceo_user, ceo_mailbox_user):
                abort(403, description='Das Geschäftsführer-Postfach ist dem Chefbereich vorbehalten. Für das Büro ist ein eigenes geschäftliches Postfach erforderlich.')
        if request.endpoint == 'assistent.page':
            return redirect('/werkstatt/buero')
        if request.blueprint == 'assistent' and request.endpoint not in ('assistent.login', 'assistent.logout'):
            if not p.office_access.allows(request.endpoint, request.method):
                abort(403, description='Dieser Bereich gehört nicht zum Bürozugang.')
        if request.blueprint in ('office_planning', 'office_sections', 'buero_belege', 'office_lexware', 'office_payroll', 'office_prices'):
            if not p.office_access.allows(request.endpoint, request.method):
                abort(403, description='Dieser Bürobereich ist für deinen Zugang nicht freigegeben.')
        return None

    @p.app.context_processor
    def office_context():
        who = current()
        return dict(buero_nav_items=navigation(who), office_mode=bool(who and who.get('actor') != 'admin'),
                    buero_preview=bool(session.get('admin')), buero_current_user=who)

    @p.app.after_request
    def office_private(response):
        if getattr(g, 'office_identity', None) or request.blueprint == 'office_cockpit':
            response.headers['Cache-Control'] = 'no-store'
            response.headers['Referrer-Policy'] = 'no-referrer'
            response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        return response

    p.app.register_blueprint(bp)
    return bp
