"""Loopback-only synthetic review server; outgoing sockets and providers disabled.

Run: python scripts/service_app_demo.py --port 5166
Every start gets a new temporary database. Nothing uses the workshop database.
"""
from argparse import ArgumentParser
from contextlib import closing
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path
import secrets
from unittest.mock import patch
from uuid import uuid4
from itsdangerous import URLSafeTimedSerializer

import smoke_test as isolated  # isolation MUST happen before importing the app
from flask import abort, redirect, render_template_string, session
from werkzeug.datastructures import FileStorage
import fitz

p = isolated.portal
p.app.config.update(TESTING=True, SERVICE_APP_ENABLED=True, SERVICE_ACCESS_ENABLED=True,
                    FLOTTENPORTAL_ENABLED=True, SERVICE_APP_DEMO=True,
                    SERVICE_APP_TEST_TRANSPORT_ENABLED=True)
p.app.secret_key = secrets.token_urlsafe(32)
p.app.config['SESSION_COOKIE_NAME'] = 'gaertner_service_local_demo'
p.service_app.signer = URLSafeTimedSerializer(p.app.secret_key, salt='service-vehicle-request-v1')
for name in ('lead_notice', 'notify_lead_workshop', 'sende_website_lead_benachrichtigung',
             'sende_autohaus_benachrichtigung_mail', 'sende_endkunden_mail',
             'notify_customer_whatsapp_fertig', 'schedule_change_backup'):
    patch.object(p, name).start()
p.init_db()
fleet = p.kundenflotten
company = fleet.create_company({'name': 'Beispiel Mobil GmbH', 'email': 'flotte@example.invalid'})
car = fleet.create_vehicle(company, {'kennzeichen': 'MOS DEMO 1', 'modell': 'Beispiel Kombi',
    'km': '48200', 'km_datum': date.today().isoformat()})
fleet.create_vehicle(company, {'kennzeichen': 'MOS DEMO 2', 'modell': 'Beispiel Transporter',
    'km': '72500', 'km_datum': date.today().isoformat()})
fleet.change_vehicle(company, car, 'fahrer', {'name': 'Alex Beispiel', 'email': 'alex@example.invalid',
    'telefon': '0000 000000', 'kanal': 'email', 'einwilligung': 'Synthetischer Testnachweis'})
password = secrets.token_urlsafe(24)
invitation = p.service_access.prepare_invitation({'name': 'Alex Beispiel', 'email': 'alex@example.invalid',
    'firma_id': company, 'fahrzeug_ids': [car], 'bestaetigt': '1', 'request_id': uuid4().hex})
p.service_access.accept(invitation['token'], password, password)
p.flottenportal.configure_access(company, {'benutzername': 'beispiel-flotte',
    'passwort': password, 'passwort_wiederholen': password})

@p.app.context_processor
def demo_context():
    if not session.get('csrf_token'):
        session['csrf_token'] = secrets.token_urlsafe(32)
    return {'service_demo': True, 'service_actor': p.service_app.principal(),
            'service_csrf': session['csrf_token']}

@p.app.get('/demo')
def demo():
    return render_template_string('''{% extends 'service_basis.html' %}{% block content %}
      <div class="sa-hero"><p class="sa-eyebrow">Lokale Vorschau</p><h1>Gärtner Service-App.</h1>
      <p>Eine Anfrage, ein Fahrzeug, ein gemeinsamer Ablauf. Wählen Sie eine Beispielrolle.</p></div>
      <div class="sa-grid"><a class="sa-card" href="/demo/fahrer"><h2>Fahrer</h2><p>Ein zugeordnetes Fahrzeug, Meldungen, Termine und Belege.</p></a>
      <a class="sa-card" href="/demo/fuhrpark"><h2>Fuhrparkleitung</h2><p>Zwei eigene Fahrzeuge und die Kostenfreigabe.</p></a>
      <a class="sa-card" href="/demo/werkstatt"><h2>Werkstatt</h2><p>Verantwortlichkeiten, Angebote und vorhandene Aufträge.</p></a></div>
      <p class="sa-note">Nur erfundene Daten. Bei jedem Neustart wird die Vorschau zurückgesetzt.</p>
      <a href="/demo/gast">Öffentlichen Einstieg ohne Anmeldung ansehen →</a>{% endblock %}''')

@p.app.get('/demo/<role>')
def demo_role(role):
    if role not in states:
        abort(404)
    session.clear()
    session.update(states[role])
    return redirect('/admin/service' if role == 'werkstatt' else '/service')


def login(path, fields):
    client = p.app.test_client()
    client.get(path)
    with client.session_transaction() as state:
        csrf = state['csrf_token']
    response = client.post(path, data={**fields, 'csrf_token': csrf})
    assert response.status_code == 303
    with client.session_transaction() as state:
        return dict(state)

states = {
    'fahrer': login('/service/anmelden', {'email': 'alex@example.invalid', 'passwort': password}),
    'fuhrpark': login('/flotte/anmelden', {'benutzername': 'beispiel-flotte', 'passwort': password}),
    'werkstatt': {'admin': True, 'csrf_token': secrets.token_urlsafe(32)},
    'gast': {},
}
with closing(p.get_db()) as db:
    lead_id = p.create_lead({'website': 'auto-lackierzentrum', 'kunde_name': 'Alex Beispiel',
        'kunde_email': 'alex@example.invalid', 'kontakt_telefon': '0000 000000',
        'fahrzeug': 'Beispiel Kombi', 'kennzeichen': 'MOS DEMO 1',
        'beschreibung': 'Kleine Beschädigung am Stoßfänger prüfen.',
        'portal_data': {'anliegen': 'reparaturanfrage'}})
    db.execute("UPDATE leads SET angebot_status='angebot_abgegeben', angebot_text=?, angebot_preis=? WHERE id=?",
               ('Stoßfänger prüfen und instand setzen – synthetisches Beispiel', '120,00 EUR netto', lead_id))
    db.commit()
actor = {'role': 'werkstatt', 'id': 'demo-admin'}
case_id = p.service_process.bind_case(lead_id=lead_id, company_id=company, vehicle_id=car, actor=actor)
view = p.service_process.view(case_id, actor)
p.service_process.save_parties(case_id, {'auftraggeber': {'name': 'Beispiel Mobil GmbH'},
    'freigebender': {'name': 'Beispiel Fuhrparkleitung', 'identity_id': p.flottenportal.access(company)['id']},
    'rechnungsempfaenger': {'name': 'Beispiel Mobil GmbH', 'strasse': 'Beispielstraße 8',
        'plz': '74821', 'ort': 'Mosbach', 'land': 'DE'}, 'zahler': {'name': 'Beispiel Mobil GmbH'},
    'fallart': 'firma', 'zahlungsweg': 'firmenrechnung', 'selbstbeteiligung': '',
    'selbstbeteiligung_quelle': ''}, actor, expected_revision=view['revision'])
p.service_process.sync_offer(case_id, actor)
fleet.change_vehicle(company, car, 'plan', {'art': 'service', 'titel': 'Inspektion laut Beispiel-Serviceplan',
    'faellig_datum': (date.today()+timedelta(days=10)).isoformat(), 'faellig_km': '49000',
    'quelle': 'Synthetische Serviceunterlage – keine Herstellervorgabe', 'request_id': uuid4().hex})
with fitz.open() as pdf:
    page = pdf.new_page()
    page.insert_text((45, 60), 'MUSTER - KEINE ECHTE RECHNUNG', fontsize=18)
    page.insert_text((45, 105), 'Beispielbeleg fuer die lokale Fahrzeugakte. Kein Zahlungsanspruch.')
    raw = pdf.tobytes()
p.flottenportal.upload(company, car, {'art': 'rechnung', 'titel': 'Beispielbeleg – nur Demo',
    'belegdatum': date.today().isoformat(), 'betrag': '0,00', 'sichtbar': '1', 'request_id': uuid4().hex},
    [FileStorage(stream=BytesIO(raw), filename='Musterbeleg-keine-Rechnung.pdf')], internal=True)

if __name__ == '__main__':
    parser = ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=5166)
    args = parser.parse_args()
    print(f'Local synthetic preview: http://127.0.0.1:{args.port}/demo', flush=True)
    p.app.run(host='127.0.0.1', port=args.port, debug=False, use_reloader=False)
