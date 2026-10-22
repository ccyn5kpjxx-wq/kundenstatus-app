"""Follow both actual card links through isolated, persistent form submissions."""
from __future__ import annotations

import base64
import json
import sys
from datetime import date
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import smoke_test as isolated
portal = isolated.portal
PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=')
passed = 0

class Cards(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.cards = {}
        self.current = None
        self.heading = False
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'article' and 'data-current-card' in attrs:
            self.current = {'heading': '', 'links': [], 'images': [], 'paragraphs': 0}
            self.cards[attrs['data-card-title']] = self.current
        if self.current is not None:
            if tag == 'h3': self.heading = True
            if tag == 'br' and self.heading: self.current['heading'] += ' '
            if tag == 'a': self.current['links'].append(attrs['href'])
            if tag == 'img': self.current['images'].append(attrs['src'])
            if tag == 'p': self.current['paragraphs'] += 1

    def handle_endtag(self, tag):
        if tag == 'h3': self.heading = False
        if tag == 'article': self.current = None

    def handle_data(self, value):
        if self.current is not None and self.heading: self.current['heading'] += value

def check(label, condition):
    global passed
    assert condition, label
    passed += 1
    print('[OK] ' + label)

def scalar(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return row[0] if row else None
    finally: db.close()

def clear_limits():
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK: portal.PUBLIC_FORM_ATTEMPTS.clear()

def main():
    portal.app.config.update(TESTING=True, SESSION_COOKIE_SECURE=False)
    portal.init_db()
    client = portal.app.test_client()
    with patch.object(portal, 'PORTAL_BASE_URL', ''):
        home = client.get('/homepage')
    cards = Cards(home.text).cards
    check('Existing five cards and exactly two additions are present', len(cards) == 7)
    damage = cards['Versicherungsschaden']
    paint = cards['Lackierarbeiten']
    check('Insurance card keeps the approved short wording', damage['heading'] == 'Versicherungsschaden? Wir kümmern uns.' and damage['paragraphs'] == 0)
    check('Paint card keeps only its heading', paint['heading'] == 'Lackierarbeiten' and paint['paragraphs'] == 0)
    check('Each new card has one real action link', len(damage['links']) == len(paint['links']) == 1)
    damage_url, paint_url = damage['links'][0], paint['links'][0]
    check('Insurance action goes directly to public intake', damage_url == '/privat/schaden')
    check('Paint action preselects paint enquiry', paint_url == '/anfrage?anliegen=lackanfrage#kurzanfrage')
    for card in (damage, paint):
        image = client.get(card['images'][0])
        check('New card photo is available as optimized WebP', image.status_code == 200 and image.mimetype == 'image/webp' and len(image.data) < 160_000)
    with patch.object(portal, 'PORTAL_BASE_URL', 'https://portal.example.invalid'):
        remote_cards = Cards(client.get('/homepage').text).cards
    check('Separated homepage uses persistent insurance portal', remote_cards['Versicherungsschaden']['links'] == ['https://portal.example.invalid/privat/schaden'])
    check('Separated homepage uses persistent paint portal', remote_cards['Lackierarbeiten']['links'] == ['https://portal.example.invalid/anfrage?anliegen=lackanfrage#kurzanfrage'])

    paint_form = client.get(paint_url)
    check('Paint form opens without login and is connected to POST', paint_form.status_code == 200 and 'name="anliegen" value="lackanfrage"' in paint_form.text and 'method="post"' in paint_form.text.lower())
    with client.session_transaction() as session: csrf = session[portal.CSRF_FIELD_NAME]
    paint_data = {portal.CSRF_FIELD_NAME: csrf, 'anliegen':'lackanfrage', 'name':'Karten Lack Testkunde', 'telefon':'0171 1234567', 'email':'lack@example.invalid', 'nachricht':'Bitte Lackierung des hinteren Stossfaengers anbieten.'}
    before_leads = scalar('SELECT COUNT(*) FROM leads')
    before_orders = scalar('SELECT COUNT(*) FROM auftraege')
    with patch.object(portal, 'sende_website_lead_benachrichtigung'), patch.object(portal, 'lead_notice'), patch.object(portal, 'notify_lead_workshop'):
        clear_limits()
        invalid = client.post('/anfrage', data={**paint_data, 'name':'', 'telefon':'', 'email':''})
        check('Missing paint contact is rejected without persistence', invalid.status_code == 400 and scalar('SELECT COUNT(*) FROM leads') == before_leads)
        clear_limits()
        bad_csrf = client.post('/anfrage', data={**paint_data, portal.CSRF_FIELD_NAME:'invalid'})
        check('Paint submission rejects invalid CSRF', bad_csrf.status_code == 400 and scalar('SELECT COUNT(*) FROM leads') == before_leads)
        clear_limits()
        paint_data['bilder'] = (BytesIO(PNG), 'lackschaden.png')
        submitted = client.post('/anfrage', data=paint_data, content_type='multipart/form-data')
        check('Paint enquiry redirects only after saving one lead', submitted.status_code == 302 and scalar('SELECT COUNT(*) FROM leads') == before_leads + 1)
        lead_id = int(scalar('SELECT MAX(id) FROM leads'))
        lead = portal.get_lead(lead_id)
        check('Paint enquiry preserves name, phone and description', lead['kunde_name'] == 'Karten Lack Testkunde' and '1234567' in lead['kontakt_telefon'] and 'Stossfaengers' in lead['beschreibung'])
        files = portal.list_lead_dateien(lead_id)
        check('Paint photo is saved intact with the enquiry', len(files) == 1 and (portal.UPLOAD_DIR/files[0]['stored_name']).read_bytes() == PNG)
        check('Paint enquiry is not prematurely turned into a workshop order', scalar('SELECT COUNT(*) FROM auftraege') == before_orders)
        success = client.get(submitted.headers['Location'])
        check('Paint enquiry displays a clear confirmation', success.status_code == 200 and 'Ihre Anfrage ist angekommen.' in success.text)

    damage_form = client.get(damage_url)
    check('Insurance intake opens without login and has a POST action', damage_form.status_code == 200 and 'action="/privat/schaden"' in damage_form.text and 'method="post"' in damage_form.text.lower())
    check('Insurance form is not cached', 'no-store' in damage_form.headers.get('Cache-Control',''))
    check('Insurance form directs customers to the current workshop address', 'Alte Neckarelzer Straße 18/1' in damage_form.text and 'Binauer Höhe' not in damage_form.text)
    keys = portal.schadenaufnahme_session_keys(public_mode=True)
    with client.session_transaction() as session:
        csrf, form_token = session[portal.CSRF_FIELD_NAME], session[keys['form']]
    data = {portal.CSRF_FIELD_NAME:csrf, 'schadenaufnahme_form_token':form_token,
        'schadenart':'teilkasko', 'kunde_name':'Karten Unfall Testkunde',
        'kunde_strasse':'Teststrasse 12', 'kunde_plz':'74821', 'kunde_ort':'Mosbach',
        'kontakt_telefon':'0171 2345678', 'kunde_email':'unfall@example.invalid',
        'kontaktweg':'telefon', 'kennzeichen':'MOS TEST 42', 'fahrzeug':'Testfahrzeug',
        'unfall_datum':date.today().isoformat(), 'unfall_zeit':'11:20', 'unfall_ort':'Testparkplatz',
        'beschreibung':'Synthetischer Lack- und Karosserieschaden fuer den Formulartest.',
        'versicherung_name':'Testversicherung', 'versicherung_police':'TEST-POLICE',
        'schaden_nummer':'TEST-SCHADEN', 'mobilitaet':'nein', 'datenschutz_bestaetigt':'1'}
    clear_limits()
    invalid = client.post(damage_url, data={**data,'kontakt_telefon':'','kunde_email':''})
    check('Missing insurance contact is rejected without a new case', invalid.status_code == 400 and scalar('SELECT COUNT(*) FROM auftraege') == before_orders)
    clear_limits()
    invalid_csrf = client.post(damage_url, data={**data,portal.CSRF_FIELD_NAME:'invalid'})
    check('Invalid insurance CSRF returns to intake without creating a case', invalid_csrf.status_code == 302 and invalid_csrf.headers['Location'].endswith('/privat/schaden') and scalar('SELECT COUNT(*) FROM auftraege') == before_orders)
    client.get(damage_url)
    with client.session_transaction() as session:
        data[portal.CSRF_FIELD_NAME] = session[portal.CSRF_FIELD_NAME]
        data['schadenaufnahme_form_token'] = session[keys['form']]
    clear_limits()
    data['dateien'] = (BytesIO(PNG), 'unfallschaden.png')
    created = client.post(damage_url, data=data, content_type='multipart/form-data')
    check('Insurance intake saves one case and redirects to confirmation', created.status_code == 302 and scalar('SELECT COUNT(*) FROM auftraege') == before_orders+1)
    case_id = int(scalar('SELECT MAX(id) FROM auftraege'))
    case = portal.get_auftrag(case_id)
    intake = json.loads(case['schaden_aufnahme_json'])
    check('Insurance case preserves customer and contact data', case['quelle'] == 'privat' and case['kunde_name'] == 'Karten Unfall Testkunde' and intake['kontakt_telefon'] == '0171 2345678')
    check('Insurance photo is attached to its case', scalar('SELECT COUNT(*) FROM dateien WHERE auftrag_id=?',(case_id,)) == 1)
    check('Customer status is protected pending contact verification', not case.get('kunden_status_aktiv') and not case.get('kunden_status_url'))
    confirmation = client.get(created.headers['Location'])
    check('Insurance confirmation displays the saved submission', confirmation.status_code == 200 and 'Ihre Schadenmeldung ist angekommen' in confirmation.text and 'Karten Unfall Testkunde' in confirmation.text)
    check('Insurance confirmation also shows the current workshop address', 'Alte Neckarelzer Straße 18/1' in confirmation.text and 'Binauer Höhe' not in confirmation.text)
    other_client = portal.app.test_client()
    foreign = other_client.get(created.headers['Location'])
    check('Another browser cannot read the saved case through its confirmation URL', 'Karten Unfall Testkunde' not in foreign.text and 'MOS TEST 42' not in foreign.text)
    clear_limits()
    replay_data = {k:v for k,v in data.items() if k != 'dateien'}
    replay = client.post(damage_url, data=replay_data)
    check('Replaying the insurance form cannot create a second case', replay.status_code == 400 and scalar('SELECT COUNT(*) FROM auftraege') == before_orders+1)
    print(f'{passed} card and submission checks passed; disposable database: {isolated.TEMP_DIR}')

if __name__ == '__main__': main()
