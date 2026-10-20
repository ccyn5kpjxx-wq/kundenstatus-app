# -*- coding: utf-8 -*-
"""Isolierter Integrationstest fuer Homepage-Anfragen und mehrere Lead-Bilder."""

from __future__ import annotations

import base64
from datetime import date, timedelta
from io import BytesIO
import pathlib
import sys

from unittest.mock import patch
from werkzeug.datastructures import MultiDict


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Reuse the shared bootstrap: disposable DB, hidden local secrets, network denied.
import smoke_test as isolated  # noqa: E402
portal = isolated.portal
TEMP_DIR = isolated.TEMP_DIR


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def check(label, condition):
    passed = bool(condition)
    print(f"[{'OK' if passed else 'FEHLER'}] {label}")
    return passed


def scalar(sql, params=()):
    db = portal.get_db()
    try:
        row = db.execute(sql, params).fetchone()
        return row[0] if row else None
    finally:
        db.close()



def reifenservice_checks():
    client = portal.app.test_client()
    selected = client.get('/anfrage?anliegen=auswahl')
    page = client.get('/anfrage?anliegen=reifenservice')
    text = page.get_data(as_text=True)
    with client.session_transaction() as session:
        csrf = session.get(portal.CSRF_FIELD_NAME)
    result = [
        check('Privatauswahl bietet Reifenservice statt Geschäftspartnergespräch', selected.status_code == 200 and 'Reifenservice &amp; Einlagerung' in selected.text and 'anliegen=gespraech' not in selected.text),
        check('Alter Fotoauswahl-Link bleibt kompatibel', client.get('/anfrage?anliegen=schaden').status_code == 200),
        check('Reifenformular zeigt Leistungen und unverbindlichen Terminwunsch', page.status_code == 200 and all(x in text for x in ('Radwechsel / Saisonwechsel', 'Reifenmontage', 'Räder auswuchten', 'Reifeneinlagerung', 'Neue Reifen anfragen', 'name="wunschdatum"', 'name="wunschzeit"', 'erst nach unserer Bestätigung', 'Fotos sind optional'))),
    ]
    def post(changes=(), files=()):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        data = MultiDict([(portal.CSRF_FIELD_NAME, csrf), ('anliegen','reifenservice'), ('name','Privater Reifen Testkunde'), ('email','reifen@example.invalid'), ('telefon',''), ('nachricht','Winterräder wechseln und den alten Satz einlagern'), ('reifen_leistung','radwechsel'), ('reifen_leistung','einlagerung'), ('reifen_leistung','radwechsel'), ('wunschdatum',(date.today()+timedelta(days=10)).isoformat()), ('wunschzeit','09:30')])
        for key,value in changes:
            data.setlist(key,value if isinstance(value,list) else [value])
        for key,value in files:
            data.add(key,value)
        return client.post('/anfrage',data=data,content_type='multipart/form-data')
    with patch.object(portal,'sende_website_lead_benachrichtigung'), patch.object(portal,'lead_notice'), patch.object(portal,'notify_lead_workshop'):
        before = scalar('SELECT COUNT(*) FROM auftraege')
        response = post()
        lead = portal.get_lead(int(scalar('SELECT MAX(id) FROM leads')))
        description = lead['beschreibung']
        result += [
            check('Reifenanfrage ohne Fahrzeug und Fotos wird gespeichert', response.status_code == 302 and lead['fahrzeug'] == '' and not portal.list_lead_dateien(lead['id'])),
            check('Mehrfachauswahl dedupliziert und Terminwunsch bleiben erhalten', description.count('Radwechsel / Saisonwechsel') == 1 and 'Reifeneinlagerung' in description and '09:30' in description and (date.today()+timedelta(days=10)).isoformat() in description),
            check('Folgeaktion ist persönliche Terminabstimmung ohne Werkstattauftrag', 'persönlich abstimmen' in lead['naechste_aktion'] and scalar('SELECT COUNT(*) FROM auftraege') == before),
        ]
        success = client.get(response.headers['Location'])
        result.append(check('Erfolg bestätigt keinen verbindlichen Termin', 'noch keine Termin-, Preis- oder Auftragsbestätigung' in success.text))
        for label,changes,error in [
            ('Leere Reifenleistung', [('reifen_leistung',[])], 'mindestens eine Reifenleistung'),
            ('Unbekannte Reifenleistung', [('reifen_leistung',['invalid'])], 'gültige Reifenleistungen'),
            ('Vergangenes Wunschdatum', [('wunschdatum',(date.today()-timedelta(days=1)).isoformat())], 'nicht in der Vergangenheit'),
            ('Ungültiges Wunschdatum', [('wunschdatum','2026-02-30')], 'gültiges Wunschdatum'),
            ('Geschäftspartnerart wird nicht eingeführt', [('anliegen','gespraech')], 'Anliegen auswählen'),
        ]:
            count = scalar('SELECT COUNT(*) FROM leads')
            bad = post(changes)
            result.append(check(label+' wird ohne Leadanlage abgewiesen', bad.status_code == 400 and error in bad.text and scalar('SELECT COUNT(*) FROM leads') == count))
            if 'Wunschdatum' in label:
                result.append(check(label+': Reifenwahl und Rückrufwunsch bleiben sichtbar', 'value="radwechsel" checked' in bad.text and 'value="09:30"' in bad.text))
        count = scalar('SELECT COUNT(*) FROM leads')
        no_csrf = post([(portal.CSRF_FIELD_NAME,'wrong')])
        result.append(check('Neue Reifenroute bleibt CSRF-geschützt', no_csrf.status_code == 400 and scalar('SELECT COUNT(*) FROM leads') == count))
        photo = post([('reifen_leistung',['pruefung'])], [('bilder',(BytesIO(PNG_1X1),'reifen-test.png'))])
        photo_lead = portal.get_lead(int(scalar('SELECT MAX(id) FROM leads')))
        documents = portal.list_lead_dateien(photo_lead['id'])
        result.append(check('Optionales Reifenfoto wird als Original gespeichert', photo.status_code == 302 and len(documents) == 1 and (portal.UPLOAD_DIR/documents[0]['stored_name']).read_bytes() == PNG_1X1))
    return result


def achsvermessung_checks():
    client = portal.app.test_client()
    page = client.get('/anfrage?anliegen=mechanikanfrage&mechanik_kategorie=achsvermessung')
    with client.session_transaction() as session:
        csrf = session.get(portal.CSRF_FIELD_NAME)
    checked = 'value="achsvermessung" checked'
    checks = [check('Achsvermessung ist im Direktformular vorausgewählt', page.status_code == 200 and checked in page.text)]
    invalid_get = client.get('/anfrage?anliegen=mechanikanfrage&mechanik_kategorie=unbekannt')
    other_get = client.get('/anfrage?anliegen=reifenservice&mechanik_kategorie=achsvermessung')
    checks += [
        check('Unbekannte GET-Kategorie wird nicht ausgewählt', invalid_get.status_code == 200 and 'value="unbekannt"' not in invalid_get.text and ' checked' not in invalid_get.text),
        check('Mechanikvorauswahl bleibt auf Mechanikanfragen begrenzt', other_get.status_code == 200 and 'name="mechanik_kategorie"' not in other_get.text),
    ]
    def post(categories, description='Fahrzeug zieht nach rechts', query=''):
        with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
            portal.PUBLIC_FORM_ATTEMPTS.clear()
        data = MultiDict([(portal.CSRF_FIELD_NAME, csrf), ('anliegen', 'mechanikanfrage'), ('name', 'Achsvermessung Testkunde'), ('telefon', '0171 1234567'), ('email', ''), ('nachricht', description)])
        data.setlist('mechanik_kategorie', categories)
        return client.post('/anfrage'+query, data=data)
    with patch.object(portal, 'sende_website_lead_benachrichtigung'), patch.object(portal, 'lead_notice'), patch.object(portal, 'notify_lead_workshop'):
        count = scalar('SELECT COUNT(*) FROM leads')
        bad = post(['achsvermessung'], '')
        checks.append(check('Fehlende Beschreibung bewahrt Achsvermessung ohne Leadanlage', bad.status_code == 400 and checked in bad.text and scalar('SELECT COUNT(*) FROM leads') == count))
        unknown = post(['unbekannt'])
        checks.append(check('Unbekannte POST-Kategorie wird ohne Leadanlage abgewiesen', unknown.status_code == 400 and scalar('SELECT COUNT(*) FROM leads') == count))
        before_orders = scalar('SELECT COUNT(*) FROM auftraege')
        good = post(['achsvermessung', 'achsvermessung'])
        lead = portal.get_lead(int(scalar('SELECT MAX(id) FROM leads')))
        checks.append(check('Achsvermessung wird genau einmal im Lead gespeichert', good.status_code == 302 and scalar('SELECT COUNT(*) FROM leads') == count+1 and lead['beschreibung'].count('Achsvermessung / Spureinstellung') == 1))
        checks.append(check('Achsvermessungsanfrage bucht keinen Werkstattauftrag', scalar('SELECT COUNT(*) FROM auftraege') == before_orders))
        changed = post(['bremsen'], query='?anliegen=mechanikanfrage&mechanik_kategorie=achsvermessung')
        changed_lead = portal.get_lead(int(scalar('SELECT MAX(id) FROM leads')))
        checks.append(check('Eigene POST-Auswahl hat Vorrang vor der URL-Vorauswahl', changed.status_code == 302 and 'Bremsen prüfen / wechseln' in changed_lead['beschreibung'] and 'Achsvermessung / Spureinstellung' not in changed_lead['beschreibung']))
    return checks


def klimaservice_checks():
    client = portal.app.test_client()
    page = client.get('/anfrage?anliegen=mechanikanfrage&mechanik_kategorie=klima')
    checked = 'value="klima" checked'
    checks = [check('Klimaservice ist im Direktformular vorausgewählt', page.status_code == 200 and checked in page.text)]
    with client.session_transaction() as session:
        csrf = session.get(portal.CSRF_FIELD_NAME)
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS.clear()
    data = {portal.CSRF_FIELD_NAME: csrf, 'anliegen': 'mechanikanfrage', 'mechanik_kategorie': 'klima', 'name': 'Klimaservice Testkunde', 'telefon': '0171 1234567', 'email': '', 'nachricht': ''}
    with patch.object(portal, 'sende_website_lead_benachrichtigung'), patch.object(portal, 'lead_notice'), patch.object(portal, 'notify_lead_workshop'):
        before = scalar('SELECT COUNT(*) FROM leads')
        invalid = client.post('/anfrage', data=data)
        checks.append(check('Klimaservice bleibt bei Fehlern ausgewählt und ohne Lead', invalid.status_code == 400 and checked in invalid.text and scalar('SELECT COUNT(*) FROM leads') == before))
        data['nachricht'] = 'Klimaanlage kühlt nicht wie gewohnt'
        response = client.post('/anfrage', data=data)
        lead = portal.get_lead(int(scalar('SELECT MAX(id) FROM leads')))
        checks.append(check('Klimaservice-Anfrage speichert genau einen korrekt zugeordneten Lead', response.status_code == 302 and scalar('SELECT COUNT(*) FROM leads') == before+1 and 'Klimaanlage' in lead['beschreibung'] and 'Achsvermessung / Spureinstellung' not in lead['beschreibung']))
    return checks


def main():
    portal.app.config["TESTING"] = True
    client = portal.app.test_client()
    checks = []

    bestand_id = portal.create_auftrag(
        "intern",
        kunde_name="Bestand Kunde",
        fahrzeug="Bestandsfahrzeug",
        kennzeichen="MOS-ALT 1",
        beschreibung="Nicht verändern",
    )
    bestand_vorher = dict(portal.get_auftrag(bestand_id))

    homepage = client.get("/homepage")
    homepage_html = homepage.get_data(as_text=True)
    auswahl = client.get("/anfrage?anliegen=schaden")
    auswahl_html = auswahl.get_data(as_text=True)
    lack = client.get("/anfrage?anliegen=lackanfrage")
    lack_html = lack.get_data(as_text=True)
    glas = client.get("/anfrage?anliegen=glasschaden")
    glas_html = glas.get_data(as_text=True)
    pflege = client.get("/anfrage?anliegen=fahrzeugpflege")
    pflege_html = pflege.get_data(as_text=True)
    mechanik = client.get("/anfrage?anliegen=mechanikanfrage")
    mechanik_html = mechanik.get_data(as_text=True)
    mietwagen = client.get("/anfrage?anliegen=mietwagenanfrage")
    mietwagen_html = mietwagen.get_data(as_text=True)
    weitere = client.get("/anfrage?anliegen=weitere_anfrage")
    weitere_html = weitere.get_data(as_text=True)
    dellen = client.get("/anfrage?anliegen=dellenreparatur")
    dellen_html = dellen.get_data(as_text=True)

    checks += [
        check("Homepage verlinkt neue Auswahl", "/anfrage?anliegen=auswahl" in homepage_html and "Anfrage mit Foto" in homepage_html),
        check("Alle Anfragewege sind auswählbar", auswahl.status_code == 200 and all(
            text in auswahl_html for text in (
                "Lackanfrage", "Glasschaden", "Reparaturanfrage", "Mechanik &amp; Wartung",
                "Fahrzeugpflege", "Mietwagen", "Fahrzeugcheck", "Leasingrückgabe-Check",
                "Weitere Anfrage", "Versicherungsschaden",
            )
        )),
        check("Versicherung bleibt eigener Prozess", 'href="/privat/schaden"' in auswahl_html),
        check("Kurzanfrage bleibt kurz", lack.status_code == 200 and all(
            marker in lack_html for marker in (
                "Nur das Nötigste", 'name="name"', 'name="telefon"',
                'name="nachricht"', 'name="bilder"', 'enctype="multipart/form-data"',
            )
        )),
        check("Kurzanfrage erlaubt mehrere Bilder", "Bilder hinzufügen" in lack_html and 'multiple' in lack_html),
        check("Glasschaden fragt FIN verpflichtend ab", glas.status_code == 200 and all(
            marker in glas_html for marker in (
                "Glasschaden melden", 'name="fin"', 'maxlength="17"', 'minlength="17"',
                'pattern="[A-HJ-NPR-Z0-9]{17}"', "Fahrzeugschein unter Feld E",
            )
        )),
        check("Fahrzeugpflege bietet Kategorien", pflege.status_code == 200 and all(
            marker in pflege_html for marker in (
                'name="pflege_kategorie"', "Innenaufbereitung", "Außenaufbereitung",
                "1-Step-Politur", "2-Step-Politur", "Komplettaufbereitung",
            )
        )),
        check("Mechanik bietet Wartungs- und Problemkategorien", mechanik.status_code == 200 and all(
            marker in mechanik_html for marker in (
                'name="mechanik_kategorie"', "Bremsen prüfen / wechseln", "Ölwechsel",
                "Inspektion / Wartung", "Motor / Antrieb", "Elektrik / Batterie",
                "Sonstiges Mechanikproblem",
            )
        )),
        check("Mietwagen zeigt den auswählbaren Fuhrpark", mietwagen.status_code == 200 and all(
            marker in mietwagen_html for marker in (
                'name="mietwagen_fahrzeug"', "Hyundai i10", "Hyundai KONA N Line X",
                "Fiat Doblò", "Fiat Doblò Maxi", "39 € / Tag", "59 € / Tag",
                "i10_symbolfoto.jpg", "kona_symbolfoto.jpg", "doblo_symbolfoto.jpg",
                "doblo_maxi_symbolfoto.jpg", "Noch unsicher oder anderer Fahrzeugwunsch",
                'name="mietbeginn"', 'name="mietende"', 'name="nachricht"', 'name="bilder"',
            )
        )),
        check("Weitere Anfrage deckt übrige Kategorien ab", weitere.status_code == 200 and all(
            marker in weitere_html for marker in (
                'name="weitere_anfrage_art"', "Werkstatttermin", "Farbton- und Lackberatung",
                "Felgenservice", "Dellenreparatur", "Allgemeine Beratung",
            )
        )),
        check("Normale Anfrage erlaubt mehrere Bilder", dellen.status_code == 200 and all(
            marker in dellen_html for marker in (
                'name="bilder"', 'multiple', 'enctype="multipart/form-data"', "Bis zu 5 Bilder",
            )
        )),
    ]

    with client.session_transaction() as session:
        csrf_token = session.get(portal.CSRF_FIELD_NAME)
    response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "lackanfrage"),
            ("besichtigungsart", "werkstatt"),
            ("name", "Kurzanfrage Kunde"),
            ("telefon", "0171 1234567"),
            ("email", ""),
            ("fahrzeug", "VW Golf"),
            ("nachricht", "Kratzer an der hinteren Tür"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "schaden-vorne.png")),
            ("bilder", (BytesIO(PNG_1X1), "schaden-seite.png")),
            ("bilder", (BytesIO(PNG_1X1), "schaden-detail.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    lead = portal.get_lead(lead_id) if lead_id else None
    lead_dateien = portal.list_lead_dateien(lead_id) if lead_id else []
    upload_paths = [portal.UPLOAD_DIR / item["stored_name"] for item in lead_dateien]
    checks += [
        check("Kurzanfrage erzeugt Lead", response.status_code == 302 and lead and lead["quelle"] == "website"),
        check("Anliegen eindeutig gespeichert", lead and "Lackanfrage" in lead["beschreibung"]),
        check("Drei Bilder sicher gespeichert", len(lead_dateien) == 3 and all(
            path.is_file() and path.read_bytes() == PNG_1X1 for path in upload_paths
        )),
        check("Bilder starten keine OCR", lead and not lead.get("ki_analyse_text")),
        check("Lead ist dem Werkstattbereich zugeordnet", lead and
              lead["website"] == "auto-lackierzentrum" and lead["herkunft_typ"] == "website_formular"),
    ]

    glas_ohne_fin = client.post(
        "/anfrage",
        data={
            portal.CSRF_FIELD_NAME: csrf_token,
            "anliegen": "glasschaden",
            "name": "Glaskunde Ohne Fin",
            "telefon": "0171 9990011",
            "email": "",
            "fahrzeug": "VW Golf",
            "fin": "",
            "nachricht": "Steinschlag in der Frontscheibe",
            "website": "",
        },
        follow_redirects=False,
    )
    glas_response = client.post(
        "/anfrage",
        data={
            portal.CSRF_FIELD_NAME: csrf_token,
            "anliegen": "glasschaden",
            "name": "Glaskunde Mit Fin",
            "telefon": "0171 9990022",
            "email": "",
            "fahrzeug": "VW Golf",
            "fin": "WVWZZZ1JZXW000001",
            "nachricht": "Steinschlag in der Frontscheibe",
            "website": "",
        },
        follow_redirects=False,
    )
    glas_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    glas_lead = portal.get_lead(glas_lead_id) if glas_lead_id else None
    checks += [
        check("Glasschaden ohne FIN wird abgewiesen", glas_ohne_fin.status_code == 400
              and "17-stellige Fahrzeug-Identifizierungsnummer" in glas_ohne_fin.get_data(as_text=True)),
        check("Glasschaden mit FIN erzeugt Lead", glas_response.status_code == 302 and glas_lead_id > lead_id),
        check("FIN steht eindeutig im Lead", glas_lead and all(
            text in glas_lead["beschreibung"] for text in ("Glasschaden", "FIN: WVWZZZ1JZXW000001")
        )),
        check("Glasschaden setzt passende Folgeaktion", glas_lead
              and "Scheibe über FIN zuordnen" in glas_lead["naechste_aktion"]),
    ]
    with portal.PUBLIC_FORM_ATTEMPTS_LOCK:
        portal.PUBLIC_FORM_ATTEMPTS.clear()

    pflege_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "fahrzeugpflege"),
            ("name", "Pflegeanfrage Kunde"),
            ("telefon", "0171 2223344"),
            ("email", ""),
            ("fahrzeug", "Audi A4"),
            ("pflege_kategorie", "innen"),
            ("pflege_kategorie", "2-step"),
            ("nachricht", "Innenraum stark verschmutzt und feine Kratzer im Lack"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "pflege-innen.png")),
            ("bilder", (BytesIO(PNG_1X1), "pflege-lack.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    pflege_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    pflege_lead = portal.get_lead(pflege_lead_id) if pflege_lead_id else None
    pflege_dateien = portal.list_lead_dateien(pflege_lead_id) if pflege_lead_id else []
    checks += [
        check("Fahrzeugpflege erzeugt Lead", pflege_response.status_code == 302 and pflege_lead_id > lead_id),
        check("Pflegekategorien stehen im Lead", pflege_lead and all(
            text in pflege_lead["beschreibung"] for text in ("Innenaufbereitung", "2-Step-Politur")
        )),
        check("Fahrzeugpflege speichert mehrere Bilder", len(pflege_dateien) == 2),
    ]

    mechanik_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "mechanikanfrage"),
            ("name", "Mechanikanfrage Kunde"),
            ("telefon", "0171 7778899"),
            ("email", ""),
            ("fahrzeug", "BMW 320d"),
            ("mechanik_kategorie", "bremsen"),
            ("mechanik_kategorie", "oelwechsel"),
            ("nachricht", "Bremsen quietschen und der nächste Ölwechsel ist fällig"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "bremse-vorne.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    mechanik_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    mechanik_lead = portal.get_lead(mechanik_lead_id) if mechanik_lead_id else None
    mechanik_dateien = portal.list_lead_dateien(mechanik_lead_id) if mechanik_lead_id else []
    checks += [
        check("Mechanikanfrage erzeugt Werkstatt-Lead", mechanik_response.status_code == 302 and mechanik_lead_id > pflege_lead_id),
        check("Mechanikkategorien stehen im Lead", mechanik_lead and all(
            text in mechanik_lead["beschreibung"] for text in ("Bremsen prüfen / wechseln", "Ölwechsel")
        )),
        check("Mechanikanfrage speichert Bild", len(mechanik_dateien) == 1),
    ]

    mietbeginn = (date.today() + timedelta(days=7)).isoformat()
    mietende = (date.today() + timedelta(days=10)).isoformat()
    mietwagen_response = client.post(
        "/anfrage",
        data={
            portal.CSRF_FIELD_NAME: csrf_token,
            "anliegen": "mietwagenanfrage",
            "name": "Mietwagenanfrage Kunde",
            "telefon": "0171 3334455",
            "email": "",
            "mietwagen_fahrzeug": "hyundai-kona",
            "fahrzeug": "",
            "mietbeginn": mietbeginn,
            "mietende": mietende,
            "nachricht": "Mietwagen für einen Werkstattaufenthalt benötigt",
            "website": "",
        },
        follow_redirects=False,
    )
    mietwagen_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    mietwagen_lead = portal.get_lead(mietwagen_lead_id) if mietwagen_lead_id else None
    checks += [
        check("Mietwagenanfrage erzeugt Werkstatt-Lead", mietwagen_response.status_code == 302 and mietwagen_lead_id > mechanik_lead_id),
        check("Mietfahrzeug, Kurzprofil und Tarif stehen im Lead", mietwagen_lead and all(
            text in mietwagen_lead["beschreibung"] for text in (
                "Hyundai KONA N Line X", "Kompakt-SUV · Automatik", "59 € / Tag",
                "Ab 3 Tagen 49 € / Tag", "Jetzt verfügbar",
            )
        )),
        check("Mietzeitraum steht im Lead", mietwagen_lead and all(
            text in mietwagen_lead["beschreibung"] for text in (mietbeginn, mietende, "Mietwagenanfrage")
        )),
    ]

    normale_response = client.post(
        "/anfrage",
        data=MultiDict([
            (portal.CSRF_FIELD_NAME, csrf_token),
            ("anliegen", "dellenreparatur"),
            ("name", "Dellenanfrage Kunde"),
            ("telefon", "0171 7654321"),
            ("email", ""),
            ("fahrzeug", "Seat Leon"),
            ("wunschdatum", ""),
            ("nachricht", "Delle in der Beifahrertür"),
            ("website", ""),
            ("bilder", (BytesIO(PNG_1X1), "delle-gesamt.png")),
            ("bilder", (BytesIO(PNG_1X1), "delle-detail.png")),
        ]),
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    normale_lead_id = int(scalar("SELECT MAX(id) FROM leads") or 0)
    normale_dateien = portal.list_lead_dateien(normale_lead_id) if normale_lead_id else []
    checks += [
        check("Normale Anfrage mit Bildern erzeugt Lead", normale_response.status_code == 302 and normale_lead_id > lead_id),
        check("Normale Anfrage speichert beide Bilder", len(normale_dateien) == 2),
    ]

    with client.session_transaction() as session:
        session["admin"] = True
        admin_csrf = session.get(portal.CSRF_FIELD_NAME)
    detail = client.get(f"/admin/leads/{lead_id}")
    datei_antworten = [
        client.get(f"/admin/leads/{lead_id}/anhaenge/{item['id']}") for item in lead_dateien
    ]
    detail_html = detail.get_data(as_text=True)
    cockpit = client.get("/admin/leads")
    cockpit_html = cockpit.get_data(as_text=True)
    checks += [
        check("Alle Bilder im Lead sichtbar", detail.status_code == 200 and all(
            item["original_name"] in detail_html for item in lead_dateien
        )),
        check("Geschützte Bild-Routen liefern Originale", all(
            antwort.status_code == 200 and antwort.data == PNG_1X1 for antwort in datei_antworten
        )),
        check("Neue Anfragearten erscheinen im Lead-Cockpit", cockpit.status_code == 200 and all(
            name in cockpit_html for name in (
                "Glaskunde Mit Fin", "Pflegeanfrage Kunde", "Mechanikanfrage Kunde",
                "Mietwagenanfrage Kunde",
            )
        )),
    ]

    umwandlung = client.post(
        f"/admin/leads/{lead_id}/auftrag",
        data={portal.CSRF_FIELD_NAME: admin_csrf},
        follow_redirects=False,
    )
    lead_nachher = portal.get_lead(lead_id)
    auftrag_id = int(lead_nachher["auftrag_id"] or 0)
    bestand_nachher = dict(portal.get_auftrag(bestand_id))
    lead_dateien_nachher = portal.list_lead_dateien(lead_id)
    checks += [
        check("Lead bleibt bis zur Angebotsannahme ohne Auftrag", umwandlung.status_code == 302
              and auftrag_id == 0 and lead_nachher["kunden_status_token"]),
        check("Bildhistorie bleibt auch am Lead erhalten", len(lead_dateien_nachher) == 3),
        check("Lead-Bilder bleiben bis zur Annahme sicher gespeichert",
              all(path.is_file() for path in upload_paths)),
        check("Bestehender Auftrag bleibt unverändert", bestand_nachher == bestand_vorher),
    ]

    checks += reifenservice_checks()
    checks += achsvermessung_checks()
    checks += klimaservice_checks()
    print(f"Temporäre Testdaten: {TEMP_DIR}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
