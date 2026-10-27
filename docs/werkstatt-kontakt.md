# Öffentlicher Werkstattkontakt

`werkstatt_kontakt.py` hält die aktuelle Besuchsadresse und allgemeine
Telefonnummer. Flask stellt sie als `werkstatt_oeffentlich` bereit; Kundenstatus,
JSON-LD, Anrufbuttons und neu verfasste Werkstattmails verwenden denselben Stand.

Besuchsadresse: Alte Neckarelzer Straße 18/1, 74821 Mosbach.

Die Telefonnummer **06261 1809260** ist am 27.10.2026 anhand der Visitenkarte
vom Nutzer für beide Homepages bestätigt worden, einschließlich Autovermietung MOS.

Die allgemeine Telefonnummer ersetzt weder WhatsApp noch Margita Höfers
persönliche Mobilnummer. Der bestehende Google-Unternehmensprofil-Link und seine
Identität bleiben erhalten. Das Anfahrtsziel nutzt die aktuelle Besuchsadresse.
Eine Änderung dieser Dateien aktualisiert nicht das Google-Unternehmensprofil.

## Abgrenzung

Keine gespeicherten Original-PDFs, alten Nachrichten, zitierten Mailtexte,
Rechnungsdaten oder Vertrags-Snapshots werden geändert. Die registrierte
Geschäftsanschrift in `mos_company_identity.py`, gespeicherte Quotes mit
`lessor_address`/Vermieterkontakt sowie ihre Hashes bleiben unverändert. Die dort
noch enthaltene Binauer Höhe ist eine getrennte Vertragsangabe. Es gibt keine
Migration und kein neues Pickup-Feld im Vertrags-Snapshot.

MOS-Einstieg, Quote-/Statusseiten, aktuelle Checkout-Wegbeschreibung und die
Textbestätigung nennen getrennt die **aktuelle Abholadresse** für die persönliche
Schlüsselübergabe. Das gilt auch beim Öffnen bestehender Buchungen, ohne deren
Vertragswerte zu ändern. Originale Vertrags-PDFs bleiben bytegleich abrufbar.
Die statische Marketingseite `static/mietwagen_vorschau/index.html`, die sowohl
`rental_app.py` als auch die Portal-Vorschau ausliefern, enthält denselben
bestätigten Standort und dieselbe Telefonnummer; ein Test prüft beide Einstiege.
Die vorhandenen PDF-Generatoren für Lackierauftrag/Abtretung und Mietvertrag in
`app.py` bleiben unverändert.

## Prüfung und Veröffentlichung

Isolierte Tests (temporäre Datenbank, externe Verbindungen gesperrt):

```powershell
python scripts/test_werkstatt_kontakt.py
python scripts/test_homepage.py
python scripts/test_mail_location.py
python scripts/test_mos_public_booking.py
python scripts/test_mos_contract_deposit.py
```

Die Kontaktprüfung erfasst echte öffentliche Routen, das strukturierte
Adress-/Telefonpaar, Anfahrt und den Erhalt von Mitarbeiter-, WhatsApp- und
Google-Profilkontakten. Die Mailprüfung schützt insbesondere Anhänge und alte
zitierte Nachrichten vor Änderungen.

Vor Livegang gelten `docs/portal-deploy-koordination.md` und der aktuelle
TomorrowWorks-Deploymentclaim: frischen Projektkontext prüfen, den erforderlichen
Neustart ankündigen, Worker/Cron pausieren, Webbuild und Health bestätigen,
Hintergrunddienste einzeln fortsetzen. CHECK24-Abrufstatus nach dem Wechsel
prüfen. Auto-Deploy, Dienstkonfiguration und Zeitpläne nicht ändern.
