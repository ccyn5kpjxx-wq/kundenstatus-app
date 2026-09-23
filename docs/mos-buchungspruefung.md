# Autovermietung MOS: Prüfung und lokaler Entwurf

> Historischer Prüfbericht. Aktueller Beschluss vom 23.09.2026: Direktbuchung ausschließlich für Hyundai i10 und Hyundai KONA. C3 bleibt Anfrage; Fiat ist ausgeschlossen. Drei-Fahrzeug-Angaben und C3-Tests unten beschreiben den damaligen Stand bzw. den isolierten Altprototyp und sind keine aktuelle Flottenkonfiguration. Aktueller Test-Starter: [Öffentlicher Testablauf](mos-public-test.md); Freigaben: [Launch-Paket](mos-launch-package.md).

Stand: 22.09.2026. TomorrowWorks-Projekt 2, Aufgabe 63.
Basis: `origin/main`, Commit `8df1240`; Branch `feature/mos-mietanfrage-klarheit`.
Keine Veröffentlichung, keine produktive Anfrage und keine Zahlung ausgelöst.

## Ergebnis

Die öffentliche Website ist https://autovermietung-mos.de/ (kanonisch mit `www`).
`rental_app.py` liefert `static/mietwagen_vorschau/index.html` aus; die Haupt-App
bietet dieselben Dateien unter `/mietwagen-vorschau/` an.
Die Seite kann derzeit unverbindliche Anfragen erfassen. Eine echte Sofortbuchung
mit digitaler Zahlung lässt sich mit der vorhandenen Verbindung noch nicht
belastbar aktivieren. Die interne Mietverwaltung bietet dafür wiederverwendbare
Grundlagen, aber noch keinen vollständigen öffentlichen Buchungsabschluss.

## Tatsächlicher Datenfluss

1. Die MOS-Seite enthält statische Modellnamen, Tagespreise und Tarifstaffeln im HTML.
   Der Browser berechnet einen unverbindlichen Preis ohne Kaution.
2. `starteAnfrage()` sendet JSON an `https://kundenstatus-app.onrender.com/api/leads`.
   Fahrzeug wird als Modellname, Zeitraum und Preis als Angaben/Beschreibung übertragen.
3. `api_public_lead()` erstellt einen allgemeinen Lead (`website_formular`,
   `website=autovermietung-mos`) und stößt bestehende Anfragebenachrichtigungen an.
   Die Referenz bestätigt den Eingang. Es entstehen dabei weder ein Mietvorgang
   noch eine gesperrte Fahrzeugverfügbarkeit oder ein Zahlungsvorgang.
4. Ein anderer, vorhandener Weg `/mietwagen` arbeitet mit `mietwagen_anfragen` und
   `mietfahrzeug_id`. Dieser Weg ist nicht das POST-Ziel des MOS-Formulars.
   `admin_mietanfrage_uebernehmen()` prüft dort atomar die Belegung und erzeugt
   einen `mietvorgaenge`-Eintrag. Ein MOS-Lead wird dadurch nicht automatisch übernommen.

Relevante Implementierung in `app.py`: `api_public_lead`,
`mietwagen_public_fahrzeuge`, `mietfahrzeug_zeitraum_frei_db`,
`admin_mietanfrage_uebernehmen`, `mietvertrag_miettage`, `mietvertrag_kosten`.

## Fahrzeuge, Preise und Verwaltung

| Öffentlich beworbenes Fahrzeug | Vorhandener Richtpreis | Darstellung |
| --- | --- | --- |
| Hyundai KONA N Line X | 59 €/Tag; ab 3 Tagen 49 €/Tag | Anfrage auswählbar |
| Hyundai i10 | 39 €/Tag; Werkstattkunden ab 29 €/Tag | Anfrage auswählbar; Sondertarif wird nicht automatisch gerechnet |
| Citroën C3, 2021 | 39 €/Tag | Anfrage auswählbar |
| Fiat Doblò | 55 €/Tag; ab 3 Tagen 45 €/Tag | Noch nicht anfragbar |
| Fiat Doblò Maxi | 59 €/Tag; ab 3 Tagen 49 €/Tag | Noch nicht anfragbar |

Alle oben genannten Beträge sind bestehende öffentliche Angaben inklusive MwSt.,
keine in diesem Auftrag neu festgelegten Preise. 150 km pro Miettag inklusive,
Mehrkilometer 0,25 €/km. Auto-Abo: individuelle Anfrage für 3, 6 oder 12 Monate;
184 Modellfamilien im Wunschkatalog sind kein verfügbarer Mietbestand.

Intern existieren `mietfahrzeuge`, `mietvorgaenge`, Fahrzeugbilder, Kalender,
Wartungs-/Inaktivstatus, Rückgabe, Mietvertragsversionen, digitale Unterschrift,
PDF und Versand. Die aktuelle Belegungslogik arbeitet mit Tagen und blockiert
überschneidende Zeiträume einschließlich des Rückgabetags. Sie ist kein
zeitgenauer öffentlicher Verfügbarkeitsdienst. Produktive Bestandsdaten wurden
bei dieser Prüfung nicht ausgelesen; tatsächliche freie Fahrzeuge sind unbestätigt.

Mietvertragsfelder enthalten editierbare Vorgabewerte, etwa 500 € Kaution. Solche
Defaults sind keine bestätigten Geschäftsbedingungen für jeden öffentlichen Tarif.
Die Vertragsfreigabe ist gesondert konfigurierbar. In der Vermietungs-App fehlen
Checkout, Zahlungsanbieter-Anbindung, Zahlungs-Webhooks und ein belastbarer
Zahlungsstatus. Zahlungsfunktionen anderer Geschäftsbereiche sind dafür kein Ersatz.

## Designbefunde und umgesetzte Änderungen

- Desktop: klare Schwarz-/Orange-Gestaltung und Fahrzeugbilder sind bereits vorhanden.
  Der frühere Einstieg priorisierte Auto-Abo/Leasing gegenüber der Tagesmiete.
  Der neue Einstieg benennt Fahrzeugwahl, Preis ab 39 € und persönliche Werkstattabholung.
- „Jetzt verfügbar“ war ein statischer Hinweis ohne Prüfung des gewünschten Zeitraums.
  Die betreffenden Fahrzeughinweise heißen nun „Auf Anfrage“.
- Der Preis erscheint vor den persönlichen Kontaktdaten. Die Trennung zwischen
  Preisorientierung, Angebot und Buchung ist auch am Absenden und nach Eingang sichtbar.
- Die Schlüsselübergabe mit Adresse und Terminvereinbarung steht im Formular.
  Drei reale Schritte stehen vor den weiteren Angeboten und dem langen Auto-Abo-Katalog.
- Zwei FAQ beantworten Onlinebuchung/-zahlung und Schlüsselabholung ausdrücklich.
  Ein Schlüsselfach wird als derzeit nicht angeboten beschrieben.
- Mobil: kompakterer Hero/Fahrzeugbereich, lesbare Eingaben, korrigierte
  Fahrzeugdetail-Spalten und Navigation auch bei 320 px.
- Preisrechner: UTC-Kalendertage verhindern einen zusätzlichen Miettag beim Ende
  der Sommerzeit. Kilometer müssen nicht mehr durch zehn teilbar sein. Bestehende
  Tages-/Tariflogik bleibt erhalten; Abhol-/Rückgabezeiten werden weiterhin vereinbart.

Dies sind konkrete Verbesserungen der Verständlichkeit. Eine tatsächliche
Conversion-Steigerung ist ohne nachgelagerte Messung nicht nachgewiesen.

## Entscheidungen und Umsetzung für die gewünschte Sofortbuchung

| Entscheidung | Benötigte Festlegung / Integration |
| --- | --- |
| Buchbarer Bestand | Welche konkreten Fahrzeug-IDs sind öffentlich buchbar? Modellnamen müssen eindeutig den intern gepflegten Fahrzeugen zugeordnet werden. |
| Tarif | Verbindliche Tagesstaffeln, 24-Stunden- oder Kalendertagsabrechnung, Mehrkilometer, Extras und Werkstattkundentarif festlegen; Gesamtbetrag serverseitig berechnen und unveränderlich speichern. |
| Übergabe | Öffnungszeiten, erlaubte Abhol-/Rückgabezeiten, Vorlauf und Reinigungs-/Übergabepuffer festlegen. Schlüssel zunächst ausschließlich persönlich in der Werkstatt. |
| Kaution / Versicherung | Pro Fahrzeug verbindliche Kaution, Selbstbeteiligung und Versicherungsumfang; Kaution als Zahlung oder Kartenreservierung und Zeitpunkt der Freigabe entscheiden. |
| Zahlung | Händlerkonto und Zahlungsanbieter festlegen; Testzugang, Checkout, signierte Webhooks, Rückerstattung und Buchhaltungsabgleich anbinden. Festlegen, ob vollständige Miete vorab bezahlt wird. |
| Vertrag / Storno | Für öffentliche Selbstbuchung freigegebene Mietbedingungen, Fahreranforderungen, Stornierung, Nichterscheinen und Umgang mit fehlgeschlagener/verspäteter Zahlung festlegen. |

Technischer Zielablauf: Fahrzeug und Zeitraum auswählen → serverseitiger Preis und
kurzzeitig gesperrter Bestand → verbindlicher Checkout → verifiziertes
Zahlungsergebnis → Buchungsbestätigung mit Werkstattadresse und Übergabetermin.
Die Verfügbarkeitsprüfung und Reservierung müssen gemeinsam in einer Transaktion
erfolgen. Alle Buchungswege einschließlich Admin, Werkstatt-Ersatzfahrzeugen und
Telefonreservierungen müssen dieselben Sperren beachten. Checkout-Abbruch gibt
eine abgelaufene Sperre frei; späte Zahlung braucht einen definierten Konfliktpfad.
Wiederholte Zahlungsereignisse dürfen keine zweite Buchung erzeugen. Eine Browser-
Weiterleitung auf eine Erfolgsseite darf niemals allein „bezahlt“ setzen.

Vor Aktivierung erforderlich: parallele Buchungsversuche, abgelaufene Sperren,
Wartung, Tarifwechsel, manipulierte Browserpreise, Zahlungsabbrüche, doppelte und
verspätete Webhooks, Storno/Rückerstattung sowie Zeitzonen testen. Dieser Entwurf
fügt bewusst keinen unvollständigen Zahlungs- oder Reservierungsmechanismus hinzu.

## Validierung

- Öffentliche Seite auf Desktop und Mobil visuell geprüft, keine Anfrage abgeschickt.
- Lokale Vorschau verwendet ausschließlich simulierte Anfragen; Analytics-Skripte
  sind dort entfernt und `connect-src 'self'` verhindert externe Formularaufrufe.
- `node scripts/test_mos_rental_estimate.js`: bestanden; tatsächliche Inline-Funktion,
  Ein-/Mehrtagestarife, Rabattgrenze, Mehrkilometer, falsche Datumsfolge, beide
  Zeitumstellungen und Schaltjahr. Kein Netzwerk und keine Kundendaten.
- `python -m py_compile app.py rental_app.py`: bestanden.
- `python scripts/smoke_test.py`: 113 erfolgreiche Prüfungen.
- `python scripts/flow_test.py`: 191 erfolgreiche Prüfungen.
  Beide vorhandenen Suiten verwenden temporäre Daten und sperren externe Verbindungen.
- Lokaler Formularversand: simuliertes HTTP 503 zeigt Fehler und ermöglicht erneut
  Absenden; simuliertes HTTP 201 zeigt nur Anfrageeingang, keine bestätigte Buchung.
- Responsive-Prüfung bei 320, 390, 768 und 1440 px: kein horizontaler Seitenüberlauf;
  Desktop- und Handyansichten visuell geprüft, mobiles Menü öffnet und schließt.
- `git diff --check`: bestanden.

Lokale Vorschau: `python .agent-hub/mos_preview.py`, http://127.0.0.1:5083/.
Die Vorschauhilfe und Testspeicher unter `.agent-hub/` bleiben unversioniert.


## Nacharbeit 22.09.2026 – Aufgabe 64: Fahrzeug-CTAs und Fragmentlinks

Fehler lokal vor der Änderung reproduziert: Klick auf C3 von /#flotte navigierte
wegen `<base href="/mietwagen-vorschau/">` nach /mietwagen-vorschau/#anfrage.
Der neue Dokumentaufruf verwarf die im Klickhandler gesetzte C3-Auswahl und zeigte
KONA mit 59 €/Tag. Die ursprüngliche Browserprüfung hatte diesen Pfadwechsel
nicht als Auswahlverlust erfasst.

Korrektur: Alle 21 Fragmentlinks werden vor der Interaktion auf die vollständige
aktuelle Dokument-URL samt Query und jeweiligem Fragment ausgerichtet. Die
Asset-Basis bleibt erhalten. Native Fragmentnavigation, Tastaturbedienung und
Browserhistorie bleiben nutzbar; kein Wechsel zwischen Root und Legacy-Pfad.

Bestanden: `node scripts/test_mos_fragment_links.js` (21 Links, vorhandene Ziele,
Root/Legacy auf beiden Domains, Query-Erhalt), bestehende Preisrechnerregression,
`git diff --check`. Browser: alle drei Fahrzeug-CTAs auf Root mit drei Tagen
(C3 117 €, i10 117 €, KONA 147 €), erhaltenem Zeitraum/Namen/Query; auf Legacy
Ein-Tagespreise (KONA 59 €, i10/C3 39 €). Alle Fragmentziele im DOM korrekt.
Mobil auf Root: C3-Auswahl bleibt nach Menü/Ablauf und Browser-Zurück erhalten,
Menü schließt. Kein Versand, keine Zahlung, kein Deploy. Vorherige Änderungen
bleiben im selben Worktree/Branch erhalten. Aufgabe 63 bleibt in Prüfung;
Nacharbeit separat als Aufgabe 64 dokumentiert.
