# Öffentlicher MOS-Testablauf – Aufgabe 68

Der Einstieg `/mietwagen-test/` führt ohne Admin-Anmeldung durch Fahrzeug-/Slotwahl, serverseitiges Preisangebot, ausdrückliche Zustimmung zum Testentwurf, Checkout und Bestätigung. Er verwendet den gemeinsamen Portalbestand aus Aufgabe 66. Originalprototyp und bestehender Anfragefluss bleiben erhalten.

Standard: `MOS_PUBLIC_BOOKING={'enabled':False}`. Ohne explizite Konfiguration bleibt der Einstieg 404 und die Mietwagenseite unverändert. Die schlanke `rental_app.py` kann über `MOS_PUBLIC_TEST_ENTRY_URL` auf den Portal-Testeinstieg verweisen, besitzt selbst aber keine zusätzliche Bestandsdatenbank. Diese Einstellung ist standardmäßig leer.

## Sicherer lokaler Start

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe scripts/run_mos_public_test.py
```

Öffnen: http://127.0.0.1:5085/mietwagen-test/. Der Starter erstellt eine frische temporäre Portal-Datenbank, legt genau zwei ausdrücklich bezeichnete Testdatensätze für Hyundai i10 und Hyundai KONA an und liest keine `.env`-/Kundendatenbank. Preise und Termine in diesem Starter sind Beispielkonfiguration, keine Aktivierung verbindlicher Tarife. Es werden keine E-Mails versendet und keine echten Zahlungen durchgeführt.

Die lokale Originaldatei `data/auftraege.db` im Hauptprojekt enthielt bei der gezielten read-only-Abfrage keine Mietfahrzeuge. Im verwandten Projekt war die Mietfahrzeugtabelle nicht vorhanden. Daher keine echten IDs übernommen oder behauptet; eine autoritative Zuordnung bleibt erforderlich.

## Konfiguration und Grenzen

`MOS_PUBLIC_BOOKING` verlangt `enabled`, `test_configuration=True`, `mode=offline|stripe_test`, eine feste HTTPS-/Loopback-Origin, zwei unterschiedliche explizite `fleet`-IDs für `kona/i10`, Cent-Tarife, eindeutige Europe/Berlin-Slots mit UTC-Offset, `day_rule=elapsed_24h_ceil`, Kilometerwerte, Maximaldauer, Entwurfsversion/-text und Bruttopreismarkierung. 500 € Kaution und 1.000 € Selbstbeteiligung werden separat erläutert, nicht eingezogen oder als Versicherungsnachweis dargestellt. Die Testdatenbank muss auf `.mos-public-test.sqlite3` enden; PostgreSQL/Livebetrieb ist in dieser Stufe noch gesperrt.

Für Stripe-Sandbox sind zusätzlich `MOS_PUBLIC_STRIPE_TEST_KEY` und `MOS_PUBLIC_WEBHOOK_SECRET` sicher im Flask-Konfigurationsobjekt erforderlich. Niemals Werte ins Repository oder in Nachrichten kopieren. Der bestehende Testadapter lehnt Livekeys ab. Webhook: `/mietwagen-test/webhook`. Status-Rücksprünge führen zur konkreten Buchung; sie bestätigen keine Zahlung. Der Offline-Simulator ist nur im Offline-Modus erreichbar.

Signierte Preisangebote sind 15 Minuten gültig und an die Sitzung gebunden. Vor Reservierung wird neu gerechnet; Clientpreise werden nicht verwendet. Derselbe Checkout-POST besitzt eine stabile Idempotenzreferenz. Bestätigung des Entwurfs und vollständige Preis-/Zeitreferenz werden im Hold-Snapshot gespeichert. Fremde Sitzungen erhalten keinen Zugriff auf Status oder Simulator. CSRF-Ausnahme gilt nur für den signaturgeprüften Webhook. Hostprüfung, no-store, noindex und begrenzter Webhook-Body sind aktiv.

Exakte Slotzeitpunkte werden gespeichert; Preise rechnen mit tatsächlicher verstrichener Zeit in UTC, auf volle 24-Stunden-Tage aufgerundet. Bestehende Admin-Verfügbarkeitslogik sperrt vorsorglich weiterhin ganze Tage einschließlich Rückgabetag. Keine Same-Day-Neuvermietung wird daraus abgeleitet. Zusatzkilometer werden hier nicht vorab belastet; Preis/km und Kontingent stehen in der Übersicht.

## Prüfung und nächste Stufe

8 neue HTTP-Integrationstests bestehen: vollständiger Ablauf, beide Angebote und Ausschluss des C3, Flag aus, CSRF/Sitzungsbindung/Manipulation, ungültige Slots/Fahrzeuge, Retry/Abbruch, signierter Webhook und Wiederholung sowie Rücksprung ohne Bestätigung. Dazu 21 Bestandstests und 28 frühere Checkout-Tests. Browser: vollständiger Offline-Ablauf und mobile Bestätigungsansicht bei 390 px geprüft. Bestehende Smoke-/Flow-Suiten bestehen.

Die spätere Nutzerweisung verlangt echte Kundenbuchungen: Produktionsmodus, kundengerechte verbindliche Zusammenfassung und Bedingungen, Storno-/Erstattungsverwaltung, Deployment-Konfiguration und Freigabesicherung sind inzwischen als gesperrte Implementierung in [Aufgabe 69](mos-launch-package.md) dokumentiert. Versicherung, reale Fahrzeugzuordnung, bestätigte Übergabeslots, Kautionsverfahren und sichere Stripe-Livezugänge sind keine erfundenen Testwerte. Die technische Platzierung der endgültigen Zahlungsbestellung nach § 312j BGB muss im realen Hosted Checkout abgenommen werden; der Testbutton ist ausdrücklich keine echte zahlungspflichtige Bestellung.


## Aufgabe 71 – Isolation des Offline-Starters

Der Starter leert vor dem Portalimport `MOS_BOOKING_CONFIG_FILE`, `MOS_STRIPE_TEST_KEY`, `MOS_STRIPE_LIVE_KEY` und `MOS_STRIPE_WEBHOOK_SECRET`. Dadurch kann eine geerbte Live-Konfiguration weder gelesen werden noch bereits beim Import den Live-Routenpräfix registrieren. Der Regressionstest startet mit absichtlich unbrauchbarem Konfigurationspfad und synthetischen Schlüsselwerten: davor reproduzierbarer Importfehler, danach 9 Public-Tests bestanden, zusätzlich 13 Produktionslogiktests. Die Produktivkonfiguration selbst wird nicht verändert.
