# MOS: lokaler Buchungs- und Zahlungstest

> Historischer Prüfbericht. Aktueller Beschluss vom 23.09.2026: Direktbuchung ausschließlich für Hyundai i10 und Hyundai KONA. C3 bleibt Anfrage; Fiat ist ausgeschlossen. Drei-Fahrzeug-Angaben und C3-Tests unten beschreiben den damaligen Stand bzw. den isolierten Altprototyp und sind keine aktuelle Flottenkonfiguration. Aktueller Test-Starter: [Öffentlicher Testablauf](mos-public-test.md); Freigaben: [Launch-Paket](mos-launch-package.md).

Stand: 22.09.2026. TomorrowWorks Aufgabe 65, Branch `feature/mos-mietanfrage-klarheit`.

## Ergebnis und Grenzen

Der isolierte Prototyp unter `mos_booking/` bildet Fahrzeugwahl, Zeitraumprüfung, serverseitige Preisbestätigung, Checkout, Zahlungsprüfung und Bestätigungsseite ab. Er verändert weder die öffentliche Website noch den echten Mietwagenbestand. Auto-Abos bleiben Anfragen; die Schlüsselübergabe ist persönlich bei Gärtner, Binauer Höhe 4, 74821 Mosbach-Lohrbach. Ein Übergabetermin ist noch zu vereinbaren.

Standardmodus ist `off`. `offline` arbeitet vollständig lokal mit synthetischen Zahlungen. `stripe_test` enthält einen Adapter für Stripe Hosted Checkout; ohne bereitgestellte Testzugänge wurde dieser nur mit API-Mocks getestet. Live-Schlüssel und Live-Ereignisse werden abgewiesen. Keine Konten eröffnet, echten Zahlungen oder Kundenbenachrichtigungen ausgelöst. Apple Pay und Google Pay wurden nicht auf echten Geräten getestet.

## Lokal starten

```powershell
python -m venv --system-site-packages .agent-hub/booking-venv
.agent-hub/booking-venv/Scripts/python.exe -m pip install -r requirements-mos-booking.txt
$env:MOS_BOOKING_MODE='offline'
$env:MOS_BOOKING_TEST_DB=(Join-Path (Get-Location) '.agent-hub/preview.mos-test.sqlite3')
.agent-hub/booking-venv/Scripts/python.exe -m mos_booking.app
```

Vorschau: http://127.0.0.1:5084/. Nur Loopback-Host und lokale Anfragen sind zugelassen. Eine Datenbank mit Endung `.mos-test.sqlite3` ist erforderlich. Die drei Test-IDs `test-kona`, `test-i10`, `test-c3` sind keine Zuordnung zu echten Portal-Fahrzeugen. Für persistente Browsersitzungen `MOS_BOOKING_SESSION_SECRET` lokal sicher setzen; ohne Vorgabe wird bei jedem Start ein neuer Wert erzeugt. Test-Verwaltung unter `/admin` nur mit lokal gesetztem `MOS_BOOKING_ADMIN_TOKEN`; ohne Token deaktiviert.

Für einen später ausdrücklich gestarteten Stripe-Sandbox-Test: `MOS_BOOKING_MODE=stripe_test`, `MOS_STRIPE_TEST_KEY` und `MOS_STRIPE_WEBHOOK_SECRET` sicher lokal konfigurieren, niemals ins Repository oder in den Chat schreiben. Signierte Webhooks an `/webhooks/stripe` weiterleiten, beispielsweise über die offizielle Stripe CLI. Der Adapter nutzt Hosted Checkout mit `card`, EUR, deutschem Checkout und unveränderlicher Preisreferenz. Die Haupt-App importiert dieses Modul nicht; ihre Abhängigkeiten bleiben unverändert.

## Absicherung

- SQLite-Transaktion mit `BEGIN IMMEDIATE` reserviert das Testfahrzeug vor dem externen Checkout-Aufruf. Überlappende Zeiträume einschließlich Rückgabetag werden gesperrt.
- Preise entstehen ausschließlich auf dem Server. Ein Hash bindet die sichtbare Preisvorschau an den Checkout; geänderte Eingaben oder Preise erfordern eine neue Vorschau. Idempotenz verhindert doppelte Checkouts bei Wiederholung.
- Preise sind bisherige öffentliche Richtwerte: KONA 59 EUR/Tag, ab drei Tagen 49; i10 und C3 39; 150 km/Tag inklusive, Mehrkilometer 0,25 EUR. Werkstatttarife werden nicht automatisch verwendet. Maximal 90 Miettage und 366 Tage Vorlauf sind ausdrücklich Testgrenzen, keine beschlossenen Geschäftsregeln.
- Ein Rücksprung auf die Erfolgsseite bestätigt nichts. Erst ein mit dem offiziellen SDK geprüfter Webhook und eine erneut geladene Checkout Session können bestätigen. Betrag, Währung, Modus, Session, Referenz und Preis-Hash müssen passen.
- Ereignisse werden dauerhaft dedupliziert; Statusfortschritt und Ereignisverarbeitung erfolgen gemeinsam in einer Transaktion. Eine Payment Intent ID kann keine zwei Buchungen bestätigen. Unbezahlte abgeschlossene Sessions bleiben offen für Zahlungsprüfung.
- Checkout läuft nach 35 Minuten ab. Freigabe erst nach bestätigtem Provider-Status `expired` und ohne Zahlung. Lokaler Zeitablauf, Netzwerkfehler oder unbekannter Ausgang der Session-Erstellung reichen nicht. Solche Fälle halten das Fahrzeug gesperrt und brauchen Klärung. Wiederholung einer unklaren Erstellung verwendet dieselben Stripe-Parameter und dieselbe Idempotenz-ID; nach dem kurzen Wiederholungsfenster keine automatische Neuanlage.
- Späte Zahlung nach Freigabe, Bestandskonflikte oder abweichende Provider-Daten führen zur Prüfung statt einer zweiten Bestätigung. Keine automatischen Erstattungen.
- CSRF, sitzungsgebundener Zugriff auf Buchungen und geschützte Test-Verwaltung; keine personenbezogenen Kundendaten im Prototyp. Der Offline-Zahlungssimulator ist nur im Offline-Modus verfügbar.

## Vor echter Integration zu klären

1. Existiert bereits ein Stripe-Konto für den rechtlichen Vermieter? Kontoinhaber, Verifizierung und Bankverbindung richtet der Nutzer ein. Testzugänge anschließend sicher lokal bereitstellen. Wallet-Einstellungen und Sichtbarkeit mit geeigneten Apple-/Google-Geräten prüfen.
2. Drei tatsächliche Fahrzeug-IDs und gemeinsamer Belegungskalender: Website, Telefonbuchungen, Werkstatt-Ersatzwagen, Wartung und Admin müssen dieselbe verbindliche Sperrlogik verwenden. Der aktuelle Testbestand reicht dafür nicht. Für PostgreSQL ist eine eigene transaktionale Sperrstrategie nötig.
3. Verbindliche Bruttopreise, Miettagberechnung, Abhol-/Rückgabezeiten, Öffnungszeiten, Puffer, Vorlauf, maximale Mietdauer und spätere Kilometerabrechnung festlegen. Die inklusive Rückgabetag-Sperre entspricht der bestehenden Prüfung, ist aber noch keine bestätigte öffentliche Buchungsregel.
4. Kaution und Zahlungs-/Autorisierungsverfahren, Versicherung und Selbstbeteiligung, Fahrer-/Führerscheinvorgaben sowie Storno, Nichterscheinen und Erstattungsregeln bestimmen. Der interne Vertragsvorgabewert wird nicht automatisch übernommen.
5. Kundenkontakt, erforderliche Vertragsdaten und verbindliche Buchungsbedingungen in den echten Ablauf integrieren. Bestätigungs-E-Mail mit einmaliger Versandverarbeitung, bestehender Admin-Anmeldung, Protokollierung, Hintergrundabgleich und Rückgabeprozess ergänzen. Momentan gibt es eine Test-Bestätigungsseite, keinen E-Mail-Versand oder endgültigen Mietvertrag.
6. Erst danach Produktionsmigration, echtes Bestandslocking, HTTPS, Live-Konfiguration und vollständige Abnahme. Der Prototyp ist absichtlich nicht öffentlich betreibbar und nicht als sofortige Produktionsfreigabe gedacht.

## Verifikation

```powershell
.agent-hub/booking-venv/Scripts/python.exe scripts/test_mos_booking.py
node scripts/test_mos_rental_estimate.js
node scripts/test_mos_fragment_links.js
git diff --check
```

28 Python-Tests bestanden: konkurrierende Buchungen, wiederholte Requests, Preismanipulation/-änderung, DST, Signaturprüfung inklusive veralteter Signaturen, parallele/doppelte und vertauschte Webhooks, Webhook vor Create-Antwort, Payment-Intent-Duplikate, unbezahlte Sessions, Provider-Ausfälle, Ablauf, Zahlung gegen Abbruch und verspätete Zahlung. Externe Netzwerkaufrufe sind in der Testsuite gesperrt; Stripe-API-Verhalten ist gemockt, Signaturprüfung verwendet das echte SDK.

Browserprüfung: vollständige Offline-Zahlung bis Bestätigung; Rückkehr ohne Zahlung bleibt offen; explizites Beenden gibt erst nach Provider-Ablauf frei. Mobile Ansicht mit 390 px geprüft, Preise mit deutschem Dezimalformat. KONA drei Tage = 147 EUR; C3 drei Tage/501 km = 129,75 EUR. Keine echte Zahlung ausgeführt.

## Offizielle Quellen

- [Stripe Checkout](https://stripe.com/de/payments/checkout): Hosted Checkout unterstützt Karten und Wallets.
- [Wallets in Hosted Checkout](https://docs.stripe.com/payments/save-and-reuse-cards-only?locale=en-GB&platform=web): Wallets hängen von Konto-Einstellungen und geeignetem Gerät ab; Google Pay gegebenenfalls in den Zahlungsmethodeneinstellungen aktivieren. Keine selbst gebauten Wallet-Schaltflächen im Prototyp.
- [Checkout Session erstellen](https://docs.stripe.com/api/checkout/sessions/create): Session-Parameter und Ablaufzeit zwischen 30 Minuten und 24 Stunden.
- [Zahlungen erfüllen](https://docs.stripe.com/checkout/fulfillment?payment-ui=stripe-hosted): Webhooks, erneute Session-Abfrage, Zahlungsstatus und idempotente Verarbeitung statt Vertrauen in den Redirect.
- [Session beenden](https://docs.stripe.com/api/checkout/sessions/expire): offene Sessions gezielt beenden; Abschlussrennen separat behandeln.
- [Webhook-Signaturen](https://docs.stripe.com/webhooks/signature): unveränderten Request-Body und Endpoint-Secret für die Prüfung verwenden.
