# Gemeinsamer Mietwagenbestand für Checkout-Reservierungen

22.09.2026 – TomorrowWorks Aufgabe 66. Implementierung lokal, keine Veröffentlichung.

`mietwagen_checkout.SharedCheckout` verwendet dieselbe Portal-Datenbank und dieselbe Fahrzeugzeilensperre wie die vorhandene Mietverwaltung. `app.py` legt beim normalen Schemaaufbau die Tabellen `miet_checkout_holds` und `miet_checkout_events` an. Es gibt keine zweite Bestandsdatenbank. Der frühere isolierte Prototyp `mos_booking/` und die Website bleiben unverändert.

## Funktionsschalter und Aufrufgrenze

`app.config['MOS_SHARED_CHECKOUT_ENABLED']` steht ausdrücklich auf `False`. Kein Umgebungswert aktiviert ihn automatisch. Es gibt keinen neuen öffentlichen Endpunkt, keine Live-Zahlungsunterstützung und keinen Nachrichtenversand. Der Adapter akzeptiert nur den vorhandenen Stripe-Testadapter bzw. den Offline-Testprovider. Beide werden erst beim expliziten Erzeugen von `SharedCheckout` importiert; die normale Portal-App benötigt dadurch keine zusätzliche Stripe-Abhängigkeit.

Der neue Adapter ist eine interne Integrationsschnittstelle. Nur vertrauenswürdiger Servercode darf `reserve(request_key, vehicle_id, start, end, customer, quote)` aufrufen. `quote` enthält einen in Cent berechneten Betrag, `currency='eur'` und eine bestätigte `rules_version`; diese Daten dürfen später keinesfalls direkt aus Browser-JSON übernommen werden. Es sind keine realen Fahrzeug-IDs, Tarife oder Mietbedingungen eingebaut. Der bestehende lokale Prototyp nutzt weiterhin seinen synthetischen Bestand und ist nicht auf die echte Portal-Datenbank umgestellt.

## Ablauf und Sperren

1. Reservierung: SQLite `BEGIN IMMEDIATE` oder PostgreSQL `SELECT ... FOR UPDATE` auf `mietfahrzeuge`. Danach Prüfung von Aktivität/Wartung, bestehenden Mietvorgängen und offenen Checkout-Reservierungen. Unveränderliche Kundendaten/Preisreferenz werden gespeichert. Dieselbe Idempotenzreferenz mit anderem Inhalt wird abgelehnt.
2. Checkout: `create_checkout(hold_id)` erstellt außerhalb der Datenbanksperre eine Testsession mit stabiler Stripe-Idempotenzreferenz und 35 Minuten Ablaufzeit. Timeout lässt den Zeitraum gesperrt. Eine unklare Erstellung kann kurz mit identischen Parametern wiederholt werden; später ist Prüfung erforderlich. Redirects sind absichtlich fest auf die lokale Testvorschau gesetzt und noch keine integrierte Kunden-Bestätigungsseite.
3. Zahlung: `handle_signed_event(raw_body, signature, endpoint_secret)` prüft mit dem offiziellen SDK, lädt den Providerstatus erneut und vergleicht Testmodus, Session, Betrag, Währung und Preisreferenz. Innerhalb derselben Fahrzeugtransaktion werden Ereignis, Mietvorgang und Reservierungsstatus gemeinsam gespeichert. Eine Wiederholung erzeugt keinen zweiten Mietvorgang, auch nach dessen Stornierung/Rückgabe.
4. Abbruch/Ablauf: `cancel_or_reconcile(hold_id, cancel=True/False)` gibt nur eine vom Provider als abgelaufen und unbezahlt bestätigte Session frei. Kein Freigeben aufgrund der lokalen Uhr, eines Redirects oder Netzfehlers. Zahlung während Abbruch wartet auf den signierten Webhook.
5. Späte Zahlungen, Wartung oder andere Bestandskonflikte gelangen in `review`; keine automatische Bestätigung oder Erstattung. Prüfzustände bleiben gesperrt und werden durch spätere Ereignisse nicht stillschweigend aufgelöst.

Die zentrale Funktion `mietfahrzeug_zeitraum_frei_db` berücksichtigt Reservierungen **auch bei ausgeschaltetem Erstellungsschalter**. So kann ein Zurücksetzen des Flags bestehende Sperren nicht umgehen. Davon profitieren Admin-Anlage, Anfrageübernahme und Vertrags-Datumsänderung mit ihren bestehenden Transaktionen. Die Umwandlung in einen Mietvorgang ignoriert ausschließlich die eigene Reservierung; alle anderen Sperren gelten weiterhin. Die bestehende inklusive Rückgabetag-Prüfung bleibt erhalten und ist keine neu beschlossene öffentliche Mietregel.

## Prüfung

```powershell
.agent-hub/booking-venv/Scripts/python.exe scripts/test_mietwagen_checkout.py
.agent-hub/booking-venv/Scripts/python.exe scripts/test_mos_booking.py
python scripts/smoke_test.py
python scripts/flow_test.py
```

Die neue Suite verwendet eine frische temporäre SQLite-Portal-Datenbank und synthetische Fahrzeuge/Kunden. Externe Netzwerkverbindungen sind gesperrt. Geprüft werden echte Portal-Funktionen und authentifizierte Testclient-Routen: konkurrierende Admin-Anlage, Anfrageübernahme und Datumsänderung gegen Checkout; wiederholte Anfragen/Webhooks; atomarer Rollback; Timeout; verfrühte Freigabe; Wartung; doppelte Zahlungsreferenz; Webhook vor Create-Antwort; Zahlung gegen Abbruch; verspätete Zahlung. PostgreSQL verwendet die vorhandene Portal-Connection und deren SQL-Übersetzung; ein Test gegen eine echte PostgreSQL-Testinstanz steht noch aus.

## Noch vor öffentlicher Aktivierung nötig

Stripe-Konto und Wallets sind laut Ursprungstask inzwischen aktiv. Für echte Nutzung fehlen weiterhin: geprüfte Fahrzeugzuordnung und verbindlicher Kalender, serverseitige freigegebene Preis-/Mietregeln, sicher konfigurierte Testzugänge und echter Sandbox-/Wallet-Test. Danach öffentliche Authentifizierung/CSRF und Kundenstatus, Kontakt-/Vertragszustimmung, Admin-Prüfliste, Hintergrundabgleich, einmalige Bestätigung und ein gesondert abgesicherter Liveadapter samt HTTPS-Webhooks. Der aktuelle Adapter ist die getestete gemeinsame Bestandsgrundlage, keine Freischaltung der Website. Keine Keys in Dokumentation oder Chat kopieren.
