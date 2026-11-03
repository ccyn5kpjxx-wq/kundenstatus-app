# Kartenautorisierung vor automatischer TYREsystem-Beschaffung

Stand 03.11.2026. Codevertrag für `reifen_zahlung.py`; kein eingerichtetes
Produktivkonto, keine neuen API-Rechte, kein gestarteter Kauf oder Provider-Test.
Die vorhandene `reifen-zahlung-konfiguration.txt` bleibt unverändert. Dieser
Modus aktiviert keine Kauf-, Datenrechte-, Supplier-, SMS- oder Vertragsgates.

## Eingefrorenes Zahlungsmodell

Bestehende Bestellungen ohne `payment_capture_method` behalten den bisherigen
automatischen Stripe-Einzug. Ihre Metadaten und Checkout-Idempotenz bleiben
kompatibel. Der neue Pfad wird ausschließlich durch einen serverseitig
gespeicherten Bestellsnapshot mit `payment_capture_method='manual'` gewählt.
Das ist keine erlaubte Browser-Preis-/Workfloweingabe.

Der Core speichert vor IO unveränderlich Umgebung/Kontobindung, Bestell-ID,
Produktart, Quotehash, Gesamtbetrag in EUR-Cent und `payment_attempt` sowie die
Checkout-Erzeugungs-/Ablauffrist. `create(order)` setzt im manuellen Pfad
`payment_intent_data.capture_method='manual'`. Session und PaymentIntent tragen
Integration, Bestell-ID und Quotehash sowie zusätzlich `capture_method=manual`
und die Versuchskennung. Der manuelle Checkout hat einen von Legacyautomatic
unterschiedlichen Idempotenzschlüssel; ein Moduswechsel ist kein Retry.

Nur die unterstützten Karten-Zahlmittel werden angeboten. Keine automatische
Ausweitung auf Zahlungsmittel, die getrennte Autorisierung/Einzug nicht
unterstützen, und keine private Stripe-Funktion zum späteren automatischen
Einziehen aktivieren: Der Einzug hängt ausdrücklich vom belegten Supplier-Ack ab.

## Gateway-Schnittstelle

`supports_manual_capture=True` bedeutet nur, dass diese Implementierung die
folgenden Operationen unterstützt. Es ist kein Nachweis ausreichender Rechte
des konfigurierten Schlüssels und ersetzt keine entsprechende Readinessprüfung.

### `retrieve(session_id, expected_order=order)`

Für manuelle Zahlungen ist die gespeicherte Bestellung erforderlich. Wenn der
Checkout abgeschlossen ist, wird der PaymentIntent serverseitig erneut gelesen.
Geprüft werden tatsächliche ID, Live-/Testmodus, `capture_method=manual`,
Währung, exakter Betrag und sämtliche oben genannten Metadaten. Ein im Snapshot
vorhandenes `payment_id` beziehungsweise `payment_intent` muss ebenfalls passen.

Ein nutzbarer Hold verlangt `PaymentIntent.status=requires_capture`,
`amount_capturable` exakt gleich dem vereinbarten Gesamtbetrag und
`amount_received=0`. Anschließend wird der tatsächliche `latest_charge`
abgerufen: Kartenzahlung, gleiche Intent-/Modus-/Betrags-/Währungsbindung,
noch nicht eingezogen und `payment_method_details.card.capture_before` als
noch zukünftiger Unix-Zeitpunkt. Fehlende Frist oder fehlendes Charges-Leserecht
führen zu einem sicheren Fehler, niemals zu einer geschätzten Sieben-Tage-Frist.

Der Rückgabewert behält `id=Session-ID` und enthält im Holdzustand:

```text
status='authorized', payment_status='authorized'
payment_intent, payment_capture_method='manual'
amount_total, amount_capturable, amount_received=0, capture_before
livemode, currency='eur', reference, quote_hash, stripe_status='requires_capture'
```

`authorized` ist ausdrücklich kein Zahlungseingang. Stripe `Charge.paid=True`
kann bereits eine Autorisierung bezeichnen. Auch eine abgeschlossene Session
oder ein Erfolgsredirect genügt nicht als Zahlungsbeleg. Nach tatsächlichem
Einzug gilt nur ein erneut gebundener `PaymentIntent.status=succeeded` mit exakt
passendem `amount_received` und `amount_capturable=0` als bezahlt.

Checkout-`expires_at`, Karten-`capture_before`, Katalog-TTL und eine etwaige
tatsächliche Warenreservierung sind verschiedene Fristen. Der Core muss vor
einem Supplier-POST genug verbleibende echte Autorisierungszeit für dessen
Zeitbudget, Einzug und Abgleich prüfen. Die übliche Netzwerklaufzeit einer
Autorisierung ist kein individuell zugesagter Ablaufzeitpunkt.

### `capture(order)`

Der Caller muss zuvor einen eindeutigen Supplier-Ack und die beabsichtigte
Capture-Operation dauerhaft speichern. Der Adapter prüft keinen Supplier und
legt keinen Warenauftrag an. Die Bestellung muss `payment_intent` und das
eingefrorene manuelle Zahlungsmodell enthalten.

Vor dem POST wird derselbe PaymentIntent frisch gelesen und einschließlich
tatsächlicher Autorisierungsfrist geprüft. Es wird genau der vereinbarte volle
Centbetrag mit `final_capture=True` eingezogen, ohne Mehr-/Teilbeträge oder
Multicapture. Der stabile eigene Capture-Schlüssel bindet Umgebung, Bestellung,
Quote, Versuch, manuelles Modell, Intent und Betrag. Automatische SDK-Retries
bleiben aus; Core und dauerhafter Versuch besitzen den Wiederholungsablauf.

Bei bestätigtem Einzug lautet der Rückgabewert `id=PaymentIntent-ID`,
`status='succeeded'`, `payment_status='paid'`, `amount_received=Gesamtbetrag`
und `amount_capturable=0`, mit derselben Bestellbindung. Wird vor dem POST
bereits ein entsprechender Erfolg gelesen, erfolgt kein zweiter Capture-POST.
Eine unklare Transportantwort ist ein sicherer Fehler und verlangt erneutes
Lesen derselben Session/des selben Intents. Niemals einen neuen Intent, eine
neue Checkoutsession oder eine zweite Supplierbestellung als Ersatz erzeugen.

### `cancel_authorization(order)`

Es wird ausschließlich der gespeicherte und aktuell gebundene manuelle
PaymentIntent storniert, wenn Stripe ihn noch als `requires_capture` führt.
Eine bereits abgelaufene `capture_before`-Frist wird für das Freigeben nicht
durch eine erfundene neue Frist ersetzt. Ein bereits passend `canceled`
gelesener Vorgang wird idempotent wiedererkannt. Nach tatsächlicher Zahlung
erfolgt keine automatische Erstattung über diese Methode.

Der Cancel-POST benutzt einen eigenen stabilen, vom Capture getrennten Schlüssel
und den festen Grund `abandoned`. Bestätigte Rückgabe:
`id=PaymentIntent-ID`, `status='canceled'`, `payment_status='canceled'`,
`amount_received=0`, `amount_capturable=0`. Ein unklarer Ausgang bleibt zur
Statusabfrage offen. Ein nicht autorisierter, noch offener Checkout ist nicht
über diese Methode freizugeben; dafür wäre die Session-Expire-API zuständig.
Ein Checkout-PaymentIntent ist nach aktueller Stripe-Referenz direkt cancelbar,
wenn er `requires_capture` hat; die frühere pauschale Gegenannahme ist falsch.

## Automatischer Core-Ablauf und unbekannte Ausgänge

Der vorgesehene Normalpfad lautet:

```text
Checkout offen → Autorisierung belegt → Supplier-POST dauerhaft beansprucht
→ eindeutiger Supplier-Ack dauerhaft gespeichert → Capture beansprucht
→ Zahlung tatsächlich belegt → verbindliche Bestellannahme/Vertragskopie
```

Es gibt keine reguläre zusätzliche Dashboardfreigabe. Ein Absturz, Timeout oder
mehrdeutiger Supplierausgang ist aber keine Erlaubnis zum zweiten Warenauftrag:
Vorgang sperren und anhand einer tatsächlich verfügbaren Supplier-Statusabfrage
oder durch Klärung zuordnen. Ohne eindeutigen Ack kein Capture. Ohne bewiesenen
Supplier-Nichtauftrag nicht automatisch so tun, als sei alles storniert. Ein
späterer Ack oder ein inzwischen abgelaufener Bankhold kann Klärung erfordern.

TYREsystem-POST und Stripe-Capture bilden keine gemeinsame atomare Transaktion.
Nach Supplier-Ack kann der Einzug noch scheitern; dann existiert möglicherweise
bereits eine Warenverpflichtung. Gespeicherten Ack erhalten, keine Wiederholung
der Beschaffung und keine unbelegte Erfolgsmeldung erzeugen. Den Zustand getrennt
von „bezahlt/angenommen“ führen und den Ausnahmeprozess anwenden.

Ein CSV-Bestandspreis ist keine Warenreservierung. Wenn TYREsystem keine
tatsächliche Reservierungsfunktion bereitstellt, dürfen `reserve()`, eine
Reservierungs-ID oder deren Gültigkeit nicht aus Preis-TTL oder Zufallswerten
simuliert werden. Vor Beschaffung erneut echte Verfügbarkeit, Fracht und den
vereinbarten Lieferumfang prüfen; Kunde stimmt keinem höheren Betrag allein
durch die vorhandene Autorisierung zu. Supplier-Protokoll und harte Grenzen
für abweichende Einkaufskonditionen müssen separat belegt sein.

## Webhooks und minimale Rechte

Die vorhandenen signierten Checkout-Session-Ereignisse bleiben unverändert.
`checkout.session.completed` kann den serverseitigen Abgleich anstoßen; der
unabhängige Worker muss den Ablauf auch ohne Browserrückkehr fertigstellen.
Der Eventinhalt allein führt keine Bestellung oder Zahlung aus. Doppelte und
anders geordnete Events sind anhand persistierter IDs und Zustandsübergänge
idempotent zu verarbeiten. Eine durable Warteschlange erlaubt schnelle HTTP-2xx-
Antworten vor Supplier-/Capture-Arbeit. Neue PaymentIntent-Ereignisse sind für
diesen Adapter nicht automatisch abonniert oder als Sessionpayload interpretiert.

Für den beschriebenen Callgraph als kleinste zu prüfende Ressourcenrechte:

| Ressource | Recht | Verwendung |
| --- | --- | --- |
| Checkout Sessions | Write | Erstellen/Lesen, gegebenenfalls explizites Expire |
| Payment Intents | Write | Lesen, Capture, Cancel |
| Charges | Read | Tatsächliche Kartendaten zur Autorisierungsfrist lesen |

Write schließt Read dieser Ressource ein. Keine Payout-, Refund-, Kunden-,
Kontoverwaltungs- oder Webhookverwaltungsrechte für diesen Ablauf hinzufügen.
Webhook-Signaturprüfung benötigt den eigenen Endpointsecret, kein Events-Leserecht.
Der bisher nachgewiesene Session-only-Testschlüssel belegt die zusätzlichen
PaymentIntent-/Charges-Rechte nicht. Eine erforderliche Rechteänderung bleibt
eine gesonderte konkrete Kontoaktion; diese Codeänderung führt sie nicht aus.
Den genauen Callgraph zuerst mit eigenen eingeschränkten Sandboxrechten prüfen;
fehlende Berechtigungen nicht durch einen allgemeinen Livekey umgehen.

## Validierung und Quellen

Die erweiterten Adaptertests arbeiten mit Fake-SDK-Services und gesperrtem
Netzwerk. Sie prüfen echte Status-/Betrags-/Metadatenbindung, abgelehnte fehlende
Fristen, getrennte Idempotenz, verlorene Capture-/Cancel-Antworten und unveränderte
Legacyautomatic-Fälle. Das ist noch kein echter Checkout-/Karten-/TYRE-End-to-End-
Test. Tatsächliche Konto-/Webhook-APIversion und eingeschränkte Rechte gesondert
gegen diesen Ablauf prüfen; kein Versionswechsel wird durch diese Datei veranlasst.

Stripe-Primärquellen, geprüft 03.11.2026:

- Manuelle Autorisierung, tatsächliche Frist, Capture und Freigabe:
  https://docs.stripe.com/payments/place-a-hold-on-a-payment-method
- PaymentIntent-Zustände und Beträge:
  https://docs.stripe.com/api/payment_intents/object
- Charge: `paid` kann bereits autorisiert bedeuten, `captured` unterscheidet Einzug:
  https://docs.stripe.com/api/charges/object
- Capture, voller Betrag und abschließender Einzug:
  https://docs.stripe.com/api/payment_intents/capture
- Checkout-PaymentIntent bei `requires_capture` stornieren:
  https://docs.stripe.com/api/payment_intents/cancel
- Nur offene Checkoutsession expirieren:
  https://docs.stripe.com/api/checkout/sessions/expire
- Wiederholungen, Parameterbindung und mindestens 24 Stunden Idempotenzspeicherung:
  https://docs.stripe.com/api/idempotent_requests
- Webhook-Signatur, Duplikate, Reihenfolge und schnelle Antworten:
  https://docs.stripe.com/webhooks
- Ressourcenbezogene eingeschränkte Rechte und Sandboxprüfung:
  https://docs.stripe.com/keys/restricted-api-keys
