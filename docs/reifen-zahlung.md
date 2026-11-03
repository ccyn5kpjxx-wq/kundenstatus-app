# Stripe-Zahlungsadapter: vollständiger Einzelcapture

Stand 03.11.2026. Dieses Release enthält ausschließlich den bereits isoliert
geprüften Stripe-Adapter und dessen Vertragstests. Der automatische
TYREsystem-Beschaffungsablauf und seine Core-Integration bleiben auf dem
unveröffentlichten Featurebranch. Es werden keine Kaufgates, Lieferanten,
Kontorechte, Schlüssel, Webhooks oder Umgebungsvariablen eingerichtet.

## Kompatibilität des vorhandenen Ablaufs

Bestellungen ohne `payment_capture_method` verwenden weiterhin `automatic`.
Ihre Checkout-Parameter, Metadaten und Idempotenzschlüssel bleiben kompatibel.
Der bestehende Main-Core nutzt ausschließlich `create`, `retrieve` und
`verify_webhook`; er ruft weder `capture` noch `cancel_authorization` auf.
`supports_manual_capture=True` beschreibt eine Adapterfähigkeit und aktiviert
keinen neuen Bestellablauf.

Nur ein bereits serverseitig gespeicherter Snapshot mit
`payment_capture_method='manual'` wählt die getrennte Kartenautorisierung.
Die Bestellung bindet Modus, Referenz, Quotehash, Zahlungsversuch, Intent und
exakten Gesamtbetrag in EUR-Cent. Das Feld ist keine Browser-Workfloweingabe.

## Autorisierung, Einzug und Freigabe

Für eine nutzbare Autorisierung liest der Adapter den gebundenen PaymentIntent
mit `status='requires_capture'`, `amount_capturable=Gesamtbetrag` und
`amount_received=0`. Die zugehörige Charge muss noch uneingezogen sein und die
tatsächliche, noch zukünftige Kartenfrist `capture_before` liefern. Ein
Checkout-Erfolgsredirect oder `Charge.paid=True` allein belegt keinen Einzug.

Der vollständige Einzelcapture sendet ausschließlich
`amount_to_capture=Gesamtbetrag`. Der Parameter `final_capture` wird weggelassen:
Stripe lehnt ihn für einen PaymentIntent ohne Multicapture-Unterstützung auch
mit dem Wert `True` ab. Der korrigierte Request benutzt einen stabilen
Capture-Schlüssel mit der festen Requestfassung `single_capture_v2`. Die
Bestell-/Intent-/Betragsbindung bleibt erhalten; Cancel-Schlüssel bleiben
unverändert. Ein früherer offener Capture-Versuch muss vor einem Versionswechsel
anhand seiner Antwort und eines frischen Providerstatus eindeutig als nicht
vollzogen und nicht mehr laufend abgeglichen sein. Ein unbekannter Ausgang
erlaubt keinen solchen Wechsel.

Vor jedem Capture-POST wird derselbe Intent frisch geprüft. Bereits bestätigtes
`succeeded` mit passendem `amount_received` und `amount_capturable=0` führt ohne
weiteren POST zum Erfolg. Nach unklarer Antwort wird derselbe Vorgang gelesen;
es wird kein neuer Checkout oder Intent als Ersatz angelegt. Automatische
SDK-Retries bleiben ausgeschaltet.

`cancel_authorization` gibt ausschließlich einen noch gebundenen,
uneingezogenen `requires_capture`-Vorgang frei. Bestätigtes `canceled` verlangt
`amount_received=0` und `amount_capturable=0`; die Methode erstattet keine
bereits eingezogene Zahlung. Für einen noch offenen Checkout wäre stattdessen
die Session-Expire-API erforderlich.

## Nachweise und Betriebsgrenze

Die Adaptertests verwenden Fake-SDK-Services und gesperrte Sockets. Sie prüfen
unter anderem unveränderte automatische Altaufträge, manuelle Betrags- und
Metadatenbindung, die tatsächliche Frist, die korrigierte Requestform, getrennte
Idempotenz sowie verlorene Capture-/Cancel-Antworten. Die Main-Core-, Wiring-
und Demo-Bestellsuiten werden zusätzlich auf isolierten Daten geprüft.

Zwei getrennte synthetische 1-EUR-Fälle wurden bereits in einer separaten
Stripe-Sandbox abgenommen: Autorisierung mit vollständigem Einzug sowie
Autorisierung mit Freigabe ohne Einzug. Das belegt diesen Stripe-Adapter,
keinen Lieferanten- oder vollständigen Shop-End-to-End-Ablauf. Die automatische
Bestellintegration wird durch dieses Adapterrelease nicht veröffentlicht.

Stripe-Primärquellen:

- Autorisierung und tatsächliche Kartenfrist:
  https://docs.stripe.com/payments/place-a-hold-on-a-payment-method
- Vollständiger Capture und optionale Multicapture-Parameter:
  https://docs.stripe.com/api/payment_intents/capture
- Cancel des PaymentIntent:
  https://docs.stripe.com/api/payment_intents/cancel
- Requestparameter und Idempotenz:
  https://docs.stripe.com/error-low-level#content-errors
  https://docs.stripe.com/api/idempotent_requests
