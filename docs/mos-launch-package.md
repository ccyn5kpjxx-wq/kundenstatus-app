# MOS: Freigabepaket für echte Kundenbuchungen

Stand 24.09.2026. **Noch kein Zahlungs-Go-live.** Ziel sind öffentliche entgeltliche Selbstfahrer-Mieten ausschließlich für **Hyundai i10 und Hyundai KONA**. C3 und Fiat sind von der Direktbuchung ausgeschlossen; Werkstatttarife bleiben manuelle Anfragen. Der Kernablauf mit separater Kautionsautorisierung, Mietzahlung, signiertem Webhook und Storno wurde in einer isolierten lokalen Stripe-Testinstanz ausgeführt. Die öffentliche Zahlungsroute ist weiterhin deaktiviert; es gab keine Livezahlung und keine Autorisierung einer echten Kundenkarte.

## Aktuelle Nutzerentscheidung und Zielablauf

- Das konkrete Fahrzeug, die Mietzeit, der Bruttopreis, **500 € Kaution**, **1.000 € vertragliche Selbstbeteiligung**, der rechtliche Vermieter **Gärtner GmbH Karosserie + Lack** und die vollständigen Bedingungen erscheinen vor der Zahlung. „Autovermietung MOS“ ist nur die Überschrift. Der Kunde unterschreibt den gebundenen Vertragssnapshot digital, bevor die Online-Reservierung und die beiden Stripe-Schritte beginnen. Die gezeichnete Unterschrift ist keine qualifizierte elektronische Signatur.
- Unmittelbar bei der Online-Reservierung werden **500 € auf einer Kreditkarte autorisiert** (`PaymentIntent` mit `capture_method=manual`, Kartenzahlung). Das ist eine vorübergehende Kartenreservierung **ohne Abbuchung**. `payment_method_types[]=card` allein unterscheidet Kredit-, Debit- und Prepaid-Karten nicht; der neue Prüfpfad verlangt deshalb den vom Provider bestätigten Kartentyp `funding=credit` und lehnt unbekannte, Debit- und Prepaid-Karten vor dem Miet-Checkout ab. Erst nach erfolgreicher, ausreichend lange gültiger Autorisierung führt ein separater Stripe Checkout zur **Online-Zahlung nur des Mietpreises**. Vor Ort erfolgt weder eine Zahlung noch eine weitere Kartenbestätigung. Eine Buchung entsteht erst nach geprüfter Mietzahlung und verifiziertem Checkout-Ereignis; ein Browser-Rücksprung reicht nicht aus. [Stripe: Charge-Kartendetails](https://docs.stripe.com/api/charges/object)
- Die Kartenautorisierung ist zeitlich begrenzt. Der Stripe-Wert `payment_method_details.card.capture_before` muss über den geplanten Rückgabezeitpunkt zuzüglich **24 Stunden Sicherheitsabstand** reichen; reicht er nicht, darf die Miete nicht eingezogen und die Buchung nicht automatisch bestätigt werden. Fern in der Zukunft liegende oder lange Mieten sind mit einer sofortigen Kartenreservierung nicht generell möglich. Eine längere Autorisierung ist nur bei entsprechender Stripe-/Kartennetz-Berechtigung möglich und nicht vorauszusetzen. [Stripe: Autorisierung und Gültigkeit](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method), [verlängerte Autorisierung](https://docs.stripe.com/payments/extended-authorization)
- Der lokale Fahrzeug-Hold startet mit 35 Minuten. Nach der ersten serverseitig verifizierten Kreditkartenautorisierung wird seine Frist einmalig auf mindestens 60 Minuten ab diesem Zeitpunkt verlängert, solange noch kein Miet-Checkout-Versuch existiert. Für eine neue Stripe-Checkout-Session werden mindestens 31 Minuten Restlaufzeit verlangt; damit bleiben nach der Kartenbestätigung ungefähr 29 Minuten für den Übergang zur Mietzahlung. Ein abgelaufener Hold ohne Miet-Session wird vom regelmäßigen Abgleich erst nach bestätigter Stornierung des Karten-PaymentIntents freigegeben. Bei unklarem Erzeugungsausgang von Kartenautorisierung oder Checkout bleibt das Fahrzeug zur manuellen Klärung gesperrt.
- Bei Storno, abgebrochenem Miet-Checkout und nach dokumentierter ordnungsgemäßer Rückgabe ist die ungenutzte Kartenautorisierung durch Stornierung des PaymentIntents freizugeben und der Providerstatus abzugleichen. Das ist **keine Rückerstattung**, weil die 500 € nicht eingezogen wurden. Unklare Zustände benötigen manuelle Prüfung; Schäden werden nicht automatisch von der Kaution abgebucht. [Stripe: PaymentIntent stornieren](https://docs.stripe.com/api/payment_intents/cancel)
- Es gibt **keinen Mindestvorlauf**: Buchung und persönliche Abholung am selben Tag sind möglich, wenn das Fahrzeug frei ist und ein gültiger künftiger Übergabeslot angeboten wird. Der Betreiber hat Abholung und Rückgabe **montags bis samstags stündlich von 08:00 bis einschließlich 20:00 Uhr** und **höchstens fünf berechnete Miettage** für die Direktbuchung bestätigt. Die Werkstatt kann einzelne persönliche Termine in der geschützten Portalverwaltung schließen und wieder öffnen. Verstreicht der gewählte Abholzeitpunkt oder wird der Slot während des Online-Ablaufs geschlossen, wird kein neuer Miet-Checkout mehr angeboten; eine bereits eingegangene oder unklare Zahlung geht zur manuellen Prüfung statt in eine automatische Buchungsbestätigung. Das Auto wird vollgetankt übergeben und vollgetankt zurückgegeben. Die 1.000 € Kunden-Selbstbeteiligung beweisen keine Versicherungsdeckung.
- Der Nutzer hat die Regel nun ausdrücklich bestätigt: **bis einschließlich 48 Stunden vor Abholung kostenlos stornieren; danach 10 % ausschließlich vom Mietpreis**, niemals von der Kaution. Der neue Quote speichert einen eindeutigen Policy-Kennwert, der mit dem Vertragssnapshot unterschrieben wird. Bereits unterschriebene ältere Buchungen ohne diesen Kennwert behalten ihre ursprüngliche 24-Stunden-/erster-Miettag-Regel. Für pauschalierten Ausfallersatz müssen ersparte Aufwendungen und anderweitige Vermietung angerechnet werden; der Kunde darf einen fehlenden oder wesentlich geringeren Schaden nachweisen. Die endgültige Klausel benötigt vor Veröffentlichung die rechtliche Freigabe. [§ 309 Nr. 5 und 6 BGB](https://www.gesetze-im-internet.de/bgb/__309.html), [§ 537 BGB](https://www.gesetze-im-internet.de/bgb/__537.html)

Die frühere Implementierung zog Miete und Kaution zusammen im Checkout ein und erstattete die Kaution später. Historische Buchungssnapshots und Tests können diesen Ablauf noch enthalten; für neue Livebuchungen ist ausschließlich `card_authorization_at_booking` vorgesehen. Gemeinsamer Portalbestand, signierter Checkout-Webhook, unveränderlicher Vertragssnapshot und idempotente Mietpreiserstattungen bleiben Teil des Entwurfs. Der Produktivbetrieb verlangt geprüfte Konfiguration, sicheren dauerhaften Flask-Schlüssel, HTTPS und die autoritative PostgreSQL-Datenbank.

## Noch fehlende Tatsachen und Abnahme – aktuell NO-GO für öffentliche Zahlung

| Freigabe | Aktueller Stand / benötigter Beleg |
|---|---|
| Versicherung beider konkreten Autos | Schriftliche Bestätigung der öffentlichen entgeltlichen Selbstfahrervermietung für i10 **und** KONA fehlt. Eine Police für einen Hyundai beweist nicht beide Zuordnungen oder diese Nutzung. Vertrags-Selbstbeteiligung und Versicherer-Selbstbehalt nicht gleichsetzen. |
| Reale Flotte und Kalender | Portal-Zuordnung inzwischen read-only gefunden (Details lokal im Hub). KONA-Stammdaten wurden in Aufgabe76 anhand der dokumentierten Übernahme und Kennzeichen/FIN korrigiert; die Mietwagennutzung ist laut neuer Quellenprüfung des Ursprungstasks in der angenommenen Leasingvereinbarung dokumentiert. Die wiederkehrenden Übergabezeiten sind entschieden und technisch vorbereitet. Ob die beiden Fahrzeuge zu jedem angebotenen Termin tatsächlich frei und übergabebereit sind, muss der vollständige betriebliche Kalender abbilden; individuelle Schließungen und Aufbereitungspuffer bleiben zu pflegen. |
| Betrieb / Preise / Bedingungen | i10 39 €/Tag, KONA 59 €/Tag beziehungsweise ab drei berechneten Miettagen 49 €/Tag **für alle gebuchten Tage**, 500 € Kaution, 1.000 € vertragliche Selbstbeteiligung, kein Mindestvorlauf, Montag bis Samstag stündlich 08:00–20:00 Uhr einschließlich, höchstens fünf berechnete Miettage, voll/voll sowie 48 Stunden kostenlos und danach 10 % nur des Mietpreises sind entschieden. Die veröffentlichten 150 km/Tag und 0,25 €/Mehr-km, die 24-Stunden-Tagesberechnung, Schaden-/Rückgabeablauf, Puffer, Erreichbarkeit außerhalb der Übergabezeiten und vollständige Mietbedingungen benötigen noch eine abschließende Betreiber- und Rechtsfreigabe. |
| Kaution | 500 € bei der Online-Reservierung auf Kreditkarte autorisieren, nicht einziehen. Der lokale Stripe-Test belegte Autorisierung ohne Einzug und Freigabe bei Storno. Freigabe nach dokumentierter Rückgabe und weitere Karten-/Fristfälle sind noch separat abzunehmen. Begrenzte Autorisierungsdauer schließt ungeeignete lange oder weit vorausliegende Mieten aus. |
| Stripe / Hosting | Der gemeinsame lokale Stripe-/PostgreSQL-Browserlauf belegte Unterschrift vor Zahlung, 500 € Kreditkartenautorisierung ohne Einzug, automatische Weiterleitung zum Hosted Checkout, 39 € Mietzahlung, signierten Webhook, genau einen Mietvorgang, Vertrags-PDF, Storno mit 3,90 € Gebühr und 35,10 € Erstattung sowie einen gescheiterten 3-D-Secure-Fall; [Einzelheiten](mos-staging-abnahme.md). Der zuvor deaktiviert bereitgestellte Code wurde auf dem tatsächlichen Render-Dienst `kundenstatus-app` mit `/healthz` 200 und `/mieten/` 404 geprüft. Für die Änderungen dieses Pakets ist der Deploymentstand nach dem Push erneut zu prüfen. Restricted-Key-Rechte, dauerhafter Live-Webhook, periodischer Abgleich mit Alarm und Hosting-/Live-Zahlungsabnahme fehlen; [Render-Betriebsanleitung](mos-render-betrieb.md). Keine Livezahlung durchgeführt. |
| Datenbank / Abnahme | SQLite-Konkurrenz-/HTTP-/Erstattungstests, PostgreSQL-Kaltstart und vollständiger isolierter PostgreSQL-/Stripe-HTTP-Signierlauf mit synthetischen Daten bestanden. Die Deployment-Abnahme des deaktivierten Codes ist erfolgt; ein echter Live-Zahlungsdurchlauf nach allen Freigaben steht aus. |
| Recht / Datenschutz | Finale Mietbedingungen und konkrete Versicherungsklausel rechtlich prüfen. Vor Livebetrieb den tatsächlich angezeigten Hosted Checkout einschließlich letzter Zusammenfassung und Zahlungsschaltfläche nach [§ 312j BGB](https://www.gesetze-im-internet.de/bgb/__312j.html) abnehmen. Der Buchungseinstieg nennt die Zahlungsarten; die Preisübersicht erklärt Korrektur und Ablauf und lässt sich mit den Bedingungen vor der Unterschrift speichern. Die unverzügliche elektronische Eingangsbestätigung nach [§ 312i BGB](https://www.gesetze-im-internet.de/bgb/__312i.html) ist damit noch nicht als erfüllt nachgewiesen. Eine idempotente Vertrags-E-Mail-Outbox und ein gesperrter Versandstarter sind vorbereitet; ein dauerhaft laufender, überwachter Mailworker und die rechtzeitige Zustellung vor Abholung fehlen weiterhin nach [§ 312f Abs. 2 BGB](https://www.gesetze-im-internet.de/bgb/__312f.html). Details: [Vertragszustellung](mos-contract-delivery.md). Die mögliche Pflicht zu einem öffentlichen [Kündigungsbutton nach § 312k BGB](https://www.gesetze-im-internet.de/bgb/__312k.html) für befristete Online-Mieten rechtlich klären; der vorhandene sitzungsgebundene Storno-Knopf ist dafür nicht nachweislich ausreichend. Ein [schreibgeschützter Bericht](mos-unterschriften-aufbewahrung.md) zu abgebrochenen Unterschriften ist vorbereitet; Aufbewahrungsfrist, Datenschutzhinweis und tatsächliche Löschung bleiben offen. Der Stripe-Datenfluss steht in der Datenschutzerklärung, die Ergänzung ist rechtlich ungeprüft. Entwurf nicht als freigegebene AGB einsetzen. |

Eine öffentliche Zahlung ist damit noch nicht freigegeben. Codevorbereitung und Testzahlungen ersetzen diese Nachweise nicht. Laut verifiziertem Bericht des Ursprungstasks wurde die Versicherungsanfrage am 23.09.2026 um 10:57 über IONOS gesendet; eine Antwort oder Deckungsbestätigung liegt weiterhin nicht vor.

## Konfiguration ohne Geheimnisse im Repository

`MOS_BOOKING_CONFIG_FILE` verweist im Portalprozess auf eine private JSON-Konfiguration. Standard ohne Datei ist deaktiviert. Ein Beispiel mit absichtlich fehlenden Freigaben:

```json
{
  "enabled": false,
  "mode": "live",
  "live_enabled": false,
  "origin": "https://kundenstatus-app.onrender.com",
  "fleet": {
    "kona": {"id": 3, "expected_name": "Hyundai Kona 1.6 T-GDI 132 kW N Line X DCT (Leasing)", "daily_cents": 5900, "discount_after_days": 3, "discount_cents": 4900},
    "i10": {"id": 1, "expected_name": "Hyundai i10", "daily_cents": 3900}
  },
  "slots": [],
  "weekly_handover": {"timezone": "Europe/Berlin", "weekdays": [0, 1, 2, 3, 4, 5], "first_hour": 8, "last_hour": 20, "step_minutes": 60},
  "day_rule": "elapsed_24h_ceil",
  "max_days": 5,
  "included_km_day": 150,
  "extra_km_cents": 25,
  "vat_included": true,
  "deposit_cents": 50000,
  "deductible_cents": 100000,
  "deposit_method": "card_authorization_at_booking",
  "cancellation_policy": "free_48h_then_10pct_rent",
  "terms_version": "draft:noch-nicht-freigegeben",
  "terms_text": "",
  "privacy_url": "https://kundenstatus-app.onrender.com/datenschutz",
  "merchant_name": "Gärtner GmbH Karosserie + Lack",
  "merchant_address": "Binauer Höhe 4, 74821 Mosbach, Deutschland",
  "merchant_email": "info@auto-lackierzentrum.de",
  "merchant_phone": "+49 1522 7706694",
  "launch": {}
}
```

Diese JSON-Datei ist bewusst **nicht startfähig**: Bedingungen und Freigaben fehlen. Die Fahrzeug-IDs und exakten Modellnamen wurden am 24.09.2026 im produktiven Adminportal nur lesend geprüft: i10 ID 1, KONA ID 3; beide dort als verfügbar angezeigt. Das ist keine künftige Verfügbarkeitszusage. KONA-Staffeltarif, Fünf-Tage-Grenze und die genannten Übergabezeiten sind vom Betreiber bestätigt. Die 150 km/Tag und 0,25 €/Mehr-km sind veröffentlicht, aber noch nicht als verbindliche Direktbuchungswerte bestätigt. Die Mobilnummer **+49 1522 7706694** hat der Betreiber als Storno- und Notfallkontakt genannt; eine durchgängige Erreichbarkeit außerhalb der Übergabezeiten ist nicht bestätigt. `weekly_handover` bietet im rollenden Sieben-Kalendertage-Fenster einschließlich heute nur zukünftige persönliche Termine an; `slots` enthält zusätzliche manuelle Termine. Die zwei für eine Preisübersicht ausgewählten Wochenzeiten werden für die nachfolgenden Buchungs- und Zahlungssperren als einzelne Zeilen gespeichert. Deshalb können solche noch künftigen Termine auch nach einer späteren Deaktivierung der Wochenregel offen bleiben und müssen bei Bedarf im Adminportal geschlossen werden. Die Werkstatt kann einzelne Zeiten unter `/mieten/admin/termine` schließen und wieder öffnen, auch vor Aktivierung der Kundenbuchung. Geschlossene Termine bleiben nach Neustart geschlossen. Kein pauschaler Mindestvorlauf: Ein noch künftiger Slot kann auch heute liegen. Die tatsächliche Kreditkarten-Autorisierungsfrist wird vor der Mietzahlung bei Stripe geprüft; ein angebotener Termin ist keine Garantie, dass die Karte für den gesamten Mietzeitraum geeignet ist. Eine neue Konfigurationsversion erfordert Prozessneustart; bestehende Buchungen behalten ihren gespeicherten Snapshot. `merchant_name` und `merchant_address` müssen genau den Impressumsdaten des Vermieters entsprechen. „Autovermietung MOS“ bleibt nur die Angebotsüberschrift.

Unter `launch` benötigt jede der Freigaben `business_review`, `legal_review`, `finance_review`, `privacy_review`, `sandbox_acceptance`, `postgres_acceptance` und `contract_delivery_acceptance` die Felder `approved_by`, `approved_at`, `evidence`. Die neue Zustellfreigabe darf erst nach eingerichtetem, überwachtem Versandjob und nachgewiesener rechtzeitiger Vertragskopie vor Übergabe dokumentiert werden. Unter `launch.insurance.kona` und `.i10` sind `verified=true`, `use=paid_self_drive`, eine echte Belegreferenz und die passende `vehicle_id` nötig. Das sind dokumentierte Betreiberprüfungen, keine automatischen Versicherungsnachweise. Niemals bloß zur Umgehung der Sperre ausfüllen.

Stripe-Konfiguration im Hosting-Secretstore: `MOS_STRIPE_TEST_KEY` oder `MOS_STRIPE_LIVE_KEY` für **serverseitige** API-Aufrufe, `MOS_STRIPE_WEBHOOK_SECRET` für die Signaturprüfung und `MOS_STRIPE_PUBLISHABLE_KEY` für Stripe.js/Payment Element im Browser. Der Publishable Key muss zum Modus passen (`pk_test_` oder `pk_live_`) und ist technisch kein Geheimnis; ihn dennoch zur richtigen Umgebung konfigurieren. Für Server-Keys werden eingeschränkte `rk_test_`/`rk_live_` oder `sk_test_`/`sk_live_` unterstützt. Eingeschränkte Rechte bevorzugen; keine Server- oder Webhook-Secrets in JSON, Chat oder Git. Test- und Live-Schlüssel strikt trennen. Das Webhook-Secret ist vom API-Key unabhängig. [Stripe: Schlüsseltypen](https://docs.stripe.com/keys), [Payment Element und Bestätigung](https://docs.stripe.com/js/payment_intents/confirm_payment)

## Exakte Stripe-Schnittstellen

| Zweck | Methode / REST-Pfad |
|---|---|
| 500-€-Kartenautorisierung anlegen | `POST /v1/payment_intents` mit `capture_method=manual`, `payment_method_types[]=card` |
| Autorisierung/Frist prüfen | `GET /v1/payment_intents/{id}` mit aktueller Charge und `capture_before` |
| Ungenutzte Autorisierung freigeben | `POST /v1/payment_intents/{id}/cancel` |
| Checkout erzeugen | `POST /v1/checkout/sessions` |
| Status erneut prüfen | `GET /v1/checkout/sessions/{id}` |
| Offenen Checkout beenden | `POST /v1/checkout/sessions/{id}/expire` |
| Mietpreis bei Storno erstatten | `POST /v1/refunds` mit `payment_intent`, `amount`, Idempotency-Key |
| Erstattungsstatus | `GET /v1/refunds/{id}` |

Berechtigungen zunächst gezielt **Payment Intents schreiben/lesen** einschließlich Stornierung, **Checkout Sessions schreiben/lesen** und **Refunds schreiben/lesen** konfigurieren; die Leseberechtigung für die expandierte Charge und tatsächliche Stripe-Rechteabhängigkeiten mit einem Restricted-Testkey verifizieren. Alle nicht benötigten Ressourcen auf None, insbesondere keine Auszahlungs-, Transfer-, Konto- oder Schlüsselverwaltungsrechte. Das Portal zeigt ein Stripe Payment Element; rohe Kartendaten werden nicht im Portalserver gespeichert oder verarbeitet. Für die neue Kaution wird **kein Refund** erzeugt; die Freigabe erfolgt durch Stornieren des ungenutzten PaymentIntents. Eine Schadenbelastung durch Capture ist kein Bestandteil des automatischen Rückgabeablaufs. [PaymentIntent-API](https://docs.stripe.com/api/payment_intents/create), [Checkout-API](https://docs.stripe.com/api/checkout/sessions/create), [Erstattungs-API](https://docs.stripe.com/api/refunds/create)

Geplante produktive Webhook-Adresse nach Deployment auf der konfigurierten Origin: **https://kundenstatus-app.onrender.com/mieten/webhook**. Sie ist derzeit nicht als live verfügbar bestätigt. Testmodus: `{staging-origin}/mietwagen-test/webhook`. Der aktuelle Buchungscode verarbeitet signiert `checkout.session.completed`, `checkout.session.expired`, `checkout.session.async_payment_succeeded` und `checkout.session.async_payment_failed`. Die separate Kaution wird zusätzlich serverseitig über die PaymentIntent-API abgeglichen; ein Browser-Erfolgssignal ist kein Autorisierungsnachweis. Refund-Status wird über die Refund-API abgeglichen; keine ungeprüften Refund-Webhooks als Erfolg übernehmen. [Stripe: Webhooks](https://docs.stripe.com/webhooks)

Der Refund-Datensatz enthält laut API kein garantiertes `livemode`-Feld; Modus wird durch getrennten Gateway/Key, die zuvor verifizierte Session/Payment-Intent-Zuordnung und die erneut gelesene Refund-Antwort abgesichert. [Stripe: Refund-Objekt](https://docs.stripe.com/api/refunds/object)

## Deployment- und Betriebsablauf zur Freigabe

1. Änderungen reviewen; vollständige Dateien einschließlich `mos_booking/`, Templates und Stripe-Abhängigkeit übernehmen. Datenbankschema, Migration/Backup und Rollback auf separater PostgreSQL-Testinstanz prüfen. Neue Livebuchungen dürfen nie auf das historische Kautionseinzug-Verfahren zurückfallen.
2. Vor Freigabe den vollständigen Mietvertrag samt bestätigter 48-Stunden-/10-%-Stornoregel, Schäden, voll/voll und Kautionsfreigabe rechtlich und betrieblich abnehmen. Neue und ältere Stornoregel anhand des gespeicherten Vertragssnapshots getrennt prüfen. Die bestätigten Übergabezeiten und Fünf-Tage-Grenze mit dem realen Fahrzeugkalender und dem tatsächlichen Karten-Zeitfenster abnehmen. Ohne künftigen Slot keine gleichentägige Abholung anbieten.
3. Staging auf getrennter Portal-Testdatenbank mit Stripe-Testschlüsseln und passendem `pk_test_`-Schlüssel. Der vorhandene öffentliche Testmodus verlangt eine SQLite-Datei mit Endung `.mos-public-test.sqlite3`; den PostgreSQL-Sperrpfad zusätzlich separat abnehmen. Vertragsunterschrift → 500-€-Kartenautorisierung → ausschließlich Mietpreis im Checkout → signierter Webhook → bestätigte Buchung vollständig durchspielen. Prüfen: 3-D-Secure/Abbruch, Kredit- gegenüber Debit-/Prepaid-Karte, unzureichendes `capture_before`, zu weit entfernte Rückgabe, parallele Reservierungen, verspätete und doppelte Events, Checkout-Timeout, Zahlung/Storno-Rennen, Freigabe bei Storno und nach Rückgabe, unklare Stripe-Antworten. Der Kartenhold muss ohne Vor-Ort-Schritt entstehen.
4. Auf geeigneten Geräten Kartenautorisierung und gesonderten Miet-Checkout, Stripe-Rechte, Beträge und finalen zahlungspflichtigen Button abnehmen. Kartenhold und Mietzahlung im Stripe-Dashboard getrennt prüfen: Kaution als **nicht eingezogene** Autorisierung, Mietpreis als Zahlung. Wallets nur für den Miet-Checkout prüfen; die Kaution verlangt eine Kreditkarte. Ein Test mit einem simulierten Gateway beweist keine echte Stripe-Zustellung.
5. Nach externen Nachweisen für **beide** Fahrzeuge und vollständiger Abnahme private Livekonfiguration, sicheren dauerhaften Flask-Schlüssel, passende Keys/Webhook-Signatur und HTTPS setzen. Das Portal läuft auf dem autoritativen PostgreSQL-Bestand, nicht auf einer zweiten Homepage-Datenbank. Der Nutzer hat echte Zahlung beauftragt; das ersetzt keinen Versicherungs- oder Integrationsnachweis. `enabled` und `live_enabled` bleiben bis dahin false. Den öffentlichen Link `MOS_PUBLIC_LIVE_ENTRY_URL=https://kundenstatus-app.onrender.com/mieten/` erst nach erfolgreicher Liveprobe einblenden.
6. Regelmäßig `python -m flask --app app mos-booking-reconcile` ausführen und die Admin-Prüfliste überwachen. Abgebrochene oder stornierte Fälle brauchen eine verifizierte Freigabe der Kartenautorisierung; bei Rückgabe muss zuerst der Fahrzeugzustand protokolliert und danach die Freigabe ausgeführt werden. Providerfristen und ungeklärte Autorisierungen täglich prüfen. Mietpreiserstattungen separat abgleichen. Zusätzlich den getrennten `mos-contract-delivery`-Worker auf derselben Datenbank eng getaktet und mit Alarm betreiben; bei kurzfristiger Abholung den Zustellstatus vor Übergabe prüfen. Beide Betriebsjobs sind nur vorbereitet und noch nicht auf Render eingerichtet. Ein manueller Testlauf ersetzt keinen dauerhaften Betrieb.
7. Adminansicht: `/mieten/admin` und Terminverwaltung `/mieten/admin/termine` (Test entsprechend unter `/mietwagen-test/`), geschützt durch bestehenden Admin-Login/CSRF. Die Terminverwaltung ist auch bei ausgeschalteter Kundenbuchung erreichbar, damit künftige persönliche Übergaben vor dem Launch eingetragen werden können. Kundenstatus und unterschriebene PDF-Kopie sind sitzungsgebunden. Bei verlorenem Browserzugang hilft die Werkstatt nach Identitätsprüfung mit der gespeicherten Vertragskopie. Ein Versandauftrag wird bei echter bestätigter Buchung vorbereitet; ohne explizit aktivierten und überwachten Worker wird keine E-Mail verschickt.
8. Bei Störung `enabled=false`: keine neuen Buchungen, vorhandene Holds bleiben bis zu bestätigter Zahlungs-/Autorisierungslage gesperrt; Webhook, Status, Freigabe und Erstattung müssen weiterlaufen. Keine Datenbankzeilen löschen oder unklare Zahlungszustände blind freigeben. Hinter einem Proxy die vertrauenswürdig konfigurierte Client-IP prüfen: die persistente Rategrenze nutzt `remote_addr`, nicht ungeprüfte Forwarded-Header.

## Historische Verifikation des früheren Einzug-plus-Erstattungspfads

Die damals 13 neuen Produktionslogiktests betrafen fehlende Freigaben, zwei erlaubte Fahrzeuge, Restricted-Key-Modus, **eingezogene** getrennte Miet-/Kautionsposten, die alte 24-Stunden-Stornogrenze, Minderungen, Nichterscheinen, Refund-Wiederholung nach Timeout, Sperre alter unklarer Aufträge, parallele Stornierungen, Kautionsrückzahlung und Adminschutz. Externe Sockets waren gesperrt; sämtliche Live-API-Objekte synthetische Mocks. Diese Ergebnisse sind kein Nachweis für die neue separate Kartenautorisierung, tatsächliche Stripe-Zustellung oder Versicherung. Der neue Ablauf braucht eigene Sandbox-, Datenbank- und HTTP-Abnahme.


Die folgenden Abschnitte dokumentieren abgeschlossene Zwischenstände. Wo sie noch den Einzug und die Rückzahlung der Kaution nennen, beschreiben sie **nicht** den oben festgelegten aktuellen Zielablauf.

## Abschlussprüfung am 23.09.2026

Der ursprüngliche Bearbeiter hat die Arbeit gestoppt. Nach Ablauf der alten Lease wurde Aufgabe 69 ohne konkurrierenden Claim für die Abschlussprüfung übernommen. Erneut bestanden: 13 Produktions-, 8 Public-, 21 Bestands- und 28 Prototyptests (70 insgesamt), Syntaxprüfung und `git diff --check`. Die vorhandenen Smoke-/Flow-Protokolle enden erfolgreich; diese beiden Läufe wurden bei der Abschlussprüfung nicht wiederholt. Tests laufen mit `.agent-hub/booking-venv/Scripts/python.exe`; das normale Python scheitert derzeit am fehlenden Stripe-Modul.

Vor Deployment zusätzlich separat erledigen: Stripe-Abhängigkeit in den tatsächlichen Hosting-Installationspfad aufnehmen (aktuell nur `requirements-mos-booking.txt`), den historischen C3-Datensatz aus dem Test-Seeder entfernen und ältere Drei-Fahrzeug-Dokumentation auf die aktuelle Zweierflotte korrigieren. Der öffentliche Selector schließt C3 bereits aus. Diese Dateien liegen außerhalb von Aufgabe 69 und wurden bei der Abschlussprüfung nicht geändert. Vorhandene Preview-Prozesse können älteren Code zeigen und gelten nicht als Live-Abnahme. Alle Änderungen sind uncommitted; kein Push, Deployment oder echter Zahlungsauftrag.


## Folgeaufgabe 70 – reguläres Setup und Zweier-Testflotte

Die zuvor dokumentierten Setup-/Seed-Restpunkte sind behoben: `requirements.txt` bindet `requirements-mos-booking.txt` ein; damit installiert auch der vorhandene Render-Build die identischen Flask-/Stripe-Versionen. Reguläres Setup: `python -m pip install -r requirements.txt`. Die kleine Datei bleibt als eigenständiges Minimalsetup für den isolierten Prototyp nutzbar; für den Portal-Teststarter ist das vollständige Setup erforderlich. Installation und Offline-Tests benötigen keine Stripe-Secrets und aktivieren keinen Livebetrieb.

Der öffentliche Test-Starter erzeugt ausschließlich zwei synthetische Datensätze (i10/KONA), mit lokal erzeugten IDs. Alte Prototyp-/Anfrageprüfungen sind ausdrücklich historische Berichte; aktuelle Test- und Bedingungsdokumentation verwendet die Zweierflotte. Reale IDs, Live-Freigaben und externe Abnahmen bleiben unverändert offen.

Verifikation Aufgabe 70: vollständiges `pip install -r requirements.txt` in neuer lokaler Test-venv mit vorhandenen Systempaketen erfolgreich (kein vollständig leeres Build-Abbild); Stripe-/Buchungsmodule ohne Secrets importiert. 8 Public-Tests einschließlich exakter Zweier-Seed-Prüfung und 13 Produktionslogiktests bestanden; `git diff --check` bestanden. Kein echter Stripe-/PostgreSQL-/Hostinglauf.


## Aufgabe 72 – reale Bestandsprüfung und Zulaufsperre

Read-only im Portal: Beide Fahrzeugdatensätze gefunden. KONA ist trotz verfügbar wirkender Übersicht im Grundzustand noch „In Kürze verfügbar“; tatsächliche Übergabe und Zulassung sind nicht nachgewiesen. Die leasingseitige Mietwagennutzung wurde nachträglich anhand der Annahmebestätigung belegt (siehe Korrektur unten). i10-Portalpreis und geplanter öffentlicher Tarif weichen voneinander ab. Der geprüfte KONA-Vertrag ist ausdrücklich gesperrter Entwurf ohne gespeicherte Version. Keine konkreten freigegebenen Online-Zeitfenster festgestellt. Details bleiben wegen Betriebsdaten lokal in `.agent-hub/mos-bestandspruefung-20260923.md`.

Die neue Buchungslogik lehnt Zulauffahrzeuge jetzt bei Angebot und Reservierung ab. Wechsel auf Zulauf nach Checkout führt bei Zahlung zur manuellen Prüfung statt automatischer Vermietung. Zwei Regressionen vor Änderung fehlgeschlagen; danach 23 Bestands-,10 Public- und13 Produktionslogiktests bestanden (46), diff-check bestanden. Der alte Anzeigeeffekt im Admin wurde nicht im Rahmen dieses Claims geändert; separate Folgeaufgabe erforderlich. Kein Portal-Datensatz geändert, keine Livekonfiguration aktiviert.


## Aufgabe 73 – korrigierter Leasingnachweis

Der Ursprungstask hat in IONOS die Hyundai-Leasing-Annahmebestätigung vom 09.07.2026 geprüft: Anhang `Annahmebestaetigung.pdf`, Seite 3, Allane SE als Leasinggeber, passendes KONA-Modell und ausdrücklich „Fahrzeugnutzung: Mietwagen (ohne Carsharing)“. Eine Nachricht des Hyundai-Kundenservice vom 24.07.2026 bestätigt die Vertragsaktivierung. Die angenommene Mietwagennutzung ist damit dokumentiert; „fehlende leasingseitige Mietwagenfreigabe“ wird nicht länger als offener Nachweis geführt. Diese Korrektur beruht auf dem Quellenbericht des Ursprungstasks; in dieser Folgeaufgabe wurde das PDF nicht erneut geöffnet.

Das belegt weder BGV-Deckung noch tatsächliche Fahrzeugübergabe oder Zulassung. KONA bleibt im Portal auf Zulauf mit Platzhalterkennzeichen und fehlender FIN. Diese Stammdaten, Versicherungsdeckung beider Wagen, vollständiger Kalender/Slots, finale Bedingungen sowie Stripe-/PostgreSQL-End-to-End-Abnahme bleiben offen. Kein Live-Gate wurde geändert. Die BGV-Anfrage wurde laut Ursprungstask am 23.09.2026 um 10:57 über IONOS versandt und im Gesendetordner geprüft; eine Deckungsbestätigung liegt weiterhin nicht vor.


## Aufgabe74 – echte lokale PostgreSQL-Prüfung

Eine getrennte lokale PostgreSQL17.11-Testinstanz wurde eingerichtet. 23 Bestands-/Konkurrenz-/Rollback-/Refundtests mit synthetischem Anbieter bestanden; Cluster anschließend gestoppt. Kein echter Stripe-Lauf: sicher hinterlegte Testschlüssel/Webhook fehlen. Leere Portalinitialisierung hat eine DDL-Reihenfolgelücke; der Testbootstrap ordnet das synthetische Schema vorab. Details und Grenzen: [Staging-Abnahme](mos-staging-abnahme.md). Keine Livefreigabe.


## Aufgabe75 – PostgreSQL-Kaltstart behoben

Die in Aufgabe74 gefundene Vorwärtsreferenz ist durch korrektes Anlegen von `auftraege` vor abhängigen Tabellen behoben. Der Test-Runner verwendet keinen Schema-Workaround mehr: Leere PostgreSQL-Datenbank direkt initialisiert,23 Tests bestanden. Smoke/Flow auf isolierten SQLite-Daten, Syntax/diff ebenfalls bestanden. Cluster anschließend gestoppt. Echte Stripe-Sandbox weiterhin mangels sicher hinterlegter Testzugänge nicht ausgeführt.


## Aufgabe76 – KONA-Stammdaten korrigiert

Der Ursprungstask hat zusätzlich die Vertragsaktivierung vom24.07.2026 mit dokumentierter Übernahme am17.07.2026, Kennzeichen und FIN geprüft. Modell und Vertragsreferenz stimmten mit Fahrzeug3 im Portal überein. Kennzeichen, FIN, Grundzustand und Quellennotiz wurden dort autorisiert gespeichert und anschließend erneut gelesen. Die frühere Zulauf-/fehlende-Stammdaten-Feststellung ist damit überholt. Die Notiz hält fehlende BGV-Deckung und Online-Freigabe ausdrücklich fest. Tarif, Checkout-Gates und Vertragsfreigabe wurden nicht geändert. Die gespeicherten Fahrzeugdetails bleiben lokal im Hub.


## Aufgabe77 – Nutzerentscheidung und Releaseprüfung

Nutzer bestätigt i10 39€/Tag,500€ rückzahlbareKaution und1.000€ vertraglicheSelbstbeteiligung; BGV-Klärung soll nachgelagert erfolgen. i10-Portalpreis wurde von30 auf39€ korrigiert und geprüft. Damit entfällt die bisherige Tarifabweichung. Eine Versicherungsdeckung wird nicht als belegt markiert.

Stripe-Testmodus bildet jetzt Miete und500€ Testkaution als zwei getrennte Checkout-Positionen ab; Tests verifizieren647€ Gesamtbetrag für drei KONA-Tage (147€+500€) und ausgeschalteten Livemodus. Das ersetzt keine echte Stripe-Zustellung. Eingeschränkter Testschlüssel/Webhook fehlen weiterhin; gesonderte Schlüsselfreigabe angefragt. Der Code bleibt ohne explizite Zahlungskonfiguration deaktiviert.

## Aufgabe80 – Unterschrift vor Zahlung und Livezugang

Die Reihenfolge ist nun Vertragsübersicht mit konkretem Fahrzeug, Vermieter, Preis und Bedingungen → gezeichnete Unterschrift mit ausdrücklicher Bestätigung → Reservierung und Checkout. Der Server prüft Unterschrift und gebundene Vertragsfassung vor der Reservierung und bei jedem erneuten Öffnen des Checkout. Vertragsdaten, Signaturbild und Zeitpunkt sind in einem separaten SHA-256-Nachweis miteinander verbunden. Erst nach bestätigter Zahlung entsteht die unveränderliche PDF-Kopie; offene oder abgebrochene Zahlungen werden nicht als bestätigte Buchung ausgegeben. Einfache gezeichnete Unterschrift und Hash sind kein qualifiziertes Signaturverfahren. Vorhandene alte Vertragskopien werden weiterhin lesbar angezeigt.

Lokale Verifikation: 16 öffentliche Tests, 13 Produktionslogiktests, 23 gemeinsame Bestands-/Konkurrenztests und 28 Prototyptests bestanden; Smoke- und Flow-Test ebenfalls. Ein synthetisches Vertrags-PDF wurde gerendert und visuell auf beide Seiten geprüft. Das belegt weder einen echten Stripe-Lauf noch den Remote-Deploy.

Bei der Liveprüfung antworteten `https://kundenstatus-app.onrender.com/mieten/` und `/mietwagen-test/` mit 404. Stripe ist im Browser im Live-Modus angemeldet, aber ein für diesen Dienst nutzbarer Server-Key/Webhook wurde nicht gefunden. Die Render-Verwaltung war nicht angemeldet. Es wurde kein Secret erzeugt, übertragen oder als Platzhalter eingetragen. Zur Liveaktivierung fehlen weiterhin der verwaltete Secret-Zielort, finaler Bedingungstext und Betreiberfreigaben, bestätigte Nutzung/Deckung für beide konkreten Fahrzeuge sowie die echte Testzahlung und Webhook-Abnahme. Die sichtbare Stripe-Kontobezeichnung und der im Impressum genannte Vermieter sind vor Livebetrieb auf die rechtliche Firmierung abzugleichen. Der Wunsch, die BGV-Klärung nachzuholen, wurde dokumentiert; er ist kein Nachweis für bestehende Deckung.

## Aktueller Nachtrag vom 23.09.2026 – Kaution nur autorisieren

Nach dem damaligen Unterschrift-vor-Zahlung-Stand hat der Nutzer den Kautionsablauf ausdrücklich geändert: 500 € sofort bei der Online-Reservierung auf der Kreditkarte reservieren, **nicht abbuchen**, und vor Ort keine Zahlung oder Kartenbestätigung verlangen. Der Mietpreis wird erst nach der Vertragsunterschrift online bezahlt. Diese Umstellung ersetzt für neue Buchungen den in Aufgabe 77 und 80 beschriebenen Kautionseinzug mit späterer Rückerstattung. Die bereits gespeicherten alten Vertragssnapshots werden dadurch nicht rückwirkend umgedeutet.

Ebenfalls entschieden sind kein Mindestvorlauf bei freiem Fahrzeug und künftigem Übergabeslot sowie vollgetankte Übergabe und vollgetankte Rückgabe. **Später im Gespräch wurde Storno bis einschließlich 48 Stunden vor Abholung kostenlos und danach 10 % nur des Mietpreises ausdrücklich bestätigt.** Die technischen Änderungen für den getrennten PaymentIntent und Miet-Checkout sowie die versionierte Stornoregel sind im Arbeitszweig umgesetzt. Der lokale Stripe-Test des Kernablaufs ist abgeschlossen; Live-Key/Webhook, weitere Staging-/Hosting-Prüfungen und eine Betreiber-/Versicherungsfreigabe fehlen. Der Live-Schalter bleibt aus.

## Aktueller Nachtrag vom 23.09.2026 – lokaler Stripe-Testmodus

Eine Testbuchung für den i10 zu 39 € wurde nach digitaler Unterschrift mit
500 € Kreditkartenautorisierung ohne Einzug, separatem bezahlten Checkout und
signiertem Webhook genau einmal bestätigt. Die signierte PDF wurde gespeichert.
Beim anschließenden Storno weniger als 48 Stunden vor Abholung bestätigte
Stripe 35,10 € Erstattung nach 3,90 € Gebühr; der Kautions-PaymentIntent wurde
ohne Einzug storniert. Vorher lehnte Stripe die erzwungene verlängerte
Autorisierung für das vorhandene Testkonto ab. Der Gateway fordert diese
Option nun nicht mehr automatisch an und prüft weiterhin die tatsächliche
`capture_before`-Frist. [Testprotokoll und Grenzen](mos-staging-abnahme.md).

Dies belegt nur den lokalen Kernpfad im Testmodus. Die übrigen Fälle der
Abnahme-Checkliste sowie die oben genannten Live-Gates bleiben offen.

## Nachtrag 24.09.2026 – geprüfter Code deaktiviert auf Render

Der gemeinsame lokale Stripe-/PostgreSQL-Test, die Korrektur der Checkout-CSP
und der vorbereitete Render-Abgleich sind in den Arbeitszweig und anschließend
per Fast-Forward auf `main` gelangt. Der tatsächliche Dienst `kundenstatus-app`
antwortete nach dem Auto-Deploy auf `/healthz` mit 200 und auf die neue
Admin-Terminroute mit Login-Weiterleitung. `/mieten/` antwortet weiterhin mit
404, weil keine Live-Konfiguration gesetzt ist. Es wurden keine Stripe-Live-
Schlüssel, kein Webhook, kein kostenpflichtiger Cron und keine Livezahlung
eingerichtet. Aktuelle Schritte stehen in [Render-Betrieb](mos-render-betrieb.md)
und im [Preis- und Betriebsregelvorschlag](mos-preis-betriebsregeln-vorschlag.md).

Der anschließende vollständige Chrome-Test auf der isolierten lokalen
PostgreSQL-/Stripe-Testinstanz bestätigte auch den zuvor offenen automatischen
Sprung zum Stripe Checkout. Nach Unterschrift wurde die 500-€-Testkaution nur
autorisiert; 39 € Testmiete wurden separat bezahlt, der signierte Webhook
bestätigte genau eine Buchung und der PDF-Vertrag war abrufbar. Das Teststorno
bestätigte 35,10 € Erstattung nach 3,90 € Gebühr sowie die Freigabe der
Kautionsautorisierung. Ein zweiter Browserlauf mit gescheiterter 3-D-Secure-
Authentifizierung erzeugte keine Buchung oder Mietzahlung; der Zeitraum wurde
freigegeben. [Prüfprotokoll](mos-staging-abnahme.md). Im Render-Dashboard ist
der neueste Dokumentations-Commit `019a932` als „Live“ deployed sichtbar;
`/healthz` antwortet mit 200 und `/mieten/` bleibt absichtlich bei 404.
