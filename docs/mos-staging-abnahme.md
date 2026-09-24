# MOS: getrennte PostgreSQL- und Stripe-Abnahme

Stand 24.09.2026, aktualisiert nach dem gemeinsamen lokalen
PostgreSQL-/Stripe-Testlauf. Keine Livefreigabe, kein Deployment.

## Tatsächlich ausgeführt

Portable PostgreSQL 17.11 (EDB Windows-Binaries, Revision 4) aus dem von
[PostgreSQL verlinkten Anbieter](https://www.postgresql.org/download/windows/)
und dessen [Downloadseite](https://www.enterprisedb.com/download-postgresql-binaries).
Lokaler Testcluster ausschließlich auf `127.0.0.1:55439`, eigener Testbenutzer,
SCRAM-Passwort, keine Windows-Dienstinstallation und keine Cloudkosten.
Runtime, Passwort und Verbindungsdatei liegen ausschließlich im ignorierten
`.agent-hub/postgres-runtime/`. Keine Geheimnisse in Git oder Konsolenausgaben.

```powershell
.agent-hub/setup-venv/Scripts/python.exe scripts/test_mos_postgres.py --connection-file .agent-hub/postgres-runtime/connection.json
```

Der Runner akzeptiert ausschließlich den dedizierten Loopback-Testcluster,
erstellt pro Lauf eine neue `mos_acceptance_<zufall>`-Datenbank und initialisiert
sie direkt mit dem echten `init_db()` der Anwendung, ohne Schemavorbelegung.
Bestehende Datenbanken werden weder überschrieben noch gelöscht. Die Anwendung
verwendet anschließend ihren echten PostgreSQL-Adapter und ihre echten
Bestands-/Checkout-Funktionen.

**23 Tests bestanden:** 21 wiederverwendete Bestandstests einschließlich
konkurrierender Admin-/Checkout-Anlage, Anfrageübernahme und Datumsänderung,
idempotenter Webhooks, Timeout-/Abbruchfällen und Zulaufsperre. Hinzu kommen
ein PostgreSQL-Trigger-Test für vollständigen Rollback von Mietvorgang und
Webhook-Ereignis sowie Storno/Erstattung mit idempotenter Wiederholung.
Zahlungsanbieter ist hierbei der synthetische Offline-Gateway, nicht Stripe.

Ein vorhandener HTTP-Test verwendet direkt SQLite-PRAGMA und wird hier nicht
ausgeführt; der konkurrierende Anfrageübernahme-Test läuft auf PostgreSQL.
Der SQLite-Trigger-Test wurde durch einen echten PostgreSQL-Trigger-Test ersetzt.
Das ist eine lokale Datenbank-/Geschäftslogikprüfung, keine vollständige
Hosting-, Browser-, Migrations- oder Stripe-Abnahme.

Nachtrag: Nach Einführung der separaten Kautionsautorisierung und der
persistenten Übergabeslots bestanden auf dem isolierten PostgreSQL-Kaltstart
36 Tests. Auch dieser Lauf verwendete einen synthetischen Gateway; er enthält
keinen Stripe-API-Aufruf und keine signierte Stripe-Zustellung.

## Gefundene Initialisierungslücke

Aufgabe 74 reproduzierte auf einer leeren PostgreSQL-DB den Fehler
`relation "auftraege" does not exist`. Aufgabe 75 hat die DDL-Reihenfolge in
`app.py` korrigiert: `auftraege` wird unmittelbar nach `autohaeuser` angelegt,
bevor abhängige Tabellen darauf verweisen. Kein Spalten-/Dateninhalt geändert.
Der temporäre Schema-Workaround wurde aus dem Runner entfernt. Direkte
Initialisierung einer neuen leeren PostgreSQL-Datenbank und anschließend alle
23 Tests bestanden. Zusätzlich bestanden Smoke- und Flow-Test auf isolierten
SQLite-Testdaten sowie Syntax-/Diff-Prüfung. Der Testcluster wurde danach
wieder gestoppt; keine Produktivdatenbank wurde kontaktiert.

## Stripe-Sandbox: Voraussetzungen und Abnahme

Der lokale Kernablauf wurde am 23.09.2026 mit vorhandenen Schlüsseln des
regulären Stripe-**Testmodus** und einem temporären signierten Stripe-CLI-
Webhook-Listener ausgeführt. Die Schlüssel und das Listener-Secret wurden für
den Lauf in lokalen Prozessvariablen verwendet; eine kurzzeitige ignorierte
Transferdatei wurde gelöscht. Nichts davon wurde in Git oder der
Testdatenbank gespeichert.
`scripts/run_mos_public_test.py` löscht geerbte Stripe-Variablen absichtlich
und startet ausschließlich den synthetischen Offline-Modus; er ist kein
Sandbox-Runner.

Beim ersten Versuch lehnte Stripe die für jede Kartenautorisierung erzwungene
Option `request_extended_authorization=if_available` mit einem Fehler zur
Kontoberechtigung ab. Der Gateway fordert diese Erweiterung für neue
PaymentIntents jetzt nicht mehr automatisch an. `capture_method=manual` und
die Prüfung der **tatsächlich** von Stripe gemeldeten Frist `capture_before`
bleiben erhalten. Damit werden für dieses Konto nur Mieten zugelassen, deren
Rückgabe plus 24 Stunden in das normale Autorisierungsfenster passt. Eine
verlängerte Autorisierung wäre eine gesonderte Stripe-Kontofreigabe und ist
für den getesteten Ablauf nicht vorausgesetzt.

**Tatsächlicher Stripe-Testlauf:** Ein synthetischer i10 wurde für
25.–26.09.2026, jeweils 09:00 Uhr, zu 39 € gebucht. Der Entwurfsvertrag wurde
zuerst digital unterschrieben. Stripe bestätigte auf einer Test-Kreditkarte
einen 500-€-PaymentIntent mit `requires_capture`, 50.000 Cent autorisierbar,
`amount_received=0` und `capture_before` am 30.09.2026; die Frist reichte
über die Rückgabe plus 24 Stunden. Danach wurde eine getrennte 39-€-
Checkout-Session im Testmodus bezahlt (`complete`, `paid`,
`livemode=false`). Der signierte `checkout.session.completed`-Webhook wurde
lokal zugestellt und genau einmal gespeichert; in der isolierten Datenbank
standen genau ein bestätigter Mietvorgang und eine signierte Vertrags-PDF.
PDF-Header und gespeicherter SHA-256-Hash wurden geprüft.

Anschließend wurde dieselbe synthetische Buchung weniger als 48 Stunden vor
Abholung storniert: 3,90 € Stornogebühr, 35,10 € Stripe-Erstattung mit Status
`succeeded`. Die Kautionsautorisierung wurde separat storniert; Stripe meldete
für sie `canceled`, `amount_received=0`, `amount_capturable=0`. Dabei wurde
kein echtes Geld bewegt und kein Kundenfahrzeug gebucht.

## Gemeinsamer PostgreSQL-/Stripe-Test am 24.09.2026

`scripts/run_mos_stripe_postgres_staging.py` startet den echten Portalcode nur
gegen den privaten Loopback-Cluster `127.0.0.1:55439`. Er prüft Testschlüssel,
Testmodus, festen Clusterbenutzer und eine private Verbindungsdatei, bereinigt
geerbte Live-/Mail-/Datenbankeinstellungen und erstellt pro Aufruf eine neue
`mos_stripe_acceptance_<zufall>`-Datenbank. Nur synthetische i10-/KONA-Datensätze
und Entwurfsbedingungen werden angelegt. Eine vorhandene Mietdatenbank wird nicht
geöffnet, überschrieben oder gelöscht. Die Testseite läuft ausschließlich auf
`http://127.0.0.1:5087/mietwagen-test/`. Testschlüssel und das kurzlebige
Webhook-Secret wurden nur in lokalen Prozessvariablen verwendet; der lokale
Transferhelfer und Stripe-CLI-Listener wurden nach dem Lauf beendet.

Im normalen Gärtner-Stripe-**Testmodus** wurden mit separaten synthetischen
Buchungen folgende Kartenfälle im Browser geprüft:

| Fall | Beobachtetes Ergebnis |
| --- | --- |
| Ablehnung `4000 0000 0000 0002` | Stripe verweigerte die Kautionsautorisierung; keine Mietzahlung. |
| Debit `4000 0566 5566 5556` | Für die Kaution abgelehnt; Testautorisierung und Zeitraum freigegeben, keine Mietzahlung. |
| Prepaid `5105 1051 0510 5100` | Ebenso freigegeben, ohne Mietzahlung. |
| 3-D-Secure `4000 0000 0000 3220` | Nach erfolgreicher Testauthentifizierung 500 € autorisiert, nicht abgebucht; danach separate 39-€-Testzahlung. |

Beim ersten gemeinsamen Lauf legten Zahlung und signierter Webhook genau einen
bestätigten Mietvorgang in PostgreSQL an. Die Vertrags-PDF-Erstellung deckte
aber einen Adapterfehler auf: `PostgresConnection.execute()` hängte an den
Vertrags-INSERT fälschlich `RETURNING id`, obwohl die Tabelle `hold_id` als
Primärschlüssel verwendet. Nach gezielter Korrektur wurde die PDF für diesen
synthetischen Vorgang nachträglich erzeugt und geprüft.

Ein **zweiter frischer End-to-End-Lauf mit korrigiertem Code** bestätigte die
automatische Erstellung direkt nach der Testzahlung: ein signierter Webhook,
genau ein bestätigter Mietvorgang und genau eine Vertrags-PDF. PDF-Header und
gespeicherter SHA-256-Hash stimmten. Die 500-€-Autorisierung hatte einen von
Stripe gemeldeten `capture_before` am 01.10.2026, nach der Rückgabe am
27.09.2026 plus 24 Stunden. Anschließend wurde die Buchung mehr als 48 Stunden
vor Abholung kostenlos storniert: 39,00 € Test-Erstattung `succeeded`, Kaution
`released`; die Vertragskopie blieb erhalten. Beide lokalen Testdatenbanken
bleiben ausschließlich im privaten Cluster zur Inspektion; der Cluster wurde
anschließend gestoppt. Es gab kein echtes Geld und keine Produktivbuchung.

Ein direkter automatischer Sprung vom lokalen POST zum externen Stripe-Checkout
war im gesteuerten Chrome weiterhin nicht beobachtbar. Der von der Anwendung
bereits erzeugte offene Test-Checkout wurde anhand seiner Stripe-Session direkt
im Browser geöffnet. Dieser Browserübergang muss vor dem Live-Start auf der
Zielumgebung separat verifiziert werden.

```powershell
& .agent-hub/setup-venv/Scripts/python.exe scripts/run_mos_stripe_postgres_staging.py --connection-file .agent-hub/postgres-runtime/connection.json
```

Dieser Starter verlangt die drei `MOS_STRIPE_*`-Testzugangsdaten als
Prozessvariablen und lehnt Live-Keys ab. Keinen Schlüssel in Befehlsargumente,
Dateien, Konsolenausgaben oder Git schreiben.

Für die lokale Stripe-Sandbox steht jetzt `scripts/run_mos_stripe_staging.py`
bereit. Dieser Starter prüft vor dem Anlegen einer Datenbank drei
Test-Zugangsdaten, lehnt einen gesetzten Live-Key ab und erzeugt ausschließlich
synthetische Fahrzeuge in einer frischen temporären
`*.mos-public-test.sqlite3`-Datenbank. Die Buchungsseite ist nur unter
`http://127.0.0.1:5086/mietwagen-test/` erreichbar. Er setzt die
Kartenautorisierung bei der Reservierung und die bestätigte Stornoregel. Für
den Start ist die vorbereitete Python-Umgebung nötig, da das systemweite
Python-Paketset das Stripe-SDK nicht enthält:

```powershell
& .agent-hub/setup-venv/Scripts/python.exe scripts/run_mos_stripe_staging.py
```

Der Starter benötigt **keine** `MOS_BOOKING_CONFIG_FILE`; er setzt die
isolierte Testkonfiguration im Prozess. Der getrennte Stripe-CLI-Listener
benötigt denselben Test-Serverkey und leitet Testereignisse an
`http://127.0.0.1:5086/mietwagen-test/webhook` weiter. Sein dabei erzeugtes
Signatursecret muss vor dem Start des Portals in
`MOS_STRIPE_WEBHOOK_SECRET` stehen. Das `whsec_`-Präfix beweist allein nicht,
ob ein Secret zum Test- oder Live-Modus gehört. Schlüssel und Secret niemals
in Befehlsargumenten, Konsolenausgaben oder Git hinterlegen.

Für die öffentliche Sandbox muss eine **separate** Portal-Testdatenbank mit
Dateiendung `.mos-public-test.sqlite3` verwendet werden, mit ausschließlich
synthetischen i10-/KONA-Datensätzen. Die private Testkonfiguration benötigt
`mode=stripe_test`, `test_configuration=true`, `enabled=true`, die passende
HTTPS- oder Loopback-Origin, zwei unterschiedliche Testfahrzeug-IDs, künftige
aktive Übergabeslots, Entwurfsbedingungen, Bruttopreise und ausdrücklich
`deposit_method=card_authorization_at_booking` sowie
`cancellation_policy=free_48h_then_10pct_rent`. Ohne die Deposit-Einstellung
kann der historische Einzugspfad statt der beschlossenen Autorisierung laufen.
Die öffentliche Test-Route ist `/mietwagen-test/`; der Webhook liegt unter
`/mietwagen-test/webhook`. PostgreSQL ist im Testmodus ausschließlich für den
oben beschriebenen lokalen Starter mit genau passender, frisch angelegter
Testdatenbank und Loopback-Origin freigegeben. Andere PostgreSQL-Testpfade
bleiben gesperrt.

Für ein separates Deployment werden im geschützten Test-Secretstore ein
eingeschränkter Stripe-Test-Serverkey (`rk_test_`, alternativ `sk_test_`), der zugehörige
Publishable Key (`pk_test_`) und das Signatursecret **des tatsächlich
verwendeten Test-Webhooks** (`whsec_`) hinterlegt. Der private Config-Dateipfad
wird über `MOS_BOOKING_CONFIG_FILE` gesetzt. Test- und Live-Keys dürfen nicht
vermischt werden; Server- und Webhook-Secrets gehören weder in Config-JSON,
Git noch Chat. Der Serverkey braucht Payment Intents lesen/schreiben samt
Stornierung, Checkout Sessions lesen/schreiben samt Ablauf und Refunds
lesen/schreiben; die Berechtigung für die expandierte Charge und tatsächliche
Stripe-Rechteabhängigkeiten mit dem eingeschränkten Testkey prüfen. Keine
Auszahlungs-, Transfer- oder Schlüsselverwaltungsrechte geben. Ein dauerhafter
Flask-Sitzungsschlüssel ist für den Browser-End-to-End-Lauf nötig.

Die **aktuelle** Abnahmekette lautet: konkretes Testfahrzeug und Zeitraum
wählen, den vollständigen Vertrag **vor jeder Zahlung digital unterschreiben**,
500 € unmittelbar online auf einer als Kreditkarte bestätigten Karte mit
`capture_method=manual` autorisieren, danach nur den Mietpreis über Stripe
Hosted Checkout bezahlen. Der Kautions-PaymentIntent muss
`requires_capture` und `amount_received=0` zeigen. Der vom Kartenumsatz
gemeldete Wert `capture_before` muss nach der geplanten Rückgabe **plus
24 Stunden** liegen; andernfalls darf kein Mietpreis-Checkout starten.
Debit-/Prepaid- oder unbekannte Kartentypen werden nicht akzeptiert und die
Autorisierung wird ohne Mietbelastung freigegeben. Erst ein signierter
Checkout-Webhook mit erneut geprüftem Providerstatus darf genau einen
Mietvorgang bestätigen; ein Erfolgs-Redirect genügt nicht.

### Kurze Abnahme-Checkliste

1. Isolierte Testdatenbank und private `stripe_test`-Konfiguration mit
   separater Kartenautorisierung verifizieren; keine Produktivdaten und keine
   Live-Keys verwenden. Test-Webhook tatsächlich zustellen und Signatur gegen
   den unveränderten Request-Body prüfen.
2. Mit Stripe-Testkarten Vertragssignatur → 500-€-Kreditkarten-Hold ohne Einzug
   → separaten Mietpreis-Checkout → signierten Webhook → genau eine bestätigte
   Buchung durchspielen. Beträge, Kartenart und `capture_before` im Testkonto
   und Portalzustand abgleichen; Checkout-Button auch am geeigneten Gerät prüfen.
3. 3-D-Secure/Abbruch, Ablehnung, Debit/Prepaid, zu kurze Autorisierungsfrist,
   parallele Reservierung, geschlossenen oder verstrichenen Slot, doppelte und
   verspätete Webhooks, Checkout-Timeout sowie Zahlung/Storno-Rennen prüfen.
   Unklare Ergebnisse bleiben zur manuellen Prüfung gesperrt.
4. Bis einschließlich 48 Stunden vor Abholung kostenlosen Storno und danach
   10 % nur vom Mietpreis prüfen. Erstattung des verbleibenden Mietpreises
   idempotent abgleichen. Kaution bei Abbruch/Storno und nach dokumentierter
   Rückgabe durch **Stornierung der ungenutzten Autorisierung** freigeben;
   dies ist keine Kautionserstattung, da kein Einzug erfolgte.
5. `python -m flask --app app mos-booking-reconcile` mit Testkonfiguration
   ausführen und offene Autorisierungen, Checkouts, Refunds und Prüffälle
   kontrollieren. Der Job meldet jetzt bezahlte Checkouts ohne zugestellten
   signierten Webhook, nach fünf Minuten noch unklare Checkout-Erzeugung,
   ungeklärte Kartenfreigaben/-erzeugung, `review`-Buchungen und endgültig
   fehlgeschlagene Erstattungen mit Fehlerstatus. Er bestätigt selbst keine
   Buchung und ersetzt keine manuelle Klärung in Stripe und im Portal. Für
   Livebetrieb zusätzlich einen dauerhaften Job mit Alarmierung vorsehen;
   ein manueller Probelauf ersetzt ihn nicht.

### Stripe-Kartenfälle und verbleibende Negativtests

Die folgenden vier Kartenfälle wurden mit frischen synthetischen Buchungen
und ausschließlich Testschlüsseln ausgeführt:

| Fall | Stripe-Testkarte | Erwartung vor dem Mietpreis-Checkout |
| --- | --- | --- |
| 3-D-Secure | `4000 0000 0000 3220` | Vor erfolgreicher Authentifizierung keine verwendbare Kautionsautorisierung; danach nur bei `requires_capture`, 500 € autorisierbar und `amount_received=0` fortfahren. |
| Ablehnung | `4000 0000 0000 0002` | Keine gültige Autorisierung, kein Mietpreis-Checkout, keine bestätigte Miete. |
| Debit | `4000 0566 5566 5556` | Trotz möglicher Autorisierung anhand `funding=debit` ablehnen und die Kaution ohne Einzug freigeben. |
| Prepaid | `5105 1051 0510 5100` | Anhand `funding=prepaid` ablehnen und die Kaution ohne Einzug freigeben. |

Die [Stripe-Testkarten](https://docs.stripe.com/testing) simulieren diese
Karten- und Fehlerzustände. Eine gezielt zu kurze `capture_before`-Frist lässt
sich damit nicht zuverlässig erzeugen: Der tatsächliche Wert muss beim
Stripe-Lauf geprüft und die Ablehnung kurzer/fehlender Fristen separat mit
einem kontrollierten Gateway-Test belegt werden. Die maßgebliche Frist steht
laut [Stripe-Autorisierungsdokumentation](https://docs.stripe.com/payments/place-a-hold-on-a-payment-method)
am Kartenumsatz. Zusätzlich einen geöffneten Miet-Checkout abbrechen und
prüfen, dass Rücksprung oder Tab-Schließen keine Buchung bestätigen; eine
offene Session kann über die [Expire-API](https://docs.stripe.com/api/checkout/sessions/expire)
beendet werden.

Die lokale Stripe-Kernkette einschließlich PostgreSQL, signiertem Webhook,
PDF, Erstattung und Kautionsfreigabe ist damit nachgewiesen. 3-D-Secure-
Fehlschlag, eine gezielt zu kurze Autorisierungsfrist, weitere reale
Provider-Rennen, Restricted-Key-Rechte, dauerhafte Webhook-Konfiguration,
Geräteprüfung und Hosting-Abnahme bleiben offen. Der automatische Browser-
Sprung vom lokalen POST zum Stripe-Checkout muss in der Zielumgebung geprüft
werden.
Für Kundenzahlungen fehlen außerdem belegter Selbstfahrervermietungsschutz
für beide Fahrzeuge, reale freigegebene Termine, finale Bedingungen und
weitere Freigaben laut [Launch-Paket](mos-launch-package.md). Die dokumentierte
Allane-Mietwagennutzung und KONA-Übernahme ersetzen den Versicherungsnachweis
nicht.
