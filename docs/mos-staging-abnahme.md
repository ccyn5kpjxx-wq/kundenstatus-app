# MOS: getrennte PostgreSQL- und Stripe-Abnahme

Stand 23.09.2026, aktualisiert nach der Entscheidung für eine separate
Kreditkartenautorisierung. Keine Livefreigabe, kein Deployment.

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

In der am 23.09.2026 geprüften lokalen Umgebung fehlen
`MOS_STRIPE_TEST_KEY`, `MOS_STRIPE_PUBLISHABLE_KEY` und
`MOS_STRIPE_WEBHOOK_SECRET`. Das Stripe-SDK ist in der vorbereiteten lokalen
Python-Umgebung installiert; die Stripe CLI wurde im ignorierten Agenten-Hub
installiert, aber noch nicht authentifiziert oder als Listener gestartet.
Deshalb wurden keine echte Stripe-
Testautorisierung, Checkout-Session, Webhook-Zustellung oder Erstattung
ausgeführt. `scripts/run_mos_public_test.py` löscht geerbte Stripe-Variablen
absichtlich und startet ausschließlich den synthetischen Offline-Modus; er
ist kein Sandbox-Runner.

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
`/mietwagen-test/webhook`. Der isolierte öffentliche Testpfad akzeptiert
derzeit kein PostgreSQL. Der PostgreSQL-Sperrpfad wurde separat geprüft; für
einen gemeinsamen Stripe-/Browser-/PostgreSQL-Lauf ist ein eigener sicherer
Staging-Pfad nötig.

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
   kontrollieren. Für Livebetrieb zusätzlich einen dauerhaften Job vorsehen;
   ein manueller Probelauf ersetzt ihn nicht.

Offline- und Mock-Tests bestätigen Geschäftslogik und Fehlerbehandlung, **nicht**
die Stripe-Rechte, Kartenbestätigung, tatsächliche Webhook-Zustellung oder
Erstattung. Die hier beschriebene Sandbox-Abnahme ist noch offen. Für
Kundenzahlungen fehlen außerdem belegter Selbstfahrervermietungsschutz für
beide Fahrzeuge, reale freigegebene Termine, finale Bedingungen und weitere
Freigaben laut [Launch-Paket](mos-launch-package.md). Die dokumentierte
Allane-Mietwagennutzung und KONA-Übernahme ersetzen den Versicherungsnachweis
nicht.
