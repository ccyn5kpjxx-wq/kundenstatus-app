# MOS-Vertragsbestätigung per E-Mail: vorbereiteter Versand

Nach einem **signierten Stripe-Webhook** wird ein bestätigter Mietvorgang mit der zuvor unterschriebenen Bedingungsversion als unveränderliches Vertrags-PDF in der autoritativen Portal-Datenbank gespeichert. Derselbe Datenbankabschluss reiht bei echten Buchungen genau eine Zustellung an die im unterschriebenen Vertrag festgehaltene E-Mail-Adresse ein. Testbuchungen werden nie eingereiht. Der Webhook selbst kontaktiert keinen Mailserver.

Der neue CLI-Job `python -m flask --app app mos-contract-delivery` ist **standardmäßig gesperrt**. Für einen künftigen Render-Dienst steht der fail-closed Starter `python scripts/run_mos_render_contract_delivery.py` bereit; er erstellt selbst keinen Dienst. Er erfordert zugleich:

- `MOS_PUBLIC_BOOKING.mode` ist `live`;
- dieselbe autoritative PostgreSQL-Datenbank wie Portal und Zahlungsabgleich;
- ausdrücklich gesetztes `MOS_CONTRACT_EMAIL_ENABLED=1` nur im zuständigen Live-Worker;
- verschlüsselten und vollständig konfigurierten Werkstatt-SMTP-Zugang aus `get_werkstatt_smtp_config()`. Der Render-Starter verlangt dafür ausdrücklich `MAIL_SMTP_HOST`, `MAIL_SMTP_PORT`, `MAIL_SMTP_USER`, `MAIL_SMTP_PASS`, `MAIL_IMAP_USER` als identische Absenderadresse sowie genau eine aktivierte Option `MAIL_SMTP_SSL` oder `MAIL_SMTP_TLS`. Alle Zugangsdaten bleiben in Render-Secrets.

Der Render-Starter prüft vor dem App-Import eine private Konfigurationsdatei unter `/etc/secrets/`, eine nichtlokale PostgreSQL-Verbindung, den Live-Modus, HTTPS-Origin, den dauerhaften Flask-Schlüssel und das explizite Versandflag. Er braucht keinen Stripe-Schlüssel und entfernt eventuell geerbte Stripe-Schlüssel aus dem Mail-Unterprozess. Auch bei abgeschalteter Annahme **neuer** Buchungen darf er bereits geschlossene Verträge noch zustellen. Im Testmodus oder ohne Flag findet kein SMTP-Aufruf statt.

Vor SMTP prüft der Worker erneut bestätigten Hold, Mietvorgang, Payment Intent, unterschriebenen Vertrag, Testmarkierung, Empfänger, Vertragssnapshot-Hash und PDF-Hash. Eine atomare Datenbank-Reservierung verhindert parallelen Doppelversand. Nach positiver SMTP-`DATA`-Antwort hält er den Zustand `sent` fest; das beweist **Annahme durch den Mailserver, nicht Zustellung im Posteingang**. Eindeutig vor `DATA` abgelehnte Versuche werden zeitverzögert erneut versucht. Ist die Antwort auf `DATA` oder nach einem Prozessabbruch unklar, bleibt der Eintrag zur manuellen Prüfung gesperrt; ein erneuter Versand darf erst nach Abgleich im Postfach erfolgen. Der Job beendet sich mit Fehlerstatus, solange Zustellungen offen, unklar oder ohne PDF sind; der Alarm darf nicht ignoriert werden. Unbestätigte und Testbuchungen werden nie versandt, auch nicht beim Wiederanlauf.

Vor einer Freigabe sind erforderlich: SMTP-Absender und Empfängerpfad mit ausschließlich synthetischen Adressen in einer isolierten Umgebung prüfen, einen realen Test der Postfachannahme und Anhangdarstellung durchführen, den **dauerhaften, eng getakteten Versandworker** mit derselben PostgreSQL-Datenbank und demselben Git-Stand wie das Webportal einrichten, Fehlermeldungen samt manueller Zuständigkeitsregel überwachen und die tatsächliche Zustellung rechtlich prüfen. Der vorhandene Render-Reconcile-Cron startet diesen Job **nicht** automatisch; ein bloßer Code-Deploy richtet keine E-Mail-Zustellung ein. Die noch nicht vorliegende schriftliche Selbstfahrer-Versicherung und die übrigen Live-Freigaben bleiben eigenständige Sperren.

Bei Buchung und Abholung am selben Tag kann zwischen Zahlung und Übergabe nur wenig Zeit liegen. Selbst ein zehnminütiger Cron garantiert keine Vertragskopie vor Leistungsbeginn. Deshalb ist eine nachweislich rechtzeitige Zustellung mit Alarm **und einer verbindlichen Übergabesperre für noch nicht bestätigte Zustellungen** ein zusätzliches Go-live-Kriterium. Die bloße Outbox ist keine Erfüllungs- oder Compliance-Zusage. Bei SMTP-`review`, fehlendem PDF oder ausgefallenem Worker muss ein Mensch den Fall vor Schlüsselübergabe klären; die Kundin oder der Kunde kann die PDF im bestehenden Statusbereich herunterladen, doch dieser Abruf allein beweist keine Zustellung auf dauerhaftem Datenträger.

Die technische Abnahme umfasst SQLite mit vollständig simuliertem SMTP, einen isoliert geprüften Render-Preflight und einen Lauf über den echten Portal-PostgreSQL-Adapter auf einer **neu erzeugten synthetischen Datenbank** im dedizierten lokalen Testcluster `127.0.0.1:55439`. Dort wurden Einreihung, Ausschluss von Testholds, atomare Reservierung, einmalige Fake-SMTP-Annahme und persistierter `sent`-Status geprüft (`scripts/check_mos_contract_delivery_postgres.py`). Es wurde weder eine echte E-Mail versandt noch eine operative Datenbank geöffnet. SMTP mit dem vorgesehenen echten Postfach, kontinuierlicher Betrieb, Alarmierung und Zustellung vor kurzfristiger Übergabe sind noch nicht abgenommen.

Der optionale Wiederholungstest läuft nur bei gestartetem dediziertem Testcluster mit der lokalen privaten Verbindungsdatei:

```powershell
.agent-hub/setup-venv/Scripts/python.exe scripts/check_mos_contract_delivery_postgres.py --connection-file .agent-hub/postgres-runtime/connection.json
```

Die Vertragsbestätigung auf dauerhaftem Datenträger verlangt [§ 312f Abs. 2 BGB](https://www.gesetze-im-internet.de/bgb/__312f.html); die elektronische Eingangsbestätigung und speicherbare Vertragsbedingungen regelt [§ 312i Abs. 1 BGB](https://www.gesetze-im-internet.de/bgb/__312i.html). Diese technische Vorbereitung ersetzt keine rechtliche Prüfung des vollständigen Bestellablaufs.
