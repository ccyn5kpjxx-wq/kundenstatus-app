# MOS: Render-Betrieb für Stripe-Webhook und Zahlungsabgleich

Stand 24.09.2026. Dies ist eine **vorbereitete Betriebsanleitung**, keine eingerichtete Liveverbindung. Der tatsächlich beobachtete Render-Portal-Dienst heißt `kundenstatus-app` und läuft auf `main`. Der geprüfte MOS-Code ist deaktiviert bereitgestellt: `/healthz` antwortete mit 200, die Admin-Terminroute mit Login-Weiterleitung und `/mieten/` weiterhin mit 404. Weder eine MOS-Live-Konfiguration noch ein Stripe-Live-Webhook oder ein regelmäßiger Render-Job wurde angelegt.

## Gemeinsame Laufzeitkonfiguration

Die Portal-App erhält die private MOS-Konfiguration über `MOS_BOOKING_CONFIG_FILE`. Für den künftigen Render-Cron-Job ist der persistente Portal-Datenträger **nicht zugänglich**. Deshalb muss dieselbe freigegebene JSON-Konfiguration als Render **Secret File** in einer Environment Group liegen, die sowohl dem Portal-Webdienst als auch dem Cron-Job zugeordnet ist, z. B. unter `/etc/secrets/mos-booking.json`. `MOS_BOOKING_CONFIG_FILE` zeigt in beiden Diensten auf diesen Pfad. Das Secret File enthält keine Stripe-Schlüssel; diese bleiben separate geschützte Umgebungsvariablen. Änderungen an der Konfiguration erfordern bei beiden Diensten einen neuen Lauf bzw. Deployment; vor der Freigabe Version und Inhalt vergleichen.

Der Cron-Job verwendet **denselben Git-Stand und dieselbe autoritative PostgreSQL-Datenbank** wie der Portal-Webdienst. In beiden Diensten gelten `PUBLIC_SITE_ONLY=false`, `REQUIRE_POSTGRES_ON_RENDER=true` und ein dauerhafter sicherer `FLASK_SECRET_KEY`. Für den Cron-Job werden `DATABASE_URL`, `MOS_STRIPE_LIVE_KEY`, `MOS_STRIPE_PUBLISHABLE_KEY` und `MOS_STRIPE_WEBHOOK_SECRET` benötigt. Test- und Live-Schlüssel nicht mischen. Der Secret-File-Pfad und die Schlüssel werden in Render gesetzt, nie in Git oder im Chat. Der Cron-Starter `scripts/run_mos_render_reconcile.py` lehnt fehlende Freigaben, Testschlüssel, lokale Datenbanken und einen Konfigurationspfad außerhalb von Render Secret Files ab. Er importiert die Portal-App erst nach dieser Prüfung.

Die bereits vorhandene `render.yaml` enthält **keinen Cron-Dienst** und benennt andere Webdienste als den tatsächlich beobachteten `kundenstatus-app`. Sie darf deshalb nicht als Beweis gelten, dass Änderungen an ihr den laufenden Dienst konfigurieren. Einen Cron-Eintrag dort hinzuzufügen würde bei einem Blueprint-Sync womöglich einen kostenpflichtigen Dienst provisionieren. Diese Anleitung und der Starter allein verändern Render nicht.

## Stripe-Webhook am Portal-Webdienst

1. Erst nach vollständigen Betreiber-/Versicherungsfreigaben und sicherer Live-Konfiguration die öffentliche HTTPS-Route `/mieten/webhook` am tatsächlichen Portal-Origin prüfen. Die `/mieten/`-Antwort 404 zeigt derzeit, dass die Live-Konfiguration nicht aktiviert ist.
2. Im **richtigen Stripe-Live-Konto** ein Webhook-Ziel auf `{origin}/mieten/webhook` für `checkout.session.completed`, `checkout.session.expired`, `checkout.session.async_payment_succeeded` und `checkout.session.async_payment_failed` anlegen. Stripe stellt für dieses Ziel ein eigenes Signatur-Secret aus; `whsec_` verrät allein nicht, ob es Test oder Live ist. Die Zuordnung im Stripe-Dashboard prüfen und den Wert geschützt als `MOS_STRIPE_WEBHOOK_SECRET` im Portal-Webdienst speichern.
3. Einen signierten Zustelltest im passenden Modus ausführen und im Stripe-Dashboard die erfolgreiche Zustellung sowie im Portal genau einen Mietvorgang für eine tatsächlich bezahlte Buchung prüfen. Rücksprung im Browser und bloßer HTTP-Erfolg ersetzen diese Prüfung nicht. Doppelte und verzögerte Zustellungen ebenfalls abnehmen. Webhook-Ausfälle bleiben im Zahlungsabgleich sichtbar, führen aber **nie** zu einer automatischen Bestätigung ohne signiertes Ereignis.

## Periodischer Abgleich mit Fehlerbenachrichtigung

Nach den externen Freigaben einen separaten Render-Cron-Job auf demselben Repository-Commit wie den Portal-Webdienst anlegen:

| Einstellung | Geplanter Wert |
|---|---|
| Build | `pip install -r requirements.txt` |
| Command | `python scripts/run_mos_render_reconcile.py` |
| Zeitplan | alle zehn Minuten, UTC: `*/10 * * * *` |
| Datenbank | dieselbe Render-PostgreSQL-Instanz wie das Portal |
| Konfiguration | dieselbe private MOS-Secret-Datei und passende Live-Stripe-Variablen |
| Benachrichtigung | Render-Servicebenachrichtigungen mindestens „Only failure notifications“, E-Mail-Empfänger prüfen |

Der Starter setzt für den kurzlebigen Cron-Prozess lokale Datenverzeichnisse unter `/tmp`, schaltet nicht benötigte Backup-, Mail- und Ads-Nebenläufe aus und beendet einen hängenden Abgleich nach 15 Minuten mit Fehlerstatus. Er gibt den Exitcode des vorhandenen `mos-booking-reconcile` unverändert an Render zurück. Dieser meldet bezahlte Checkouts ohne signierten Webhook, unklare Checkout-Erzeugung nach fünf Minuten, fehlende oder freigegebene Kartenautorisierungen bei noch offenem Miet-Checkout, ungeklärte Kartenfreigaben, `review`-Fälle und fehlgeschlagene Erstattungen als Fehler. Ein Fehler erfordert Abgleich zwischen Portal und Stripe sowie dokumentierte manuelle Klärung; der Job bestätigt keine Buchung ohne signierten Webhook und bucht keine Kaution ein.

Render garantiert höchstens einen aktiven Lauf **dieses** Cron-Jobs und kann bei fehlgeschlagenen Cron-Läufen E-Mail senden, wenn entsprechende Benachrichtigungen eingerichtet sind. Nach Einrichtung einen manuellen erfolgreichen Lauf und einen kontrollierten Alarmtest außerhalb echter Kundenbuchungen protokollieren. Eine kontrollierte Live-Abnahme anschließend im Stripe-Dashboard und in der Portal-Datenbank einschließlich Webhook, Kartenautorisierung, Mietpreis, Vertrag, Storno/Rückgabe und Abgleich nachvollziehen. Cron-Läufe und Benachrichtigungen täglich kontrollieren.

## Vor dem Anlegen noch erforderlich

- Schriftliche Bestätigung der entgeltlichen Selbstfahrervermietung für **i10 und KONA**; niemals durch Testdaten oder erfundene Belegreferenzen ersetzen.
- Tatsächliche persönliche Übergabeslots, maximale Mietdauer, KONA-Tarif und finale Mietbedingungen/Schaden- und Rückgabeabläufe samt rechtlicher Freigabe; alle `launch`-Nachweise müssen echt sein.
- Privates gemeinsames Render Secret File, Live-Schlüssel mit erforderlichen Stripe-Rechten, Webhook-Ziel und dessen Signatur-Secret; anschließend Hosting- und Live-Abnahme. Der geprüfte Code ist bereits deaktiviert ausgerollt, der bestehende lokale PostgreSQL-/Stripe-Test ersetzt diese Schritte nicht.
- Für einen neuen Render-Cron-Dienst fallen laut Render derzeit mindestens **1 US-Dollar pro Monat** an. Diesen Dienst erst als konkret geprüften Betriebsschritt anlegen; das bloße Einchecken dieser Dateien erzeugt keinen Dienst.

Quellen: [Render Cron Jobs](https://render.com/docs/cronjobs), [Render Secret Files und Environment Groups](https://render.com/docs/configure-environment-variables), [Render-Benachrichtigungen](https://render.com/docs/notifications), [Stripe-Webhooks](https://docs.stripe.com/webhooks).
