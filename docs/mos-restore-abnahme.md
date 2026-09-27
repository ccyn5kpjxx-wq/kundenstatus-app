# MOS: PostgreSQL- und Upload-Wiederherstellung

Stand 27.09.2026. **Nur synthetisch lokal abgenommen; Render-PITR und Render-Datenträger wurden nicht wiederhergestellt.** Diese Anleitung schaltet keine Buchung und keine Zahlung frei.

## Was zusammen wiederhergestellt werden muss

- Die autoritativen `miet_checkout_*`-Tabellen liegen in PostgreSQL. `miet_checkout_contracts` enthält den signierten Vertragstext, die Unterschrift und das PDF als Base64 samt SHA-256. Historische `mietvertrag_versionen` können ebenfalls signierte PDFs enthalten.
- Der Portal-Datenträger enthält `/var/data/uploads`. PostgreSQL-Zeilen verweisen unter anderem aus `dateien`, `mietfahrzeug_bilder`, `lead_dateien`, `fahrzeugsuche_dateien` und `einkauf_belege` auf diese Dateien. Einige Dateitypen haben zusätzlich Datenbankkopien, aber nicht alle. Ein PostgreSQL-Restore allein beweist deshalb keine vollständige Wiederherstellung.
- Das alte Anwendungs-ZIP ist hierfür keine Alternative: Es schließt `miet_checkout_*` aus, der Admin-Import lehnt bestehende MOS-Daten ab und die beobachteten Render-Archive überschritten inzwischen das Größenlimit. Den alten Admin-Import nicht als MOS-Disaster-Recovery verwenden.

## Lokal ausgeführte technische Probe

`scripts/test_mos_restore_postgres.py` akzeptiert nur die private Konfiguration des dedizierten PostgreSQL-Testclusters `127.0.0.1:55439`. Es erzeugt zwei **neue** synthetische Datenbanken, initialisiert das echte Portal-/MOS-Schema, speichert einen synthetischen signierten Vertrag samt PDF und eine referenzierte Upload-Datei, führt `pg_dump`/`pg_restore` aus und kopiert die synthetische Datei in ein getrenntes Upload-Verzeichnis. Quelle und Restore werden mit `scripts/check_mos_restore.py` read-only verglichen. Keine vorhandene Datenbank wird überschrieben oder gelöscht; keine Stripe- oder SMTP-Verbindung wird geöffnet.

```powershell
& .agent-hub/setup-venv/Scripts/python.exe scripts/test_mos_restore_audit.py
& .agent-hub/setup-venv/Scripts/python.exe scripts/test_mos_restore_postgres.py --connection-file .agent-hub/postgres-runtime/connection.json
```

Ergebnis am 27.09.2026: **84 Tabellen, 85 Zeilen, ein signiertes synthetisches MOS-PDF, eine referenzierte Upload-Datei und identische Tabellen-/Schema-/Sequenz-/Datei-Prüfsummen nach Restore**. Drei absichtliche Fehler wurden erkannt: geänderter Upload-Inhalt, fehlender referenzierter Upload und falscher PDF-SHA-256. Sechs zusätzliche Offline-Tests des Auditors bestanden, darunter ein Test gegen Secret-Ausgabe bei Verbindungsfehlern. Der frühere isolierte Volltest mit 76 Tabellen/291 Zeilen bestätigte unabhängig die PostgreSQL-Zeilenübernahme, enthielt aber weder MOS-Vertrags-PDF noch Upload-Datei. Diese beiden Belege ergänzen sich; sie beweisen keinen Render-PITR- oder Snapshot-Restore.

## Read-only Vergleich eines kontrollierten Wiederherstellungspaares

`scripts/check_mos_restore.py` erwartet die Datenbank-URL ausschließlich in `MOS_RESTORE_AUDIT_DATABASE_URL` und das Upload-Verzeichnis in `MOS_RESTORE_AUDIT_UPLOAD_DIR`. Der exakte Datenbankname muss zusätzlich mit `--expected-database` angegeben werden. Die Verbindung ist transaktional read-only. Das Manifest enthält Tabellenzahlen und Prüfsummen, aber weder Kundendatensätze noch Dateinamen. `--output` legt nur eine **neue** Datei an; bestehende Manifeste werden nicht ersetzt. Baselines gehören in einen geschützten, nicht eingecheckten Speicherort. Ohne Baseline meldet der Runner nur `SOURCE_AUDIT_PASS`; erst ein identischer Vergleich meldet `RESTORE_MATCH`. Standardmäßig verlangt er mindestens ein MOS-PDF und eine Upload-Datei.

```powershell
python scripts/check_mos_restore.py --expected-database <quellname> --min-mos-contracts 1 --min-uploads 1 --output <geschuetztes-manifest.json>
python scripts/check_mos_restore.py --expected-database <restorename> --min-mos-contracts 1 --min-uploads 1 --baseline <geschuetztes-manifest.json>
```

Die Quelle muss während der Erfassung schreibruhig sein und der Upload-Stand zum gleichen Sicherungszeitpunkt gehören. Sonst ist ein exakter Manifestvergleich irreführend: Ein nach dem Datenbankzeitpunkt hochgeladenes Dokument liegt eventuell nur auf dem Datenträger, ein danach gebuchter Vertrag nur in PostgreSQL. Ein PITR-Test für einen früheren Zeitpunkt braucht eine dafür passende Baseline oder konkret dokumentierte Soll-Datensätze; der Vergleich mit dem heutigen laufenden System ist kein gültiger Nachweis.

Der Auditor prüft sämtliche öffentlichen PostgreSQL-Tabellen, gespeicherte Vertragsbytes samt Hash und PNG-Kennung sowie alle flachen Upload-Dateien; er verlangt auch, dass jede gespeicherte Upload-Referenz tatsächlich vorhanden ist. Die derzeitigen `stored_name`-, `datei_stored_name`- und `pdf_stored_name`-Spalten verweisen im Anwendungscode auf `UPLOAD_DIR`. Historische oder unvollständige MOS-Vertragszeilen ohne PNG/PDF und alte Dateireferenzen ohne Datei führen ausdrücklich zu **FAIL** und müssen einzeln geklärt werden; eine Datenbankkopie als möglicher Datei-Fallback gilt nicht als belegter Datenträger-Restore. Die Prüfung belegt Byte-Integrität und Vollständigkeit der referenzierten Dateien, keine rechtliche Wirksamkeit der Unterschrift oder tatsächliche Zustellung an Kunden. Sie verändert weder Datenbank noch Uploads. Verbindungs-/SQL-Fehler erscheinen ohne geheime DSN-Details.

## Render-Gate vor öffentlicher Zahlung

[Render PostgreSQL-PITR](https://render.com/docs/postgresql-backups) erzeugt eine **neue** Datenbankinstanz. Die verfügbare Zeitspanne hängt vom Workspace-Plan ab; den aktuellen Zeitraum im Dashboard prüfen. Den produktiven Portal-/Stripe-Dienst nicht auf eine ungetestete Kopie umstellen. [Render-Datenträger-Snapshots](https://render.com/docs/disks) entstehen ungefähr täglich, sind mindestens sieben Tage verfügbar und überschreiben bei einer Wiederherstellung den **ganzen** Datenträger. Ein Snapshot des aktiven `/var/data` darf für einen Test daher nicht direkt auf den aktiven Dienst zurückgespielt werden. PostgreSQL-PITR und Datenträger-Snapshot sind getrennte Zeitpunkte und ohne Schreibpause nicht automatisch konsistent.

Für die noch offene Zielbetriebs-Abnahme ist eine isolierte Render-PITR-Datenbank plus ein isolierter Upload-Zielbestand erforderlich. Nach Restore: Datenbankname, Tabellen-/PDF-Hashes, Upload-Inventar und Referenzen read-only prüfen; eine signierte Vertrags-PDF und eine Upload-Datei über einen isolierten Portalzugang öffnen; anschließend Zahlung/Webhook/Outbox-Ereignisse gegen Stripe abgleichen, bevor ein Dienst umgehängt wird. Eine zusätzliche Render-Datenbank bzw. ein separater Dienst/Datenträger kann Kosten auslösen und wurde hier **nicht** angelegt. Ein logischer Export und eine sichere getrennte Upload-Kopie sind eine Alternative für eine kontrollierte, zeitgleiche Probe; auch sie wurden nicht von Produktionsdaten erstellt.

Bis dieser Zielbetriebs-Test einschließlich Dateistand und Wiederanlauf dokumentiert ist, bleibt die MOS-Live-Freigabe gesperrt.
