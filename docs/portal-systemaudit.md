# Systemaudit: wiederkehrende Portalfehler

Stand: 8. Oktober 2026. Ausgangsbasis: `9af88427`.

## Nachgewiesene Fehler und Reparaturen

| Befund | Auswirkung | Reparatur und Nachweis |
| --- | --- | --- |
| Erinnerungen verwendeten `db.total_changes`, das der PostgreSQL-Adapter nicht besitzt. Render meldete denselben Fehler am 5. und 8. Oktober. | Die Änderung wurde gespeichert, danach erschien trotzdem HTTP 500. | Zeilenanzahl des UPDATE-Cursors auswerten, Verbindung im `finally` schließen. Neuer Routenfall und Fall ohne passende Erinnerung. Beide zusätzlich auf echtem PostgreSQL mit temporärer Tabelle geprüft. |
| Persönliche Mitarbeiter-Aufträge iterierten direkt über `PostgresCursor`. | Ein gültiger Auftrag scheiterte auch ohne Unterlagen. | Explizit `fetchall()` verwenden. Zwei neue Fälle mit dem echten Cursorvertrag; insgesamt 30 Auftragsfälle bestanden, einschließlich Original- und Berechtigungsschutz. |
| Mietwagenbilder verwendeten SQLite-spezifisches `INSERT OR REPLACE`. Der abgefangene Fehler konnte auf PostgreSQL die gesamte Transaktion abbrechen. | Erfolgsmeldung und gespeicherte Datei trotz fehlender Bildmetadaten. | Portables UPSERT mit `RETURNING bild_id`; optionale Sicherung mit SAVEPOINT isolieren. Erfolgsfall und absichtlicher SQL-Fehler bestanden auch auf echtem PostgreSQL; Metadaten blieben erhalten. |
| Dieselben Cockpit-Zähler wurden in Navigation und Inhalt wiederholt abgefragt. | SQL-Budget überschritten: 52 SELECTs statt höchstens 50. | Zähler je Templateausgabe einmal berechnen; neue Ausgabe erhält frische Werte. Danach 42 SELECTs, alle Leistungschecks bestanden. |
| Pillow/OpenCV liefen bei Materialfotos ohne harte Laufzeitgrenze im aufrufenden Prozess. Ein gültiges Bild mit 256 QR-Codes überschritt den sechssekündigen Diagnoseabbruch. | Native Bildverarbeitung konnte einen HTTP-Thread unbegrenzt belegen. | Ein wegwerfbarer Kindprozess für Normalisierung und optionale Codeauslese: 3 Sekunden, ein sofort reservierter Platz, unter Linux 1 GiB Adressraum, ein OpenCV-Thread. Timeout beendet und wartet auf das Kind; ein fertig geschriebenes JPEG bleibt erhalten. |
| HTTP-Anfragen warteten unbegrenzt auf die globale Dateisperre, während Analyse oder Versand dieselbe Sperre halten konnten. | Mehrere wartende Anfragen konnten alle vier Webthreads belegen. | Zwei Sekunden Wartebudget je lokaler/übergreifender Sperrphase; PostgreSQL prüft mit `pg_try_advisory_lock`, lokale Dateisperren werden nicht blockierend versucht. Bei Belegung HTTP 503 mit `Retry-After: 2`. Reentranz, Besitzprüfung und Freigabe bleiben erhalten. PostgreSQL-Verbindungsaufbau ist auf fünf Sekunden je Host begrenzt. |
| Materialdialog und Materialworker hielten die globale Dateisperre während externer KI-Auslese, obwohl der Analyse-Claim bereits gespeichert war. | Unabhängige geschützte Vorgänge mussten bis zum Ende der KI warten. | Nur die einzelnen Datenbankphasen sperren. Vor jeder Ergebnisübernahme aktuelle Rechte, Quelle, Lease, Revision, Ablauf und Abbruch erneut prüfen. Ein Eventbarriere-Test reproduzierte vorher die Sperre beim direkten Aufruf und beim Worker. Parallele Abbrüche, Rechteentzug und Wiederherstellung dürfen keine veralteten Ergebnisse oder Workerfehler übernehmen. |

Die Sperrgrenze ist keine Gesamtlaufzeitgrenze für eine Operation: zwei
Sperrphasen und gegebenenfalls der Verbindungsaufbau können sich addieren.
Hintergrundimporte behalten ihre bisherige Serialisierung. Es gibt kein neues
globales SQL-Statementlimit, das große Backups oder Exporte abbrechen könnte.

## Prüfung

Die breite Ausgangsprüfung lief auf einem Git-Snapshot mit synthetischen
Daten, deaktivierten Hintergrunddiensten und gesperrten ausgehenden
Python-Netzwerkverbindungen. 70 Testskripte wurden einzeln mit einer
120-Sekunden-Grenze gestartet. Keines überschritt diese Grenze. 55 Skripte
meldeten zusammen 998 unittest-Fälle; die übrigen verwenden eigene Checks
oder einen Voraussetzungsskip. Der erste Lauf ergab 64 bestandene Skripte,
fünf veraltete Fixtures/Erwartungen und einen fehlenden separaten lokalen
PostgreSQL-Testcluster. Die fünf Testvorlagen wurden mit erhaltenen
Sicherheits- und Fachprüfungen aktualisiert; alle fünf Skripte bestanden
anschließend. Weitere 44 Restorefälle bestanden mit den echten neuen
Sperrhelfern in ihrer isolierten Testvorlage.

Gezielte neue Prüfungen:

- Acht Systemaudit-Fälle: Erinnerungen, Mietbild-Transaktion, frische Zähler,
  vier konkurrierende Sperrwartefälle, PostgreSQL-Sperrfreigabe und
  Verbindungswiederverwendung mit Verbindungsfrist.
- Neun Prozessgrenzenfälle, 15 Artikelscan-Vorschaufälle, 17 Materialfoto-
  und 51 Materialportal-Fälle; zusätzlich 15 echte QR/EAN/Portal/Restore-Fälle.
- Nach Entkopplung der KI-Auslese bestanden 54 Materialportal- und 93
  Materialdialog-Fälle, zusammen 147. Neue parallele Fälle prüfen direkte
  und Worker-Analyse, nur einen KI-Aufruf, unabhängige Mutation während
  Auslese, Rechteentzug, Abbruch, Wiederherstellung, eine neue Mengenantwort
  während erneuter Auslese und die Abwehr veralteter Workerfehler.
- Smoke-, Angebots-/Ablauf- und Leistungsprüfung nach Integration bestanden.
- Auf dem echten Render-Linux: OpenCV 4.11 erkannte einen synthetischen QR-Code
  mit dem neuen Reader in 0,667 Sekunden unter der 1-GiB-Grenze. Ein dichtes
  Bild mit 256 QR-Codes endete nach 3,014 Sekunden; das bereinigte JPEG
  mit 1.471.123 Bytes blieb nutzbar, die Codeauslese wurde als nicht verfügbar
  zurückgegeben. Zusätzlich bestanden alle neun Prozessgrenzenfälle auf
  dem echten Linux-Server in 6,597 Sekunden.
- Die PostgreSQL-Prüfung verwendete ausschließlich verbindungslokale
  `pg_temp`-Tabellen, einen absichtlichen Division-durch-null-Fehler im
  optionalen Sicherungspfad und abschließendes Rollback. Keine Betriebsdaten
  wurden geändert; Testdateien lagen in einem anschließend entfernten
  temporären Verzeichnis.
- 29 Portalansichten wurden lesend im Browser geprüft;
  keine zeigte 502, 500 oder 405. Das ersetzt keine Prüfung aller denkbaren
  Inhalte oder Drittanbieter-Ausfälle. 16 zusätzliche Gesundheitsabrufe mit
  vier parallelen Aufrufern lieferten alle HTTP 200; langsamster Abruf:
  1,348 Sekunden. Diese Abrufe fanden vor dem gebündelten Update statt.

Die Serverlogs der letzten sieben Tage enthalten außerdem alte ungültige
Mitarbeiter-IDs und eine direkte Cursoriteration in der Belegansicht. Beide
Ursachen sind bereits in der Ausgangsbasis korrigiert. Sie werden nicht
erneut als neue Reparatur gezählt.

## Betrieb und Grenzen

Automatische Deploys bleiben ausgeschaltet. Die gebündelten technischen
Reparaturen werden erst nach Integration und Prüfung einmal kontrolliert
bereitgestellt. Die bestehende Render-Disk und deren Originale bleiben
erhalten. Es werden keine Kundenmitteilungen oder Bestellungen zu
Testzwecken versendet.

OCR, externe KI, SMTP und Datenbank können weiterhin zeitweise nicht
verfügbar sein. Die neuen Grenzen verhindern die konkret gefundenen
unbegrenzten Wartepfade; sie garantieren keine Fehlerfreiheit bei jedem
künftigen Betriebszustand. Fotoauslese bleibt prüfpflichtig und erzeugt
aus unsicheren Codehinweisen keine belastbaren Artikelbelege.

Referenzen für die verwendeten PostgreSQL-/Treiberverträge:
[Psycopg-Verbindungen](https://www.psycopg.org/psycopg3/docs/api/connections.html),
[PostgreSQL-Advisory-Locks](https://www.postgresql.org/docs/18/functions-admin.html),
[PostgreSQL-Verbindungsvorgaben](https://www.postgresql.org/docs/18/runtime-config-client.html).
