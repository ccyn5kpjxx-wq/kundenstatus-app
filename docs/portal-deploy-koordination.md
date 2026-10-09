# Portal-Neustarts und erneute 502-Meldung

Stand: 9. Oktober 2026, Tasks 123 und 127. Diese Aenderung betrifft die gemeinsame
Arbeitsroutine und Dokumentation; sie erfordert keinen Serverneustart.

## Beobachtung und Grenzen

Nach dem Systemaudit-Deploy `370573dd` um 17:23:38 Uhr Europe/Berlin
wurde das Werkstattfoto-/Auftragsnummernupdate `53daef086733` manuell
bereitgestellt: Render `dep-db3rnm6i0phs73bb0apg`, Start 17:50:49 Uhr,
Gesamtdauer 2 Minuten 35 Sekunden. Das Workerlog belegt den alten
Workerabschluss um 17:51:42 Uhr und den neuen Workerstart um 17:53:07 Uhr.
Die gesamte Build-/Deploydauer wird nicht als gemessene Ausfallzeit ausgegeben.

Die neue Nutzerstoerung wurde um 17:53 Uhr gemeldet. Der erste direkte
Healthabruf um 17:53:35 Uhr antwortete bereits HTTP 200 mit Build
`53daef086733`. Spaeter lautete die Zeitangabe nur "jetzt gerade"; eine
genaue betroffene URL wurde angefragt. Der Serverwechsel ist belegt,
die Zuordnung jeder gemeldeten 502-Anzeige dazu ist nicht bewiesen.

Cockpit, Werkstatttafel und Belegformular luden authentifiziert. Die direkte
Belegadresse fuehrte zum Formular. Zwoelf weitere Healthabrufe ueber ein
kurzes Beobachtungsfenster lieferten alle HTTP 200, maximal 0,190 Sekunden.
Render- und eigene Domain lieferten denselben Build. Im eingesehenen
Workerlog gab es keinen Worker-Timeout. Eine noch offene Browseransicht
mit Titel "502" auf dem Zugangseinrichtungspfad zeigte nach frischem Laden
wieder das Passwortformular; das Alter ihrer vorherigen Fehleranzeige ist
unbekannt. Es wurde kein Passwort eingegeben oder geaendert.

Die spaetere Kontrollprobe um etwa 18:58 Uhr lieferte weiterhin HTTP 200
mit demselben Build. Zwischenzeitlich blockierte ein lokaler GitHub-Abruf
bei der DNS-Aufloesung; er loeste keinen Deploy aus. Weitere Netzdiagnosen
wurden anschliessend durch eine harte Prozessfrist begrenzt.

Private Nachweise liegen unter `C:/tmp/gaertner-wide-audit-20261008/`:
`incident-repeat-initial.json`, `incident-repeat-deploy.txt`,
`incident-repeat-worker-log.txt`, `incident-repeat-health-window.json`
und `incident-repeat-live-ui.json`. Tokens, Kundeninhalte und Rohlogs
werden nicht mit dieser Dokumentation committet.

## Erneuter Vorfall am 9. Oktober

Das Featureupdate `5b4f14282900` wurde manuell mit Render-Deploy
`dep-db48l4dg1s2s738ks35g` um 08:32:50 Uhr Europe/Berlin gestartet.
Der alte Worker endete um 08:33:34 Uhr, der neue Worker bootete um
08:33:58 Uhr. Ein Workerboot ist noch kein Beleg fuer eine erreichbare App.
Die Render-HTTP-Anfragelogs, gefiltert mit `status_code:502`, zeigen:

| Zeit Europe/Berlin | Methode | Pfad | Status |
| --- | --- | --- | --- |
| 08:33:51 | POST | `/session/ping` | 502 |
| 08:33:57 | POST | `/session/ping` | 502 |
| 08:34:16 | GET | `/` | 502 |
| 08:34:25 | GET | `/werkstatt/tafel` | 502 |

Damit ist die Werkstatttafel waehrend dieses Serverwechsels nachweislich
nicht erreichbar gewesen. Die erste eigene Healthprobe um 08:35:38 Uhr
antwortete bereits HTTP 200 mit Build `5b4f14282900`. Die Zeitspanne zwischen
erstem und letztem protokollierten 502 ist keine gemessene Gesamtausfallzeit.
Eine minutengenaue Zuordnung jeder Nutzeranzeige bleibt ohne ihren
Anfragebeleg offen; der Neustartfehler der Werkstatttafel ist konkret belegt.

Die anschliessende authentifizierte Pruefung lud Cockpit, Bestellordner und
Belegformular. Die HTTP-Logs zeigten fuer Bestellordner 200 / 1445 ms,
Werkstatttafel 200 / 171 ms und Belegformular 200 / 322 ms. Es wurde durch
diese Stoerungspruefung kein weiterer Serverneustart ausgeloest.

Zwei nachgewiesene Codebefunde werden getrennt repariert: wiederholtes
Laden desselben historischen Preiskatalogs (Task 125) und eine unbehandelte
Rechteausnahme beim Schulplanabruf der Urlaubsseite (Task 126). Beide sind
keine nachgewiesene Ursache des protokollierten Deploy-502. Ein weiterer
wachsenden Bestellmengen betreffender Projektionsscan bleibt eine getrennte
Optimierungsaufgabe; der Zugriff auf alle Archivtage muss erhalten bleiben.

Private Rohbelege liegen unter `C:/tmp/gaertner-incident-20261009/` in
`initial-health.json`, `worker-log.txt`, `request-502.txt` und
`verified-requests.json`. IP-Adressen,
Browserkennungen, Tokens und Kundendaten werden nicht mit diesem Bericht
veroeffentlicht. Die Koordination wurde im TomorrowWorks-Projektkontext
erneut hinterlegt; sie ersetzt keine technische Verfuegbarkeitsloesung.

## Konsequenz fuer die gemeinsame Arbeit

Render beendet bei einer persistenten Disk die bisherige Instanz vor dem
Start der naechsten. Auto-Deploy Off verhindert automatische Ausloeser;
manuelle Deploys behalten diese Unterbrechung. Gunicorn- und Healthcheck-
Einstellungen koennen diese Speicherbeschraenkung nicht aufheben.
[Render: Disk-Einschraenkungen](https://render.com/docs/disks#disk-limitations-and-considerations),
[Render: Deployments](https://render.com/docs/deploys).

Deshalb gelten dieselben Updatevorgaben in `AGENTS.md` und `CLAUDE.md`:
Projektkontext vor jeder Liveaktion lesen; waehrend einer aktiven
Stoerungspruefung keine zusaetzlichen Featuredeploys; Featureaenderungen
in ein abgestimmtes Wartungsfenster sammeln; unmittelbar autorisierte
Liveaktionen vor dem Neustart ankuendigen. Der Deploymentclaim bleibt
bis zur geprueften Liveuebergabe aktiv. Dokumentationsaenderungen allein
werden nicht manuell auf dem Webserver bereitgestellt.

Diese Regeln sind eine betriebliche Koordination, keine technische Sperre
der Render-Schaltflaeche. Unterbrechungsfreie Deploys erfordern eine
getrennt geplante und getestete Migration aller benoetigten Dateien von
der Webservice-Disk in gemeinsam verfuegbaren Speicher. Vorher muessen
Uploads, Sicherungen, Papierkorb und weitere Dateipfade vollstaendig
inventarisiert werden. Das blosse Entfernen der Disk ist kein sicherer Fix.


## Vorbereiteter Umfang fuer unterbrechungsfreie Updates

Eine reine Aenderung des Uploadpfads reicht nicht. Das Codeinventar zeigt
neben Portaluploads auch MIME-Dateien, Recoveryjournale und Dateisperren des
Mailausgangs, Sicherungen und Papierkorb sowie die eingebundene
TomorrowWorks-SQLite-Datenbank mit WAL/SHM und eigenen Anhaengen. Einige
Originalklassen sind bereits in PostgreSQL gesichert; das ersetzt nicht die
noch benoetigten Dateibereiche. Die tatsaechlichen Livepfade und Mengen sind
vor einer Migration separat zu erfassen, ohne Zugangsdaten auszulesen.

Der zu pruefende Umbau besteht aus vier Schritten:

1. Alle dauerhaft benoetigten Dateiklassen mit Anzahl, Bytes und Pruefsummen
   erfassen; insbesondere ungesendete Mailauftraege und SQLite-Daten sichern.
2. Gemeinsame dauerhafte Ablage und Outbox-Sperren einbauen. TomorrowWorks
   entweder auf eine gemeinsame Datenbank umstellen oder getrennt betreiben.
   SMTP-Versand und andere Hintergrundarbeiten duerfen bei parallel laufenden
   Webinstanzen weiterhin nur einmal ausgefuehrt werden.
3. Einen Webservice ohne Disk parallel mit synthetischen Daten pruefen:
   Upload/Oeffnen/Loeschen/Wiederherstellen, Rechtewiderruf, Versand-Recovery,
   Backup-Restore und gleichzeitigen Wechsel zwischen alter und neuer Version.
4. Erst nach vollstaendigem Abgleich kontrolliert umschalten; bisherigen
   Dienst und Disk fuer einen getesteten Rueckweg erhalten.

Das ist ein vorbereiteter Migrationsumfang, keine bereits erfolgte Migration.
Speicheranbieter, vollstaendiger verifizierter Migrationsbestand, laufende Zusatzkosten und ein
Umschaltfenster sind noch nicht bestimmt. Es wurde kein weiterer Dienst
gebucht, keine Disk geloescht und keine Produktionsdatei verschoben.

## Gepruefter Reparaturstand nach dem Vorfall

Tasks 125, 126 und 128 sind getrennt getestet und unabhaengig geprueft.
Task 129 integriert ihre freigegebenen Commits auf dem aktuellen Hauptstand:

- Die Bestellhistorie und der echte Preisvergleich teilen pro GET einen
  frischen, nach Quellenrechten gefilterten Katalogstand. 551 unterschiedliche
  historische Identitaeten erfordern einen Katalogabruf; der Vergleich von
  zwoelf Kandidaten nutzt denselben Stand. Die Folgeanfrage beruecksichtigt
  widerrufene Lieferantenfreigaben und quarantinisierte Quellen.
- Technische Ausfaelle der optionalen Historie oder Rechnungskandidaten
  ergeben einen sichtbaren neutralen Hinweis. Angefangene Kandidaten werden
  verworfen; es gibt keinen Nullpreis oder alten Ersatzkandidaten.
- Ein Rechteentzug zwischen Urlaubsuebersicht und Schulplanabruf fuehrt vor
  der Ausgabe persoenlicher Daten zu HTTP 403. Ungueltige Angaben ergeben 400.

Die isolierte Integrationspruefung auf Git-Snapshot `7c0ee857` besteht:
29 Bestelluebersichts-, 23 Preisvergleichs- und 13 Schulplantests, insgesamt
65 Faelle. Externe Python-Verbindungen und SQLite ausserhalb des synthetischen
Testbereichs waren gesperrt; dotenv-Dateien wurden nicht eingelesen.
Syntaxpruefung und bytegleicher Abgleich mit den geprueften Agentenstaenden
bestehen. Nachweise: `C:/tmp/gaertner-integration-20261009/`.

Ein parallel gespeichertes Mitarbeiterupdate `7cacaf87` wurde danach
konfliktfrei uebernommen. Die Reparaturdateien bleiben bytegleich mit den
unabhaengig geprueften Staenden. Die neue isolierte Schnittstellenpruefung
auf Snapshot `55e0b159` besteht mit 13 Schul- und 17 Krankmeldungsfaellen;
die urspruenglichen Nachweise bleiben separat erhalten. Ergebnisse liegen
unter `C:/tmp/gaertner-integration-20261009/rebased/`.

Diese zusaetzlichen Codefixes sind zur Bereitstellung vorbereitet. Die
Stoerungspruefung loest keinen weiteren Runtime-Deploy aus. Die
nachgewiesene Unterbrechung bei Disk-Deploys erfordert eine Trennung der
dauerhaften Ablage vom Webservice. Der Livecheck um
09:08:27 Uhr Europe/Berlin lieferte weiterhin HTTP 200 / 0,152 Sekunden mit
Build `5b4f14282900`; Auto-Deploy wurde im Render-Dashboard erneut als Off
gelesen. Ein Git-Push allein stellt diese Reparaturen daher nicht live.

## Tatsaechliche Speichermetadaten

Eine begrenzte, lesende Zaehlung am laufenden Dienst ergab um 09:08 Uhr:

| Bereich | Tatsaechlicher Pfad | Dateien | Bytes |
| --- | --- | ---: | ---: |
| Portaluploads | `/var/data/uploads` | 124 | 137572330 |
| Mailausgang | `/var/data/mail_outbox` | 55 | 7864 |
| TomorrowWorks | `/var/data/tomorrowworks_dashboard` | 19 | 48874967 |
| Backup-Standardpfad | `/opt/render/project/src/data/backups` | 5 | 1019431134 |

`/var/data/backups` existierte nicht. `BACKUP_DIR` war in der Shellumgebung
nicht gesetzt; der zugehoerige Code verwendet bei PostgreSQL den
Source-Standardpfad. Diese Sicherungen liegen deshalb ausserhalb der Disk.
Die Disk hatte 1020702720 Bytes insgesamt und 814534656 Bytes frei.
Gespeichert wurden nur Pfade und aggregierte Metadaten; Dateiinhalte wurden
nicht geoeffnet. Zaehlungen ersetzen weder Pruefsummenabgleich noch
Restoretest. Der Kostenrahmen fuer eine gemeinsame
Ablage wurde angefragt; ein Anbieter ist noch nicht ausgewaehlt.
