# Portal-Neustarts und erneute 502-Meldung

Stand: 8. Oktober 2026, Task 123. Diese Aenderung betrifft die gemeinsame
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
