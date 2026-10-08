# Portal: Webserver-Zuverlaessigkeit

## Betrieb

`requirements.txt` pinnt Gunicorn 26.2.0. Die bisherige Version 23.0.0
enthaelt bekannte gthread-Blockaden bei ausgelastetem Verbindungslimit und
bei nicht gelesenen HTTP-Request-Bodies. Der korrigierte Poller wurde upstream
mit [PR 3440](https://github.com/benoitc/gunicorn/pull/3440) eingefuehrt.

`gunicorn.conf.py` wird automatisch aus dem Repository-Root geladen, auch
mit dem bestehenden Render-Startbefehl. Beide Services in `render.yaml`
starten daher aus dem Repository-Root. Ein abweichender Start aus einem
anderen Verzeichnis muss die Datei explizit mit `--config` angeben.

- Ein Worker und vier Threads erhalten die bestehende App-Startlogik.
- `portal_gunicorn.PortalThreadWorker` delegiert Request-Verarbeitung und
  Verbindungsbuchhaltung an den gepinnten gthread-Worker. Ausschliesslich dessen
  graceful-close-Aufruf wird an eine getrennte Socket-Verwaltung uebergeben.
- `keepalive = 0` schliesst Verbindungen zwischen Render-Proxy und Gunicorn
  nach einer Antwort; Browser-Verbindungen verwaltet weiterhin der Proxy.
- Der Worker-Heartbeat verwendet unter Linux `/dev/shm`, damit
  Dateisystem-Metadatenzugriffe den Worker nicht auf einer Platte blockieren.
- Requestbasiertes Recycling bleibt ausgeschaltet: Mit nur einem Worker
  verursachte es bisher kurze Ausfaelle.
- Der nicht benoetigte lokale Gunicorn-Control-Socket bleibt deaktiviert.

Im **tatsaechlichen Render-Dashboard** muss unter Settings / Health Checks
`/healthz` eingetragen sein. Ein Eintrag allein in `render.yaml` aendert
manuell angelegte Services nicht. Ohne HTTP-Pfad kann ein haengender
gthread-Worker weiterhin den TCP-Port offenhalten und als gesund gelten.
Am 08.10.2026 wurde der bisher leere Dashboard-Wert auf `/healthz` gesetzt.
Render kann damit ausbleibende HTTP-Antworten erkennen und die Instanz
automatisch ersetzen. Dies verhindert keine voruebergehende Unterbrechung.

## Regressionstest auf Linux

Der Test importiert keine Portal-App. Er startet eine kleine synthetische
WSGI-Anwendung mit eigenen Loopback-Sockets und beendet ausschliesslich
seine eigene Prozessgruppe. Er verwendet weder Kundendaten noch die
Produktionsdatenbank. Alte und neue Gunicorn-Pakete separat installieren;
niemals dafuer das aktive Produktions-Virtualenv veraendern.

```sh
python -m pip install --no-deps --target /tmp/gunicorn23 gunicorn==23.0.0
python -m pip install --no-deps --target /tmp/gunicorn26 gunicorn==26.2.0
python scripts/test_gunicorn_reliability.py --suite legacy --gunicorn-path /tmp/gunicorn23
python scripts/test_gunicorn_reliability.py --suite corrected --gunicorn-path /tmp/gunicorn26
python scripts/test_gunicorn_reliability.py --suite production --gunicorn-path /tmp/gunicorn26 --config "$PWD/gunicorn.conf.py"
```

Die Legacy-Suite besteht nur, wenn beide alten Blockaden nachweisbar sind.
Die korrigierte Suite verlangt Antworten bei denselben Bedingungen. Die
Produktionssuite prueft drei Wellen mit jeweils 32 parallelen GET-/POST-
Anfragen, geschlossene Verbindungen und einen anschliessend antwortenden
Health-Endpunkt ohne Worker-Neustart. Windows kann diese POSIX-Servertests
nicht ausfuehren; die Syntaxpruefung allein ersetzt den Linux-Test nicht.

Am 08.10.2026 auf Render/Linux mit Python 3.14.3 ausgefuehrt:

| Pruefung | Ergebnis |
| --- | --- |
| Gunicorn 23.0.0, vier wartende Verbindungen am Limit | Alle vier Antworten liefen in den Timeout |
| Gunicorn 23.0.0, vier ungelesene 16-KiB-POSTs | Health blockiert, nach Schliessen der Verbindungen wieder erreichbar |
| Gunicorn 26.2.0, dieselben beiden Faelle | Alle Antworten und Health-Pruefungen bestanden |
| Gunicorn 26.2.0 mit Produktionskonfiguration | 3 x 32 Anfragen bestanden; langsamste Antwort 0,0215 s |
| Bestehender Ablauf-Test (`scripts/flow_test.py`) | Bestanden |

Der Windows-Smoke-Test hat zwei bestehende Fehler in UI-Textassertionen;
der Performance-Test ueberschreitet das bestehende Cockpit-SQL-Budget
(52 SELECTs). Diese Tests importieren Flask direkt und laden keine
Gunicorn-Konfiguration. App, Templates und diese Tests bleiben durch
den Webserver-Fix unveraendert. Details stehen in der lokalen Uebergabe.

## Nacharbeit: wartende Gegenstellen beim Verbindungsabschluss

Gunicorn 26.2.0 ruft `close_graceful()` aus `finish_request()` im Mainthread auf.
Nach FIN wartet diese Funktion bis zu zwei Sekunden auf die schliessende
Gegenstelle. Mehrere fertige Verbindungen werden als Callbacks nacheinander
beendet; damit koennen sich die Wartezeiten addieren, waehrend keine neuen
HTTP-Verbindungen angenommen werden. Die bisherigen Tests schlossen ihre
Client-Sockets sofort und deckten diesen Fall nicht ab. Dieser Codepfad ist
nachgewiesen; seine Rolle beim erneuten Live-Ausfall bleibt ohne Threadstack
offen.

`portal_gunicorn.py` verlagert ausschliesslich diesen Verbindungsabschluss in
einen einzelnen Worker-lokalen Selector-Thread. Es erzeugt keinen Thread pro
Client. FIN, maximal zwei Sekunden Linger und maximal 64 KiB Drain bleiben
erhalten; alle Lesezugriffe erfolgen nichtblockierend. Ein gemeinsames Limit
von 512 Sockets umfasst wartende und bereits registrierte Abschluesse. Bei
erschoepftem Limit wird FIN gesendet und unmittelbar geschlossen, statt den
Mainthread anzuhalten oder unbegrenzt Dateideskriptoren vorzuhalten. Der
normale unmittelbare Close-Pfad bleibt unveraendert. Die Verwaltung startet
erst im geforkten Worker und schliesst ihre Ressourcen beim Worker-Ende.

Die zusaetzlichen Linux-Suiten verwenden 32 bereits angenommene, zunaechst an
einer temporaeren Datei wartende Requests. Nach gemeinsamer Freigabe lesen
die Clients vollstaendige Antworten, behalten jedoch ihre Schreibrichtung
offen. Die Haelfte der Requests enthaelt 16-KiB-POST-Bodies und liefert
256-KiB-Antworten; damit wird auch die vollstaendige Auslieferung geprueft.

```sh
python scripts/test_gunicorn_reliability.py --suite close-baseline --gunicorn-path /tmp/gunicorn26
python scripts/test_gunicorn_reliability.py --suite close-corrected --gunicorn-path /tmp/gunicorn26 --config "$PWD/gunicorn.conf.py"
```

`close-baseline` verlangt einen reproduzierten Health-Timeout beim normalen
26.2.0-Worker und anschliessende Erholung nach Schliessen der Clients.
`close-corrected` verlangt drei erfolgreiche Wellen mit einer maximalen
Requestdauer von drei Sekunden, unveraenderter Worker-PID, genau sechs
Worker-Threads und Rueckkehr zur urspruenglichen Dateideskriptorzahl. Die
bisherige Produktionssuite bleibt ebenfalls auszufuehren. Windows-Syntax-
und Socket-Manager-Pruefungen ersetzen diese Linux-Integration nicht.

Am 08.10.2026 auf Render/Linux mit Python 3.14.3 und Gunicorn 26.2.0:

| Pruefung | Ergebnis |
| --- | --- |
| Normaler gthread-Worker, 32 offenbleibende Clients | 32 vollstaendige Antworten; Health-Timeout nach 3,0029 s reproduziert; Erholung und FD-Rueckkehr nach Client-Close, gleiche PID |
| PortalThreadWorker, drei Wellen mit je 32 offenbleibenden Clients | Alle 96 Antworten vollstaendig; Health 0,0004 / 0,0005 / 0,0004 s; sechs Threads, FD-Rueckkehr und gleiche PID |
| PortalThreadWorker, bisherige Produktionssuite | 96 GET-/POST-Requests bestanden; Maxima 0,0213 / 0,0190 / 0,0187 s; Connection-Close verifiziert |
| SIGURG am synthetischen PortalThreadWorker | Threadstack-Dump und anschliessender Health-Request bestanden |

Lokal unter Windows wurde die unveraenderte Socket-Verwaltung separat von
Gunicorn mit echten Socketpairs geprueft: 32 Clients bei Cap 8, begrenzte
Admission, FIN, Linger-Ablauf, Byte-Limit, sofortige Freigabe beim Stop und
kein Threadleck. Diese Pruefung ersetzt nicht den oben aufgefuehrten echten
Gunicorn-Test auf Linux.

Zusaetzlich liefen lokal drei konkurrierende Stop-/Submit-Pruefungen mit
je vier Produzenten und 240 Socketpairs: alle Server-Sockets geschlossen,
alle Kapazitaetsslots wieder frei, keine verbliebenen Threads.

Der `post_fork`-Hook registriert `SIGURG` nur im Worker fuer einen
`faulthandler`-Dump aller Threadstacks nach stderr, ohne Frame-Locals.
Bei einem erneuten Vorfall kann `kill -URG <Worker-PID>` vor einem Neustart
den Ausfuehrungspfad sichern. Die korrigierte Suite prueft dieses Signal
ausschliesslich an ihrem eigenen synthetischen Worker.

## Einordnung des Vorfalls

Beim wiederholten Ausfall am 08.10.2026 antworteten selbst `/healthz` und
statische Dateien nicht. Freier Speicher und die zum Diagnosezeitpunkt
kurzen PostgreSQL-Transaktionen erklaerten die Blockade nicht. Eine andere
Veroeffentlichung ersetzte die haengende Instanz vor einem Threadstack-Dump.
Die exakte Ursache dieser Live-Instanz ist daher nicht abschliessend
bewiesen. Der Fix beseitigt nachweisbare Blockaden der eingesetzten
Serverversion und ergaenzt die bisher fehlende HTTP-Selbstueberwachung.
