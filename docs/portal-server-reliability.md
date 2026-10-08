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

## Einordnung des Vorfalls

Beim wiederholten Ausfall am 08.10.2026 antworteten selbst `/healthz` und
statische Dateien nicht. Freier Speicher und die zum Diagnosezeitpunkt
kurzen PostgreSQL-Transaktionen erklaerten die Blockade nicht. Eine andere
Veroeffentlichung ersetzte die haengende Instanz vor einem Threadstack-Dump.
Die exakte Ursache dieser Live-Instanz ist daher nicht abschliessend
bewiesen. Der Fix beseitigt nachweisbare Blockaden der eingesetzten
Serverversion und ergaenzt die bisher fehlende HTTP-Selbstueberwachung.
