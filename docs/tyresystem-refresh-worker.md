# Privater TYREsystem-Refresh im Portal-Webworker

Vorbereiteter Code, standardmäßig ausgeschaltet. Dieser Manager aktiviert
weder Katalogveröffentlichung noch Zahlungen, SMS, Lieferantenbestellungen oder
Montage. Er lädt keine Konfiguration und startet keine Arbeit beim Import.

Der Prozess muss im bestehenden Portal-Webservice laufen: Nur dort ist seine
Render-Disk eingebunden. Ein separater Render-Worker oder Cronservice teilt
diese Disk nicht. Private Caches gehören auf die tatsächlich eingebundene,
dauerhafte Disk, beispielsweise unter `/var/data/tyresystem-private/`.
Diese Dokumentation legt keinen Dienst und keine Disk an.

## Explizite Einrichtung

Nur `TYRESYSTEM_REFRESH_ENABLED=1` aktiviert den Manager. Fehlender Wert,
andere Schreibweisen, `portal.PUBLIC_SITE_ONLY` ohne den ausdrücklichen Wert
`False`, `app.config['PUBLIC_SITE_ONLY']=True` oder aktivierter
`STORAGE_PRESSURE_MAINTENANCE` verhindern den Start. Das Exportziel wird allein
als `TYRESYSTEM_EXPORT_URL` im serverseitigen Secretstore hinterlegt. Nicht in
Chat, Git, Prozessargumente oder Logs schreiben.

Diese vier Pfade müssen ausdrücklich konfiguriert sein:

- `TYRESYSTEM_PRIVATE_ROOT`: bestehendes privates Stammverzeichnis der Reifen.
- `TYRESYSTEM_CATALOG_DIR`: eigenes Unterverzeichnis dieses Stamms.
- `FELGEN_PRIVATE_ROOT`: bestehendes privates Stammverzeichnis der Felgen.
- `FELGEN_CATALOG_DIR`: anderes, nicht verschachteltes Cache-Unterverzeichnis.

Beide privaten Stammverzeichnisse dürfen gleich sein. Verzeichnisse müssen
absolute lokale, kanonische Pfade ohne Symlinkumleitung sein. UNC-/Netzadressen,
öffentliche Verzeichnisse wie `static`/`public`/`templates`/`outputs`, ein Cache
gleich seinem Stamm und identische oder ineinander verschachtelte Caches werden
abgewiesen. Der Manager erzeugt oder verändert keine Cachedateien selbst.
Das Einrichten der Verzeichnisse und ihre Zuordnung zur tatsächlichen Web-Disk
bleiben Teil der betrieblichen Bereitstellung.

## Lifecycle-Vertrag

Die öffentliche Schnittstelle lautet:

```python
from tyresystem_refresh_worker import start_refresh_worker, stop_refresh_worker

manager = start_refresh_worker(portal, environment=environment)
stopped = stop_refresh_worker(portal)
```

`environment` ist optional; ohne Angabe wird beim expliziten Start die aktuelle
Prozessumgebung verwendet. `portal` stellt `app.config`, `app.extensions` und
den Logger der vorhandenen Portal-App bereit. Start liefert den Manager oder
`None`; Stop liefert `True` bei abgeschlossenem Stop beziehungsweise fehlender
Instanz, sonst `False`. Wiederholter Start derselben App liefert dieselbe
Instanz, statt einen zweiten Timer anzulegen. Vor einem Wechsel der laufenden
Konfiguration die alte Instanz explizit stoppen. Invalides Neuinitialisieren
oder Abschalten versucht ebenfalls zuerst, den bisherigen Manager zu stoppen.

Startup erfolgt im vorbereiteten Code erst nach dem Laden der App im einzelnen
Gunicorn-Webworker, nicht im Master, beim Modulimport oder im Request. Die
vorhandene `gunicorn.conf.py` verbindet `post_worker_init` mit dem Start und
`worker_exit` mit dem Stop und prüft den Einzelworkerbetrieb. Diese Konfiguration
muss auch im tatsächlich bereitgestellten Build geladen sein. Ohne den dazu
geprüften Shutdownhook darf der Timer nicht aktiviert werden. Der Manager
installiert und ersetzt selbst keine SIGTERM-Handler. Normales SIGTERM an den
Webworker führt bei kontrolliertem Worker-Ende über den Gunicorn-Lifecycle zum Stop.
Ein bloßer `atexit`-Eintrag oder ein App-Importstart ersetzt diesen Vertrag nicht.

Die Instanz steht privat unter
`app.extensions['tyresystem_refresh_worker']`. Die sichere Startdiagnose liegt
unter `app.extensions['tyresystem_refresh_status']`. Konfigurations-/Startfehler
verhindern nur den Refresh und erzeugen einen allgemeinen Fehlercode im Logger;
URLs, Rohantworten, Credentials und private Pfade werden nicht protokolliert.

## Ablauf und Grenzen

Nach explizitem Start beginnt ein Daemonthread mit einer einmaligen Erneuerung;
danach wartet er jeweils 300 Sekunden nach dem Lauf. Kein Kunden-GET führt einen
Download aus. Die vorhandenen Katalogprovider behalten die Original-Empfangszeit
und ihre 15-Minuten-TTL. Ein Fehler etikettiert alte Daten nicht als frisch.

Ein Child läuft über `sys.executable`, `shell=False` und die feste Datei
`scripts/refresh_tyresystem_catalog.py` mit `--run`, `--cache-dir`,
`--private-root`, `--felgen-cache-dir` und `--felgen-private-root`. Nur das
minimierte Child-Environment enthält den Exportlink; andere Geschäftsgeheimnisse
der App werden nicht übernommen. Ein-/Ausgabe sind `DEVNULL`, sodass weder
Rohdaten noch Anbieterfehler im Weblog landen und kein voller Pipepuffer hängt.

Die CLI lädt genau einen autorisierten, begrenzten HTTP-Export und verarbeitet
beide Kategorien. Der Manager wartet einschließlich Abbruchreserve höchstens
240 Sekunden an kontrollierten Wartezeiten: spätestens nach 235 Sekunden folgt
SIGTERM/Terminate mit bis zu drei Sekunden Wartezeit, danach nötigenfalls Kill
mit bis zu zwei Sekunden.
Die CLI besitzt zusätzlich eigene kürzere HTTP-/Ablaufbudgets und
prozessübergreifende OS-Advisorylocks gegen doppelte Downloads. Die dauerhafte
Lockdatei ist kein Nachweis eines noch laufenden Prozesses und wird nicht vom
Manager gelöscht. Einzelne Cachedateien werden atomar geschrieben; die zwei
Cachedateien bilden keine gemeinsame atomare Dateitransaktion.

Wie bei Python-Subprozess-Timeouts allgemein lässt sich der native
`Popen`-Prozessstart selbst nicht durch dessen späteres Wait-Timeout abbrechen.
Der Deadline-Zeitpunkt wird deshalb bereits vor dem Start festgehalten. Kehrt
der native Start verspätet zurück, wird der Child sofort ohne zusätzliche
Grace-Wartezeit beendet. Während eines noch nicht zurückgekehrten Starts bleibt
ein unvollständig gestoppter Manager registriert; es wird kein zweiter gestartet.

Stop setzt zuerst das Stopereignis, beendet einen aktiven Child und wartet
höchstens acht Sekunden insgesamt auf Child/Daemon. Er wartet nicht dessen
240-Sekunden-Budget ab. Dies begrenzt den kontrollierten Cleanupversuch;
es garantiert keine verbleibende Zeit im externen Render-Shutdownfenster.
Gunicorn kann dieses Fenster beim vorherigen Abwarten laufender Requests
bereits verbrauchen. Ein SIGKILL kann den Hook verhindern oder unterbrechen.
Ein nicht nachgewiesen beendeter Child blockiert weitere Starts dieser
Managerinstanz. Fehlgeschlagener Stop wird als `stop_incomplete` gemeldet;
ein noch lebender Manager wird nicht still aus `app.extensions` entfernt.

`manager.status` liefert ausschließlich sichere Zustände, Laufanzahl und
`cache_must_be_checked`. Jeder erzwungene Abbruch setzt dieses Flag bereits vor
SIGTERM konservativ auf `True`: Auch ein kontrolliert behandeltes SIGTERM kann
zwischen den beiden Kategorie-Schreibvorgängen liegen. Ein harter Kill kann
einen untergeordneten atomaren Cachewriter unterbrechen und dessen
O_EXCL-Schreiblock zurücklassen; bei einem
fehlgeschlagenen CLI-Lauf kann auch nur eine Kategorie bereits erneuert sein.
Ein CLI-Fehler setzt ebenfalls `cache_must_be_checked=True`. Es gibt keine
automatische Locklöschung, kein Receipt-Rollback, keine erfundene gemeinsame Frische und keine
Shopfreigabe. Nach erzwungenem Abbruch Prozessende sowie beide Caches und
gegebenenfalls verwaiste Cachewriterlocks betrieblich prüfen. Ein SIGKILL des
ganzen Webworkers kann keinen Python-Shutdownhook garantieren.

## Offlineprüfung

`python -B scripts/test_tyresystem_refresh_worker.py` verwendet ausschließlich
Fakeprozesse, Fakethreads und eine virtuelle Uhr. Kein echter Thread, Child,
Download oder Cachewrite wird ausgelöst. Die Fälle prüfen Default-off,
Public-/Wartungssperre, private Pfade, App-Singleton, Geheimnisfreiheit von argv
und Status, Exit-/Timeout-/Killpfade, verspäteten Childstart und den kurzen Stop.
Diese Tests beweisen weder eine aktivierte Render-Konfiguration noch einen
echten Providerabruf. Die gesonderten Hooktests prüfen zusätzlich den Codepfad
zwischen der vorhandenen Gunicorn-Konfiguration und dem tatsächlichen Manager
mit unterbundenen Thread- und Prozessstarts.
