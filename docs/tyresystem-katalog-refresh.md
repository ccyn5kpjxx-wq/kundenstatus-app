# Gemeinsamer TYREsystem-Katalogrefresh

Stand: 03.11.2026. Expliziter Serverbefehl; keine Aktivierung, kein Lieferantenauftrag
und keine Änderung von Veröffentlichungs-, Bild- oder Kaufrechten.

## Richtiger Ausführungsort

Das eingecheckte render.yaml beschreibt den Portal-Webdienst mit einem Gunicorn-
Worker und persistenter Disk unter /var/data (DATA_DIR=/var/data). Der zusätzliche
öffentliche Webdienst hat diese Disk nicht. Ein separater Render-Cronjob oder
Background-Worker erhält ebenfalls keinen Zugriff auf die Disk des Portal-
Webdienstes. Ein erfolgreicher Abruf dort würde den vom Portal gelesenen Cache
nicht aktualisieren.

Diesen Befehl deshalb im Dateisystemkontext des Portal-Webdienstes ausführen,
mit dessen privatem Persistent-Disk-Pfad und dem serverseitigen Exportsecret.
Eine spätere regelmäßige Ausführung braucht einen ausdrücklich aktivierten
Manager in genau diesem Dienstkontext oder eine separat geplante Umstellung
auf gemeinsamen Objektspeicher. Dieser Befehl startet keinen Timer, Webthread
oder HTTP-Download aus einer Kundenanfrage. Deployment und Manageraktivierung
sind eigene, koordinierte Schritte.

## Aufruf

Beispielpfade, keine automatisch gesetzte Produktionskonfiguration:

```sh
python -B scripts/refresh_tyresystem_catalog.py --run \
  --cache-dir /var/data/tyresystem/catalog \
  --private-root /var/data/tyresystem \
  --felgen-cache-dir /var/data/tyresystem/felgen-catalog \
  --felgen-private-root /var/data/tyresystem
```

Die tatsächlichen Flags müssen genau zu TYRESYSTEM_CATALOG_DIR / TYRESYSTEM_PRIVATE_ROOT
und FELGEN_CATALOG_DIR / FELGEN_PRIVATE_ROOT des lesenden Webdienstes passen.
Die Dateien bleiben catalog.json und felgen-catalog.json; kein Formatwechsel.
Ohne --felgen-private-root gilt für Felgen derselbe --private-root.
Der bisherige Reifenaufruf ohne --felgen-cache-dir bleibt unterstützt.
--run ist zwingend; ohne diese ausdrückliche Autorisierung werden weder
Credentials gelesen noch Cacheordner angelegt oder HTTP-Aufrufe ausgelöst.

TYRESYSTEM_EXPORT_URL ausschließlich als eigenes serverseitiges Env-Secret
setzen. Nicht in Argumente, Quellen, Logs oder einen Weblink schreiben.
Kein dotenv-/Flask-/Appimport. Aufrufausgabe ist ein kleines JSON mit Erfolg,
Anzahlen, ursprünglichem UTC-Receipt, Quellhash und Ablaufzeit bzw. festen
Fehlerflags. Keine URL, Einkaufspreise, Rohdaten, Pfade oder Providerfehlertexte.

## Transport und Daten

Ein autorisierter GET, keine Redirects oder Retries, TLS-Prüfung, isolierte
Session ohne Umgebungsproxy/Cookies. Vorhandene Exportgrenzen bleiben erhalten:
maximal 64 MiB, Connecttimeout 3,05 s, Readtimeout 10 s, insgesamt 45 s für
Download und bestehenden fetch-Parser. fetch_raw() benutzt dieselbe Transport-
strecke und gibt die Bytes mit dem einmaligen ursprünglichen Empfangszeitpunkt
zurück. Sie ist eine private API und behauptet allein noch keine CSV-Gültigkeit.

Der Dualrefresh verwendet die tatsächlichen parse_feed- und parse_felgen_feed-
Parser. Beide Snapshots müssen nichtleer sein und denselben Original-Quellhash
und Receipt besitzen. Erst danach darf die erste Zieldatei ersetzt werden.
Identische Artikelzeilen werden von den Parsern dedupliziert, widersprüchliche
SKU-Zeilen einschließlich Konflikten zwischen Kategorien quarantänisiert.
Ein leerer oder strukturell ungültiger Teil verwirft den gesamten Abruf.

Die Gültigkeit bleibt Receipt + 15 Minuten. Lesen, Neuparsen, Schreiben oder
Fehler erneuern diese Zeit nicht. Eine langsamere Aktualisierung kann einen
vorhandenen neueren Receipt in keinem Ziel zurücksetzen. Ein frischer, vollständig
validierter Export kann einen beschädigten Altcache reparieren. Der Dualbefehl
hat zusätzlich ein 90-s-Budget mit Prüfungen nach den begrenzten Arbeitsschritten;
blockierende Dateisystemoperationen sind damit keine harte Echtzeitgarantie.
Keine Bestandsreservierung, keine Lieferzusage, kein Setpreis und keine neue
Quote werden daraus erfunden. Katalogleser müssen weiterhin selbst abgelaufene
Daten ablehnen.

## Sperren, Dateiersetzung und Fehler

Vor dem GET werden auf beiden Zielordnern nichtblockierende Betriebssystem-
sperren gehalten (Linux flock, Windows msvcrt). Eine überlappende Aktualisierung
führt ohne zweiten GET zu Exit 1. Die neutralen .tyresystem-refresh.lock-Dateien
bleiben liegen: Dateiexistenz bedeutet keine aktive Sperre; Prozessende gibt
die Sperre frei. Diese Dateien nicht löschen, um keine noch aktive Sperre über
einen zweiten Inode zu umgehen.

Die existierenden Cache.replace-APIs schreiben je eine vollständige, geprüfte,
checksummierte Datei über temporäre Datei, fsync und os.replace. Leser sehen
je Kategorie die vollständige alte oder vollständige neue Datei. Zwei Dateien
sind keine gemeinsame Dateisystemtransaktion. Scheitert die zweite Ersetzung,
kann Reifen bereits den neuen Receipt haben, während Felgen beim alten bleibt:
Exit 1, updated=false, partially_updated=true, cache_must_be_checked=true.
Zusätzlich kennzeichnet cache_may_have_changed=true jede bereits begonnene
Schreibphase konservativ. Ein Signal kann unmittelbar nach os.replace, aber vor
dessen Rückkehr eintreffen; partially_updated=false beweist deshalb keine
unveränderten Dateien. Bei cache_may_have_changed=true immer beide Ziele prüfen.
Kein gefährlicher Rollback und kein Umstempeln alter Daten. Nach Ursachenprüfung
führt ein neuer expliziter Aufruf einen neuen GET mit seinem echten Receipt aus.

SIGTERM wird im CLI kontrolliert abgewickelt, damit HTTP- und bestehende
Cache-Schreiblocks ihre finally-Bereinigung ausführen. SIGKILL, Stromausfall oder
harter Prozessabbruch während Cache.replace können zusätzlich dessen eigene
.catalog-write.lock / .felgen-write.lock hinterlassen. Vor Eingriffen laufende
Prozesse und Zielzustand prüfen; keine automatische altersbasierte Löschung.

Exitcodes: 0 = vollständig erfolgreich; 1 = Abruf/Prüfung/Sperre/Schreiben fehlgeschlagen;
2 = ausdrücklicher Aufruf oder nötige Argumente fehlen. Keine automatische
Wiederholung. Ein späterer Manager muss Laufzeitlimit, Stopgrace und Retryrhythmus
bewusst konfigurieren und den Cache bei Fehlern auslaufen lassen.

## Offline-Prüfung

```sh
python -B scripts/test_tyresystem_catalog_refresh.py
python -B scripts/test_tyresystem_export.py
python -B scripts/test_tyresystem_provider.py
```

Die Tests benutzen synthetische CSVs, Fake-HTTP und temporäre private Verzeichnisse.
Sie prüfen unter anderem einen einzigen GET, Originalreceipt/TTL/Hash, Kategorie-
Deduplizierung, Deadline-/Authentifizierungs-/Diskfehler, Teilaktualisierung,
Betriebssystemsperren und Freigabe nach einem beendeten isolierten Testprozess.
Kein echter Export und keine Veröffentlichung werden ausgeführt.
