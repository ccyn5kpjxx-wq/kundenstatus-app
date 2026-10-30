# CHECK24-Termine in der Werkstatt

Der Import übernimmt CHECK24-Montagebuchungen aus dem konfigurierten IONOS-Postfach in die Werkstatt. Er schreibt weder in CHECK24 noch in das Postfach. Die Integration ist kein beidseitiger Kalenderabgleich: Freie Werkstattzeiten werden nicht an CHECK24 übertragen.

## Automatik und Prüfung

Nur eine neue Buchung im tatsächlich beobachteten CHECK24-Mailformat kann automatisch einen Werkstattauftrag erhalten. Dafür muss die Originalnachricht direkt über die konfigurierte, verschlüsselte IONOS-IMAP-Verbindung gelesen werden. Absender, Empfänger, der oberste IONOS-Authentifizierungsbericht mit passenden DKIM-/SPF-/DMARC-Ergebnissen und die Empfangsheader müssen die Herkunft bestätigen. Ein Absendername oder eine weitergeleitete Mail genügt nicht.

Unbekannte Formate, widersprüchliche Angaben, Änderungen, Stornierungen und Terminkonflikte bleiben zur Prüfung unter `/admin/check24`. Bestehende Buchungen bleiben dabei erhalten. Die Werkstatt muss die Änderung oder Stornierung mit CHECK24 abgleichen und anschließend im Portal bearbeiten. Eine hochgeladene `.eml` ist immer manuell zu prüfen; ihre mitgelieferten Header geben keine automatische Freigabe.

Ein Admin kann einen tatsächlich in CHECK24 bestätigten Storno mit Prüfvermerk ausdrücklich übernehmen. Dabei wird ausschließlich der zugehörige CHECK24-Auftrag archiviert und dessen Zeit freigegeben. Die Aktion ist gesperrt, sobald der Auftrag bereits in Arbeit oder abgerechnet ist. Das bloße Einlesen einer Stornomail oder das Dokumentieren einer Prüfung gibt keinen Termin frei.

Eine Umbuchungsmail verschiebt keine Zeit automatisch. Nach Abgleich mit CHECK24 kann ein Admin über „Bestätigte Terminänderung übernehmen“ Datum, Beginn und Ende mit Prüfvermerk setzen. Der neue Zeitraum wird auf Überschneidungen und Öffnungszeiten geprüft; Auftrag, gespeicherte Terminangaben und Reservierung ändern sich gemeinsam. Bereits begonnene oder abgerechnete Aufträge bleiben gesperrt. Der normale Auftrag-Editor verweist für diese Änderung in den CHECK24-Bereich. „Prüfung erledigt“ bleibt dagegen ein Vermerk und verschiebt keine reservierte Zeit.

Offene Prüfungen stehen vor der Historie. Die Liste ist seitenweise abrufbar, damit ältere ungeklärte Nachrichten auch nach vielen späteren Importen erreichbar bleiben.

Der Import speichert Herkunftskennungen und Inhaltsprüfsummen, damit wiederholtes Einlesen keine doppelten Aufträge erzeugt. Bei abweichenden Inhalten zu einer bestehenden Buchung ist eine erneute Prüfung erforderlich. Die Anzeige unterscheidet eingelesene Nachrichten von tatsächlich übernommenen Buchungen.

## Postfachabgleich

Die bestätigten Neumail-Varianten „Montagetermin“ und „Servicetermin“ werden erkannt, auch bei mitgebrachten Reifen ohne Lieferangaben. Leistung, Termin, Bestellnummer und Herkunft werden weiterhin geprüft. Ein Hinweis auf Änderung oder Storno führt weiterhin zur manuellen Prüfung.

Nach einer Parserkorrektur werden zuvor unbekannte, noch ungeklärte Nachrichten beim nächsten regulären Abgleich einmal erneut aus dem Originalpostfach gelesen. Die Parser-Version steht im gespeicherten Ergebnis. Nur identische Originalbytes dürfen den bisherigen Prüfdatensatz aktualisieren; Herkunft und Terminprüfungen laufen erneut. Bereits übernommene oder abgeschlossene Prüfungen werden nicht wieder geöffnet. Fehlende Originalmails und weiterhin unklare Buchungen bleiben im Prüfbereich. Das Reifenportal zeigt hierfür oben einen Warnhinweis mit direktem Link; ein erfolgreicher Postfachabruf bedeutet nicht, dass alle Buchungen übernommen wurden.

Es gilt die bereits hinterlegte Werkstatt-IMAP-Konfiguration. Der Ordner wird nur lesend geöffnet; Nachrichten werden weder als gelesen markiert noch verschoben oder gelöscht. Der Suchzeitraum umfasst die letzten 45 Tage, einschließlich bereits gelesener Nachrichten. Ein Durchlauf verarbeitet höchstens 50 noch nicht erfasste Nachrichten. Weitere Nachrichten folgen in späteren Durchläufen. UIDVALIDITY und Nachrichten-UID gehören zur Herkunftskennung.

Ein Durchlauf beginnt nach 60 Sekunden Laufzeit keine weitere Nachricht mehr. Ein bereits laufender IMAP-Aufruf darf noch zurückkehren; sein Socket-Timeout ist auf höchstens 20 Sekunden begrenzt. Das Zeitbudget ist deshalb kein harter Abbruch einer bereits begonnenen Netzwerkoperation. Die 300-Sekunden-Lease wird vor jeder Nachricht erneuert und in der Schreibtransaktion erneut geprüft. Ein verspäteter Prozess kann nach Ablauf oder Übernahme seiner Lease nichts mehr veröffentlichen. Netzwerkaufrufe halten keine Datenbanktransaktion offen.

Der Import kann lokal Aufträge und Prüfdatensätze schreiben. Er versendet keine E-Mails, WhatsApp-Nachrichten oder sonstigen Kundenbenachrichtigungen. Eine erfolgreiche CLI-Ausgabe nennt nur die Anzahl neu geprüfter Nachrichten; sie bestätigt weder eine Kundenbenachrichtigung noch die Zahl neu angelegter Aufträge.

## Regelmäßiger Abruf im vorhandenen Webprozess

Der eigene CHECK24-Thread benötigt keinen weiteren Hosting-Dienst und verwendet keine MOS-Aufgaben. Er ist standardmäßig ausgeschaltet. Nur im vorgesehenen Werkstatt-Webdienst aktivieren:

```text
CHECK24_WORKER_ENABLED=1
CHECK24_WORKER_INTERVAL_SECONDS=300
```

Das Intervall wird auf 60 bis 86.400 Sekunden begrenzt. Der Launcher `start_check24_worker(service)` startet im Testmodus und im öffentlichen Homepage-Modus keinen Thread; wiederholte Aufrufe im selben Prozess erzeugen keinen zweiten Thread. Der erste Abruf beginnt frühestens nach 15 Sekunden. Registrierung und Webstart führen keinen synchronen Postfachaufruf aus. Die bestehende Gunicorn-Konfiguration verwendet keinen Preload; bei einer späteren Umstellung muss der Start ausdrücklich in den geforkten Webworker verlegt werden.

Worker, manuelle Abgleiche und CLI teilen dieselbe dauerhafte DB-Lease. Manuelle Aufrufe umgehen nur die Wartezeit bis zum nächsten geplanten Lauf. Eine belegte Lease wird als bereits laufender Abgleich gemeldet. Nach Fehlern wartet der automatische Abruf zunächst 60 Sekunden, bei wiederholten Fehlern bis zu 15 Minuten. Ein geänderter Postfachzugang oder ein angehaltener/deaktivierter Worker verhindert weitere Schreibvorgänge des alten Laufs.

Der Prüfbereich zeigt den letzten erfolgreichen Lauf, den nächsten geplanten Lauf, einen verständlichen Verbindungsstatus und die zuletzt geprüfte Anzahl. „Automatischer Abruf aktiv“ verlangt einen tatsächlichen Thread im aktuellen Webprozess und einen aktuellen Heartbeat; andernfalls erscheint „wartet auf Rückmeldung“. Die Statusanzeige liest nur die Datenbank. Tokens, Postfachkennungen und rohe Fehlermeldungen erscheinen dort nicht.

## Separates CLI-Werkzeug

Ohne Freigabeschalter passiert nichts:

```powershell
python scripts/check24_sync.py
```

Dieser Aufruf importiert die App nicht und greift weder auf Postfach noch Datenbank zu. Er ist ein Statushinweis, kein Testlauf mit echten Mails.

Ein ausdrücklich aktivierter einzelner Durchlauf:

```powershell
$env:CHECK24_SYNC_ENABLED = '1'
python scripts/check24_sync.py --run --once
```

Beide Voraussetzungen sind notwendig: `--run` und die im Prozess gesetzte Umgebungsvariable `CHECK24_SYNC_ENABLED=1`. Die Variable wird bewusst vor dem Appimport geprüft; ein Eintrag nur in einer erst durch `app.py` geladenen `.env.local` aktiviert das CLI nicht. Mit `--run` ohne Moduswahl gilt ebenfalls ein einzelner Durchlauf. Dieser Modus prüft die Mails und übernimmt geeignete neue Buchungen automatisch.

Für einen eigens eingerichteten Worker:

```powershell
$env:CHECK24_SYNC_ENABLED = '1'
python scripts/check24_sync.py --run --loop --interval 300
```

Das Intervall ist die Pause nach dem abgeschlossenen Durchlauf und beträgt mindestens 60 Sekunden. Bei einem Fehler beendet sich der Prozess mit Status 1 statt selbst weiterzulaufen. Fehlende Aktivierung führt zu Status 2, regulär erfolgreiche Einzelläufe zu 0, Abbruch mit Strg+C zu 130. Eine Dienstverwaltung kann einen kontrollierten Neustart übernehmen. Das Werkzeug startet keinen Thread im Webserver.

Beim Appimport deaktiviert das CLI ausschließlich in seinem eigenen Prozess die fachfremden Worker, automatischen Backups sowie Lexware-/Google-Ads-Synchronisation. Die gespeicherte App- oder Dienstkonfiguration bleibt unverändert. Der reguläre Appimport kann Schemas initialisieren; eine produktive Ausführung muss daher mit den üblichen Deployment- und Datenbankregeln koordiniert werden.

**Der automatische Abruf bleibt ohne Aktivierung ausgeschaltet; seine Bereitstellung allein bestätigt keinen laufenden Live-Abgleich.** Vor Aktivierung müssen Postfach, Ziel-Datenbank, Prüfzuständigkeit und Überwachung eingerichtet und ein einzelner kontrollierter Lauf geprüft werden. Keine Geheimnisse als Kommandozeilenargumente oder in Protokollen hinterlegen. Für den regelmäßigen Betrieb genügt der vorhandene Webprozess; ein zusätzliches CLI-Loop oder externer Zeitplan ist dafür nicht nötig. Das CLI erzwingt `CHECK24_WORKER_ENABLED=0` in seinem eigenen Prozess und startet beim Appimport keinen zweiten CHECK24-Thread.

## Abschalten und Rücknahme

Im Webdienst `CHECK24_WORKER_ENABLED=0` setzen und den Webprozess kontrolliert neu starten. Ein eventuell separat laufendes CLI oder dessen Zeitplan muss zusätzlich beendet und `CHECK24_SYNC_ENABLED` entfernt bzw. auf `0` gesetzt werden. Bei einem bereits laufenden Prozess reicht eine Änderung der äußeren Umgebungsvariable nicht. Bereits importierte Aufträge und Prüfdatensätze bleiben erhalten. Sie werden bei einer Rücknahme niemals automatisch gelöscht. Falsche Zuordnungen oder Termine werden nach Prüfung im normalen Werkstattablauf korrigiert.

Das Abschalten des CLI verhindert keine bewusst im angemeldeten Adminbereich gestarteten manuellen Abgleiche. Mailimport und Prüfbereich sind weiterhin getrennt von der providerseitigen CHECK24-Terminverwaltung.
