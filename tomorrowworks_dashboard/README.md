# Tomorrow Works – lokales Projektcockpit

Das Cockpit läuft getrennt von der Werkstatt-App auf Port `5070`, speichert seine
Daten unter `data/tomorrowworks_dashboard/` und ist im privaten Firmennetz für das
Team erreichbar. Beim ersten Öffnen wird der erste Admin-Zugang eingerichtet.

## Start

```powershell
python tomorrowworks_dashboard/app.py
```

Danach im Browser öffnen:

```text
http://127.0.0.1:5070
```

Die zusätzlich im Dashboard angezeigte Netzwerkadresse (zum Beispiel
`http://192.168.x.x:5070`) öffnen die anderen Mitarbeiter im selben LAN/WLAN.
Falls Windows nachfragt, Python nur für private Netzwerke freigeben.

## Enthalten

- persönliche Zugänge für Admin und Team
- Kunden- und Projektverwaltung
- automatisch erzeugtes, dauerhaftes Kunden-Ticket mit geheimem Kundenlink
- beidseitige Nachrichten, Änderungsverlauf sowie Logo-/Datei-Uploads
- ungelesene Kundenhinweise im gemeinsamen Dashboard
- vorbereitete E-Mail-Benachrichtigungen in beide Richtungen
- editierbare Angebote mit einmaligen und monatlichen Konditionen
- Verantwortliche und weitere Projektmitglieder
- persönlicher Tagesfokus je Mitarbeiter
- Status, Fortschritt, nächste Aufgabe, Priorität und Termine
- automatischer Git-Stand aus dem verbundenen Projektordner
- interne Website-Vorschau im Firmennetz, ganz ohne Kundendomain
- geschützter Codex-/Claude-Endpunkt für Status, Aufgabe und Fortschritt
- externe Vorschau-, Repository- und lokale Projektpfade
- geschützte PDF- und Bilddateien
- Projektaktivität und Notizen
- Filter nach Status, Mitarbeiter und Suchbegriff

## Empfohlener Ablauf pro Projekt

1. Kunde und Projekt anlegen.
2. Im Projekt den lokalen Git-Projektordner und den Vorschau-Unterordner
   hinterlegen (`.` bei einer einfachen Website, oft `dist` nach einem Build).
3. Die interne Vorschau starten. Jede gespeicherte Änderung in diesem Ordner ist
   anschließend sofort über die angezeigte LAN-Adresse sichtbar.
4. Den im Projekt angezeigten Agenten-Befehl in Codex oder Claude ausführen. Damit
   können Status, Fortschritt, nächste Aufgabe und eine Übergabenotiz automatisch
   im Dashboard erscheinen. Mit `--customer-update` wird zusätzlich eine echte
   kundenlesbare Neuerung im Ticket veröffentlicht und die Kunden-E-Mail ausgelöst.
5. Erst nach Bezahlung/Freigabe eine Kundendomain oder ein öffentliches Hosting
   hinterlegen.

Beispiel für ein Agenten-Update (den fertigen Befehl mit Projekt-ID und Schlüssel
zeigt das Dashboard bereits an):

```powershell
$env:TW_PROJECT_TOKEN="..."
python tomorrowworks_dashboard/agent_sync.py --project-id 1 --status in_arbeit --progress 60 --task "Mobile Startseite fertigstellen" --note "Interne Übergabe" --customer-update "Die mobile Navigation wurde verbessert"
```

Der Projekt-Schlüssel ist ein lokales Secret und gehört nicht in Git, `AGENTS.md`
oder `CLAUDE.md`.

## Kundenportal, Domain und E-Mail

Beim Anlegen eines Kunden entsteht automatisch ein permanentes Ticket. Solange
`TW_PUBLIC_BASE_URL` und SMTP fehlen, kann das Portal lokal vollständig getestet
werden; Kunden-E-Mails werden mit dem Grund `keine_domain` beziehungsweise
`nicht_konfiguriert` protokolliert und nicht fälschlich als versendet angezeigt.

Für den späteren echten Kundenbetrieb benötigt die App einen dauerhaft laufenden,
HTTPS-geschützten Server, regelmäßige Backups für Datenbank und Uploads sowie die
Werte aus `.env.example`. Ein Notebook im Büro ist dafür keine dauerhafte
Produktionsumgebung.

Die Angebotsannahme startet standardmäßig im sicheren Prüfmodus. Erst nach
juristischer Freigabe der konkret verwendeten Vertrags-, Verbraucher- und
Datenschutztexte darf `TW_CONTRACT_LEGAL_APPROVED=1` gesetzt werden. Dann verwendet
das Portal die ausdrücklich zahlungspflichtige Annahme.

Für Tests können Datenbank und Uploadordner mit `TW_DASHBOARD_DB_PATH` und
`TW_DASHBOARD_UPLOAD_DIR` auf temporäre Verzeichnisse gesetzt werden.

## Gemeinsamer Render-Dienst

Auf dem vorhandenen Render-Dienst kann das Cockpit ohne zweiten Webserver unter
`/agentur` mitlaufen. Gunicorn startet dafür `tomorrowworks_production:application`.
Die Dashboard-Daten liegen getrennt unter `/var/data/tomorrowworks_dashboard`;
der Render-Datenträger muss an `/var/data` eingehängt sein. Für die öffentliche
Adresse werden `TW_PUBLIC_BASE_URL=https://<portal-domain>/agentur` und die
gleichnamigen Werte aus `.env.example` gesetzt.
