# Deployment auf Render

Diese App kann aus GitHub als Render Web Service gestartet werden.

## Vorbereitung

Diese Dateien sind fuer Render wichtig:

- `requirements.txt` installiert Flask, OCR-Abhaengigkeiten, Postgres-Treiber und `gunicorn`.
- `Procfile` startet die App mit `gunicorn app:app`.
- `render.yaml` enthaelt eine Render-Vorlage.

Nicht nach GitHub pushen:

- `.env.local`
- `data/auftraege.db`
- `data/uploads/`
- echte Kundendaten, Belege, Bilder, API-Keys

## Was stabil ist und was nicht

Der lokale Cloudflare-/Tunnel-Link ist nur ein Testzugang. Wenn der Tunnel oder der PC aus ist,
ist nur der Link weg. Die lokalen Daten bleiben unter `data/` erhalten.

Für stabilen Betrieb gehört die App auf Render:

- Datenbank: Render Postgres über `DATABASE_URL`
- Uploads/Backups: Render Disk unter `/var/data`
- Öffentlicher Link: feste Render-Adresse, z. B. `https://kundenstatus-app.onrender.com`

## Render einrichten

1. GitHub-Repo bei Render als neuen Web Service verbinden.
2. Build Command:

```text
pip install -r requirements.txt
```

3. Start Command:

```text
gunicorn app:app
```

4. Environment Variables setzen:

```text
ADMIN_PASS=<eigenes-sicheres-admin-passwort>
FLASK_SECRET_KEY=<langer-zufallswert>
OPENAI_API_KEY=<optional>
OPENAI_EXTRACTION_MODEL=gpt-4o
UPLOAD_DIR=/var/data/uploads
```

Wichtig: Im `render.yaml` ist eine Postgres-Datenbank vorbereitet. `DATABASE_URL` wird daraus automatisch gesetzt.

## Lokale Daten nach Render übernehmen

Vor dem Umzug lokal ein Importpaket erzeugen:

```powershell
python scripts/export_render_import_package.py
```

Die Ausgabe liegt unter:

```text
data/exports/kundenstatus-render-import-<datum>.zip
```

Nach dem Render-Deploy:

1. Render-Admin öffnen: `https://<render-app>.onrender.com/admin`
2. Mit `ADMIN_PASS` einloggen.
3. Im Dashboard das Datenpaket unter "Datenimport" hochladen.

Der Import übernimmt Fahrzeuge, Autohäuser, Chats, Statuslog, Hinweise, Reklamationen,
Dateiverweise und die Upload-Dateien. Vor dem Import legt die App auf dem Server ein
Sicherheitsbackup an.

## Uploads

`UPLOAD_DIR=/var/data/uploads` liegt auf dem im `render.yaml` eingetragenen Render Disk. Damit bleiben hochgeladene Dateien erhalten, solange der Render Disk bestehen bleibt.

## Links verschicken

Nach dem Deploy bekommst du eine oeffentliche Render-Adresse, zum Beispiel:

```text
https://kundenstatus-app.onrender.com
```

Dann ersetzt du `localhost:5000` durch diese Domain.

Der einfachste Kundenweg ist der zentrale Einstieg:

```text
https://kundenstatus-app.onrender.com/partner
```

Der Kunde waehlt sein Autohaus aus und meldet sich mit dem Zugangscode an. Einzelne Autohaus-Links funktionieren weiter, sind aber nicht mehr zwingend noetig.

Beispiele:

```text
Autohaus Mueller:
https://kundenstatus-app.onrender.com/portal/9847b961ecdf4387
Passwort/Zugangscode: MUELLER2026

Auto Pfaff:
https://kundenstatus-app.onrender.com/portal/b900b7d3d54f4afa
Passwort/Zugangscode: PFAFF2026

HSE Autowelt:
https://kundenstatus-app.onrender.com/portal/ecd6b48321124e96
Passwort/Zugangscode: HSE2026
```

## Nach dem Deploy testen

1. Admin oeffnen:

```text
https://kundenstatus-app.onrender.com/admin
```

2. Mit `ADMIN_PASS` einloggen.
3. Autohaus-Portal oeffnen:

```text
https://kundenstatus-app.onrender.com/portal/9847b961ecdf4387
```

4. Mit dem Autohaus-Code einloggen.
5. Testfahrzeug anlegen und eine Datei hochladen.
