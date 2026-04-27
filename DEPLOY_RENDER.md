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
DATABASE_URL=<postgres-datenbank-url>
OPENAI_API_KEY=<optional>
OPENAI_EXTRACTION_MODEL=gpt-4o
UPLOAD_DIR=/tmp/kundenstatus-uploads
```

Wichtig: `DATABASE_URL` sollte auf eine Postgres-Datenbank zeigen. Ohne `DATABASE_URL` nutzt die App SQLite, was auf Hosting-Plattformen nicht dauerhaft genug ist.

## Uploads

`UPLOAD_DIR=/tmp/kundenstatus-uploads` funktioniert technisch, ist aber auf vielen Hostern nicht dauerhaft. Fuer echte Nutzung sollte ein persistenter Speicher verwendet werden, zum Beispiel ein Render Disk oder spaeter ein Objektspeicher.

## Links verschicken

Nach dem Deploy bekommst du eine oeffentliche Render-Adresse, zum Beispiel:

```text
https://kundenstatus-app.onrender.com
```

Dann ersetzt du `localhost:5000` durch diese Domain.

Beispiele:

```text
Autohaus Mueller:
https://kundenstatus-app.onrender.com/portal/9847b961ecdf4387
Code: MUELLER2026

Auto Pfaff:
https://kundenstatus-app.onrender.com/portal/b900b7d3d54f4afa
Code: PFAFF2026

HSE Autowelt:
https://kundenstatus-app.onrender.com/portal/ecd6b48321124e96
Code: HSE2026
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
