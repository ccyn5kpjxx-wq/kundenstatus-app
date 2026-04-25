# Gärtner Autohaus-Terminportal

Browserbasierte Flask-App für die Zusammenarbeit zwischen `Gärtner Karosserie & Lack` und Autohaus-Partnern.

Die App verwaltet Fahrzeuge, Termine, Angebotsanfragen, Verzögerungen, Reklamationen und hochgeladene Unterlagen. Dokumente wie Lackieraufträge, Gutachten, DAT-Kalkulationen und Bilder können lokal per OCR analysiert werden. Die stabilere Analyse über Google Document AI und OpenAI ist vorbereitet.

## Funktionen

- Admin-Cockpit für Werkstatt und Disposition
- Eigene Partnerportale pro Autohaus
- Fahrzeuge und Termine pflegen
- Angebotsanfragen einreichen und intern annehmen
- Upload von Bildern, PDFs und Dokumenten
- Dokumentanalyse mit lokalem Fallback
- Vorbereitung für Google Document AI + OpenAI
- Archivieren, Löschen und Sammelauswahl
- Verzögerungen und Reklamationen mit Alarm

## Tech-Stack

- Python
- Flask
- Jinja2 Templates
- SQLite
- Bootstrap CDN
- OCR: RapidOCR, pytesseract, PyMuPDF/fitz, pypdf
- API-Vorbereitung: Google Document AI + OpenAI

## Setup

```powershell
cd "C:\Users\info\OneDrive\Desktop\Arbeit\Kundenstatus-App"
python app.py
```

Danach öffnen:

```text
http://localhost:5000/admin
```

## Konfiguration

Lokale Konfiguration liegt in `.env.local`. Diese Datei darf nicht committed werden.

Vorlage:

```powershell
Copy-Item .env.example .env.local
```

Wichtige Variablen:

- `ADMIN_PASS`
- `FLASK_SECRET_KEY`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_DOC_AI_PROJECT_ID`
- `GOOGLE_DOC_AI_LOCATION`
- `GOOGLE_DOC_AI_PROCESSOR_ID`
- `OPENAI_API_KEY`
- `OPENAI_EXTRACTION_MODEL`

## Daten

Nicht committen:

- `data/auftraege.db`
- `data/uploads/`
- `.env.local`
- Google-Service-Account-JSON
- echte Kundendaten und Fahrzeugunterlagen

## Dokumentanalyse

Ohne API-Schlüssel nutzt die App lokale OCR. Mit konfiguriertem Google Document AI und OpenAI wird die stabilere Pipeline verwendet.

Details stehen in `KI_SETUP.md`.

## Entwicklung

Syntax prüfen:

```powershell
python -m py_compile app.py
```

Server starten:

```powershell
python app.py
```

## Lizenz

Proprietär. Nutzung und Weitergabe nur mit Zustimmung von Gärtner Karosserie & Lack.

