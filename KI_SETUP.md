# KI-Setup fuer die Dokumentanalyse

Die App ist auf `OpenAI Vision + optional Google Document AI` vorbereitet.

Empfohlene Reihenfolge:

1. OpenAI einrichten, damit Bilder und PDF-Vorschauen direkt analysiert werden.
2. Google Document AI spaeter optional ergaenzen, wenn noch mehr OCR-Stabilitaet noetig ist.

## 1. Google Document AI

1. In Google Cloud ein Projekt anlegen.
2. `Document AI API` aktivieren.
3. Einen `Document OCR Processor` anlegen.
4. Einen `Service Account` mit Zugriff auf Document AI anlegen.
5. Den JSON-Schluessel herunterladen.
6. Den JSON-Schluessel lokal auf dem Rechner ablegen.

Eintragen in `.env.local`:

- `GOOGLE_APPLICATION_CREDENTIALS`
- `GOOGLE_DOC_AI_PROJECT_ID`
- `GOOGLE_DOC_AI_LOCATION`
- `GOOGLE_DOC_AI_PROCESSOR_ID`

## 2. OpenAI

1. Auf der OpenAI Platform einen API-Key erstellen.
2. Den Key in `.env.local` eintragen.

Eintragen in `.env.local`:

- `OPENAI_API_KEY`
- optional `OPENAI_EXTRACTION_MODEL`

Die App sendet bei PDF- und Bild-Uploads bis zu 4 PDF-Seiten bzw. das hochgeladene Bild an OpenAI Vision. OpenAI gleicht das Original mit lokaler OCR ab und gibt strukturierte Felder zurueck.

## 3. Starten

1. `.env.local` fuellen
2. App neu starten:

```powershell
cd "C:\Users\info\OneDrive\Desktop\Arbeit\Kundenstatus-App"
python app.py
```

## 4. Pruefen

Im Admin-Dashboard erscheint eine Karte `KI-Dokumentanalyse`.

Dort siehst du:

- ob `Google OCR` bereit ist
- ob `OpenAI` bereit ist
- ob die volle Pipeline aktiv ist oder ob noch der Fallback laeuft

## 5. Verhalten der App

- Ohne Zugangsdaten bleibt die lokale OCR aktiv.
- Mit OpenAI-Key nutzt die App zusaetzlich `OpenAI Vision` fuer Bilder und PDF-Vorschauen.
- Mit Google-Zugangsdaten nutzt die App zusaetzlich `Google Document AI` als OCR-Stufe.
- Unsichere Faelle werden mit Review-Hinweis markiert.
