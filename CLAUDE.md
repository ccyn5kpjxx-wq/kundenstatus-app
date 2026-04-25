# Gärtner Autohaus-Terminportal

## Wer ich bin und was ich baue

Der Nutzer betreibt bzw. entwickelt für `Gärtner Karosserie & Lack` ein Browser-Portal für die Zusammenarbeit mit Autohäusern.

Ziel der App ist ein sauberer digitaler Ablauf für Partner wie `Käsmann`:

- Autohäuser melden Fahrzeuge, Termine, Angebotsanfragen, Verzögerungen und Reklamationen.
- Die Werkstatt sieht intern alle Fahrzeuge, Termine, Angebote, Dokumente und Alarme zentral.
- Belege wie Lackieraufträge, Gutachten, DAT-Kalkulationen und Bilder sollen automatisch ausgelesen werden.
- Unsichere Dokumentauslese soll markiert werden, damit keine falschen Daten blind übernommen werden.

Hauptnutzer:

- Intern: Gärtner Karosserie & Lack / Werkstatt-Admin
- Extern: Autohäuser und Partnerbetriebe, z. B. Käsmann
- Indirekt: Endkunden/Fahrzeughalter, deren Daten vom Autohaus eingetragen werden

## Tech-Stack

- Sprache: Python
- Framework: Flask
- Templates: Jinja2 unter `templates/`
- Frontend: serverseitiges HTML/CSS mit Bootstrap CDN
- Datenbank: SQLite unter `data/auftraege.db`
- Uploads: `data/uploads/`
- Dokumentanalyse lokal: RapidOCR, pytesseract, PyMuPDF/fitz, pypdf
- Dokumentanalyse vorbereitet: Google Document AI + OpenAI
- Konfiguration: `.env.local`

## Wichtige Commands

App starten:

```powershell
cd "C:\Users\info\OneDrive\Desktop\Arbeit\Kundenstatus-App"
python app.py
```

Admin öffnen:

```text
http://localhost:5000/admin
```

Partner-Einstieg:

```text
http://localhost:5000/partner
```

Käsmann-Dashboard:

```text
http://localhost:5000/partner/kaesmann/dashboard
```

Syntax prüfen:

```powershell
python -m py_compile app.py
```

Aktuell gibt es noch keinen eigenen Build-Schritt und keinen formalen Testordner.

## Aktueller Stand

Funktioniert:

- Admin-Login und internes Dashboard
- Autohaus-Zugänge mit eigenem Partnerportal
- Käsmann-Portal
- Fahrzeuge anlegen und bearbeiten
- Angebotsanfragen anlegen, bearbeiten und annehmen
- Upload von Unterlagen, Bildern und PDFs
- lokale OCR/Fallback-Auslese
- Vorbereitung für Google Document AI + OpenAI
- Kalender und Tagesübersichten
- Hol- und Bringservice / Kunde bringt und holt
- Archivieren, Reaktivieren und Löschen
- Sammelauswahl für mehrere Fahrzeuge
- Verzögerungen melden und übernehmen
- Reklamationen mit Alarm und Anhängen

Zuletzt verbessert:

- Dokumentanalyse läuft nur noch bei normalen Beleg-Unterlagen.
- Fertigbilder und Reklamationsbilder verändern keine Fahrzeugdaten mehr.
- Neue Belege dürfen erkannte Felder gezielt aktualisieren.
- Alte automatisch erzeugte Angebotstexte werden nicht mehr als Kundentext weitergeschleppt.
- Löschlogik entfernt Upload-Dateien sauberer.
- Admin-Passwort und Flask-Secret können über `.env.local` gesetzt werden.

Noch offen / nächste Meilensteine:

- Google Document AI und OpenAI API-Schlüssel in `.env.local` eintragen.
- Mit echten Belegen testen und Regeln für automatische Übernahme vs. Prüfung festlegen.
- Vorschau vor Übernahme für erkannte Felder ausbauen.
- `requirements.txt` oder `pyproject.toml` anlegen.
- GitHub-Repository einrichten und ersten sauberen Commit pushen.
- Kleine automatisierte Smoke-Tests für Kernrouten ergänzen.

## Konventionen

Projektstruktur:

- `app.py`: zentrale Flask-App, Routen, Datenbanklogik, OCR/KI-Analyse
- `templates/`: Jinja2-Seiten
- `static/`: Bilder und statische Dateien
- `data/auftraege.db`: lokale SQLite-Datenbank, nicht committen
- `data/uploads/`: hochgeladene Kunden-/Auftragsdateien, nicht committen
- `.env.local`: lokale Secrets und API-Konfiguration, nicht committen
- `KI_SETUP.md`: Anleitung für Google Document AI + OpenAI

Naming:

- Autohaus-Slugs klein und URL-freundlich, z. B. `kaesmann`
- Routen und Funktionen überwiegend deutsch benannt
- Status- und Termin-Felder möglichst bestehende Namen weiterverwenden

Branch-Namen:

- `main` für stabile Version
- `feature/<kurzer-name>` für neue Funktionen
- `fix/<kurzer-name>` für Bugfixes
- Beispiele: `feature/ki-dokumentanalyse`, `fix/angebot-termine`

Commits:

- Conventional Commits bevorzugen
- Beispiele:
  - `feat: add document ai extraction pipeline`
  - `fix: prevent finished images from changing order data`
  - `docs: add project context`

## Bekannte Stolperfallen

- Niemals `.env`, `.env.local`, API-Keys, Google-Service-Account-JSON oder echte Kundendaten committen.
- `data/auftraege.db` und `data/uploads/` enthalten lokale Betriebs-/Kundendaten und gehören nicht nach GitHub.
- Der Server darf nicht mehrfach parallel auf Port `5000` laufen; sonst sieht der Browser manchmal alte Zustände.
- Ohne Google/OpenAI-Schlüssel bleibt die lokale OCR aktiv.
- OCR von Fotos, Tabellen und Handschrift ist nicht zuverlässig genug für blinde Übernahme.
- Upload-Kategorien beachten: nur `standard`-Unterlagen sollen Fahrzeugdaten verändern.
- `.env.local` enthält aktuell auch `ADMIN_PASS` und `FLASK_SECRET_KEY`.
- Wenn neue Datenbankspalten nötig sind, `ensure_column(...)` in `init_db()` ergänzen.
- Git ist lokal initialisiert auf Branch `main`.
- GitHub CLI `gh` ist aktuell nicht installiert; GitHub-Repo daher manuell im Browser anlegen oder `gh` nachinstallieren.
- `.gitignore` muss Secrets, Datenbank und Uploads schützen.

## Session-Start für Codex/Claude

Beim Start einer neuen Session:

1. Diese Datei lesen.
2. `git status` prüfen.
3. Prüfen, ob der Server läuft und ob mehrere `app.py`-Prozesse aktiv sind.
4. Wichtige Routen testen: `/admin`, `/partner/kaesmann/dashboard`, `/partner/kaesmann/angebot/38`.
5. Vor Änderungen an Upload-/Analyse-Logik besonders auf Seiteneffekte achten.
