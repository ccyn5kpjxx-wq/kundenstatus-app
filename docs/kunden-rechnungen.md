# Rechnungsübersicht für Kunden

Partner erhalten im Dashboard den Button **Rechnungen**. Die separate Liste zeigt
freigegebene Original-PDFs ihrer eigenen Aufträge, einschließlich archivierter
Aufträge. Im persönlichen Statusportal gibt es den gleichnamigen Tab, begrenzt
auf den einen Auftrag dieses aktiven Links. PDF öffnen und Herunterladen
funktionieren als normale serverseitige Links, auch ohne JavaScript.

## Integration

`app.py` registriert den Bereich direkt nach der Service-App:

```python
from kunden_rechnungen import register_customer_invoices
register_customer_invoices(sys.modules[__name__])
```

Die Tests und die lokale Vorschau verwenden dieselbe App-Registrierung. Der
Integrationstest bricht ab, falls sie fehlt. Die Templates zeigen den Button nur
bei registriertem Modul.

## Auswahl und Freigabe

Die Rechnungsübersicht verwendet ausschließlich Dateien der Kategorie `rechnung`
aus interner Werkstatt-/Lexware-Quelle, mit PDF-Dateiname und PDF-kompatiblem MIME-
Typ. Zusätzlich müssen `sichtbarkeit_geprueft=1` sowie die passende Freigabe
`partner_sichtbar=1` beziehungsweise `kunde_sichtbar=1` gesetzt sein. Die bestehende
`document_visible`-Prüfung bleibt zusätzlich aktiv.

Das Büro lädt die Originalrechnung im Auftrag als Werkstattrechnung hoch und
erteilt nach Prüfung im bestehenden Dokumentbereich **Freigaben** den passenden
Zugriff. Standardmäßig interne Rechnungen bleiben intern. Es erfolgt keine
automatische Veröffentlichung und keine Kundenbenachrichtigung. Lieferanten-
und Bonusrechnungen sowie Kunden-/Partneruploads werden nicht übernommen.

Für Storno oder Ersatzrechnung gibt es bisher keine strukturierte Dateiverknüpfung.
Deshalb erfindet die Ansicht keinen Status „aktuell“, „bezahlt“ oder „storniert“
und liest keine Beträge aus OCR. Sie zeigt den Originalnamen und den zugehörigen
Auftrag. Das Büro muss bei ersetzten Rechnungen die bisherige PDF-Freigabe prüfen
und gegebenenfalls entziehen; die alte Datei wird dabei nicht gelöscht.

## Sicherheitsgrenzen

- Partner-ID wird aus dem bestehenden authentifizierten Partnerzugang geprüft.
  Jede Datei wird mit `auftraege.autohaus_id` verknüpft, auch beim direkten Download.
- Statuslinks prüfen den aktiven Token und die konkrete `auftrag_id`. Gleiche
  Kundennamen oder E-Mail-Adressen erweitern den Zugriff niemals.
- Die neuen Kunden-PDF-Routen liegen unter `/status/`. Die bestehenden Service-
  und Fahrerberechtigungen bleiben damit vorgeschaltet; ein alter Statuslink
  umgeht keine aktuelle Fahrzeugzuordnung und gewährt Fahrern keine Finanzsicht.
- Die Datenabfrage liest nur Metadaten, ohne OCR oder Änderung von Aufträgen.
  Originaldateien werden über die bestehende sichere Auslieferung gesendet.
- Listen und Original-PDFs erhalten private No-Store-/No-Index-Header. Sichtbarkeit
  und Besitz werden bei jedem Öffnen und Download erneut geprüft.
- PDF öffnen ist eine begrenzte Inline-Ausnahme für diese freigegebenen Belege:
  zusätzlich PDF-Headerprüfung und CSP-Sandbox. Die allgemeine Download-Regel
  für übrige Uploads bleibt bestehen. Der Download liefert dieselben Originalbytes.

## Prüfung

```powershell
python scripts/test_kunden_rechnungen.py
```

Die Prüfung startet die echte Flask-App mit temporärer synthetischer Datenbank,
Uploads und gesperrten ausgehenden Netzwerkverbindungen. Sie deckt Anmeldung,
Partnertrennung, Tokenbindung, Widerruf, Archiv, fehlende Freigabe, Dokumentauswahl,
Originalbytes/Download, Servicezugang, Navigation, leere Liste und Pagination ab.
