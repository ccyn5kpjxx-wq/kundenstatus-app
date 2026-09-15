# Kundenportal und Werkstatt-Cockpit – September 2026

## Kundensicht

Die Auftragsseite unter `/status/<token>` zeigt Fahrzeug und Bearbeitungsstand zuerst. Die Bereiche Übersicht, Termine und Auftrag, Unterlagen, Bilder (wenn vorhanden) und Nachrichten lassen sich separat öffnen. Ein offenes Angebot bleibt vollständig in der Übersicht sichtbar; ein angenommenes Angebot ist unter Termine und Auftrag abrufbar.

- Keine erfundene Prozentanzeige: Der Reparaturverlauf verwendet die bestehenden Werkstattstufen.
- Ein Terminwunsch bleibt als unbestätigt erkennbar. Während der Reparatur steht die Rückgabe im Vordergrund; abgeschlossene Aufträge verlangen keinen neuen Termin.
- Nachrichtenentwürfe und ausgewählte Uploads bleiben beim Bereichswechsel erhalten. Aktualisieren verwirft Eingaben nur nach ausdrücklicher Auswahl.
- Direktlinks öffnen den passenden Bereich. Die Navigation funktioniert per Tastatur, ohne JavaScript bleiben alle Bereiche zugänglich.
- Die Übersicht zeigt die neueste freigegebene Nachricht. Interne Vermerke und Dokumente werden auch aus Metadaten, Verlauf und direkten Datei-URLs herausgefiltert.
- Das Werkstattmotiv im Kopfbereich ist ausdrücklich als Symbolbild gekennzeichnet; es behauptet keinen konkreten Fahrzeugfortschritt.

## Werkstatt

Der interne Auftrag gliedert sich in Übersicht, Arbeiten und Termine, Dokumente, Preise und Rechnung sowie Verlauf. Die Tagesaktionen stehen vor Kalender und Nebeninformationen. Noch nicht eingeplante, tatsächlich verfügbare Arbeiten können in das bestehende Aufgabenformular übernommen werden; dabei wird nichts automatisch gespeichert.

### Preise und Rechnungen

Lieferantenkosten und bestätigter Kundenpreis haben getrennte Netto-, Steuer- und Bruttobeträge mit Quelle und optionalem Originalbeleg. Erkannte Werte werden nicht automatisch zum Kundenpreis. Einzelne Dokumentwerte müssen gezielt übernommen werden; zwischenzeitliche Änderungen verhindern eine veraltete Übernahme.

Vor einer Rechnung sind Empfänger und Betrag bewusst zu prüfen. Vorhandene Rechnungsdaten, Rechnungsdateien und bereits laufende Erstellungen verhindern eine zweite Rechnung. Nach einer unklaren Providerantwort bleibt die Erstellung gesperrt, bis ein Admin in Lexware geprüft hat, dass dort kein Beleg existiert. Eine laufende Erstellung kann nicht über diese Freigabe zurückgesetzt werden. Nach einem Prozessabbruch mit verbleibendem Status `laeuft` ist eine technische Prüfung nötig; es gibt keinen automatischen Wiederholungsversuch.

### Dokumente

Kunde, Partner und Versicherung haben getrennte Freigaben. Alte interne Dokumente werden nicht pauschal öffentlich. Eigene Kundenuploads und bestehende ausdrückliche Freigaben bleiben im jeweiligen Empfängerkreis erhalten. Wenn ein bisher intern abgelegter Beleg extern sichtbar werden soll, muss die Werkstatt ihn bewusst für diesen Empfänger freigeben. Eine interne Notiz reicht nicht.

## Lokale Vorschau

`python scripts/cockpit_preview.py` startet ausschließlich auf `127.0.0.1:5093` mit einer neu erzeugten temporären Datenbank. `/demo/kunde` öffnet die Kundenseite; `/demo` öffnet die interne Ansicht. Die Demo verwendet erfundene Daten und sperrt ausgehende Provideraufrufe und E-Mail-Versand. Sie ist kein Produktionsstartskript.

## Prüfung

- `scripts/test_cockpit_safety.py`: 21 isolierte Flask-Tests zu Preisen, Datenübernahme, Freigaben, sichtbaren Metadaten, Kontakten, Rechnungsrennen, CSRF und Planung.
- `scripts/test_cockpit_customer.py`: Browserprüfungen mit synthetischen Daten, einschließlich Entwurfserhalt, Direkteinstieg, Tastatur, Angebotsbestätigung, Handybreite und Verhalten ohne JavaScript.
- `scripts/test_cockpit_planning.py`: 9 Planungsprüfungen.
- Smoke-, Ablauf- und Lead-Portal-Tests laufen mit synthetischen temporären Daten, unterdrückten lokalen Umgebungsdateien und gesperrten Netzwerkverbindungen.

Die Schemaänderungen sind zusätzliche Spalten über die bestehende Initialisierung. Es werden keine Kundendaten gelöscht und keine bestehenden Preiswerte stillschweigend in einen bestätigten Kundenpreis umgewandelt. Die Prüfung verwendet SQLite; die PostgreSQL-Initialisierung bleibt über den bestehenden Datenbankadapter bestehen.
