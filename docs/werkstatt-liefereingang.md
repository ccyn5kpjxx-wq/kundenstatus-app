# Lieferscheine zur Bestellung

Unter **Eingänge & Werkstatt → Bestellordner** die Bestellung öffnen und
**Lieferschein erfassen** wählen. Originalfoto oder PDF speichern, **Beleg
analysieren** drücken und die Angaben am Original kontrollieren. Anschließend
Seite, gedruckte Position, Liefermenge, Artikel und Gebinde bestätigen und
**Geprüfte Lieferung zuordnen** wählen.

Die Bestellübersicht zeigt offene, teilweise oder vollständige Lieferungen.
Mehrlieferungen und beschädigte beziehungsweise fehlende Nachweise erhalten
einen Prüfstatus. Der ursprüngliche Bestell- und Versandnachweis bleibt erhalten.

## Analyse und Mengen

Die Auslese läuft lokal. Bei Fotos werden OCR-Koordinaten verwendet, damit
Tabellenzeilen zusammenbleiben. Die konservative Tabellenerkennung unterstützt
den Top-Color-Lieferscheintyp; andere Layouts bleiben über Original und Auslesetext
manuell prüfbar. OCR-Ergebnisse sind Vorschläge und buchen keine Lieferung.

Ein geliefertes Gebinde mit 0,5 Liter Inhalt bedeutet **1 Stück**, sofern dies
die gespeicherte Bestelleinheit ist. Inhalt, Gesamtmenge und gelieferte Gebinde
bleiben getrennt. Logistik- und Energiepauschalen erscheinen als Nebenkosten.
Gedruckte Positionen dürfen bei **0** beginnen. Eine Rechnungsposition oder ein
Lieferschein ohne Preise erzeugt keinen neuen Rechnungspreis.

Bei einer abweichenden OCR-Artikelnummer oder einer fälschlich erkannten
Nebenkostenposition kann der Admin die Auslese nach Originalprüfung ausdrücklich
mit Begründung korrigieren. Diese Begründung bleibt im Liefernachweis gespeichert.

## Daten und Schutz

Originale verwenden die vorhandene Belegsammlung
`order-receipts:<kanonischer Bestellschlüssel>`. Rechnungen und Lieferscheine
können dort gemeinsam liegen. Die neue Tabelle `assistent_bestelllieferungen`
speichert bestätigte Zuordnungen mit Bestellfingerprint, Originalhash, Seite,
gedruckter Position, Menge und Prüfvermerk.

Der Originalhash zusammen mit Seite und Position verhindert eine zweite Buchung
derselben Belegposition, auch über Bestell-Aliase und andere Uploadsammlungen.
Korrekturen bestehender Bestellidentitäten führen zum Prüfstatus; Liefernachweise
werden nicht auf eine veränderte Bestellung übertragen. Schreibaktionen sind
auf Admins beschränkt, CSRF-geschützt und mit dem Restore-Lock abgesichert.

Backups enthalten die neue Tabelle und Originale. Vorhandene bestätigte
Lieferungen einschließlich ihrer Originalzuordnung dürfen durch einen älteren
Import nicht verloren gehen. Alte Backups bleiben auf einem Ziel ohne bereits
bestätigte Liefernachweise kompatibel.

## Prüfung dieser Erweiterung

- 17 neue Liefer-/Analyseprüfungen, 15 Backupprüfungen, 25 bestehende
  Preisoberflächenprüfungen und 54 Einkaufseingangsprüfungen bestanden.
- Vollständiger Flow-Test, Python-Syntax und Diffprüfung bestanden.
- Echte Bildauslese des bereitgestellten Lieferscheins lokal geprüft;
  Belegnummer, Datum, Position 0, Artikel, ein Gebinde, 0,5-Liter-Inhalt und
  getrennte Logistikposition erkannt. Beschreibungen bleiben rohe OCR.
- Browseransicht bei Desktop, 390 und 320 Pixeln geprüft, ohne horizontalen
  Dokumentüberlauf. Breite Tabellen scrollen innerhalb ihres Bereichs.
- Allgemeiner Smoke-Test: 111 bestanden, zwei auch auf dem unveränderten
  Ausgangsstand vorhandene Fehler zu Cockpitmodulen und Mitarbeiter-Urlaub.
- PostgreSQL-Kompatibilität über den vorhandenen Adapter getestet; keine echte
  PostgreSQL-Testinstanz verwendet.

Die Grundfunktion wurde am 08.10.2026 mit `77f6e2eb` veröffentlicht. Die
bereitgestellte Bilddatei liegt nur in der privaten lokalen Vorschau; eine
produktive Lieferbuchung erfolgte nicht.

## Upload unter Eingänge & Belege

Unter „Lieferung angekommen?“ kann der Admin den Lieferschein direkt als Foto oder PDF auswählen und einer gespeicherten Bestellung zuordnen. Die Bestellwahl hat keine Vorauswahl und bietet nur verifizierbare Bestellsnapshots; Alias-Schlüssel erscheinen einmal. Nach dem Speichern öffnet sich die Beleganalyse an der gewählten Bestellung. Upload und Analyse buchen weiterhin keine Lieferung. Fehler führen zurück zum Upload; eine neue Dateiauswahl ist dann erforderlich.
