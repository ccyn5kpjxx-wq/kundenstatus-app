# Bestellungen bearbeiten und nach Versanddatum ablegen

In **Bestellungen → Offene Anforderungen** führt **Bearbeiten** direkt zum
Artikel. Die Werkstattleitung kann Artikelname, optionale Artikelnummer,
Variante, Menge und Bestelleinheit ändern. Ein Änderungsgrund dokumentiert die
Korrektur. Mitarbeiterangaben, Fotos und Nachrichten bleiben erhalten.

Eine Korrektur setzt die bisherige Preisprüfung zurück. Speichern löst keine
Bestellung und keine Nachricht aus. Ein späterer Bestelllauf benötigt weiterhin
eine vollständige Artikelzuordnung und die bestehende Kostenfreigabe.

Über die Auswahlkästchen können mehrere noch nicht übergebene Anforderungen
gemeinsam entfernt werden. Unter **Entfernte Anforderungen** lassen sie sich mit
Begründung wiederherstellen. Die Revision schützt vor gleichzeitig geänderten
Vorgängen. Versandgebundene und extern reservierte Bestellinhalte bleiben als
Nachweis erhalten und können nicht nachträglich überschrieben werden.

## Datierte Bestellblöcke

Nach bestätigter SMTP-Annahme bzw. nachgewiesenem externem Versand erscheinen die
Positionen in **Bestellung am TT.MM.JJJJ**, einschließlich des jeweiligen
Mitarbeiters. Neue Anforderungen bleiben im offenen Bereich. Jeder Datumsblock
hat eine eigene Seitennavigation mit 25 Positionen; auch große Bestellläufe
werden vollständig angezeigt. Ältere Datumsblöcke sind separat paginiert.

Der erste bestätigte Versandzeitpunkt bleibt auch bei einer späteren Reparatur
der Gesendet-Kopie unverändert. Für alte Bestellungen ohne gespeicherten
Versandzeitpunkt heißt der Block ausdrücklich **Bestelllauf am TT.MM.JJJJ**.
Unklare oder nur teilweise bestätigte Versandvorgänge gelten nicht als vollständig
versandt.

## Historische TopColor-Preise

**Artikel & Preisquellen** nimmt Originalrechnungen entgegen. Derselbe bereits
gespeicherte Rechnungsinhalt wird anhand des SHA-256-Fingerprints erkannt.
Artikelvorschläge behalten Rechnungsdatum, Originalreferenz, Seite, Position und
die Preisberechnung einschließlich Gebindeinhalt und Positionsrabatt.

Bei einer eindeutigen Übereinstimmung von Lieferant, Artikelnummer,
Bestelleinheit und Gebinde zeigt die Übersicht den jüngsten belegten
Rechnungspreis als historischen Richtwert. Datum und Belegreferenz stehen direkt
daneben. Währung und Netto-/Bruttobasis stammen aus dem Originalbeleg; sie werden
nicht pauschal angenommen. Ein Zahlungs-Skonto ist kein Positionsrabatt.

Fehlende Zuordnungen, widersprüchliche Preise oder ein unklarer jüngster Beleg
bleiben offen. Ein älterer günstiger Preis wird nicht stillschweigend als Ersatz
eingesetzt. Die historischen Werte ändern weder bestehende Versandnachweise noch
die aktuelle Preisfreigabe. Nicht eindeutig lesbare Rechnungen benötigen eine
manuelle Prüfung.

Die Übersicht lädt den aktuell nach Quellen und Lieferantenfreigaben gefilterten
Rechnungskatalog einmal pro GET-Anzeige. Alle historischen Artikelidentitäten
und die Kandidaten im ausgewählten Preisvergleich verwenden diesen Stand. Er
gilt nur während dieser Anzeigeberechnung; die nächste Anfrage prüft Freigaben,
Quarantäne und Quellen erneut. Schreibaktionen verwenden weiterhin ihre eigenen
aktuellen Prüfungen.

Scheitert ausschließlich die optionale Historienauswertung technisch, erscheint
**Historischer Rechnungspreis derzeit nicht verfügbar**. Der fehlende Wert wird
weder durch 0 Euro noch durch einen älteren Rechnungswert ersetzt.

Scheitert die optionale Katalogabfrage für die Kandidaten im ausgewählten
Preisvergleich technisch, erscheint **Rechnungskandidaten derzeit nicht
verfügbar**. Das gilt auch, wenn erst die Auflösung eines späteren Kandidaten
ausfällt: Bereits gesammelte Kandidaten werden für diese Anzeige verworfen.
Gespeicherte Vergleichsnachweise, Bestell- und Lieferdaten bleiben erhalten.
Die nächste GET-Anfrage prüft den Katalog erneut. Fachliche Ablehnungen wegen
fehlender Freigaben oder unpassender Identitäten behalten ihre bisherige
Behandlung; andere Datenbank-, Liefer- oder Schreibfehler werden nicht als
erfolgreiche Kataloganzeige ausgegeben.

## Prüfung

Die Regressionen verwenden temporäre Datenbanken und simulierte Kommunikation.
Sie prüfen unter anderem Admin/CSRF, revisionsgebundene Korrekturen,
Wiederherstellung, unveränderliche Bestellinhalte, vollständige Datumsarchive,
Rechnungsdatum, Preisgrundlage und doppelte Rechnungsimporte. Der lokale
Browsercheck verwendet ausschließlich synthetische Anforderungen und einen
deaktivierten Versand.

Die Preisabfrage-Regressionen prüfen 551 verschiedene Artikelidentitäten mit
einem Katalogabruf und einen echten ausgewählten Vergleich mit zwölf Kandidaten,
der denselben Abruf mit der Historie teilt. Weitere Fälle prüfen widerrufene
Lieferantenfreigaben und Quellenquarantäne bei der nächsten GET-Anfrage,
technische Nichtverfügbarkeit ohne Ersatzpreis, unveränderte Mutationsfehler
sowie die Trennung gleichzeitiger Anzeigekontexte und deren Auflösung bei Fehlern.

Der Historienfix wurde am 09.10.2026 zuvor geprüft: `test_topcolor_preisstand.py` (23 bestanden, ein optionaler
externer Archivtest übersprungen), `test_bestelluebersicht.py` (25),
`test_bestellungen.py` (28), `test_artikel_import.py` (43) und
`test_bestellvergleich.py` (23): zusammen 142 bestandene Tests. Syntaxprüfung und
`git diff --check` sind ebenfalls bestanden. Alle neuen Testdaten sind synthetisch.

Die gezielte Kandidatenabsicherung wurde anschließend mit
`test_bestelluebersicht.py` (29), `test_bestellvergleich.py` (23) und
`test_bestellvergleich_ui.py` (36) geprüft: 88 bestandene Testausführungen,
darunter 59 verschiedene Testfälle, weil die UI-Suite die 29 Übersichtstests
erneut ausführt. Neue echte GET-Fälle prüfen technische Katalogfehler, einen
Ausfall nach einem gültigen Kandidaten, die erneute erfolgreiche Anzeige,
unveränderte fachliche Ablehnungen sowie weiterhin propagierende Bestell- und
Lieferfehler. Die Prozesse liefen mit temporären synthetischen Datenbanken,
gesperrtem Netzwerk und einer harten Frist von jeweils 120 Sekunden.
