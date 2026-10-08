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

## Prüfung

Die Regressionen verwenden temporäre Datenbanken und simulierte Kommunikation.
Sie prüfen unter anderem Admin/CSRF, revisionsgebundene Korrekturen,
Wiederherstellung, unveränderliche Bestellinhalte, vollständige Datumsarchive,
Rechnungsdatum, Preisgrundlage und doppelte Rechnungsimporte. Der lokale
Browsercheck verwendet ausschließlich synthetische Anforderungen und einen
deaktivierten Versand.
