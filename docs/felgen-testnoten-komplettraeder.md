# Lokaler Räderkatalog und Testbewertungen

Reifen, Felgen und Kompletträder haben getrennte Einstiege unter `/reifen`, `/felgen` und `/komplettraeder`. Die bestehende private Datenfreigabe, Einkaufsdaten und Kaufgates bleiben erhalten. Diese Erweiterung aktiviert keinen verbindlichen Kauf und keinen Lieferantenauftrag.

## Testnoten

`ReifenBewertungsStore` liest eine begrenzte private Nachweisliste. Nur exakt belegte Kombinationen aus Artikelnummer, Hersteller, vollständigem Profil, Saison, Dimension und Last-/Geschwindigkeitsindex erhalten eine Note. Öffentliche Anzeige benötigt einen eigenen Freigabeverweis; die lokale Loopbackansicht erlaubt ausdrücklich private Sichtprüfung. Ohne Nachweis erscheint „Testnote noch nicht hinterlegt“.

Ausgegeben werden ADAC-Originalnote, Urteil, Testjahr, getestete Größe und der Originaltestlink. Kleine Noten sind besser. Es werden keine Kundensterne, Logos, umgerechneten Sterne oder aus Preisen abgeleiteten Qualitätsklassen erzeugt. Historische Testjahre stehen sichtbar neben der Note. Andere Modellvarianten und Größen übernehmen die Note nicht.

Die Suche unterstützt `testnote=mit_note`, `testnote=gut` (Originalnote bis 2,5) und `sortierung=testnote_auf`; die Vorauswahl berücksichtigt dies vor Gesamtzahl und Seitenteilung. Fehlende Noten stehen bei Notensortierung zuletzt. Die signierte Artikelauswahl und Preisberechnung bleiben davon unabhängig.

## Felgen

`felgen_katalog.py` verarbeitet den bestehenden Originalexport separat. Nur echte Felgen mit unterstütztem Alu-/Stahltyp, bestätigtem Preis/Bestand und ohne Demo-/Da-Kennzeichnung werden berücksichtigt. Generische Felgen- und Komplettradkategorien sind keine konfigurierten Verkaufssets. Ein eigener privater atomarer Cache behält Originalabrufzeit, Quellhash und 15-Minuten-Gültigkeit. Alte, beschädigte oder nicht freigegebene Daten liefern keine Angebote.

Suche nach Hersteller, Modellbegriffen, Artikelnummer, Herstellerartikelnummer oder EAN; Material-, Mengen-, Sortier- und Seitenfilter arbeiten vor den zwölf sichtbaren Treffern. Verkaufspreis ist Netto-EK mal 1,10, danach 19 Prozent Umsatzsteuer mit der bestehenden Cent-Rundung. Die explizit geprüfte Frachtpolicy für normale Pkw-Felgen gilt gesondert; sie darf nicht pauschal für Lkw/Spezialware übernommen werden.

`felgen_shop.py` ergänzt reine GET-Endpunkte für Katalog, Auswahlmerkmale und signierte Auswahl. Der eigene Salt bindet Artikelnummer und Menge; bei jeder Auswahl werden aktueller Preis und Bestand erneut geprüft. Keine Kundendaten, Datenbankmutation, Sessioninhaber, Reservierung, SMS, Zahlung oder Lieferantenbestellung entstehen. HTML/JSON sind privat und nicht zwischenspeicherbar. Formularfreie Ansichten erzeugen keinen CSRF-Sessioncookie; bestehende POST-Formulare behalten ihren Schutz.

Felgenmaße in Produktnamen werden nicht als strukturierte Fahrzeugfreigabe ausgegeben. Kein Artikel wird ohne bestätigte Zuordnung als passend bezeichnet. Bilder benötigen reine Felgen als Motiv und eine eigenständige geprüfte Quelle; Reifenbilder und Komplettradbilder werden nicht wiederverwendet. Aktuell ist noch kein Felgenbildbestand angeschlossen.

## Offene Voraussetzung für Kompletträder

Die Kategorie erklärt den fehlenden Konfigurator und bietet keine erfundenen Sets oder addierten Teilepreise als verbindliche Komplettangebote an. Benötigt werden dokumentierte Fahrzeugzuordnung, Felgenmaße/Gutachten/Auflagen, zulässige Reifenkombinationen, RDKS/Zubehör, bestätigte Setpreise einschließlich Montage/Wuchten/Fracht sowie Bestell- und Datenrechte. Der aktuelle CSV-Export liefert diese Zuordnungen nicht.

Die aktuelle öffentliche [Funktionsübersicht](https://www.tyresystem.de/funktionsuebersicht) beschreibt Testbewertungen und Fahrzeugauswahl im Lieferantenportal. Die [Komplettradbeschreibung](https://www.tyresystem.de/produkte/komplettraeder) bestätigt die zusätzliche Konfiguration. Daraus folgt noch kein dokumentierter Zugang dieser Funktionen über die vorhandene Shop-Schnittstelle. Die [REST-Beschreibung](https://www.tyresystem.de/static/Anleitung/API-REST-Architektur.pdf) und der vorhandene Kundenbereich sind auf zusätzliche Konfigurationsdaten zu prüfen. Frachtgrundlage: [Lieferbedingungen](https://www.tyresystem.de/lieferbedingungen).
