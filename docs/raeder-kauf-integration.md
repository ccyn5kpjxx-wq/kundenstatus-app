# Bestellabschluss für Reifen und einzelne Felgen

Stand 03.11.2026. Der Code erweitert den vorhandenen, dauerhaft gespeicherten
Reifenabschluss um eindeutig bezeichnete einzelne Felgen. Er schaltet keine
Livebestellungen frei. Komplettrad-Sets sind mangels bestätigter Fahrzeug- und
Komponentenzuordnung sowie Setpreisen weiterhin nicht eingerichtet.

## Ablauf und Identität

Die Felgenauswahl verwendet einen eigenen kurzlebigen signierten Token mit
Präfix `felgen.`. Der gemeinsame GET-Einstieg ist
`/raeder/bestellen/<token>/kauf`; der bisherige Reifen-Einstieg bleibt bestehen.
Nach Anlage eines eigenen Entwurfs werden SKU, Menge, Kategorie und aktuelle
Preise anhand der gespeicherten Auswahl erneut geprüft. Der alte Suchlink ist
dann keine dauerhafte Voraussetzung. Eine Preisänderung benötigt eine erneute
Bestätigung. Fremde Entwürfe sind nicht zugänglich.

Felgen erhalten keine erfundene Reifengröße, Saison oder Montageposition.
Fahrzeugpassung ist ausdrücklich ungeprüft. Eine Bestellung einzelner Felgen
enthält keine Fahrzeugfreigabe; vor Verwendung oder Montage ist die Eignung
anhand der konkreten Fahrzeug- und Felgendaten zu prüfen. Eine fahrzeugbezogene
Komplettrad-Zusammenstellung lässt sich daraus nicht ableiten.

`readiness_for(produktart)` liefert die Bereitschaft ausschließlich für die
angefragte Kategorie. Der Felgen-CTA akzeptiert nur ein registriertes
Checkoutziel, `ready is True` und exakt `produktart == "felgen"`. Der alte
Reifenstatus ist dafür kein Ersatz. SMS-Versand und Codeprüfung prüfen die
Kategorie des eigenen gespeicherten Entwurfs ebenfalls.

## Gemeinsame Bedingungen und Lieferant

Die vorhandenen Einstellungen `REIFEN_KAUF_RECHTSTEXTE`,
`REIFEN_TERMS_FILE` und `REIFEN_TERMS_APPROVED_SHA256` bleiben maßgeblich.
Für Felgen muss die unverändert freigegebene Fassung zusätzlich eine gültige
Liste `produktarten` mit `"felgen"` enthalten. Die neue Text-Prüffassung ist
kein freigegebenes Laufzeitdokument. Insbesondere die vorgeschlagene
Annahmefrist und deren Rückzahlungsfolgen sind noch nicht umgesetzt.

Der Lieferantenadapter muss Felgen bei `inspect`, `reserve` und `commit`
ausdrücklich als Produktart bestätigen. Alle bisherigen Betrags-, Mengen-,
Preis-, Modus-, Liefertermin- und Wiederholungssicherungen bleiben erforderlich.
Die öffentliche TYREsystem-REST-Dokumentation eines Bestell-POSTs beweist keine
Reservierungsdauer, Abbruchmöglichkeit oder Doppelbestellsperre. Ein frischer
Katalogpreis ersetzt keine Lieferreservierung.

## Zahlung, Vertrag und Betrieb

Stripe erhält die zutreffende Bezeichnung Reifenbestellung oder Felgenbestellung.
Der derzeitige Onlineablauf zieht nach abgeschlossenem Checkout ein; er ist
keine reine Kartenautorisierung mit späterer Werkstattfreigabe. Ein solcher
Freigabeablauf erfordert eine gesonderte Entscheidung und Implementierung.
Zahlung vor Ort bleibt eine verbindliche Bestellung mit offenem Zahlbetrag.

Die Vertragskopie speichert die passende Artikelidentität, Menge und brutto
Preisaufteilung sowie die tatsächlich zugrunde gelegte Bedingungsfassung.
Felgen benötigen passende Bedingungen und stimmige Einzel-, Waren- und
Gesamtbeträge. Lieferanteninterna und Einkaufspreise gehören nicht in diese
Kopie. Die öffentliche zweistufige Widerrufsfunktion nennt auch Felgen und
Kompletträder; sie storniert oder erstattet nicht selbstständig. Ein vorgemerkter
E-Mail-Versand beweist noch keine Zustellung.

Vor dem Produktionsstart fehlen insbesondere das bestätigte Lieferprotokoll,
öffentliche Daten-/Bildrechte und erforderliche Produktinformationen, aktuelle
Katalogversorgung auf dem Server, eigene Stripe-Liveschlüssel samt Webhook,
produktiver SMS-Versand mit Kostenlimit, Vertragsmail und die freigegebene
vollständig umgesetzte Bedingungsfassung. Diese Voraussetzungen werden durch
Offline-Fakes in Tests nicht erfüllt.
