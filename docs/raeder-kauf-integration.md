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

## Private Serverkataloge

`configure_integrations()` richtet die nur lesenden Reifen- und Felgenprovider
getrennt ein. Eine reine Konfiguration liest nur die ausdrücklich gewählte
begrenzte private Policydatei; sie lädt keinen Produktcache, erzeugt keine
Verzeichnisse, startet keinen Worker und ruft keinen Anbieter auf.

Reifen verwenden die vorhandenen `TYRESYSTEM_PRIVATE_ROOT` und
`TYRESYSTEM_CATALOG_*`-Einstellungen. Für Felgen gelten ausschließlich:

```text
FELGEN_PRIVATE_ROOT=/var/data
FELGEN_CATALOG_DIR=/var/data/tyresystem-rim-catalog
FELGEN_CATALOG_POLICY_FILE=/var/data/tyresystem/rim-policy.json
FELGEN_CATALOG_PUBLICATION_APPROVED=0
FELGEN_CATALOG_PUBLICATION_REFERENCE=
```

Ohne ausdrückliche Veröffentlichungsfreigabe und nichtleere, begrenzte
Freigabereferenz wird keine Policy geöffnet. Diese Einstellungen dokumentieren
keine tatsächlich erteilten Datenrechte. Freigaben für Reifen werden nicht auf
Felgen übertragen. Ein eigener verwalteter Provider wird bei Widerruf,
entfernten Einstellungen oder fehlerhafter Neukonfiguration entfernt.
Fehlt jede Felgeneinstellung, bleibt ein unabhängig injizierter Provider
unberührt; explizite neue Felgeneinstellungen ersetzen ihn auch im Fehlerfall.

Die Felgenpolicy ist ein privates JSON-Objekt mit genau diesem Schema und dem
am 03.11.2026 geprüften Preis-/Frachtstand für normale Pkw-Felgen:

```json
{
  "currency": "EUR",
  "price_basis": "net_unit",
  "single_item_shipping_net": "6.00",
  "free_shipping_from": 2,
  "verified_on": "2026-11-03",
  "sources": {
    "price": "https://www.tyresystem.de/agb",
    "shipping": "https://www.tyresystem.de/lieferbedingungen"
  }
}
```

Eine Reifenpolicy mit `single_tyre_shipping_net` ist keine Felgenpolicy. Der
vorhandene gemeinsame Kalkulator berechnet weiter Netto-EK plus zehn Prozent
und anschließend Umsatzsteuer. Der Provider begründet keine Fahrzeugpassung,
Montageleistung oder Lieferreservierung. Bild- und Bewertungsresolver sowie
private Ansichtscallbacks werden durch diesen Produktionsloader nicht gesetzt.

Cache und Policy müssen außerhalb öffentlicher Pfade auf der privaten
persistenten Webdisk liegen. Für beide Kataloge muss der ausdrücklich gestartete
Refreshprozess dieselbe Webdisk erreichen; ein separater Dienst teilt sie nicht
allein durch identische Pfadnamen. Beispiel für den gemeinsamen Refresh:

```text
python scripts/refresh_tyresystem_catalog.py --run --cache-dir /var/data/tyresystem-catalog --private-root /var/data --felgen-cache-dir /var/data/tyresystem-rim-catalog --felgen-private-root /var/data
```

Nur der Refreshprozess erhält `TYRESYSTEM_EXPORT_URL` aus dem privaten
Secretstore. Beide Parser verwenden denselben wirklichen Abrufzeitpunkt;
Schreiben jedes einzelnen Caches ist atomar. Die zwei Dateien bilden keine
gemeinsame Dateisystemtransaktion: ein Fehler zwischen den Writes muss als
fehlgeschlagener Refresh erkannt und geprüft werden. Das Lesen verlängert keine
Frist; nach spätestens 15 Minuten liefern alte Daten keine Angebote mehr.
Die Katalogkonfiguration installiert keinen automatischen Refreshzeitplan.

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
