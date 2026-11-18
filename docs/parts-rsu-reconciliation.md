# Offline-Abgleich für Ersatzteile und Zubehör

`scripts/reconcile_rsu_parts.py` verarbeitet ausschließlich ausdrücklich angegebene,
SHA256-geprüfte lokale Dateien. Es enthält keinen Netzwerkclient, liest keine
Zugangsdaten und verändert weder Katalog, Preisbuch noch Bestellungen.

Der aktuelle unveränderte Katalog mit SHA256
`231b53c9759daae0782d78669a20802f5f85229a1d692ac63e9ad2bf0dc19179`
enthält 789 Artikel: 339 Fahrzeugteile und 450 weitere Zubehörartikel. Alle haben
Hersteller und Herstellerartikelnummer; 762 haben eine gültige EAN, 27 nicht.
Die 339 Fahrzeugteile besitzen zusätzlich erhaltene ALZURA-Quellbelege und 355
Zuordnungen zu Fahrzeugabfragen. Eine Abfragezuordnung ist keine bestätigte
Fahrzeugpassung und keine RSU-Beschaffungszusage.

## Eingaben

Der Aufruf verlangt Katalogdatei und Hash, ein Quellenmanifest und dessen Hash,
einen expliziten UTC-Auswertungszeitpunkt und einen privaten Ausgabeordner:

```powershell
python scripts/reconcile_rsu_parts.py `
  --catalogue C:/private/bundle.json --catalogue-sha <SHA256> `
  --sources C:/private/sources.json --sources-sha <SHA256> `
  --as-of 2026-11-18T12:00:00+00:00 --output C:/private/reconciled
```

Quellenmanifest:

```json
{
  "schema_version": 1,
  "sources": [
    {
      "file": "rsu-read-observations.json",
      "sha256": "<SHA256>",
      "format": "rsu_read_json"
    }
  ]
}
```

`rsu_csv` verwendet die exakten RSU-Spalten `Artikelnummer`, `Hersteller`,
`Herstellerartikelnummer`, `ean`; optionale Preis-/Bestandsfelder heißen
`Einkaufspreis` und `Lagerbestand`. Das Quellenmanifest muss dafür den belegten
Zeitpunkt als `observed_at` enthalten. Ein Empfangszeitpunkt ist keine
Warenreservierung. UTF-8, Semikolon und eindeutige Spaltennamen sind erforderlich.
Zeilennummern beginnen bei der ersten Datenzeile mit 1, ohne Kopfzeile.

Ein normalisiertes, zuvor geprüftes RSU-Leseergebnis hat diese Form:

```json
{
  "schema_version": 1,
  "supplier": "RSU",
  "currency": "EUR",
  "price_basis": "net",
  "source_receipts": [
    {"file": "C:/private/original-response.json", "sha256": "<SHA256>"}
  ],
  "records": [
    {
      "article_id": "123456",
      "manufacturer": "SYNTHETIC",
      "mpn": "SKU-1",
      "ean": "4006381333931",
      "read_kind": "inquiry",
      "observed_at": "2026-11-18T11:59:00+00:00",
      "quantity": 1,
      "unit_net": "10.00",
      "stock": 2,
      "category": "accessory",
      "unit_label": "Stück",
      "packing_quantity": 1,
      "condition": "new",
      "freight": null,
      "supplier_offer": null
    }
  ]
}
```

`supplier_ui` kann die Identität belegen, wird aber nicht als maschineller
Preisbezug für einen Preisbuch-Seed verwendet. Unbekannte Werte bleiben `null`.
ALZURA-Preise, ALZURA-Händlernummern, native IDs, OE-Vergleichsnummern und ähnliche
Artikelbezeichnungen werden niemals als RSU-Preise oder RSU-Artikelnummern benutzt.

## Abgleich und Grenzen

- Hersteller und MPN werden nur mit Unicode-NFKC, Groß-/Kleinschreibung und
  Leerzeichen normalisiert. Satzzeichen und Suffixe bleiben erhalten.
- Vorhandene gültige EANs müssen auf beiden Seiten übereinstimmen. Fehlende
  RSU-EANs passen nicht zum bestehenden Bestellvertrag. Ohne Katalog-EAN werden
  zusätzlich explizit gleiche Verkaufseinheit und Packungsgröße verlangt.
- Mehrdeutige RSU-Artikelnummern, widersprüchliche Quellidentitäten oder doppelte
  Katalogidentitäten erzeugen keinen freigegebenen Treffer. Auch Konflikte mit
  anderen Zeilen derselben Lieferantenquelle werden geprüft.
- Die gemeinsamen Prüffunktionen in `shop_rsu_parts_evidence.py` erzeugen und
  validieren den vollständigen Identitätsbeleg. Der Katalogbeleg verwendet den
  unveränderten Bundle-Hash, die Produktposition und die öffentliche Produkt-ID.
- Preis-Seeds benötigen einen echten RSU-Netto-Bezug für Menge 1, einen gültigen
  Zeitpunkt innerhalb von fünf Tagen und einen numerischen gemeldeten Bestand.
  Preisberechnung: Netto × 1,15 × 1,19, am Ende kaufmännisch auf Cent gerundet.
  Ältere Bestände werden nicht als aktuell umgedeutet; Menge 1 belegt nicht Menge 2.
- Eine Teile-Versandklasse entsteht nur für eine einzige eindeutig beobachtete
  Lieferoption bei Menge 1, wenn ursprüngliche DOM-Quelle und frische REST-Abfrage
  zu Artikelnummer, EAN, Hersteller/MPN, Nettopreis und Bestand übereinstimmen.
  Ein isoliert sichtbarer Versandbetrag reicht nicht. Mehrere Lieferoptionen oder
  Menge 2 benötigen weitere Belege. Die Frist endet 15 Minuten nach dem älteren
  der beiden Beobachtungszeitpunkte. Fahrzeugpassung bleibt eine eigene Regel.

## Ausgaben

- `parts-rsu-reconciliation.private.json`: vollständiger Abgleich mit einem
  Eintrag und konkreten Blockiergründen je Artikel.
- `parts-rsu-bindings.private.json`: separat prüfbare Identitätskandidaten.
- `parts-rsu-price-seeds-15pct.private.json`: ausschließlich belegte RSU-Preise.
- `parts-rsu-candidates.private.json`: gemeinsames privates Kandidatenformat.
- `parts-rsu-direct-overlay.private.json`: nur vollständig aktivierungsfähige
  Artikel; bei fehlender Versand-/Bestandsgrundlage bleiben die Maps leer.
- `parts-rsu-missing-identity-requests.private.json`: unmittelbar weiterverwendbare
  Liste der noch benötigten Hersteller-/MPN-/EAN-Abgleiche mit Einheit und
  Packungsangabe, ohne geratene Lieferanten-ID und ohne ausgeführte Anfrage.

Beide Overlay-Dateien verwenden ausschließlich:

```text
schema_version, base_bundle_sha256, rsu_bindings,
rsu_price_seeds {records, evidence_id}, shipping_classes,
source_refs [{file, sha256}]
```

Ein Kandidaten-Overlay darf nicht als Direktkauf-Freigabe installiert werden.
Ausgabedateien werden nur neu erstellt oder bei identischem Inhalt wiederverwendet;
abweichende bestehende Dateien werden nicht überschrieben.

`shipping_classes` enthält bei solchen Einzelangeboten einen Beleg des Typs
`rsu_ui_unique_q1_freight_v1`, keinen pauschalen Teile-Tarif. Er bindet Produkt-ID,
Variantenhash und Binding-Beleg, Lieferantenidentität, exakte Menge, Nettopreis,
Bestand, Nettoversand, beide unabhängigen Quelldateien und Zeitpunkte sowie die
früheste Ablaufzeit. `retail_gross_cent` rechnet bestätigten Nettoversand mit 15%
Aufschlag und 19% MwSt. um; 6,00 € netto ergeben 8,21 € brutto. Ein explizit
bestätigter Nullbetrag bleibt null; fehlender Versand wird niemals als null ersetzt.

## Neue DOM- und REST-Belege normalisieren

`scripts/normalize_rsu_parts_reads.py` kombiniert ein gepinntes originales
DOM-Abbild und erhaltene erfolgreiche `REST_INQUIRY_GET`-Datensätze. Es bedient
keinen Browser und führt keine Abfrage aus. Das Capture-Manifest lautet:

```json
{
  "schema_version": 1,
  "captures": [
    {
      "file": "original-supplier.dom.txt",
      "sha256": "<SHA256>",
      "observed_at": "2026-11-18T11:58:00+00:00"
    }
  ]
}
```

Der Helfer verlangt eindeutige Originalfelder für Marke, Herstellerartikelnummer,
EAN und RSU-Artikelnummer. Er liest die ausgewählte Menge und jede Angebotszeile,
prüft deren Rechnung und korreliert die Daten mit dem tatsächlichen REST-Ergebnis.
Mtime, Dateiname, Artikelname oder spätere Abfragen verlängern keine Beobachtung.
Originale und Capture-Metadaten stehen anschließend mit Hash im Quellenverzeichnis.

Aufruf mit `--catalogue`, `--catalogue-sha`, `--rest`, `--rest-sha`, `--captures`,
`--captures-sha`, optional `--known`/`--known-sha` und `--output`. Die daraus
entstehende normalisierte Datei wird im oben beschriebenen Quellenmanifest als
`rsu_read_json` verwendet. Das Abgleichdatum `--as-of` muss den tatsächlich
beabsichtigten Prüfzeitpunkt wiedergeben; der Runtime-Prüfer muss die originale
Ablaufzeit auch später erneut prüfen.

## Bereits nutzbare erhaltene Belege

`scripts/prepare_known_rsu_parts_observations.py` prüft die zwei vorhandenen
positiven Quellen erneut: AIC 75983 sowie Diederichs 1033106. Es kontrolliert die
originalen Produktfelder und bei AIC zusätzlich die erfolgreiche Anfrage für
Menge 1. Es führt selbst keine Anfrage aus.

Der erste tatsächliche Lauf vom 18. November hat 172.836 CSV-Zeilen und diese zwei
Beobachtungen geprüft: 2 Identitätskandidaten, 1 belegter 15%-Preis-Seed und
787 Artikel ohne passende RSU-Identitätsquelle. Noch kein Artikel besitzt eine
vollständige neue Direktkaufgrundlage. Benötigt werden insbesondere der passende
RSU-Kfz-/Zubehörbestand, aktuelle exakte Mengenabfragen, Packungs-/Einheitsbelege
und dokumentiert auswählbare Lieferoptionen. Kein negativer Exporttreffer wird
als Aussage verwendet, dass der Artikel im RSU-Webshop nicht existiert.
