# Reifenbilder

Die optionale Bildanbindung verändert weder Preise noch Montage- oder Qualitätsmerkmale. Der Preisexport enthält keine Bildadressen. Bilder werden deshalb separat vorbereitet und nach exaktem Hersteller und vollständigem Katalogprofil zugeordnet. Eine beobachtete Artikelnummer muss die Zuordnung belegen; ähnliche Modellnamen reichen nicht.

`reifen_bilder.ReifenBildStore` liest ausschließlich ein privates lokales Verzeichnis mit unveränderten, geprüften WebP-Dateien (`<sha256>.webp`) und einem Manifest:

```json
{
  "schema": 1,
  "profiles": [{
    "hersteller": "Beispielhersteller",
    "profil": "Vollständiger bestätigter Profilname",
    "sha256": "<64 Zeichen SHA-256>",
    "bezug": "profil",
    "nachweis": "Private Referenz zur Artikelzuordnung und Sichtprüfung"
  }]
}
```

Vor Aufnahme ins Manifest muss die konkrete Bilddatei visuell geprüft sein. Ein erfolgreicher CDN-Abruf oder ein gültiges WebP kann auch eine „Kein Bild“-Grafik liefern und genügt nicht als Nachweis. Quellen, Bilddateien und Freigabebelege gehören nicht ins Repository.

Der Store benötigt einen `catalog_source`-Callback auf den aktuellen `IndexedCatalog`. Der Katalogprovider erhält `image_resolver=store.describe`, Flask `REIFEN_BILD_STORE=store`. Ohne expliziten privaten Prüfcallback oder gesonderte öffentliche Freigabe samt Referenz liefert der Store keine Bilder. Die lokale Prüfung ersetzt keine Freigabe zur öffentlichen Veröffentlichung.

Die Suchantwort enthält nur die optionale `bild`-Struktur mit lokaler artikelgebundener URL, Alternativtext und `bezug: "profil"`. Die Bildroute prüft Artikel, Profilzuordnung, Hash, Dateigrenzen und vollständiges WebP-Decoding. Such- und Bildanfragen führen keine Lieferantenabrufe aus. Ungültige oder fehlende Bilder beeinflussen ein gültiges Preisangebot nicht.

Die Oberfläche nennt die Abbildung einen Profilbezug, weil Größe und Ausführung abweichen können. Fehlende oder nicht ladbare Fotos erhalten einen neutralen Texthinweis. Das Bild bestätigt keine Fahrzeugfreigabe und keine mitgelieferte Felge.

Prüfung: `python -m unittest scripts.test_reifen_bilder` und `node scripts/test_reifen_suche_ui.js`.
