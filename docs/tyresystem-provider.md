# Privater TYREsystem-Katalog und Suchprovider

Stand: 02.11.2026. `tyresystem_provider.py` verbindet einen lokal gespeicherten,
validierten Export mit dem internen Providervertrag von `reifen_suche.py`.
Es importiert weder die Portal-App noch dotenv, ruft keine URL auf und enthält
keine Reservierungs-, Bestell- oder Zahlungsfunktion. Eine Aktualisierung gibt
weder die öffentliche Anzeige noch den Kauf frei.

## Schnittstelle

```python
cache = TyreSystemCatalogCache(
    "/var/data/tyresystem-catalog", private_root="/var/data"
)
cache.replace(snapshot)  # ausschließlich vollständiger FeedSnapshot
indexed = cache.load()   # indexed.snapshot und unveränderlicher Dimensionsindex
provider = TyreSystemCatalogProvider(
    cache, publication_approved=False, publication_reference="",
    policy=None, article_metadata=None,
)
```

Der Cacheordner muss absolut, ein echtes Unterverzeichnis des ausdrücklich
angegebenen `private_root` und außerhalb öffentlicher Pfade liegen. UNC/URLs,
`static`, `public`, `templates`, `outputs` und `wwwroot` werden abgewiesen. Im
Repository ist ausschließlich der private `data`-Bereich zugelassen; Exporte
gehören nicht nach Git. Bei Render muss die Ablage auf der bestehenden privaten
persistenten Disk liegen, niemals im stateless Homepage-Dateisystem.

`catalog.json` ist maximal 64 MiB groß, UTF-8-JSON mit festem Schema, ursprünglicher
UTC-Empfangszeit, SHA256 der Quelle, normalisierten Artikeln und Payload-Prüfsumme.
Keine URL, Credentials, Cookies oder rohe Lieferantenantwort werden gespeichert.
Beim Laden wird das gesamte normalisierte Artikelformat erneut durch den
vorhandenen Feedparser geprüft; Teilimporte, doppelte Schlüssel/IDs, unbekannte
Felder, ungültige Typen und beschädigte Prüfsummen werden abgewiesen.

Schreiben: exklusiver lokaler Writerclaim, vollständige Validierung, private
Tempdatei mit `fsync`, danach atomarer Rename. Ein fehlgeschlagener Wechsel vor
dem Rename erhält die alten Bytes und ihre echte Empfangszeit. Ältere, bereits
15 Minuten alte oder mehr als 30 Sekunden in der Zukunft liegende Snapshots
dürfen keinen vorhandenen Stand ersetzen. Bei vorhandenem `.catalog-write.lock`
wird nicht blind übernommen; nach einem Prozessabbruch ist dessen Zustand vor
einer manuellen Freigabe zu prüfen. Leser sehen einen vollständigen alten oder
neuen Stand und laden beschädigte Änderungen nicht aus altem Arbeitsspeicher nach.

## Explizite Preis-/Frachtpolicy

Die folgenden am 02.11.2026 von der verantwortlichen Aufgabe geprüften offiziellen
Quellen belegen Netto-EUR-Produktpreise, PKW-Reifen ab zwei Stück frachtfrei,
6 EUR Fracht bei einem Reifen sowie die Standardlieferzeit von 1–4 Werktagen:

- [AGB, §3](https://www.tyresystem.de/agb)
- [Lieferbedingungen](https://www.tyresystem.de/lieferbedingungen)
- [Leistungen](https://www.tyresystem.de/leistungen)

Diese Fakten werden ausdrücklich übergeben; ohne vollständige passende Policy
liefert der Provider keine Angebote. Die CSV selbst erhält dadurch keine neuen
Felder oder impliziten Bestätigungen. Die derzeitige Policy ist auf diesen
geprüften Stand begrenzt; andere Kostenregeln benötigen eine erneute Prüfung
und entsprechende Code-/Konfigurationsanpassung.

```python
policy = {
    "currency": "EUR",
    "price_basis": "net_unit",
    "single_tyre_shipping_net": "6.00",
    "free_shipping_from": 2,
    "delivery_text": "Standardlieferung 1–4 Werktage; keine Reservierung.",
    "verified_on": "2026-11-02",
    "sources": {
        "price": "https://www.tyresystem.de/agb",
        "shipping": "https://www.tyresystem.de/lieferbedingungen",
        "delivery": "https://www.tyresystem.de/leistungen",
    },
}
```

Öffentliche Anzeige benötigt unabhängig davon `publication_approved is True`
und eine nichtleere nicht geheime `publication_reference` auf die tatsächlich
verifizierte Veröffentlichungsfreigabe. Standard ist AUS. Ein gültiger Export,
die Preisbedingungen oder eine nichtleere Referenz allein schalten nichts frei.

Der Provider berechnet keine eigenen Verkaufspreise: Er liefert bestätigte
Netto-EK und genau einmal die mengenabhängige Fracht an `reifen_suche`, die den
vorhandenen Kalkulator mit EK +10 Prozent vor MwSt. nutzt. Die Providerantwort
ist INTERN und darf nicht direkt als JSON zum Browser gegeben werden.

## Suche und optionale Artikelmetadaten

`provider(query)` erwartet die normalisierte Suchstruktur mit `breite`,
`querschnitt`, `zoll`, optionalen `lastindex`/`geschwindigkeitsindex` (jeweils `None`
oder echter Wert), `saison`, `qualitaet`, ganzzahliger `anzahl` 1–8 und booleschem
`montage`. Maße/Saison werden exakt, LI/SI als Mindestwerte und der gemeldete
Bestand gegen die Menge geprüft. DOT-/Demo-/Da-Sonderware bleibt ausgeschlossen.
Es werden nur ganzzahlige Felgendurchmesser normalisiert; keine Umdeutung von
Halbzollformaten. Der Katalogadapter filtert und sortiert die vollständige
Treffermenge und liefert zwölf Angebote je Seite. Einfache bestehende
Funktionsprovider bleiben auf maximal 100 interne Angebote begrenzt.
Gemeldeter Bestand ist kein Hold.

Für `qualitaet="alle"` genügt die tatsächliche Artikelbeschreibung; ohne
Kuratierung bleibt `item.qualitaet=None`. Für die vier bisherigen Klassen muss
eine bestätigte Artikelklassifikation vorliegen. Auch XL, Runflat und 3PMSF
werden niemals aus Profiltexten abgeleitet. Beispiel einer ausdrücklichen
serverseitigen Ergänzung (synthetische Artikel-ID):

```python
article_metadata = {"123": {
    "bestaetigt": True,
    "nachweis": "Referenz auf geprüftes Artikeldatenblatt",
    "qualitaet": "premium",
    "runflat": False,
    "montage_standard_bestaetigt": True,
    "xl": True,
    "schneeflocke": True,
}}
```

Für Montage muss die Bauart ausdrücklich als Standardmontage bestätigt und
Runflat ausdrücklich `False` sein. Eine Niederquerschnittsschwelle wird hier
nicht erfunden. Ungültige Ergänzungen begründen weder Klasse noch Montage.
EPREL und die vier Label-Felder werden flach unter den vorhandenen Feednamen
weitergegeben; der öffentliche Suchserializer prüft und whitelistet nochmals.

`abgerufen_am` bleibt die wirkliche Exportempfangszeit. `gueltig_bis` ist exakt
diese Zeit plus maximal 15 Minuten und bezeichnet die lokale Frischegrenze;
Laden, Suchabfragen oder fehlgeschlagene Aktualisierungen verlängern sie nicht.
Die Standardlieferzeit begründet kein konkretes Lieferdatum und keine Anbieter-
reservierung. Bei beschädigtem/fehlendem/abgelaufenem Cache wird mit sicherem
`TyreSystemCatalogError` abgebrochen, ohne rohe Preise oder Fehlermeldungen.

## Ausdrückliche Aktualisierung

Die Portal-Konfiguration verwendet ausschließlich eigene Einstellungen:

```text
TYRESYSTEM_PRIVATE_ROOT=/var/data
TYRESYSTEM_CATALOG_DIR=/var/data/tyresystem-catalog
TYRESYSTEM_CATALOG_POLICY_FILE=/var/data/tyresystem/policy.json
TYRESYSTEM_CATALOG_METADATA_FILE=/var/data/tyresystem/article-metadata.json
TYRESYSTEM_CATALOG_PUBLICATION_APPROVED=0
TYRESYSTEM_CATALOG_PUBLICATION_REFERENCE=
```

Die Metadatendatei ist optional; ohne belegte Bauart bleiben Montageangebote
ausgeschlossen. Policy und Metadaten sind begrenzte private JSON-Objekte;
Doppelschlüssel, öffentliche Pfade und unvollständige Freigaben werden abgewiesen.
`configure_integrations()` liest bei expliziter Freigabe nur diese Dateien und
erzeugt den Adapter. Es liest keinen Katalog, ruft keinen Export ab und aktiviert
weder Kauf, SMS noch Zahlung. Entfernte oder widerrufene Freigaben entfernen auch
einen zuvor eingerichteten eigenen Provider. Die Freigabereferenz muss auf die
tatsächlich geprüften Nutzungs-/Veröffentlichungsrechte verweisen.

Der neue Kundeneinstieg `/reifen` benötigt vor der Suche keine Kontaktdaten.
Solange die vollständige Live-Kaufbereitschaft fehlt, kennzeichnet er den
geschlossenen Bestellabschluss und zeigt keine Bestellschaltflächen. Ein
erfolgreicher Katalogimport ändert diese Sperre nicht. Die öffentliche Preisantwort
enthält nur Verkaufspreise; Netto-EK, Download-Link und interne Nachweise bleiben
serverseitig. Der Montagezuschlag beträgt nach Betreiberentscheidung vom
02.11.2026 5 EUR brutto je Reifen bei Querschnitt 40 und kleiner.

```text
python scripts/refresh_tyresystem_catalog.py --run --cache-dir /var/data/tyresystem-catalog --private-root /var/data
```

Nur dieser ausdrücklich gestartete CLI-Pfad liest `TYRESYSTEM_EXPORT_URL` aus
dem serverseitigen Secretstore und ruft einmal den vorhandenen streng begrenzten
Exportclient auf. Ohne `--run` kein Envlesen, kein GET und keine Cacheanlage.
Kein eigener Refreshthread, Cronservice oder automatischer Retry wird installiert.
Ein später eingerichteter Fünf-Minuten-Job muss separat koordiniert werden.

Ausgaben enthalten ausschließlich Erfolg/Fehlschlag, bei Erfolg die wirkliche
Empfangszeit und Artikelanzahl; nie URLs, EK, Pfade oder Anbieterfehler. Im Fehlerfall
wird keine neue Empfangszeit ausgegeben. `cache_must_be_checked=True` weist auf
den erforderlichen Zustandsabgleich hin, ohne einen alten Stand frisch zu nennen.
Das Kaufgate und Veröffentlichungsrechte werden durch den CLI nicht geändert.

## Private lokale Echtkatalogansicht

`private_view_authorized` ist ein zusätzlicher, ausschließlich serverseitig
injizierter Callback. Nur dessen exakte Rückgabe `True` erlaubt die private
Ansicht ohne öffentliche Datenfreigabe. Boolwerte oder Ausnahmen erteilen keine
Berechtigung. `publication_ready` bleibt dabei `False`; die Produktionskonfiguration
richtet keinen solchen Callback ein. Die lokale Werkstattansicht bindet ihn an
GET, Loopback-IP, festen Host und die drei Shop-/Such-/Auswahlpfade. Kauf-, Zahlungs-,
SMS- und Mailwege bleiben geschlossen.

`facets()` liefert aktuelle unterstützte PKW-Größen und Hersteller mit gemeldetem
Bestand ab einem Stück. Sonderware und ungeeignete Maße bleiben ausgeschlossen.
`GET /api/reifenkatalog/auswahl` gibt daraus nur validierte Dimensionen, Saisons,
Hersteller und Originalzeitstempel aus. Der Browser baut daraus abhängige Felder
für Breite, Höhe und Zoll auf; Freitexteingabe und LI/SI bleiben erhalten.

Die Suchparameter `hersteller`, `sortierung` (`preis_auf`, `preis_ab`, `name`) und
`seite` sind optional. Herstellerfilter und Sortierung wirken auf den vollständigen
passenden Katalog vor der Begrenzung auf zwölf Angebote je Seite. `treffer_gesamt`,
`seite` und `seiten` sind validierte Zähler. Die erneute Prüfung einer gewählten
SKU erfolgt weiterhin unabhängig vom Suchrang. Qualitätsklassen und Montage-
Eigenschaften werden aus einem Markennamen oder Profiltext nicht erfunden.

Der lokale Betreiber-Runner erneuert den privaten Export über einen festen,
begrenzten Unterprozess im Abstand von fünf Minuten. Nur fertig validierte,
unveränderliche Snapshots werden in die lokale Suchansicht übernommen; eine neue
Dateiprüfung blockiert dadurch keine laufenden Suchabfragen. Die ursprüngliche
15-Minuten-Frist wird bei jeder Abfrage geprüft und nie durch Lesen verlängert.
Der Runner ist keine Render-Aktivierung oder öffentliche Nutzungsfreigabe.

## Offlineprüfungen

Die Kundensuche liefert zusätzlich einen freigegebenen `code`, bei ungültigen
Eingaben HTTP 400 und optional `feld`. Fehlender Katalog, veralteter Preisstand,
vorübergehende Nichterreichbarkeit, echte Nichttreffer und nicht prüfbare
Angebotsdaten bleiben unterscheidbar. Rohe Anbieterfehler werden nie ausgegeben.
Die Oberfläche markiert Eingaben am Feld und bietet ausschließlich ausdrücklich
ausgelöste Wiederholungen oder Filteränderungen an. Last-/Geschwindigkeitsindex
bleiben auch bei Kurzschreibweisen erhalten.

Die lesende Suche verwendet ein eigenes IP-Fenster mit standardmäßig 60 Abrufen
pro 60 Sekunden (`REIFEN_SUCHE_RATE_LIMIT_MAX`,
`REIFEN_SUCHE_RATE_LIMIT_WINDOW_SECONDS`); Kontaktformular-Grenzen bleiben
unverändert. Ungültige Maße werden zusätzlich im Browser erkannt. Bei HTTP 429
zeigt die Oberfläche die verbleibende Wartezeit aus `Retry-After` an.

`node scripts/test_reifen_suche_ui.js` prüft die reale Such-JS offline, unter
anderem Feldmeldungen/ARIA, getrennte Fehlerzustände, explizite Filteraktionen,
veraltete Antworten, Wartezeit und sichere Links. `python
scripts/test_reifen_suche_fehler.py` prüft den Fehlervertrag und eindeutige
Größenformate ohne Änderung von Last- oder Geschwindigkeitsanforderungen.

`python scripts/test_tyresystem_provider.py`: ausschließlich synthetische CSVs,
Tempdateien und gesperrte Socket-/DNS-Verbindungen. Die Tests importieren keine
Portal-App und keine dotenv-Datei. Geprüft werden unter anderem atomarer Wechsel,
beschädigter/abgelaufener Cache, Writerkonflikte, strenge Policy/Freigabe, Mengen,
Indexfilter, kuratierte Merkmale, Frische, echte Kalkulatoranbindung und ein
ausdrücklich injizierter CLI-Fake. Keine echte Datei oder Anbieterabfrage nötig.
