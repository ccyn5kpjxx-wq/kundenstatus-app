# Lokaler TYREsystem-CSV-Leser

Stand 02.11.2026. `tyresystem_feed.py` liest ausschließlich eine bereits lokal
vorliegende Exportdatei oder empfangene Bytes. Es lädt nichts herunter und ist
mit keiner öffentlichen Route, Preisquelle, Bestellung oder Zahlung verbunden.
`public_ready` ist im Snapshot und jedem Artikel unveränderlich `False`.

## Aufruf

```python
from datetime import datetime, timezone
from tyresystem_feed import load_feed

snapshot = load_feed(local_csv_path, abgerufen_am=datetime.now(timezone.utc))
kandidaten = snapshot.query(
    breite=225, querschnitt=40, zoll="18", lastindex=92, speedindex="V",
    saison="winter", menge=4, limit=12,
)
print(dict(snapshot.statistik))
print(dict(snapshot.fehlerkategorien))
```

`read_feed(path, *, abgerufen_am=...)` akzeptiert einen lokalen `str`/`Path`;
`parse_feed(bytes, *, abgerufen_am=...)` bereits empfangene Bytes. `load_feed`
wählt diese Varianten anhand des Eingabetyps. URL-, UNC- und Devicepfade werden
vor dem Dateiöffnen abgewiesen. Der Aufrufer muss einen vertrauenswürdigen lokalen
Pfad wählen; die Anwendung verwaltet keine Laufwerkszuordnungen oder Symlinks.

Die Abrufzeit muss ein zeitzonenbewusstes `datetime` sein und wird in UTC
gespeichert. Sie ist nur die vom Aufrufer gemeldete lokale Empfangszeit, kein
Lieferanten-Erzeugungszeitpunkt, keine Preisgültigkeit und keine Reservierung.
Der SHA256 identifiziert genau die gelesenen Bytes. Es gibt keine automatische
Frische- oder Veröffentlichungsgenehmigung durch den Import.

## Beobachtetes Dateiformat und begrenzter Umfang

Die tatsächlich am 02.11.2026 heruntergeladene Datei enthält 28 fest benannte
Spalten, UTF-8 und Semikolon-Trennung; UTF-8-BOM ist optional. CSV-Quoting wird
berücksichtigt. `HEADERS` enthält den exakt erwarteten Aufbau einschließlich
Groß-/Kleinschreibung und Reihenfolge. Ein abweichendes Schema wird ausdrücklich
abgewiesen und muss vor einer Erweiterung geprüft werden. Der Leser ist auf
64 MiB, 300.000 nichtleere Datensätze und 2.048 Zeichen je Feld begrenzt. Das
Standard-Feldlimit des Python-CSV-Lesers begrenzt zusätzlich sehr große Felder.

Nur `Artikeltyp=Reifen`, `Artikeluntertyp=PKW`, `Fahrzeugtyp=PKW`, `Bauart=R`
kommen in die Ergebnismenge. Felgen, Schläuche und andere Fahrzeugklassen werden
ausgeschlossen, auch wenn einzelne Felder ähnlich aussehen. ZR-, (Z)- und
zusammengesetzte Lastindex-/Geschwindigkeitsangaben sowie Zollbreiten erhalten
keine ungeprüfte Interpretation; sie werden mit einer Fehlerkategorie gezählt.
Die anfängliche Suche unterstützt metrische Dimensionen und die expliziten
Geschwindigkeitsindizes L/M/N/P/Q/R/S/T/U/H/V/W/Y.

`Saison=s/w/g` wird als `sommer/winter/ganzjahr` normalisiert. Der Bestandswert
bleibt die gemeldete ganze Zahl; 20 wird weder als tatsächlicher Maximalbestand
noch als Nachweis von mehr als 20 Stück ausgegeben. EAN/GTIN und Hersteller-
Artikelnummer bleiben Zeichenketten einschließlich führender Nullen. Artikel-IDs
müssen dagegen positive kanonische Dezimalzahlen ohne führende Nullen sein.

Die Suche vergleicht Maße und Saison exakt, Last-/Geschwindigkeitsindex als
Mindestanforderung und gemeldeten Bestand gegen die gewünschte Menge. Sie sortiert
nach präzisem beobachteten Einkaufspreis, bei Gleichstand nach Artikel-ID. Sie
beurteilt weder Fahrzeugzulassung noch Reifenqualität. Sie wählt keine Felgen aus.
DOT-/Demo-/Da-Ware ist standardmäßig ausgeschlossen und nur mit
`sonderware=True` intern abfragbar. Ein leeres DOT-Feld beweist kein Reifenalter.

## Fehlerisolation und Datenvertraulichkeit

Ungültige Einzelzeilen werden ausgeschlossen. Die Fehlerkategorien enthalten nur
feste Namen und Zähler, keine Rohzeilen, Pfade, Links oder Credentials. Strukturelle
CSV-, Encoding-, Header- oder Ressourcengrenzfehler verwerfen den gesamten Import.
Leere physische CSV-Zeilen werden ignoriert; falsche Spaltenzahlen werden gezählt.

Identische Wiederholungen einer ID zählen als `duplicate_rows` und überschreiben
keinen Artikel. Unterschiedliche vollständige Zeilen mit derselben ID sperren
die ID vollständig, auch über Produkttypgrenzen und bei später fehlerhaften
Produktdaten. `conflicting_ids` zählt diese gesperrten IDs. Deshalb sind
`articles + invalid_rows + ignored_non_pkw + duplicate_rows` nicht zwingend gleich
`rows_read`: Ein früher akzeptierter Artikel kann später quarantänisiert werden.
Das Modul hält unveränderliche Datensätze und begrenzte Fehlerkategorien bereit;
es schreibt keine Datei oder Datenbank und ersetzt keinen laufenden Snapshot.
Ein späterer Importjob muss einen vollständig geprüften Snapshot atomar wechseln.

`internal_dict()` enthält vertrauliche Einkaufspreise und darf niemals unverändert
an einen Kundenbrowser gegeben werden. Auch die Exportdatei und der persönliche
Download-Link gehören weder ins Repository noch in öffentliche Dateien oder Logs.
Beschreibungen sind gewöhnlicher untrusted Text, kein freigegebenes HTML.

## Noch erforderliche Lieferantenfreigaben

`Einkaufspreis` wird verlustfrei als `Decimal` gelesen. Die CSV bestätigt keine
Währung, Steuerbasis, Mengenkondition, Fracht oder bindende Lieferzeit. Diese
Felder bleiben ausdrücklich `None`; es wird insbesondere keine Fracht von null
angenommen. Der spätere Abgleich vom 02.11.2026 um 14:55 Uhr erklärt die zuvor
beobachtete EK-Abweichung durch unterschiedliche Zeitstände: Artikel 9411309
kostete im aktuellen Portal und Export übereinstimmend 93,06 EUR je Stück bzw.
372,24 EUR für vier. Ein aktueller Feed-/Portalwiderspruch wurde damit nicht
nachgewiesen. Dieser reine Parser ruft weiterhin keine Verkaufskalkulation auf;
die getrennte explizite Preis-/Frachtpolicy ist in `tyresystem-provider.md` beschrieben.

XL, Runflat und 3PMSF kommen zwar teilweise im freien Profiltext vor, sind aber
keine eigenen strukturierten CSV-Felder. Sie werden nicht als bestätigte Merkmale
ausgegeben. Dasselbe gilt für Qualitätsklassen oder übertragene Testnoten.
Akzeptierte EPREL-Links müssen HTTPS auf `eprel.ec.europa.eu` ohne Zugangsdaten,
Query, Fragment oder fremden Port verwenden; das Modul folgt keinem Link und
übernimmt weder Bilder noch eine Herstellerfreigabe aus der URL.

## Prüfung

`python scripts/test_tyresystem_feed.py` verwendet ausschließlich synthetische
CSV-Daten und gesperrte Python-Netzwerksockets. Geprüft werden unter anderem
Header/Encoding, Ressourcen- und Datei-Grenzen, exakte Decimal-/EAN-Erhaltung,
Einzelfehlerisolation, vollständige ID-Konfliktquarantäne, Produkttyp/Bauart,
Geschwindigkeitsrangfolge, Bestand, Sonderware und Geheimnisfreiheit der Fehler.
Die echte Datei wurde zusätzlich ausschließlich lokal gelesen und nicht in Git
übernommen. Weder Import noch Tests lösen HTTP, FTP, Bestellung oder Zahlung aus.
