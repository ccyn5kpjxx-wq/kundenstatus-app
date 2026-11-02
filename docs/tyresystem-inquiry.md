# Lesende TYREsystem-Artikelabfrage

Stand: 02.11.2026. `tyresystem_api.py` implementiert ausschließlich den öffentlich
dokumentierten GET-Aufruf für eine bereits bekannte TYREsystem-Artikelnummer und
Menge. Import und Konstruktion führen keine Verbindung aus. Das Modul ist nicht
mit der öffentlichen Suche oder dem Kauf-Core verbunden.

## Anschluss

Eigene API-Zugangsdaten im serverseitigen Secretstore:

- `TYRESYSTEM_API_USERNAME`
- `TYRESYSTEM_API_PASSWORD`
- `TYRESYSTEM_API_MODE`: ausdrücklich `test` oder `live`

Keine Shop-, Stripe-, Mietwagen- oder Postfach-Zugangsdaten übernehmen. Laut
öffentlicher Anleitung stellt RSU den API-Zugang gesondert auf Anfrage bereit;
er beginnt im Testmodus und benötigt später eine gesonderte Livefreischaltung.
`MODE` ist nur die lokale Konfigurationsangabe, kein aus der Antwort gewonnener
Nachweis des tatsächlichen Servermodus.

```python
client = TyreSystemInquiryClient.from_env(authorize=authorize_internal_inquiry)
result = client.inquire(article_id, amount)
```

Die Freigabefunktion erhält die kanonische Artikel-ID und Menge und muss exakt
`True` liefern. Ohne diese ausdrückliche Laufzeitfreigabe erfolgt kein Request.
Sie muss tatsächliche API-Berechtigung und den erlaubten internen Anlass prüfen.
Keine automatische Startabfrage oder periodische Abfrage eingerichtet.

Der einzige Zielpfad lautet
`https://api.tyresystem.de/Rest/Inquiry/{idArticle}/{amount}`.
Keine konfigurierbare Origin, keine Redirects, keine POSTs, keine automatischen
Wiederholungen; HTTPS-Zertifikatsprüfung aktiv. Der Standardtransport verwendet
keine Umgebungs-Proxys. Connect-/Lesezeitlimits: 3,05/8 Sekunden. Ein zusätzliches
15-Sekunden-Budget beendet die weitere Verarbeitung zwischen IO-Schritten; es
ist kein harter Abbruch eines bereits laufenden Socket-Aufrufs.

## Ergebnis und Grenzen

`ArticleInquiry` enthält Artikel-ID, EAN, angefragte/bestätigte Menge, unverändert
präzisen `Decimal`-Preis, Bestand, Beschreibung, lokale Abrufzeit und konfigurierten
Modus. Es gibt keine rohe Anbieterantwort und keine Credentials in Fehlertexten.
JSON und XML entsprechen den Beispielen; mehrdeutige Schlüssel, XML-DTD/Entities,
unpassende Artikel/Mengen, fehlende Werte und fehlerhafte Zahlen werden abgewiesen.
Antworten sind auf 64 KiB begrenzt. Lokale technische Grenzen sind 1–1.000 Stück,
18-stellige positive Artikelnummern, vier Preisnachkommastellen und höchstens
1.000.000 als positiver Preis. Das sind keine zugesagten Anbieterlimits.

Das Ergebnis ist intern. `price` ist weder automatisch als Netto-EUR-Stückpreis
freigegeben noch als öffentlicher Verkaufspreis geeignet. Die Dokumentation nennt
keine vollständige Fracht-/Steuerbasis, Preisgültigkeit, verbindliche Lieferzeit
oder verlässliche serverseitige Test-/Live-Markierung. Eine lokale Abrufzeit ist
keine Preis- oder Bestandsreservierung. Auch ein ausreichender Bestand verspricht
keine Lieferbarkeit bis zum Montage- oder Zahlungsabschluss.

Eine Dimensions-/Saisonsuche benötigt gesondert autorisierte Artikelstammdaten
oder eine aktuelle Suchschnittstelle. Reservierung, Bestell-Commit, sichere
Bestellwiederholung und der öffentliche Preisprovider bleiben bewusst offen.
Für den Kauf-Core sind eine bindende Lieferzusage und ausreichende Hold-Dauer
zusätzlich zu klären. Die Händlerfreischaltung allein aktiviert dieses Modul nicht.

## Quellen und Prüfung

Primärquelle: [TYREsystem REST-Architektur](https://www.tyresystem.de/static/Anleitung/API-REST-Architektur.pdf).
Am 02.11.2026 als PDF und Text unter `work/tyresystem-api-quellen/` gesichert.
SHA256 der heruntergeladenen PDF:
`5fa0c28656990a4fe5b5b6f244862cd3bb8847398975e5c7d6d575de4b485275`.
Die Provenienzdatei dokumentiert URL und Abrufzeit. Ein ausdrücklicher
Dokument-Versionsstand wurde nicht festgestellt; aktuelle Verbindlichkeit und
Rechte sind von RSU noch zu bestätigen.

`python scripts/test_tyresystem_api.py` prüft ausschließlich synthetische
Antworten mit gesperrten Netzwerksockets: Protokollformen, Typen, Mengen/Identität,
Fehler, Authentisierungsstatus, Größen-/Zeitlimits, XML-/JSON-Angriffe und
Geheimnisredaktion. Kein echter API-Aufruf und kein Verbindungserfolg behauptet.
