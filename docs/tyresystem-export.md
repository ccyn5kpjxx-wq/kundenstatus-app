# Lesender TYREsystem-Preisexport

Stand: 02.11.2026. `tyresystem_export.py` ruft ausschließlich den im Kundenkonto
bereitgestellten HTTP-CSV-Export ab. Der Kundenkonto-Dialog bestätigt kostenlose
Bereitstellung und Aktualisierung alle fünf Minuten. Dieses Modul wird weder beim
Import noch beim Laden der Konfiguration oder beim Appstart automatisch ausgeführt.
Es ist nicht mit öffentlichen Routen, Preisfreigabe, Bestellungen oder Zahlungen
verbunden.

## Serverkonfiguration

Den vollständigen persönlichen Download-Link ausschließlich als serverseitiges
Secret `TYRESYSTEM_EXPORT_URL` hinterlegen. Nicht in Git, Templates, öffentliche
Konfiguration, URLs des eigenen Frontends, Logs, Screenshots oder Fehlermeldungen
übernehmen. Dieser Link ist eine Zugangsinformation für vertrauliche Einkaufspreise.
Keine Browser-Cookies, Postfachzugänge oder REST-API-Zugangsdaten werden benötigt.

```python
from tyresystem_export import TyreSystemExportClient

client = TyreSystemExportClient.from_env(authorize=authorize_internal_export)
snapshot = client.fetch()
```

Alternativ kann eine serverseitige Mapping-Konfiguration mit genau dem eigenen
Schlüssel `TYRESYSTEM_EXPORT_URL` übergeben werden. Die Freigabefunktion wird bei
jedem `fetch()` ohne Parameter aufgerufen und muss exakt `True` liefern. Eine
vorhandene URL allein ist keine Abruffreigabe. Zugangsdatenbeschaffung und die
Aktivierung des Exports sind getrennte Vorgänge des verantwortlichen Betreibers.

## Abrufgrenzen

Zugelassen sind ausschließlich HTTPS, `www.tyresystem.de`, Port 443 und der genaue
Pfad `/Services/TyresystemExport`. Die Query enthält nur einmal `index` mit leerem
Wert sowie einmal den geheimen `login`-Wert. Andere Hosts, Pfade, zusätzliche oder
doppelte Parameter, URL-Benutzerinformationen und Fragmente werden abgewiesen.
Die validierte URL wird kanonisiert. Es gibt keine konfigurierbare API-Origin.

Jeder Aufruf verwendet eine neue Session mit deaktivierten Umgebungs-Proxys und
Retries. Redirects sind ausgeschaltet, TLS-Prüfung bleibt aktiv. Es werden keine
Browser-Cookies oder HTTP-Basic-Zugangsdaten übernommen. Die Antwort muss Status
200 haben und dieselbe URL behalten. Die Content-Length und der tatsächlich
gelesene, gegebenenfalls vom HTTP-Client dekomprimierte Stream sind auf 64 MiB
begrenzt. Die tatsächliche CSV-Struktur wird unabhängig vom Content-Type geprüft.

Connect-/Read-Timeouts sind 3,05/10 Sekunden. Zusätzlich gilt ein weiches Budget
von 45 Sekunden vor/nach dem HTTP-Aufruf, zwischen Leseabschnitten, nach EOF und
nach der Parservalidierung. Es kann einen bereits laufenden Socket-/Parseraufruf
nicht hart unterbrechen. Es gibt keine automatische Wiederholung und keinen
periodischen Worker. Antworten und eigene Sessions werden auch im Fehlerfall
geschlossen.

## Ergebnis und verbleibende Anbindung

Das Ergebnis ist ein interner `FeedSnapshot` aus `tyresystem_feed.parse_feed`.
Es enthält vertrauliche Einkaufspreise und bleibt `public_ready=False`. Es werden
keine Dateien geschrieben. Abrufzeit bedeutet lokalen Empfang, keine bestätigte
Anbieter-Erzeugungszeit, Preisgültigkeit, Lieferzusage oder Reservierung. Fehlende
Währung, Steuer-/Frachtbasis, technische Reifenmerkmale und Fahrzeugpassung werden
nicht aus dem erfolgreichen Download abgeleitet. Ein funktionierender CSV-Export
belegt keinen REST-Bestellzugang oder funktionierenden Checkout.

`python scripts/test_tyresystem_export.py` testet synthetische Antworten mit
gesperrten Netzwerksockets: feste URL-Grenzen, exakte Laufzeitfreigabe, Stream- und
Zeitlimits einschließlich verspätetem EOF, Parserfehler, Transportfehler und
Tokenredaktion. Die Tests verwenden keine echten Links, Zugänge oder Downloads.
