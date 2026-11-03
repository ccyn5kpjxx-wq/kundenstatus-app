# TYREsystem-Bestelladapter: eine Position, ein dauerhafter Versuch

Stand: 03.11.2026. Implementierung vorbereitet und offline geprüft. Keine echte
Bestellung, kein API-Verbindungserfolg und keine RSU-Test-/Livefreigabe behauptet.
Der Adapter aktiviert weder Shop noch Zahlung, Reservierung oder Beschaffung.

Primärquelle: https://www.tyresystem.de/static/Anleitung/API-REST-Architektur.pdf
Gesicherte PDF-SHA256: 5fa0c28656990a4fe5b5b6f244862cd3bb8847398975e5c7d6d575de4b485275.
Die Quelle dokumentiert POST /Rest/Order, JSON/XML und positionsweise Ergebnisse.
Sie dokumentiert keine serverseitige Idempotenz, Orderlookup, Hold-Dauer oder
verbindliche Liefertermine. guid wird deshalb nicht als Idempotenzschlüssel benutzt.

## Eigene Zugangsdaten und tatsächliche Lieferantenumgebung

Nur eigene serverseitige Env-Credentials:

- TYRESYSTEM_API_USERNAME
- TYRESYSTEM_API_PASSWORD
- TYRESYSTEM_API_MODE: test oder live

Kein dotenv-/Appimport und keine Shoplogin-, Exportlink- oder andere Account-
Fallbacks. RSU vergibt laut Anleitung API-Zugangsdaten gesondert auf Anfrage.
Ein neuer API-Account startet dort im Testmodus; RSU muss ihn später live
freischalten. configured_mode bleibt eine lokale Konfigurationsangabe und ist
kein aus der Antwort ermittelter Modusnachweis.

Die tatsächliche RSU-Umgebung und deren Bestätigungsreferenz, volle Mengenannahme,
Preisbindung, Netto-/Währungsbasis sowie die feste Werkstatt-Lieferadresse müssen
vorher im übergeordneten Ablauf freigegeben sein. Der lokale authorize-Callback
prüft diese persistierte Policy. Der Transport erfindet dafür kein Freigabeflag.

account_fingerprint(username, mode) aus tyresystem_api.py bildet eine gemeinsame
lokale Accountbindung für Inquiry und Order. Passwortrotation ändert sie nicht,
ein anderer API-Benutzer oder konfigurierter Modus hingegen schon. Das ist weder
ein Passwortdigest noch ein Lieferantenbeweis. Beide Clients und der dauerhafte
Wrapper stellen account_fingerprint lesbar bereit; der Orderwrapper außerdem
configured_mode.

## API und normalisierter Auftrag

```python
from tyresystem_order import (
    TyreSystemOrderClient, DurableTyreSystemOrders,
    freeze_order_request, request_sha256, init_schema, BACKUP_TABLES,
)

frozen = freeze_order_request({
    'produktart': 'reifen',             # alternativ einzelne felgen
    'artikel_id': '123',                # kanonische positive Lieferanten-SKU
    'ean': '0012345678905',             # genau 13 Stellen als String
    'anzahl': 4,                       # echter int, 1..8
    'einkauf_netto': '68.34',           # bestätigter EK, nie öffentlicher VK
    'waehrung': 'EUR',
    'reference': 'own_order_id',        # stabil und gleich submit_once(order_id)
    'delivery_address': {
        'neutraldelivery': 0, 'salutation': 'Firma',
        'name': 'Eigener Werkstattname', 'street': 'Eigene Werkstattadresse',
        'zipcode': '01234', 'city': 'Eigener Ort', 'countrycode': 'DE',
    },
})
```

Der Preis muss ein positiver Decimalstring mit maximal vier Nachkommastellen sein;
Floats, Exponenten und nachträgliche Rundung werden abgewiesen. Die Normalisierung
speichert vier feste Nachkommastellen. request_sha256(...) ist die maßgebliche
Hashfunktion für diesen normalisierten Auftrag; keine abweichende eigene JSON-
Serialisierung im Aufrufer verwenden. FrozenOrderRequest und seine Lieferadresse
sind unveränderlich. .as_dict() liefert eine neue veränderbare Kopie.

Die Adresse kommt ausschließlich aus freigegebener eigener Werkstattkonfiguration,
niemals aus den Rechnungs-/Adressfeldern des Endkunden. Die Quelle belegt Firma als
salutation im Beispiel. Die Feld-/Zahlen-/Längenobergrenzen sind zusätzliche lokale
Grenzen; sie werden nicht als zugesagte Lieferantenlimits dargestellt.

authorize erhält vor einem neuen Claim und unmittelbar vor HTTP jeweils dasselbe
inhaltlich gebundene, tief unveränderliche Mapping mit genau diesen Schlüsseln:

```text
reference
request                normalisiertes Mapping, Lieferadresse ebenfalls unveränderlich
request_sha256         nur normalisierter Request, ohne Account/Modus
configured_mode
account_fingerprint
```

Nur Rückgabe exakt True erlaubt die Aktion. Der Callback muss eine lokale
Policyprüfung sein; kein verbrauchbarer Einmalnonce und keine externe Verbindung.
Eine bereits gespeicherte identische erfolgreiche Bestätigung kann auch nach
Sperren neuer Bestellungen ohne erneute Autorisierung/HTTP zurückgegeben werden.

```python
client = TyreSystemOrderClient.from_env(authorize=authorize_persisted_flow)
orders = DurableTyreSystemOrders(get_db, client)
receipt = orders.submit_once(order_id, frozen)
```

init_schema(db) gehört in die kontrollierte Schemainitialisierung; die aufrufende
Transaktion muss committen. BACKUP_TABLES ist ('tyresystem_order_attempts',),
SCHEMA_FEATURE lautet tyresystem_order_attempts_v1. Kein Konstruktor erstellt
selbst Schema oder öffnet die Datenbank. get_db liefert pro Aufruf eine eigene
Verbindung mit Mapping-Zeilen und execute/commit/close; die SQL-Schnittstelle
entspricht dem bestehenden Portaladapter (?-Platzhalter, ON CONFLICT/RETURNING).

## Ein dauerhafter Versuch, keine automatische Wiederholung

1. Vollständigen Auftrag normalisieren und gegen eine vorhandene Bestellreferenz
   prüfen. Request, Modus, API-Account oder Adresse dürfen sich nicht ändern.
2. Neue Autorisierung prüfen und einen eindeutigen claimed-Datensatz mit eigenem
   attempt_id, Referenz, Account/Modus, normalisiertem Request, gebundenem Hash und
   XML-Hash einfügen. Vor einem möglichen POST ausdrücklich committen und die
   Verbindung schließen. Keine Datenbanksperre bleibt über das externe IO offen.
3. Genau einen POST versuchen. Ein vorhandener claimed/unknown/rejected/blocked-
   Datensatz führt nie zu einem weiteren POST, auch nicht nach Altersablauf,
   Prozessneustart, neuer Konfiguration oder verlorenem HTTP-Ergebnis.
4. Eine eindeutige Bestätigung per CAS ausschließlich auf den eigenen unveränderten
   claimed-Datensatz schreiben und committen. Erst danach Erfolg zurückgeben.

Ein Crash direkt nach dem Claim und noch vor HTTP kann einen tatsächlich nicht
versandten Auftrag blockieren. Das ist die beabsichtigte konservative Grenze:
Der Adapter beweist höchstens einen automatisch ausgelösten Versuch, keine
transaktionale Genau-einmal-Verarbeitung beim externen Lieferanten.

Bei DB-Fehler nach einer möglichen Lieferantenannahme bleibt der Vorgang unklar.
Hat der ACK-Commit tatsächlich stattgefunden und nur seine Rückmeldung ging
verloren, kann ein identischer späterer Aufruf den gespeicherten ACK wiedergeben.
Sonst bleibt manuelle Prüfung nötig. Ein später Fehler überschreibt per CAS keine
bereits dauerhaft gespeicherte Bestätigung. Kein Lesen/Fehler setzt den Versuch
zurück, keine automatische Statusheilung löst einen weiteren Auftrag aus.

## Rein lesende Wiederaufnahme zwischen Lieferanten-ACK und Core-Snapshot

`orders.acknowledged(order_id, frozen_request)` liest ausschließlich einen bereits
gespeicherten und erneut gegen vollständigen Request, Referenz, Modus und Account
geprüften ACK. Der Aufruf erzeugt weder Claim noch HTTP-/Autorisierungsaktion und
verändert keine Daten. Damit kann ein Core nach einem Crash unmittelbar nach dem
Wrapper-ACK dessen Beleg übernehmen und nur die folgenden Zahlungsschritte
fortsetzen, ohne submit_once erneut auszuführen.

Bei fehlendem Datensatz oder einem Zustand claimed/unknown/rejected/blocked ist
das Ergebnis None. Das beweist NICHT, dass kein Lieferantenauftrag existiert, und
ist keine Erlaubnis zum erneuten Senden. Identitätsabweichungen geben einen
attempt_conflict, ein beschädigter gespeicherter ACK einen attempt_unknown.
Auch diese Fehler führen zu keinem Schreib- oder HTTP-Vorgang.

## Transport, Antwort und ehrliche Bindung

Einziger URL: https://api.tyresystem.de/Rest/Order. XML mit genau einer Position
pos=1, dokumentierten Feldern idArticle/ean/guid/amount/price, eigener Lieferadresse
und ordernumber_customer. guid bleibt leer. Der Client verwendet eine isolierte
Session ohne Umgebungsproxy, Cookies oder Retryzustand; Basic Auth, TLS-Prüfung,
keine Redirects, keine Wiederholung. Maximal 16 KiB Request und 64 KiB Antwort,
Connect-/Lesetimeout 3,05/8 Sekunden und ein 15-Sekunden-Verarbeitungsbudget mit
Prüfungen nach Socket-/Stream-/EOF-/Parser-Schritten. Das Budget ist keine harte
Echtzeitunterbrechung eines bereits blockierenden DNS-/Socket-Aufrufs.

JSON und XML werden streng geprüft. Erfolgreich ist nur genau eine Position mit
pos=1, orderStatus=true/1, error=0, leerer Fehlermeldung und einer nichtleeren
positiven numerischen Lieferanten-Bestellnummer. Fehlende/doppelte/fremde Positionen,
Teilerfolge, widersprüchliche Statusangaben und mehrdeutige JSON-/XML-Strukturen
sind unklar. XML-DTD/Entities und abweichende XML-Codierungen werden abgelehnt.

Nur eine formal vollständige einzelne Position mit orderStatus=false/0, error=1
und ohne Lieferanten-Bestellnummer gilt als ausdrückliche Ablehnung. Andere
Fehler, insbesondere Timeout, HTTP-Fehler, falsche URL, Parserfehler oder verlorene
Antwort, sind unknown. errorMessage wird nicht in Receipt, DB oder Fehlertext
übernommen. Auch bei Ablehnung gibt es keine automatische Wiederholung. Eine bereits
dauerhaft gespeicherte vollständige Ablehnung bleibt bei exakt identischem
Request, Account und Modus als supplier_rejected/rejected abrufbar; ein Neustart
zwischen diesem Beleg und der übergeordneten Freigabe der Kartenautorisierung
macht die eindeutige Ablehnung nicht zu einem unbekannten Lieferantenauftrag.

Die dokumentierte Antwort enthält KEIN Echo von SKU, EAN, Menge, Preis, Währung,
Kundenreferenz oder serverseitigem Test-/Live-Modus. Der reine Client kennzeichnet
seine ergänzten Felder deshalb mit receiptbinding_source='submitted_request'.
Erst der dauerhafte Wrapper bestätigt deren Herkunft als 'persisted_request':

```text
bestaetigt=True, bestellnummer, reference, produktart, artikel_id, ean,
anzahl, einkauf_netto, waehrung, configured_mode, account_fingerprint,
receiptbinding_source='persisted_request', received_at
```

received_at ist ein lokaler UTC-Zeitstempel, keine Lieferzusage. Reservierung,
TTL, lieferdatum, serverseitige Mengen-/Preis-Echos und Freigabereferenzen werden
nicht erfunden. Eine einzelne Felge wird nicht als Komplettrad oder Montageauftrag
behandelt. Der Adapter ist kein Ersatz für die separate Zahlungs-/Annahme-Policy.

TyreSystemOrderError.code/outcome:

- configuration/input/not_authorized: not_sent für den aktuellen Aufruf.
- supplier_rejected: rejected, ausschließlich nach der oben beschriebenen Antwort.
- attempt_conflict/attempt_unknown/unknown: unknown und Prüfung erforderlich.

Insbesondere attempt_conflict bedeutet nicht, dass ein früherer Auftrag unversandt
war. Keine automatische Rückzahlung oder neue Beschaffung allein daraus ableiten.
Alle Fehlertexte sind fest und enthalten keine Zugangsdaten, rohen Anbietertexte,
HTTP-Antworten oder Endkundendaten.

## Offline-Prüfung

```sh
python -B scripts/test_tyresystem_order.py
python -B scripts/test_tyresystem_api.py
```

Synthetische API-Zugangsdaten, gesperrte Netzwerksockets und temporäre SQLite-Daten.
Fälle umfassen Claim-/ACK-Commitfehler vor und nach tatsächlichem Commit,
Parallelität, Crash/Timeout ohne Wiederholung, Request-/Account-/Preismanipulation,
Callback-Unveränderlichkeit, Positions-/Parserangriffe und Größen-/Zeitbudgets.
Kein echter API-Aufruf oder Bestellvorgang wurde dafür ausgeführt.
