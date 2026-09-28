# Werkstatt-Cockpit API v1

## Materialwissen und Postfachquellen

`/admin/assistent-mailquellen` erfasst das vorhandene betriebliche IMAP-Postfach schrittweise mit gespeichertem Ordner-/UID-Fortschritt. Bank-, Finanz- und private Ordner werden vor dem Lesen ausgeschlossen. Bekannte bzw. ausdrücklich zugeordnete Materialabsender liefern Rechnungsanhänge für den Artikelimport; eine Lesezuordnung bestätigt keine Bestelladresse. Einlesen ändert keine IMAP-Markierungen und versendet keine Nachrichten. Erfasste Header, gesicherte Anhänge und ausgelesene Artikel sind getrennte Bearbeitungsstände. Mailtexte, sonstige Unterlagen, ausgeschlossene Ordner und unklare Absender gelten nicht als vollständig ausgewertet.

Die Artikelsuche berücksichtigt Synonyme und belegte Varianten. Rechnungsmenge, Bestelleinheit und expliziter Packinhalt sind getrennte Angaben mit Quellen. Fehlende Mengen werden nicht als eins behandelt; frühere ungeprüfte Mengen-Defaults bleiben unbekannt. Wiederholte Mengen gleicher Varianten können einen Vorschlag ergeben, niemals einen sicheren Verbrauch oder eine Bestellfreigabe. Die Betriebspräferenz „ein Karton Abklebeband“ beweist keinen Kartoninhalt.

Der Avatar erhält einen kompakten, rechtegeprüften Materialkontext. Bei passenden Textfragen werden Suchtreffer vor dem ersten Modellaufruf bereitgestellt; die Artikelabfrage bleibt für weitere Varianten verfügbar. Es findet keine Rechnungs-OCR im Gespräch statt. Die Architektur reduziert zusätzliche Anfragen entsprechend der [OpenAI-Dokumentation zur Latenzoptimierung](https://developers.openai.com/api/docs/guides/latency-optimization); eine garantierte Antwortzeit oder ein erfolgreicher iPhone-Mikrofontest ist damit nicht nachgewiesen.

Die neuen Postfachquellen-Tabellen gehören zum Backup unter `werkstatt_mailquellen_v1`. Ältere Sicherungen werden beim Wiederherstellen ergänzt. Keine Kontozahlen oder unbereinigten Rechnungen werden dem Avatar als Kontext übergeben.

Diese Erweiterung stellt eine authentifizierte Lese-API für den Werkstattassistenten bereit. Bestehende Leseschlüssel bleiben lesend. Neue Funktionen ergänzen eigene Tabellen und zwei Lackierbereitschaftsfelder; beim Lesen werden keine Aufträge verändert. Bestellversand ist separat konfiguriert und standardmäßig deaktiviert.

Admin-Seite: `/admin/assistent-api`. Dort erzeugt/erneuert/widerruft die Werkstattleitung den Zugang. Nur der SHA-256-Hash des zufälligen Schlüssels wird gespeichert; der vollständige Schlüssel wird einmal angezeigt. POST benötigt die bestehende Admin-Anmeldung und CSRF. Der API-Schlüssel ist ausschließlich serverseitig zu hinterlegen, niemals in einer Browser-App oder einem Commit.

Alle Datenendpunkte verlangen `Authorization: Bearer <Schlüssel>` und liefern `Cache-Control: no-store`:

- `GET /api/werkstatt/v1/status`: Version und Zeitzone.
- `GET /api/werkstatt/v1/auftraege?q=...&offset=0&limit=100&archiv=0`: Suche und Pagination. `next_offset` zeigt weitere Seiten.
- `GET /api/werkstatt/v1/auftraege/<id>`: Fahrzeug, Arbeiten, Ausleseunsicherheiten, Freigabestatus, Termine, Transportart, Teile-Aktenstand und Dokumentverweise.
- `GET /api/werkstatt/v1/termine?datum=YYYY-MM-DD`: Abholungen, Kundenanlieferungen, Fertigstellungen und Rückgaben. Ohne Datum heute in Europe/Berlin.
- `GET /api/werkstatt/v1/dokumente/<id>`: gespeicherte Dokumentauslese mit Quelle und Unsicherheit; fehlende Auslese ist ausdrücklich keine Analyse. Maximal 24000 Zeichen, Kürzung markiert.
- `GET /api/werkstatt/v1/artikel?q=...`: Artikelnummern/Produkte aus Einkaufsbelegen, historische Preisquelle und Quellbeleg.
- `GET /api/werkstatt/v1/belege/<id>`: gespeicherte Rechnungs-/Belegauslese.

Die Berechtigungen heißen `auftraege:lesen`, `dokumente:lesen`, `einkauf:lesen`. Statuslinks, Zugangscodes und Speicherpfade gehören nicht zum API-Auftragsmodell. Zugriff erzeugt keine neue OCR und übernimmt keine automatisch erkannten Fahrzeugdaten. Rechnungsprodukte sind keine aktuelle Lieferantenzusage. Bestellversand bleibt eine gesondert zu integrierende, autorisierte Aktion mit eindeutigem Produkt, Lieferant, Menge und Budget.

Validierung: `python scripts/test_assistent_api.py`, `python scripts/smoke_test.py`, `python scripts/flow_test.py`; alle im isolierten Testdatenbestand bestanden. Kein echter Bestell- oder E-Mail-Versand getestet.


## Tagesübersicht und Werkstattzettel

`GET /api/werkstatt/v1/briefing?datum=YYYY-MM-DD` liefert fällige/überfällige Aufträge, Kundenanlieferungen, Abholfahrten und Rückgaben, eine kurze Sprachfassung und alle Einträge für die Anzeige. Jede Uhrzeit bleibt ihrem Termin zugeordnet.

`GET /api/werkstatt/v1/lackplan?zeitraum=heute|woche` liefert laufende Lackierung, Lackierbereitschaft und Aufträge mit Lackangaben im Zeitraum. Farbcode, Farbton und zweiter Farbton bleiben unverändert. Ohne dedizierte Lackiertermine ist dies ausdrücklich kein Kabinen-/Personalplan. Das angezeigte Datum bleibt die Fertigfrist.

`GET /admin/auftrag/<id>/werkstattzettel` ist ein Admin-Druckblatt mit großer stabiler interner Auftragsnummer, Fahrzeug, Arbeitsbeschreibung, Terminen und Lackdaten. Ausdruck über den Browser als A4 oder PDF. Kundenzugangslinks, Kontakt- und Preisfelder werden nicht ausgewählt.

## Interner Fortschritt

`GET /api/werkstatt/v1/auftraege/<id>/fortschritt` benötigt Leserechte. POST an denselben Endpunkt braucht zusätzlich `auftraege:fortschritt`, wird aber durch bestehende Lesezugänge abgewiesen. JSON: `action`, `expected_status`, `expected_changed_at`, `request_id`. Erlaubt sind `lackierbereit`, `lackierung_starten`, `finish_starten`; der Gesamtstatus wird nicht geändert. Wiederholungen sind idempotent, veraltete Stände werden abgelehnt. Die eng begrenzte CSRF-Ausnahme gilt nur mit gültigem Bearer auf diesem Endpunkt. Noch kein Schreibwerkzeug im lokalen Avatar aktiviert.

## Artikelimport und ausgeschlossene Daten

`/admin/assistent-artikel` verarbeitet Rechnungen schrittweise. Die Warteschlange und Artikelvorschläge bleiben dauerhaft erhalten; ein geschlossener Browser pausiert die Verarbeitung. Gespeicherte Vorschläge sind noch keine geprüften, bestellbaren Artikel. Größen/Farben/Quellpositionen werden getrennt aufbewahrt. Preisbeobachtungen sind keine bestätigten Brutto-Stückpreise.

Vor jeder Datei wird der Lieferant anhand der Quellenmetadaten geprüft. Bekannte Topcolor-, Car-Parts- und Tech-Masters-Namen sind vorgesehen; andere Quellen brauchen eine explizite Materiallieferanten-Zuordnung. Banking, Versicherungen, Beiträge, medizinische, Steuer- und Inkassobelege bleiben ausgeschlossen. `ASSISTANT_MATERIAL_SUPPLIERS` ist eine serverseitige JSON-Liste zusätzlicher exakter Firmennamen; sie kann die Ausschlüsse nicht aufheben. Bestehende alte Queueeinträge und Suchergebnisse unterliegen derselben Regel.

## Geprüfter Stand

Offlineprüfungen decken Tagesdaten, Farbcode-Erhalt, Druckfelder, Berechtigungen, parallele Fortschrittsmeldungen und die Importquellen ab. Die lokale Avatar-Vorschau hat eine reine Avatar-Startansicht und Menüseiten mit Textdownload. Echte iPhone-Audio-Latenz und Unterbrechung müssen weiterhin am Gerät geprüft werden.


## Bestellverwaltung

`/admin/assistent-bestellungen` verwaltet Lieferantenkontakte, positive Bruttokostenrahmen und eindeutige Bestellungen. Rechnungskontakte beginnen ungeprüft; Änderungen heben eine alte Bestätigung auf. Dringend wird sofort übergeben, sonst Montag 12 Uhr Europe/Berlin, getrennt nach Lieferant/Empfänger. Versandstatus unterscheidet angenommen, fehlgeschlagen und unklar; unklarer SMTP-Versand wird nicht automatisch wiederholt.

Standardmäßig bleiben `ASSISTANT_ORDER_SEND_ENABLED` und `ASSISTANT_ORDER_WORKER_ENABLED` aus. Für echten Betrieb braucht es das bestehende konfigurierte Postfach (`MAILBOX_SEND_ENABLED`), einen positiven Kostenrahmen, bestätigte Kontakte und den dedizierten Prozess `flask --app app werkstatt-bestellungen-worker`. Web und Worker müssen dieselbe dauerhafte Datenbank und denselben absoluten `MAILBOX_OUTBOX_DIR` nutzen. Die Oberfläche prüft den Worker-Heartbeat und behauptet keinen aktiven Montagsversand, solange er fehlt. Es gibt noch keinen Mitarbeiter-/Avatar-Schreibadapter für diese Admin-Oberfläche.
