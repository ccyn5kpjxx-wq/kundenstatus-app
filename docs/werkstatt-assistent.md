# Werkstatt-Cockpit API v1

Diese Erweiterung stellt eine authentifizierte Lese-API für den Werkstattassistenten bereit. Keine Datenbankmigration, keine Änderung bestehender Aufträge und kein automatischer Versand.

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
