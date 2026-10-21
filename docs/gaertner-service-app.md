# Gärtner Service-App: erste lokale Version

Stand: 21.10.2026. Lokaler Pilot, keine Veröffentlichung oder echten Nachrichten.

## Lokale Vorführung

```powershell
python scripts/service_app_demo.py --port 5166
```

Dann `http://127.0.0.1:5166/demo` öffnen. Über die drei Beispielrollen lassen sich
Fahrer, Fuhrparkleitung und Werkstatt vorführen. Der öffentliche Einstieg ist
ohne Konto und Installation erreichbar. Die Rollenumschaltung existiert
ausschließlich im Demo-Skript und wird nicht in der normalen App registriert.

Das Skript importiert zuerst den vorhandenen isolierten Smoke-Test-Bootstrap:
neue temporäre Datenbank und Uploads, ausgeblendete lokale Secret-Dateien,
gesperrte ausgehende Sockets und gemockte Benachrichtigungsfunktionen. Es bindet
nur an 127.0.0.1, verwendet einen eigenen Sitzungscookie und einen zufälligen
Sitzungsschlüssel. Alle Namen, Firmen, Kontakte, Angebote und Belege sind
synthetisch. Ein Neustart setzt die Beispieldaten zurück. Kein Passwort oder
Einladungscode muss veröffentlicht werden. Die Demo ist kein Produktionsserver.

## Ein gemeinsamer Ablauf

1. `/service` bietet Reifenbuchung, Schadenmeldung mit Fotos und
   Inspektions-/Reparaturanfrage über die vorhandenen Homepage-Formulare.
   Ein Konto ist für diese ersten öffentlichen Anfragen nicht erforderlich.
2. Persönlich angemeldete Fahrer öffnen nur ihre ausdrücklich und aktuell
   zugeordneten Fahrzeuge. Signierte Formular-Kontexte binden die Meldung nach
   erneuter Rechteprüfung an die vorhandene Firmen-/Fahrzeugakte. Ein Firmenname,
   Kennzeichen oder eine manipulierte versteckte Formularangabe genügt nicht.
3. Es entsteht ein vorhandener Lead, kein paralleles Auftragssystem.
   `/admin/service` bündelt die zugeordneten Fälle und verlinkt das bestehende
   Angebot bzw. den Werkstattauftrag.
4. Die Werkstatt erfasst Auftraggeber, Kostenfreigebenden, Rechnungsempfänger
   samt vollständiger Adresse und Zahler getrennt. Selbstbeteiligung ist nur mit
   dokumentierter Quelle möglich. Vor-Ort-Zahlung und Firmenrechnung sind die
   verfügbaren Zahlungswege.
5. Die Fuhrparkleitung sieht nur ihre eigene Firma und gibt eine konkrete Fassung
   von Umfang, Netto-/Bruttobetrag, Steuer und Verantwortlichkeiten ausdrücklich
   frei. Reifenpositionen werden mit Menge, Einheit und Bruttopreis angezeigt.
   Fahrer können Kosten nicht freigeben. Bei extern gesteuerten Fällen muss die
   Werkstatt eine tatsächlich geprüfte schriftliche Freigabereferenz erfassen;
   eine Buchung ist keine Versichererfreigabe.
6. Die freigegebene Anfrage wird über die bestehende Lead-Übernahme genau einem
   Werkstattauftrag zugeordnet. Der Auftrag erscheint im vorhandenen Ablauf und
   der Werkstatt-Tafel. Die zentrale Statusfunktion und die Mitarbeiter-
   Fortschrittsfunktion prüfen die aktuelle Freigabe im jeweiligen Schreibvorgang.
7. Die vorhandene Rechnungsintegration verwendet bei gebundenen Servicefällen
   ausschließlich den aktuell freigegebenen Umfang und den expliziten Empfänger.
   Alte übergebene Auftragsobjekte oder Fahrerkontakte werden dafür nicht
   übernommen. Der Guard hält dieselbe DB-Transaktion bis zum Ende des vorhandenen
   Provideraufrufs. Die lokale Demo löst keinen solchen Aufruf aus.

Preis-, Umfangs-, Parteien- oder Reifenleistungsänderungen entwerten eine alte
Freigabe, auch wenn der Gesamtbetrag gleich bleibt. Nach der Erfassung tatsächlicher
Reifenleistungen muss die Werkstatt den Angebotsstand erneut abgleichen und die
zuständige Person erneut freigeben lassen. Bereits vorhandene ungebundene
Direktkunden-/Partneraufträge behalten ihren bisherigen Ablauf.

## Rechte und Dokumente

- Persönliche Fahrerzugänge: `/service/anmelden`; Werkstattverwaltung unter
  `/admin/service-zugaenge`. Einladungen werden nur vorbereitet, nicht versendet.
- Firmenleitung: vorhandener eigener Firmenzugang `/flotte/anmelden`, keine
  Übernahme von Werkstatt- oder Mitarbeiterkonten.
- Ein Fahrerwechsel widerruft alte Zuordnungen, Sitzungen und offene Einladungen.
  Auch schon versandte alte `/status/`- oder `/kunden-app/fall/`-Links können die
  aktuellen Rechte nicht umgehen. Neue Links führen in den geschützten Bereich.
- Originalbelege werden nur bei passender Firma/Fahrzeugzuordnung und expliziter
  Sichtbarkeit ausgeliefert. Inhalte werden nicht aus fremden Akten übernommen.
  Private Seiten/Downloads verwenden `private, no-store` und Suchmaschinen-Sperren.
- Vollständige neue Tabellen sind in Schema und Backup enthalten. Wiederherstellen
  darf bestehende Zugänge, Widerrufe und Freigabeereignisse nicht zurücksetzen.

## Reifen und Hinweise

Der vorhandene freie Stunden-Slot bleibt die maßgebliche Reifenreservierung;
es wird kein zweiter Kalender eingeführt. Datum und konkrete Uhrzeit werden am
Fahrzeug gezeigt. Die geschätzte Dauer bleibt ca. 30–45 Minuten mit Stundenpuffer.

Die Inbox berücksichtigt ausschließlich dokumentierte offene Servicepläne,
bestätigte Flottentermine und tatsächlich reservierte, explizit gebundene
Reifenbuchungen. Datums- und Kilometerhinweise verwenden die vorhandenen Regeln;
es werden keine Herstellerintervalle erfunden. Verschiebung, Storno, geänderte
Kontakt-/Einwilligungsdaten oder entzogene Zuordnung machen eine alte Vorschau
ungültig. Keine Erinnerung wird schon durch einen Seitenaufruf versendet.

Der optionale lokale Testtransport erfordert **beide** Flags `TESTING=True` und
`SERVICE_APP_TEST_TRANSPORT_ENABLED=True`. Er akzeptiert nur synthetische
`.invalid`-Empfänger und schreibt ausschließlich ein lokales Testprotokoll.

## Noch nicht extern eingerichtet

- Kein echter E-Mail-, WhatsApp- oder Push-Erinnerungsversand, kein Scheduler,
  keine behauptete Browser-Push-Zustellung. Dafür fehlen Provider, dokumentierte
  Einwilligungen, Zustellkonfiguration und ein kontrollierter Echtpilot.
- Keine Onlinezahlung, keine Kartendaten, keine Änderung an Stripe. Die bestehende
  Mietwagen-Zahlungsintegration ist nicht für Werkstattaufträge freigeschaltet.
- Keine automatische Versicherer-/SPN-/riparo-/MHC-Kostenfreigabe oder
  Teilebestellung. Fachliche Prüfung und dokumentierte Zuständigkeit bleiben nötig.
- Kein Produktiv-Pilot und keine realen Einladungen. Vor einem PostgreSQL-Pilot
  insbesondere konkurrierende Freigabe/Übernahme und Lockreihenfolge testen;
  die vorliegende Konkurrenzprüfung wurde mit SQLite durchgeführt.

## Normale App: standardmäßig ausgeschaltet

`SERVICE_APP_ENABLED`, `SERVICE_ACCESS_ENABLED` und `FLOTTENPORTAL_ENABLED` bleiben
im normalen Start standardmäßig aus. Der Service-Einstieg und die zusätzlichen
Links erscheinen nur bei entsprechender Konfiguration. Das Demo-Skript aktiviert
die Funktionen ausschließlich für seine isolierte temporäre Datenbank.

Gezielte Prüfungen liegen in `scripts/test_service_app.py`,
`test_servicezugang.py`, `test_serviceprozess.py`, `test_service_progress_guard.py`
und `test_erinnerungen.py`. Bestehende Firmenportal-, Flottenstatus-, Reifen- und
Kurzanfrage-Regressionen sind ergänzend vorgesehen. Sämtliche Prüfungen verwenden
synthetische Daten und gesperrte bzw. gemockte externe Verbindungen.
