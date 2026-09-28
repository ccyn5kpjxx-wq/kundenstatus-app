# Werkstatt-Avatar

Die Startseite unter `/werkstatt/assistent` zeigt den Avatar und den Gesprächsstart.
Das Menü enthält **Heute wichtig**, **Heute fertig / raus**, **Kommt heute** und
**Lackierung & Farbcodes**. Die Übersicht lässt sich als Textdatei speichern.
Auftragsnummern sind die unveränderlichen internen IDs; externe Referenzen bleiben
separate Angaben. Im Admin-Auftrag führt **Werkstattzettel** zur Druckansicht.

## Zugang und Daten

Admins verwenden ihre bestehende Anmeldung. Mitarbeiter benötigen einen aktiven
Mitarbeiterdatensatz und einen persönlichen Zugang, den die Leitung unter
`/werkstatt/assistent/rechte` einrichtet. Die Mitarbeiterverwaltung verlinkt die
Seite unter **Avatar-Zugänge**. Im nativen Betrieb ist kein zusätzlicher gemeinsamer
Werkstatt-Code nötig; die persönliche Anmeldung öffnet ausschließlich den Avatar
und verleiht keine Admin- oder Werkstatt-Tafel-Sitzung. Rechte werden bei jedem
Zugriff erneut geprüft. Der Avatar liest die gemeinsame Cockpit-Datenquelle direkt;
ein zusätzlicher API-Schlüssel oder Listenimport ist dafür nicht erforderlich.

Für neue Zugänge sind Auftrags- und Artikel-Leserechte vorausgewählt, Dokumentation
und Einkaufsbudget bleiben auf null. Erst ein persönliches Passwort (mindestens
12 Zeichen) und **Persönlichen Zugang einrichten** aktivieren den Zugang. Bestehende Passwörter
bleiben bei leerem Passwortfeld erhalten. Die Leitung gibt jedem Mitarbeiter seine
eigene ID und sein eigenes Passwort über einen geschützten Weg weiter.

Die Produktionsintegration startet mit `ASSISTANT_READ_ONLY=true`. Änderungen,
Fotos, Fortschrittsmeldungen und Bestellungen über den Avatar sind gesperrt.
Diese Einstellung nicht abschalten, bevor die jeweiligen Schreibadapter vollständig
angebunden sind. Der Fortschrittsdienst und die Bestellverwaltung sind getrennte
Dienste; ihre Existenz aktiviert keinen Versand im Avatar.

## Sprache und iPhone

Sprache benötigt den serverseitigen `OPENAI_API_KEY` und HTTPS. Gesprächsdaten und
die benötigten Auftragsdaten werden zur Sprachverarbeitung an OpenAI übertragen.
Der Sprachmodus startet durch einen bewussten Tipp. Er endet beim Sperren oder
Verlassen der Seite. Unterbrechen ist im Sprachablauf vorgesehen; tatsächliche
Latenz und Mikrofonverhalten müssen auf den verwendeten iPhones geprüft werden.
Für ein App-Symbol kann die HTTPS-Seite in Safari zum Home-Bildschirm hinzugefügt
werden. Die Seite hört nicht im Hintergrund zu.

## Tages- und Lackierübersicht

Der Tag richtet sich nach Europe/Berlin. Anlieferung, Werkstatt-Abholung,
Fertigstellung, Rückbringung und Kundenabholung sind unterschiedliche Ereignisse.
Fehlende Uhrzeiten bleiben als fehlend sichtbar. Die Lackierübersicht verwendet
gespeicherte Lackangaben und Fertigfristen; es gibt noch keinen eigenständigen
Kabinen- oder Personaleinsatzplan. Ein Fertigtermin ist kein Lackiertermin.

## Lieferantenrechnungen und Artikel

`/admin/assistent-artikel` verwaltet Rechnungsvorschläge. Nur freigegebene
Materiallieferanten werden ausgelesen; unbekannte Lieferanten benötigen Zuordnung.
Bank-, Versicherungs-, Steuer- und vergleichbare Belege sind ausgeschlossen.
Rechnungspositionen bleiben ungeprüfte Vorschläge, auch wenn der Preis rechnerisch
plausibel ist. Der Topcolor-Tabellenleser erhält Größen, Gebinde, Rabatte und
Seitenbezüge, trennt Gebühren und fügt mehrzeilige Positionen zusammen.

Einzelne abgeschlossene Belege können erneut eingereiht werden. Erst nach einer
neuen veröffentlichten Auslese werden die früheren Vorschläge dieses Belegs inaktiv;
sie bleiben für die Nachvollziehbarkeit gespeichert. Ein Import löst keine Bestellung aus.

## Bestellversand

Die Regel ist vorbereitet: dringend sofort, sonst je Lieferant gesammelt montags
um zwölf Uhr in Europe/Berlin. Der Versand bleibt standardmäßig inaktiv. Er benötigt
einen geprüften Lieferantenkontakt, eindeutig bestätigte Produkte/Mengen/Kosten,
einen konfigurierten Kostenrahmen, das verbundene Mailkonto und den laufenden
Bestellworker. Verwaltung: `/admin/assistent-bestellungen`. Versand und Worker haben
eigene Aktivierungsschalter. Ein unbekanntes Versandergebnis wird nicht blind wiederholt.

## Prüfung

Die Python-Suiten für Assistent, Cockpit-API, Artikelimport, Rechnungsauslese,
Topcolor-Positionen, Tagesbriefing und Backups sowie die JavaScript-Sprachtests
prüfen die Integration. Smoke- und Ablaufprüfung laufen mit isolierten Datenbanken
und ohne externe Netzverbindungen. Ein echter iPhone-Sprachtest ersetzt dies nicht.
