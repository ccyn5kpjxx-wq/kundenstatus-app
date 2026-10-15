# Geschäftspartner und Kundenflotten – lokale Vorschau

Stand 16.10.2026. Keine Veröffentlichung und kein Kundenversand beauftragt.

## Umfang

- Öffentliche Geschäftspartnerseite `/geschaeftspartner`: Firmenflotten und feste Karosserie-/Lackpartnerschaft für Autohäuser. Margita Höfer als Ansprechpartnerin; Konditionen werden persönlich vereinbart.
- Homepage: Verweis auf die Geschäftspartnerseite. Die endgültige Bildauswahl ist noch offen. Vorgabe: gleich große Kacheln je Leistung, echte Arbeit erkennbar, keine Schrift im Foto. Zusätzlich wird ein ruhiges Hintergrundvideo geprüft.
- Team: Christopher als Geschäftsführer, Meister und Fahrzeuglackierer; Ümit ohne erfundenes Foto als erfahrener Allrounder für Lackiervorbereitung und Lackierung.
- Interne Kundenflotten: Firmen, Fahrzeuge, Fahrer, Kontaktfreigaben, Kilometerstände, zeit-/kilometerabhängige Wartung, Reifen und Einlagerung, Terminwünsche, Mobilitätsbedarf, private Konditionen und Historie.
- Kompaktes Betriebs-Cockpit mit sichtbaren Warnungen und gezielt aufklappbaren Detailbereichen.

## Grenzen der Vorschau

`/kunden-app/vorschau` ist eine ausschließlich lokale Demonstration mit synthetischem Fahrzeug. Sie erteilt keine Kundenzugänge, verschickt keine Nachrichten und speichert keine externen Schadenmeldungen. Fotos werden dort nur lokal zur Vorschau angezeigt. Produktive Kundenanmeldung, Mandantentrennung für Fahrerzugriffe, Push/WhatsApp, Terminbuchung, App-Store-Veröffentlichung und automatischer Dokumentimport benötigen eigene anschließende Implementierung und Prüfung.

Erinnerungen stützen sich auf tatsächlich hinterlegte Herstellerintervalle und aktuelle Kilometerstände. Es werden keine allgemeinen Serviceintervalle als fahrzeugbezogene Herstellervorgaben ausgegeben. Eine Schadenmeldung ist keine Zusage einer Versicherungsleistung; Mietfahrzeugbedarf ist keine bestätigte Reservierung.

## Integration und Sicherheit

Das Modul `werkstatt_kundenflotten.py` stellt Schema, Blueprint und eine geordnete Liste seiner Sicherungstabellen bereit. `app.py` registriert es, übernimmt die Tabellen und die Featurekennung in Backups und verhindert, dass Sicherungen ohne neu eingeführte Tabellen bestehende Flottendaten still löschen. Modul-Schreiboperationen und Wiederherstellungen müssen denselben Betriebslock verwenden.

Die gemeinsame Fassung liegt lokal im Branch `feature/fuhrpark-lokale-integration`. Der Vorschau-Server läuft ausschließlich mit synthetischen Daten auf `http://127.0.0.1:5152`. Dort sind `/admin/kundenflotten`, `/kunden-app/vorschau`, `/admin/cockpit` und `/geschaeftspartner` erreichbar. Diese Vorschau verwendet einen automatisch angemeldeten lokalen Test-Admin; diese Einstellung gehört nicht in einen produktiven Start.

Bestanden: 52 abschließende Modul-, Integrations-, Geschäftspartner- und Cockpit-Tests, der Homepage-Prüflauf, Smoke-Test und Flow-Test. Zusätzlich bestanden 44 bestehende Restore-Regressionen sowie die bisherigen Bestellvergleich-, Liefer- und Bestellbestätigungs-Backupprüfungen. JSON- und SQLite-Roundtrips erhalten sämtliche acht Flottentabellen. Ein getrennter Review hat auch den tatsächlichen HTTP-Import mit SQLite-Dateiaustausch geprüft. PostgreSQL wurde am Adapter geprüft, nicht an einem echten PostgreSQL-Server.

Desktop und Mobilansichten wurden im Browser kontrolliert. Der Reifen-Terminwunsch lässt sich in der Kunden-App-Vorschau inklusive Abholung und Ersatzwagen durchspielen; es erfolgt keine Speicherung oder Übermittlung. Dringende Wartungen stehen in der Fahrzeugakte zuerst, Datum und Kilometer sind deutsch formatiert.

Noch auszubauen: mehrere Reifensätze und strukturierte Zustands-/Profil-/DOT-Dokumentation, bestätigte Termine mit Kalenderübergabe, echte Ersatzwagenreservierung, Fahrerzugang mit Firmenfreigaben, Schadenoriginale und Dokumentimport sowie freigegebene Benachrichtigungen. Der aktuelle Reifenbereich speichert einen Reifensatz je Fahrzeug. Ein Terminwunsch ist noch kein bestätigter Werkstatttermin.

## Bildrecherche

- https://karosseriezentrum.de/kfz-werkstatt/ – klare Trennung der Leistungen mit einzelnen Arbeitsfotos; nur Gestaltungsreferenz.
- https://hoffmann-zur.de/#leistungen – Nahaufnahmen der Tätigkeit; nur Gestaltungsreferenz.
- https://www.pexels.com/license/ – Nutzung für Webseiten erlaubt; keine implizite Empfehlung durch abgebildete Personen/Marken.

Der Nutzer wünscht hochwertigere Motive als die ersten Referenzen. Frühere KI-Hallenbilder und die Vierer-Collage sind nicht final freigegeben. Fremde Werkstatt- oder Mitarbeiterbilder dürfen nicht als dokumentarische Aufnahmen dieses Betriebs beschrieben werden. Kein Medienkauf erfolgt.

Die abgelehnten neuen Motive sind aus der lokalen Homepage entfernt. Bis zur vollständigen Auswahl bleiben dort die bisherigen Bildmotive und die bisherige Titelgestaltung erhalten. Die neue Übersicht soll gleich große Fotokacheln pro Tätigkeit bekommen; keine Leistung wird größer gewichtet.

### Kuratierte, visuell geprüfte Kandidaten

| Einsatz | Quelle | Einordnung |
| --- | --- | --- |
| Bremsenarbeit | https://www.pexels.com/photo/a-close-up-shot-of-a-vehicle-disc-brake-6870299/ | Nahaufnahme, konkrete Arbeit, ruhiger Hintergrund |
| Reifeneinlagerung | https://www.pexels.com/photo/stack-of-rubber-tires-3806252/ | Klar erkennbare Lagerregale; sachliches Ergänzungsmotiv |
| Fahrzeugaufnahme | https://www.pexels.com/photo/person-writing-on-a-clipboard-inside-the-vehicle-8943323/ | Hände mit Klemmbrett; zeigt keine belegte Versicherungsabwicklung |
| Ölservice | https://www.pexels.com/photo/pouring-oil-to-engine-13065690/ | Gute Nahaufnahme, sichtbares Markenetikett; nicht unverändert ausgewählt |
| Hintergrundvideo: Diagnose | https://elements.envato.com/hands-of-mechanic-working-in-auto-repair-shop-29Q8W53 | 17 Sekunden, 4K; Hände und Diagnosegerät, kostenpflichtige Projektlizenz nötig |
| Hintergrundvideo: Karosserievorbereitung | https://elements.envato.com/professional-repairman-in-garage-works-with-automo-8CXLTHU | 31 Sekunden, Full HD; Stoßfänger präzise abkleben, kostenpflichtige Projektlizenz nötig |

Vorschlag für den Kopfbereich: 15–20 Sekunden ruhige, stumme Montage mit Pause-Schalter, Standbild bei reduzierter Bewegung und für sparsame mobile Nutzung. Die Montage wurde noch nicht erstellt oder eingebaut. Envato-Lizenzbasis: https://help.elements.envato.com/hc/en-us/articles/360000628966-Envato-Elements-License . Keine Lizenz gekauft und keine Stockdateien heruntergeladen.

Weiterhin fehlen überzeugende Motive für Reifenmontage, Lackierkabine, größere Unfallinstandsetzung, Richtarbeit, Dellendrücken, Autoglas und Mobilitätsservice. Die Bildrecherche ist daher eine konkrete Auswahlgrundlage, noch keine vollständige neue Galerie.
