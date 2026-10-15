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

Prüfung des aktuellen Zwischenstands: vier isolierte Seiten-/Routen-/Linktests bestanden. Gemeinsame Integration, Sicherungs-Roundtrip, bestehende Backupfixtures sowie Desktop-/Mobilprüfung sind zum Zeitpunkt dieses Zwischenstands noch offen. Der Server darf nur mit synthetischen Daten und auf Loopback als lokale Vorschau laufen.

## Bildrecherche

- https://karosseriezentrum.de/kfz-werkstatt/ – klare Trennung der Leistungen mit einzelnen Arbeitsfotos; nur Gestaltungsreferenz.
- https://hoffmann-zur.de/#leistungen – Nahaufnahmen der Tätigkeit; nur Gestaltungsreferenz.
- https://www.pexels.com/license/ – Nutzung für Webseiten erlaubt; keine implizite Empfehlung durch abgebildete Personen/Marken.

Der Nutzer wünscht hochwertigere Motive als die ersten Referenzen. Frühere KI-Hallenbilder und die Vierer-Collage sind nicht final freigegeben. Fremde Werkstatt- oder Mitarbeiterbilder dürfen nicht als dokumentarische Aufnahmen dieses Betriebs beschrieben werden. Kein Medienkauf erfolgt.
