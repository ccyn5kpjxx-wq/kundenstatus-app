# Geschäftspartner und Kundenflotten – lokale Vorschau

Stand 16.10.2026. Keine Veröffentlichung und kein Kundenversand beauftragt.

## Umfang

- Öffentliche Geschäftspartnerseite `/geschaeftspartner`: Firmenflotten und feste Karosserie-/Lackpartnerschaft für Autohäuser. Margita Höfer als Ansprechpartnerin; Konditionen werden persönlich vereinbart.
- Homepage: Verweis auf die Geschäftspartnerseite. Auf beiden Seiten ist eine erste vollständige lokale Übersicht aus zwölf gleich großen Arbeitsfoto-Kacheln eingebaut. Zusätzlich wird ein ruhiges Hintergrundvideo geprüft.
- Team: Christopher als Geschäftsführer, Meister und Fahrzeuglackierer; Ümit ohne erfundenes Foto als erfahrener Allrounder für Lackiervorbereitung und Lackierung.
- Interne Kundenflotten: Firmen, Fahrzeuge, Fahrer, Kontaktfreigaben, Kilometerstände, zeit-/kilometerabhängige Wartung, Reifen und Einlagerung, Terminwünsche, Mobilitätsbedarf, private Konditionen und Historie.
- Kompaktes Betriebs-Cockpit mit sichtbaren Warnungen und gezielt aufklappbaren Detailbereichen.

## Grenzen der Vorschau

`/kunden-app/vorschau` ist eine ausschließlich lokale Demonstration mit synthetischem Fahrzeug. Sie erteilt keine Kundenzugänge, verschickt keine Nachrichten und speichert keine externen Schadenmeldungen. Fotos werden dort nur lokal zur Vorschau angezeigt. Produktive Kundenanmeldung, Mandantentrennung für Fahrerzugriffe, Push/WhatsApp, Terminbuchung, App-Store-Veröffentlichung und automatischer Dokumentimport benötigen eigene anschließende Implementierung und Prüfung.

Erinnerungen stützen sich auf tatsächlich hinterlegte Herstellerintervalle und aktuelle Kilometerstände. Es werden keine allgemeinen Serviceintervalle als fahrzeugbezogene Herstellervorgaben ausgegeben. Eine Schadenmeldung ist keine Zusage einer Versicherungsleistung; Mietfahrzeugbedarf ist keine bestätigte Reservierung.

## Integration und Sicherheit

Das Modul `werkstatt_kundenflotten.py` stellt Schema, Blueprint und eine geordnete Liste seiner Sicherungstabellen bereit. `app.py` registriert es, übernimmt die Tabellen und die Featurekennung in Backups und verhindert, dass Sicherungen ohne neu eingeführte Tabellen bestehende Flottendaten still löschen. Modul-Schreiboperationen und Wiederherstellungen müssen denselben Betriebslock verwenden.

Die gemeinsame Fassung wird lokal in eigenen Folgebranches weiterentwickelt; der aktuelle geprüfte Stand liegt in `codex/fuhrpark-qualitaetspruefung`. Der Vorschau-Server läuft ausschließlich mit synthetischen Daten auf `http://127.0.0.1:5152`. Dort sind `/admin/kundenflotten`, `/kunden-app/vorschau`, `/admin/cockpit` und `/geschaeftspartner` erreichbar. Diese Vorschau verwendet einen automatisch angemeldeten lokalen Test-Admin; diese Einstellung gehört nicht in einen produktiven Start.

Bestanden: 52 abschließende Modul-, Integrations-, Geschäftspartner- und Cockpit-Tests, der Homepage-Prüflauf, Smoke-Test und Flow-Test. Zusätzlich bestanden 44 bestehende Restore-Regressionen sowie die bisherigen Bestellvergleich-, Liefer- und Bestellbestätigungs-Backupprüfungen. JSON- und SQLite-Roundtrips erhalten sämtliche acht Flottentabellen. Ein getrennter Review hat auch den tatsächlichen HTTP-Import mit SQLite-Dateiaustausch geprüft. PostgreSQL wurde am Adapter geprüft, nicht an einem echten PostgreSQL-Server.

Desktop und Mobilansichten wurden im Browser kontrolliert. Der Reifen-Terminwunsch lässt sich in der Kunden-App-Vorschau inklusive Abholung und Ersatzwagen durchspielen; es erfolgt keine Speicherung oder Übermittlung. Dringende Wartungen stehen in der Fahrzeugakte zuerst, Datum und Kilometer sind deutsch formatiert.

Noch auszubauen: mehrere Reifensätze und strukturierte Zustands-/Profil-/DOT-Dokumentation, bestätigte Termine mit Kalenderübergabe, echte Ersatzwagenreservierung, Fahrerzugang mit Firmenfreigaben, Schadenoriginale und Dokumentimport sowie freigegebene Benachrichtigungen. Der aktuelle Reifenbereich speichert einen Reifensatz je Fahrzeug. Ein Terminwunsch ist noch kein bestätigter Werkstatttermin.

## Bildrecherche

- https://karosseriezentrum.de/kfz-werkstatt/ – klare Trennung der Leistungen mit einzelnen Arbeitsfotos; nur Gestaltungsreferenz.
- https://hoffmann-zur.de/#leistungen – Nahaufnahmen der Tätigkeit; nur Gestaltungsreferenz.
- https://www.pexels.com/license/ – Nutzung für Webseiten erlaubt; keine implizite Empfehlung durch abgebildete Personen/Marken.

Der Nutzer wünscht hochwertigere Motive als die ersten Referenzen. Frühere KI-Hallenbilder und die Vierer-Collage sind nicht final freigegeben. Fremde Werkstatt- oder Mitarbeiterbilder dürfen nicht als dokumentarische Aufnahmen dieses Betriebs beschrieben werden. Kein Medienkauf erfolgt.

Die abgelehnten neuen Motive sind aus der lokalen Homepage entfernt. Die Titelgestaltung bleibt vorerst erhalten. Unter `#unsere-arbeiten` zeigen Homepage und Geschäftspartnerseite dieselbe Übersicht mit zwölf gleich großen Fotokacheln. Jede Kachel zeigt ein eigenes konkretes Motiv mit einer HTML-Überschrift unter dem Foto und öffnet das vollständige Bild auf Wunsch. Desktop zeigt drei Spalten, mittlere Breiten zwei und Handys eine.

Enthalten: Reifenmontage/Radwechsel, Bremsenservice, Ölservice, Inspektion, Lackierung, Unfallinstandsetzung, Smart Repair, Dellen/Hagel, Autoglas, Reifeneinlagerung, Schadenabwicklung und Fahrzeugübergabe/Ersatzmobilität. Neun frei lizenzierte Pexels-Motive ergänzen drei vorhandene Arbeitsfotos. Autoren, Detailseiten, Original-URLs und Lizenznachweise stehen in `static/homepage/leistungsfotos/quellen.json`. Alle heruntergeladenen Motive wurden visuell geprüft. Keine falschen Teamnamen oder dokumentarischen Behauptungen über fremde Werkstattbilder. Die Schadenabwicklungskachel illustriert Unterlagenarbeit, die Mobilitätskachel eine Schlüsselübergabe; sie behaupten keine abgebildete konkrete Versicherung oder Transportleistung.

### Kuratierte, visuell geprüfte Kandidaten

| Einsatz | Quelle | Einordnung |
| --- | --- | --- |
| Bremsenarbeit | https://www.pexels.com/photo/a-close-up-shot-of-a-vehicle-disc-brake-6870299/ | Nahaufnahme, konkrete Arbeit, ruhiger Hintergrund |
| Reifeneinlagerung | https://www.pexels.com/photo/stack-of-rubber-tires-3806252/ | Klar erkennbare Lagerregale; sachliches Ergänzungsmotiv |
| Fahrzeugaufnahme | https://www.pexels.com/photo/person-writing-on-a-clipboard-inside-the-vehicle-8943323/ | Hände mit Klemmbrett; zeigt keine belegte Versicherungsabwicklung |
| Ölservice | https://www.pexels.com/photo/pouring-oil-to-engine-13065690/ | Gute Nahaufnahme, sichtbares Markenetikett; nicht unverändert ausgewählt |
| Hintergrundvideo: Diagnose | https://elements.envato.com/hands-of-mechanic-working-in-auto-repair-shop-29Q8W53 | 17 Sekunden, 4K; Hände und Diagnosegerät, kostenpflichtige Projektlizenz nötig |
| Hintergrundvideo: Karosserievorbereitung | https://elements.envato.com/professional-repairman-in-garage-works-with-automo-8CXLTHU | 31 Sekunden, Full HD; Stoßfänger präzise abkleben, kostenpflichtige Projektlizenz nötig |

Vorschlag für den Kopfbereich: 15–20 Sekunden ruhige, stumme Montage mit Pause-Schalter, Standbild bei reduzierter Bewegung und für sparsame mobile Nutzung. Die Montage wurde noch nicht erstellt oder eingebaut. Envato-Lizenzbasis: https://help.elements.envato.com/hc/en-us/articles/360000628966-Envato-Elements-License . Keine kostenpflichtige Lizenz gekauft; nur die frei lizenzierten Pexels-Fotos für die Kacheln wurden heruntergeladen.

Die Galerie ist zur lokalen Bildabnahme bereit. Bei der nächsten Auswahlrunde verdienen größere Unfall-/Richtarbeiten, Ölwechsel statt Ölstandprüfung und Autoglas in einer Halle besonders passende eigene Arbeitsaufnahmen. Die aktuelle Karosseriekachel zeigt Stoßfängermontage, die Ölservicekachel eine Ölstandprüfung und die Glaskachel einen Scheibenkleberauftrag im Freien. Der Videohintergrund bleibt eine separate offene Medienentscheidung. Keine Veröffentlichung oder Kundenkommunikation.


## Lokale Gestaltung: Kunden-App und Fahrzeugvorschau

Die Kunden-App ist überwiegend weiß gestaltet: helle Fahrzeugkarte mit Foto, vier gleichwertige Serviceaktionen, klare Linien-Symbole und rote Hauptaktionen. Reifenwunsch, Service, Schaden und Panne verwenden die unveränderten lokalen Demo-Dialoge. Margita ist als persönliche Ansprechpartnerin mit dem vorhandenen Portrait eingebunden. Textaktionen und Schließen haben ausreichend große Treffflächen; Fokus und Escape bleiben erhalten.

Auf der Geschäftspartnerseite führt die Fahrzeugkarte mit Beispielfoto zur Fahreransicht. Foto und beschrifteter Button sind anklickbar, wenn die Demo ausdrücklich aktiviert ist und der Zugriff über Loopback erfolgt. Andernfalls zeigt die Seite eine Kontaktaktion; ein nicht erreichbarer Demo-Link wird nicht veröffentlicht. Die bestehende Zusammenarbeit über das Partnerportal bleibt davon unabhängig.

Die interne Fahrzeugakte fasst Fahrer, Kilometerstand, offene Wartungsvorgaben und Reifenlager oben zusammen. Es werden ausschließlich bereits erfasste Daten dargestellt; ein Marketing-Beispielfoto wird keiner echten Fahrzeugakte zugewiesen.

Geprüft: 37 Modul-/Seitentests und bestehender Homepage-Prüflauf, Demo-Link bei ausgeschalteter Konfiguration, Netzwerkzugriff sowie IPv4-/IPv6-Loopback. Desktop und 390-Pixel-Ansichten ohne Überlauf, Reifenwunsch mit Abholung/Ersatzwagen durchgespielt, alle vier Hauptdialoge und Rückkehrfokus nach Escape geprüft. Getrennter read-only Review ohne Blocker. Nur lokale Vorschau, kein Push/Deployment und keine Benachrichtigung.


### Zielgruppenfotos und direkter Pannenanruf

Die drei Einstiege unter dem Homepage-Titel erhalten gleich große Fotos: Privatkundin mit Pkw/Schlüssel, Gespräch im Autohaus und geordnete Reihen von Firmenfahrzeugen. Es sind illustrative Stockfotos; es werden keine fremden Personen als eigene Kunden oder Mitarbeiter bezeichnet. Die drei bestehenden Anfrage-/Partnerziele bleiben erhalten. Quellen, Autoren, Lizenz, Bildmaße und Hashes stehen in `static/homepage/zielgruppen/quellen.json`.

Der lokale Pannendialog zeigt nach dem unveränderten Sicherheits- und Notrufhinweis eine klare Aufforderung, online zu melden und zusätzlich die Werkstatt anzurufen. Der manuell betätigte Telefonlink verwendet die auf Homepage und Impressum bestätigte Hauptnummer +49 1522 7706694. Die Online-Demo versendet nichts; die Rufnummer ist als echter Anrufweg erkennbar. Es gibt keine automatische Anwahl oder Zusage einer sofortigen Abholung. Bei der Prüfung wurde kein Anruf ausgelöst.

Geprüft: fünf bestehende Seiten-/Linktests, Homepage-Prüflauf, Auslieferung aller drei Bilddateien sowie Desktop/390px ohne Überlauf. Die Anrufaktion ist beim Öffnen des mobilen Pannendialogs sichtbar. Unabhängiger read-only Review ohne Blocker. Weiterhin ausschließlich lokale Vorschau.


## Gesamtprüfung und Nachbesserungen vom 16.10.2026

Die lokale Fassung wurde funktional und auf kleinen Displays geprüft. Behoben wurden:

- Firmenanlage: Ein zufälliger Formularbeleg dient als dauerhafte Firmen-ID. Gleichzeitige bzw. wiederholte Übermittlung desselben Formulars erzeugt genau eine Firma, abweichende Inhalte überschreiben keinen Datensatz. Keine zusätzliche Tabelle; bestehende Backups erhalten die ID.
- Fahrzeuganlage: übliche Kennzeichentrenner vereinheitlicht, ohne die Ortskennungsgrenze zu verlieren. `B-AB 123` und `BA-B 123` bleiben verschiedene Kennzeichen; gleiche Schreibvarianten werden abgefangen. Mehrdeutige kompakte Alteinträge werden vorsichtig zur Prüfung gemeldet.
- Terminwünsche: neue Termine in der Vergangenheit werden im Server abgelehnt; die Demo setzt zusätzlich den lokalen Kalendertag als frühestes Wunschdatum. Bereits gespeicherte Formularbelege verhindern weiterhin doppelte Anlage.
- Wartung: unmögliche Folgeziele außerhalb der unterstützten Eingabegrenzen werden vor Änderungen an Plan, Fahrzeugstand oder Historie abgelehnt.
- Pannenmeldung: Anrufaktion bekommt den Fokus, der Dialog bleibt oben. Auf 320 × 568 bleiben Sicherheits-/112-Hinweis und Telefonnummer sofort sichtbar; keine automatische Bildschirmtastatur am Standortfeld.
- Navigation: auf der Geschäftspartnerseite sind auch mobil sämtliche Einstiege erreichbar. Escape auf der Startseite verschiebt den Fokus nur bei tatsächlich geöffnetem Menü.
- Formulardarstellung: CSRF-Felder werden mit dem tatsächlichen Portal-Helper als unsichtbare Felder ausgegeben. Sichtbar dargestellter HTML-Quelltext ist entfernt; übrige Eingaben bleiben escaped. Ein neuer Integrationstest verwendet die echte Portalvorlage, statt den Helper durch Markup zu ersetzen.

Abschlussprüfung: 92 automatisierte Modul-, Integrations-, Cockpit- und Seitentests bestanden, darunter acht neue gezielte Regressionen. Zusätzlich bestanden der bestehende Homepage-Prüflauf, Smoke-Test und Angebots-/Status-/Archivierungs-Flow-Test. Syntax- und Diffprüfung sauber. Drei getrennte read-only Reviews ohne offene Blocker. Ein zusätzlicher Link-/Asset-/Ankercheck über 60 lokale Ziele blieb ohne Fehler.

Im Browser wurden mit ausschließlich synthetischen Daten Firma, Fahrzeug, Fahrer, Wartungsplan, Abschluss mit Folgefälligkeit, Reifensatz/Lagerort und Terminwunsch mit Abholung/Ersatzwagen durchgespielt. Der Wartungsabschluss aktualisierte 15.000 auf 15.010 km und erzeugte das erwartete Folgeziel von 30.010 km bzw. 16.10.2027. Foto-Schadenmeldung: ungültiger Dateityp sichtbar abgelehnt, gültiger Ersatz zeigt Vorschau und lässt den lokalen Demoabschluss zu. Auf Handybreite kein horizontaler Überlauf bei Startseite, Geschäftspartnerseite, Kunden-App, Fahrzeugakte und Cockpit; aufklappbare Cockpitbereiche funktionieren.

Es wurden keine echten Betriebsdaten verändert, keine Nachrichten oder Bestellungen versendet und keine Anrufe ausgelöst. Es gab keinen Push oder Deployment. Produktive Fahrerzugänge, Benachrichtigungen, verbindliche Terminbestätigung, Ersatzwagenreservierung und mehrere strukturierte Reifensätze bleiben ausdrücklich eigenständige Ausbauschritte. PostgreSQL wurde weiterhin nur über den vorhandenen Adaptertest geprüft, nicht gegen einen echten Server.


### Lokaler visueller Feinschliff (16.10.2026)

Die zwölf gleichwertigen Leistungskacheln verwenden neun neu ausgewählte reale Arbeitsfotos. Reifenmontage an der Maschine, Bremsenarbeit, Ölservice, Batterieprüfung, Lackierung, Karosseriearbeit, Politur, Dellenbearbeitung und digitale Dokumentation sind konkret erkennbar. Glas, Reifenlager und Schlüsselübergabe bleiben vorerst bei den bereits geprüften Motiven. Originaldateien und Quellen-/Lizenznachweise liegen unter `static/homepage/leistungsfotos-v2/`; die drei erhaltenen Motive sind weiterhin unter `static/homepage/leistungsfotos/quellen.json` belegt. Stockbilder illustrieren Leistungen und werden nicht als eigene Mitarbeiter oder Betriebsräume bezeichnet. Keine eingebrannte Schrift und keine neue KI-Bilderzeugung.

Die Kunden-App erhält eine größere Fahrzeugdarstellung, lesbarere Metadaten, Fahrzeugdetails über das bestehende Beispielprofil, eine deutlichere Terminkarte, vier gleichwertige Schnellzugriffe und eine schwebende Navigation. Margitas Kontaktfoto ist auch mobil sichtbar. Bestehende Demo-Formulare, Sicherheits-/Anrufhinweise und lokale Zugangssperren bleiben erhalten. Die ausdrücklich akzeptierten Zielgruppenkarten und Startseitenkategorien sind unverändert. Alles bleibt eine lokale Vorschau ohne Kundenanmeldung, Versand oder Veröffentlichung.


### Lokale Positionierung als Werkstatt und Fuhrparkmanagement

Leitidee: „Wir managen Ihren Fuhrpark. Damit Sie sich auf Ihr Geschäft konzentrieren können.“ Der Nutzen wird auf der Startseite in einem eigenen Bereich und auf der Geschäftspartnerseite im Hero konkret erklärt: Fälligkeiten im Blick behalten, mit Fahrern abstimmen, Werkstatttermine organisieren und Arbeiten im eigenen Betrieb ausführen. Die Flottenkachel führt weiterhin in den bestehenden Firmenbereich; Privatkunden und Autohäuser bleiben sichtbar. Leistungen, Konditionen und Kostenfreigaben werden vereinbart; Abholung und Ersatzwagen nach Vereinbarung/Verfügbarkeit. Es werden keine pauschale Kostenübernahme, unbegrenzte Verfügbarkeit oder bereits produktive Fahrerbenachrichtigungen behauptet. Bestehende App und Fotos bleiben unverändert, kein Versand und keine Veröffentlichung.


### HU/AU mit externem Prüfpartner

HU/AU wird als organisierte Leistung mit externem Prüfpartner auf Startseite, Geschäftspartnerseite und beim Fahrzeugcheck genannt. Die Prüfung wird nicht als eigene amtliche Prüfbefugnis des Betriebs dargestellt. Primäre Hintergrundinformation: https://www.tuvsud.com/de-de/branchen/mobilitaet-und-automotive/hauptuntersuchung . Eine konkrete Prüforganisation wird ohne Namensbestätigung nicht genannt.

Die interne Fahrzeugakte hat eine eigene Erfassung für HU/AU-Prüfmonat und belegte Quelle. Der Prüfmonat wird als Monatsende gespeichert, inklusive Schaltjahren. HU/AU erhält kein Kilometerziel oder pauschales Wiederholungsintervall. Ein weiterer offener HU/AU-Plan wird verhindert; Wiederholung desselben Formulars bleibt idempotent. Bestehende HU-Pläne bleiben lesbar. Nach bestandener Prüfung wird der nächste Monat ausdrücklich aus dem neuen Prüfbericht erfasst; das Backend verlangt Bestätigung, Bericht und eine Frist nach dem Prüftag. Nicht bestandene Prüfungen verschieben die Fälligkeit nicht. Keine neuen Tabellen oder Spalten.

Die bestehende interne Vorwarnung greift 30 Tage vor dem gespeicherten Datum. Die Kunden-App zeigt ein synthetisches HU/AU-Erinnerungsbeispiel mit Prüfmonat, Nachricht, Fahrzeugprofil und Terminwunsch inkl. Abholung/Ersatzwagen. Dies ist eine lokale Gestaltungsvorschau, keine produktive Erinnerung. Für echten Kundenversand fehlen weiterhin produktive Fahrerzugänge, Freigaben und ein geprüfter Benachrichtigungsdienst. Es werden weder Nachrichten noch Terminbuchungen ausgelöst.


### Aktueller Werkstattfall und gemeinsame Statusquelle (lokal)

Die Kunden-App-Vorschau zeigt eine eigene Fallkarte mit Anlass, Statussymbol, fünf Statusschritten und letztem Auftragsupdate. Die allgemeine Vorschau bleibt vollständig synthetisch. Die echte lokale Kopplung erfolgt in der internen Fahrzeugakte durch ausdrückliche Zuordnung eines bestehenden Werkstattauftrags; Kennzeichen oder FIN müssen passen. Widersprechende FINs werden abgewiesen, der Fahrzeugabgleich wird bei jedem Abruf wiederholt. Ein Auftrag kann nur einer Fahrzeugakte zugeordnet werden. Die Zuordnung verändert weder den Werkstattstatus noch Termine oder Fälligkeiten.

Die Fallansicht liest direkt aus `auftraege`: erfasst, eingeplant, in Arbeit, Arbeiten abgeschlossen und zurückgegeben. Mitarbeiter verwenden ihren vorhandenen persönlichen Auftragsbereich mit serverseitig gespeicherter Aktionsbestätigung. Dieselben Änderungen werden in der Fahrzeugakte und Kundenansicht sichtbar, ohne zweite Statuspflege oder Benachrichtigungsroutine. Lackphasen werden ausschließlich bei Karosserie/Lack-Fällen eingeblendet. Eine Fertigmeldung erzeugt keine zusätzliche Abholfreigabe. Ein Wartungsplan wird durch die Fertigmeldung nicht automatisch abgeschlossen, da etwa HU/AU einen belegten Prüfbericht verlangt.

`/admin/kundenflotten/firma/<firma>/fahrzeug/<fahrzeug>/kundenansicht` bleibt admin- und firmengebunden. `/kunden-app/fall/<token>` verwendet ausschließlich einen bereits vorhandenen aktiven Auftragstoken und zeigt nur diesen einzelnen Fall; der zugehörige JSON-Abruf enthält keine internen Notizen, Preise, Fahrer-/Firmendaten oder weiteren Flottenfahrzeuge. Ein Auftragstoken gewährt keinen Flottenzugang. Gesperrte, unbekannte, archivierte, entfernte oder nicht mehr passende Fälle werden nicht angezeigt. Privates Caching, Referrer-Unterdrückung und Suchmaschinensperre sind gesetzt. Die Seite aktualisiert alle 30 Sekunden, beim Wiederöffnen und per Schaltfläche; bei Verbindungsfehlern bleibt der letzte Stand ausdrücklich als solcher erkennbar.

Neue Verknüpfungen liegen in `kundenflotten_auftraege` und gehören zum JSON-/SQLite-Backup. Das eigene Schema-Merkmal `kundenflotten_auftraege_v1` lässt ältere Sicherungen mit den ursprünglichen acht Flottentabellen weiterhin zu, wenn keine vorhandenen Verknüpfungen verloren gehen. Bestehende Verknüpfungen blockieren ein unvollständiges Restore vor der ersten Löschung.

Die Entwicklung und Browserprüfung erfolgen ausschließlich auf einer wegwerfbaren lokalen Datenbank mit Musterfirma und Testmitarbeiter. Produktive Kundenanmeldung, Zugangserteilung, Push-Mitteilungen und Veröffentlichung sind weiterhin nicht aktiviert. Bereits vorhandene produktive Bearer müssen wie bisher ausschließlich über den bestehenden Admin-Freigabeprozess verwaltet werden.


### Firmen-Flottenzugang und gemeinsame Belegablage (lokal)

Ein gesonderter Firmenzugang unter `/flotte/anmelden` führt zur Übersicht sämtlicher zugeordneter Fahrzeuge. Kennzeichen-/Modellsuche, aktuelle Werkstattfälle und fällige Arbeiten helfen bei der Orientierung. Die Firmenansicht erhält ausschließlich eine Freigabeliste von Daten; interne Konditionen, Quellen, Historien, Fahrer-/Mitarbeiterrechte und Auftrags-Bearer werden nicht übernommen. Fahrzeugdetails enthalten die gleichen Auftragstatuswerte, Fälligkeiten und die für die Firma sichtbaren Belege. Die Übersicht wird durch Öffnen bzw. „Übersicht aktualisieren“ neu aus der Datenbank gelesen. Die bestehende Einzel-Kundenfallansicht mit automatischem Statusabruf bleibt erhalten.

Pro Firma gibt es zunächst einen gemeinsamen Firmenzugang mit eindeutigem Benutzernamen und sicherem Scrypt-Passworthash. Nur ein Admin kann diesen in der Firmenakte einrichten, neu setzen oder sperren. Passwortänderung und Sperre erhöhen die Zugangsversion und beenden bestehende Sitzungen bei der nächsten Anfrage. Jede Anfrage prüft aktive Firma/Zugangsversion; Sitzungen verfallen nach acht Stunden. Login besitzt CSRF-Schutz, Fehlversuchslimit und Sitzungsrotation und entfernt gemerkte Admin-/Partneranmeldungen. Mitarbeiterzugänge und einzelne Fahrzeug-Statuslinks berechtigen niemals zur Firmenflotte. Keine Einladung oder Passwort-Mail wird erzeugt.

Kunden können beim jeweiligen Auto Tankbelege, Rechnungen, Reparaturbelege und sonstige Fahrzeugunterlagen fotografieren oder auswählen. Optionaler Titel und Bruttobetrag sowie Belegdatum strukturieren die Ablage. Unterstützt werden JPEG/PNG/WebP und PDF, ein Original je Upload bis acht MB. Die Kameraauswahl nutzt die Rückkamera auf unterstützten Mobilbrowsern; ein echter Kamera-Hardwaretest wurde nicht durchgeführt. Auswahl/Upload am Browser und Kamera-Formularfeld am Server wurden geprüft. Dateien werden nach Signatur, tatsächlichem Bild-/PDF-Inhalt und Größe validiert. Beschädigte, verschlüsselte oder zu große Dokumente werden abgewiesen. Es gibt weder OCR noch automatische Auftrags-/Fahrzeugdatenübernahme oder Buchung.

Originalbytes, Dateiname, kanonischer MIME-Typ, Größe, SHA256, Quelle Firma/Werkstatt, Sichtbarkeit und Formularbeleg werden atomar unter der gemeinsamen Original-/Restore-Sperre gespeichert. Wiederholungen desselben Formulars schreiben keinen zweiten Beleg. Ein Kunde kann ausschließlich für seine Fahrzeuge hochladen und deren freigegebene Originale abrufen. Mitarbeiter pflegen Werkstattunterlagen über die interne Fahrzeugakte: ohne ausdrückliches Sichtbarkeitshäkchen bleibt ein neuer Werkstattbeleg intern. Downloads sind zugangsgeschützt, mit Integritätsprüfung, ohne öffentliches Dateiverzeichnis, Browser-Cache oder Referrer und ausschließlich als Original-Anhang.

Neue Tabellen `kundenflotten_zugaenge` und `kundenflotten_belege` deklarieren `kundenflotten_portal_v1`. Belegoriginale werden als Binärdateien in bestehenden ZIP-Backups externalisiert und beim SQLite-/JSON-Restore wiederhergestellt. Fehlende Originale und ältere Sicherungen, die aktuelle Belege verlieren, Zugangssperren zurückrollen oder Fahrzeuge einer anderen Firma zuweisen würden, werden vor der ersten Löschung abgewiesen. SQLite-Quellen werden ausschließlich lesend geöffnet. Bestehende 8-/9-Tabellen-Sicherungen bleiben möglich, wenn keine neueren Daten verloren gehen.

Standardmäßig ist das Portal ausgeschaltet (`FLOTTENPORTAL_ENABLED=0`). Die lokale wegwerfbare Vorschau aktiviert ausschließlich eine Musterfirma und fiktive Zugangsdaten. Es wurden keine produktiven Firmenkonten, Kundendaten, Einladungen, Benachrichtigungen oder Veröffentlichungen aktiviert. Separate persönliche Fahrerrechte, Buchhaltungs-/Lexwareübernahme, Belegexport und mehrere Nutzer pro Firma sind weitere Ausbauschritte.
