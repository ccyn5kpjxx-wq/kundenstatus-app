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

## Figuren auswählen

Direkt oben auf `/werkstatt/assistent` öffnet **Avatar wählen** die Fabeltiere
Drache, Zauberfuchs, Einhorn, Phönix, Greif und Waldgeist. Chris, Mila und Roboter bleiben unter **Weitere
Figuren** erreichbar. Für neue Profile ist der Drache vorausgewählt; vorhandene
Auswahlen bleiben erhalten. Die Figur wird pro angemeldeter Person gespeichert;
Rufname, Stimme und die bisherigen Roboterfarben bleiben unabhängig davon.
Die Auswahl liegt in `assistent_profile.character`, wird gesichert und auch
beim Wiederherstellen älterer Sicherungen automatisch ergänzt.

Die Fabeltiere, Chris und Mila sind eigene KI-generierte Illustrationen mit Ruhe-, Sprech- und
Blinzelframes. Die Mundöffnung folgt lokal dem Pegel der tatsächlich abgespielten
Stimme. Nur der Mundbereich wird eingeblendet; das restliche Bild bleibt beim
Sprechen ruhig. Sprechpausen schließen den Mund. Unterbrechung, Seitenwechsel
und reduzierte Bewegung stoppen die Animation. Das ist keine phonemgenaue
Lippensynchronisierung und keine Google-Live-Avatar-Anbindung.
WebRTC wird über einen unhörbaren Analyser-Zweig des empfangenen Streams gemessen;
TTS über eine aus dem bereits vorhandenen Audioblob berechnete Hüllkurve, die der
Wiedergabezeit folgt. Der Mikrofonton wird dafür weder analysiert noch zusätzlich
übertragen. Fehlende Audioanalyse verhindert keine Sprachwiedergabe.
Quellenhinweis und Bildprompts stehen in `static/avatars/README.md`.

## Sprache und iPhone

Sprache benötigt den serverseitigen `OPENAI_API_KEY` und HTTPS. Gesprächsdaten und
die benötigten Auftragsdaten werden zur Sprachverarbeitung an OpenAI übertragen.
Der Sprachmodus startet durch einen bewussten Tipp. Er endet beim Sperren oder
Verlassen der Seite. Unterbrechen ist im Sprachablauf vorgesehen; tatsächliche
Latenz und Mikrofonverhalten müssen auf den verwendeten iPhones geprüft werden.
Beim Start werden Mikrofonfreigabe, Vorbereitung, Serverantwort und Audioverbindung
getrennt angezeigt. Alte Verbindungsrückmeldungen dürfen einen Neustart nicht beenden.
Eine blockierte automatische Wiedergabe lässt sich über **Ton einschalten** erneut
anstoßen. Fehlermeldungen bleiben nach einem Fensterwechsel sichtbar.
Das gilt auch für vorgelesene Textantworten. **Nachricht schreiben** öffnet die
Texteingabe direkt; ein bereits getippter neuer Entwurf bleibt beim Eintreffen
einer früheren Antwort erhalten. Unter **Mikrofon & Ton** stehen die Schritte zur
Browserfreigabe; bei länger ausstehendem Mikrofonstream öffnet sich die Hilfe.
Auch eine noch offene Mikrofonabfrage der Einzelaufnahme lässt sich abbrechen;
später bereitgestellte Streams werden dann sofort geschlossen. Die App kann
Browser- oder Betriebssystemberechtigungen nicht selbst erteilen.
Kurze Netzunterbrechungen erhalten bis zu fünf Sekunden zur Wiederverbindung.
Für ein App-Symbol kann die HTTPS-Seite in Safari zum Home-Bildschirm hinzugefügt
werden. Die Seite hört nicht im Hintergrund zu.

## Tages- und Lackierübersicht

Folgefragen verwenden die letzten zehn eigenen Dialognachrichten. Angaben aus
früheren Antworten gelten dabei nicht als aktueller Aktennachweis. Rechte werden
bei jeder Anfrage neu geprüft; angebotene Werkzeuge folgen dem aktuellen Zugang.
Eine auf 60 Aufträge begrenzte Übersicht wird als Teilmenge gekennzeichnet;
fehlende Aufträge werden gezielt gesucht, bevor die KI „nicht gefunden“ meldet.

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
um 14 Uhr in Europe/Berlin. Der Versand bleibt standardmäßig inaktiv. Er benötigt
einen geprüften Lieferantenkontakt, eindeutig bestätigte Produkte/Mengen/Kosten,
einen konfigurierten Kostenrahmen, das verbundene Mailkonto und den laufenden
Bestellworker. Verwaltung: `/admin/assistent-bestellungen`. Versand und Worker haben
eigene Aktivierungsschalter. Ein unbekanntes Versandergebnis wird nicht blind wiederholt.

## Persönliche Aufträge

Im Mitarbeiterkonto führt **Aufträge** zu `/werkstatt/mein-konto/auftraege`.
Die Nummer am Fahrzeug ist die feste interne Datenbank-ID, z. B. 102. Eine
Autohausreferenz im Feld `auftragsnummer` ersetzt diese Nummer nicht. Die Suche
öffnet ausschließlich den angefragten aktuellen Werkstattauftrag; Archiv,
fehlende Versicherungsfreigabe und nicht freigegebene Aufträge bleiben gesperrt.

Die Ansicht zeigt Arbeiten, prüfpflichtige Analysehinweise, Farbdaten,
Fertigtermin mit Uhrzeit und die getrennten Annahme-/Abhol-/Rückgabetermine.
Fehlende Daten bleiben sichtbar offen. Sie benötigt die eigene aktive Anmeldung
mit aktueller Rechte- und Passwortversion, keinen geteilten Werkstattcode.

`EMPLOYEE_ORDER_OPERATIONS_ENABLED=1` erlaubt in diesen persönlichen Routen
gezielt Statuswechsel und interne Arbeitsfotos. Das erweitert keine gespeicherten
Assistenten-, Einkaufs-, Personal- oder API-Rechte. Eingeplant kann nach In Arbeit
wechseln; Vorarbeit, Karosserie, Lackierung und Finish sind Arbeitsschritte.
**Fahrzeug fertig** setzt den gesamten Auftrag auf Fertig. Angelegt wird im Büro
eingeplant; abgeschlossene Aufträge werden hier nicht reaktiviert. Die festen
Formularaktionen verwenden den bestehenden Fortschrittsdienst, gebundenen
Formularstand, erneute Freigabeprüfung, Mitarbeiteraudit und Wiederholungsschutz.
Sie versenden keine automatischen Kundenmails oder WhatsApp-Nachrichten.

Fotos (1–6 JPEG/PNG, jeweils bis 8 MiB) werden dem geöffneten Auftrag zugeordnet,
intern gespeichert und im Admin beim selben Auftrag abrufbar. Metadaten werden
entfernt; die Bilder werden ohne OCR/KI verarbeitet und bleiben ohne explizite
Außenfreigabe. Ein wiederholter identischer Formularversand speichert sie einmal.
Alte Sicherungen dürfen weder verwendete Anforderungs-IDs, Arbeitsstand,
Mitarbeiterbindung noch interne Fotooriginale und Sichtbarkeiten zurücksetzen.

Arbeits-PDFs werden als neu erzeugte, bereinigte Textkopien geöffnet. Bank- und
Kostenzeilen sowie Originalgrafik, Links, Metadaten und aktive Inhalte werden
nicht weitergegeben. Rechnungen und Personal-/Bankbelege sind ausgeschlossen.
Gescannten PDFs und unbestätigten Dokumentbildern fehlt ein sicher prüfbarer
Textstand: Sie bleiben zur internen Prüfung gesperrt. Originalunterlagen im
Admin werden dadurch nicht geändert.

## Persönlicher Arbeitsplan und einheitliche Zeitansicht

Profil, Arbeitszeit und Urlaub verwenden denselben persönlichen Portalrahmen
mit dem gemeinsamen Menü. Die Arbeitszeitseite zeigt den eigenen Stempelstatus,
passende Kommen-/Pause-/Gehen-Tasten und die Monatsübersicht. Die Chefübersicht
bleibt unter `/admin/arbeitszeit` getrennt.

Ein Sollplan wird ausschließlich ausdrücklich je Mitarbeiter im privaten Profil
gepflegt: Wochenstunden, Tagesstunden, geplante Pause, Beginn und Arbeitstage.
Die separate Adminaktion `/admin/mitarbeiter/<id>/portal/arbeitsplan` verändert
keine Kontaktfelder, Lohnzettel, Urlaubsstände oder tatsächlichen Zeitstempel.
Ohne gespeicherten Plan gibt es keine Vorgabestunden. Bei 40 Wochenstunden,
fünf Arbeitstagen, acht Arbeitsstunden pro Tag, Beginn 08:00 Uhr und einer Stunde
geplanter Pause ergibt sich das geplante Ende 17:00 Uhr.

Der Plan erzeugt weder Arbeitszeit noch automatische Pausenabzüge. Maßgeblich
für die Zeitübersicht bleiben die tatsächlichen Kommen-/Pause-/Gehen-Stempel.
Die privaten Profilspalten werden auch nach einem alten Datenbankimport ergänzt;
der bestehende Schutz privater Vollzeilen verhindert das Zurückrollen eines
gespeicherten Plans. Lohnzettel bleiben personenbezogene Originalunterlagen:
Sammelabrechnungen müssen vor der Zuordnung auf die eigenen Personenseiten
getrennt werden. Unbekannte Stammdaten und Resturlaubstage werden nicht erfunden.

## Prüfung

Die Python-Suiten für Assistent, Cockpit-API, Artikelimport, Rechnungsauslese,
Topcolor-Positionen, Tagesbriefing und Backups sowie die JavaScript-Sprachtests
prüfen die Integration. Smoke- und Ablaufprüfung laufen mit isolierten Datenbanken
und ohne externe Netzverbindungen. Ein echter iPhone-Sprachtest ersetzt dies nicht.
