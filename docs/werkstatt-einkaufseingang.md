# Materialeingang und externe Bestellnachträge

Stand 06.10.2026. Diese Erweiterung wird intern entwickelt. Keine Veröffentlichung
und keine Lieferantenkommunikation sind Bestandteil dieser Abnahme.

## Nachweis statt erneuter Bestellung

Der neue Eingang liegt unter dem Bestellordner. Er speichert Materialbedarf und
bereits außerhalb des Portals erfolgte Bestellungen getrennt von der bestehenden
Versandwarteschlange. Ein Nachtrag hat keinen Versandknopf und wird vom
Montagsworker nicht gelesen. Wiederholte Übernahmen derselben Quellenreferenz
erzeugen keinen zweiten Eintrag; abweichende Inhalte erfordern eine Prüfung.

Originaler Mitarbeiter, Erfassender und Zeitpunkt sind verschiedene Angaben.
Bei weitergeleiteten WhatsApp-Bildern bleibt der ursprüngliche Mitarbeiter
unbekannt, bis die Teamnachricht eindeutig zugeordnet ist. Die Anzahl der Fotos
und der aufgedruckte Packungsinhalt sind keine Bestellmenge. Unbekannte Mengen,
Dringlichkeit und Preise bleiben ausdrücklich offen.

Originalfotos und Belege werden privat mit Prüfsumme gespeichert. Eine Auslese
ist ein Vorschlag; erst ein ausdrücklich bestätigter Positionsabgleich ordnet
belegte Liefermengen zu. Ein Lieferschein ohne Preise belegt keine Ausgaben.
Teillieferungen und wiederholte Uploads müssen getrennt von neuen Lieferungen
behandelt werden. Unbekannte Sollmengen dürfen nicht als vollständig geliefert
gelten. Die neuen Tabellen und Originaldateien sind in Backup/Restore enthalten.

## Historische Preise und Preisänderungen

Vorhandene Rechnungsartikel werden weiterverwendet. Die Katalogsuche liefert
Kandidaten, nicht automatisch den zeitlich neuesten bestätigten Preis. Es zählen
identische Artikelnummer, Variante, Bestelleinheit und Gebinde sowie ein bekanntes
Belegdatum. Ein unvollständiger Altdatensatz bleibt ein historischer Preishinweis.
Eine am Original geprüfte Position kann als letzter belegter Einkaufspreis mit
Belegnummer, Datum, Seite/Position, Preisbasis, Währung, Steuer und Rabattbasis
gespeichert werden. Er ist ein Plan-/Vergleichspreis, keine heutige Lieferantenzusage.

Netto und Brutto, Liter und Gebinde sowie verschiedene Packungsinhalte werden
nicht still vermischt. Ein konditionales Skonto wird ohne Nachweis nicht abgezogen.
Nur vergleichbare Preisstände ergeben eine absolute und prozentuale Abweichung.
Eine ungeklärte Abweichung hat keine behauptete Ursache und löst keine Reklamation
aus. Summen zeigen fehlende Mengen und nicht vergleichbare Preisgrundlagen an.

## WhatsApp: nachgewiesene Konfiguration und offene Freischaltung

Am 05.10.2026 wurde die Konfiguration des vorhandenen Render-Dienstes nur gelesen:
WhatsApp ist eingeschaltet; Zugang und Phone-Number-ID sind als Variablen vorhanden.
Die Werkstatt-Empfangsnummer und die separat konfigurierte Portal-Absendernummer
sind verschieden. Geheimnisse und Nummern-IDs werden weder dokumentiert noch
im Diagnoseergebnis angezeigt. Eine konfigurierte Absendernummer allein beweist
keine aktuelle Registrierung, Kontoberechtigung oder Zustellfähigkeit bei Meta.

Die zusätzliche nur lesende Graph-Abfrage lieferte HTTP 200 für die konfigurierte
Absendernummer und den Geschäftsnamen, `status=CONNECTED`,
`platform_type=CLOUD_API`, `quality_rating=GREEN`, `is_on_biz_app=false`,
`is_official_business_account=false` und `code_verification_status=EXPIRED`.
Der letzte Wert bezieht sich auf die Codeverifizierung, nicht auf einen
nachgewiesenen Ablauf des API-Tokens. Er allein beweist keinen Versandfehler;
das separate Verbindungsfeld meldet CONNECTED. Ein tatsächlicher Sendetest,
eine Neuregistrierung oder eine Umstellung des normalen WhatsApp-Kontos wurden
nicht durchgeführt. Die Gruppenberechtigung ist damit nicht bestätigt.

Eine normale WhatsApp-Web-Gruppe wird nicht allein durch ihre Erstellung zu
einem API-Eingang. Vor einem automatischen Materialeingang sind die konkrete
Cloud-API-Nummer, Kontovoraussetzungen, Webhook-Signatur und verifizierte Zuordnung
der Mitarbeiter zu prüfen. Direkte Mitarbeiter-Fotonachrichten sind ein möglicher
Eingang. Die offizielle Groups API ist ein gesonderter Kanal mit eigenen
Teilnehmer-/Kontobeschränkungen; die vorhandene Gruppe darf nicht als dafür
registriert angenommen werden. Die gewöhnliche Gruppe „Gärtner Bestellungen“
ist organisatorisch vorhanden; das verbindet sie noch nicht mit dem Bestellordner.
Kein Kontowechsel oder Gruppenneuanlage erfolgt
automatisch. Bis zur Entscheidung sind manuelle Originaluploads möglich.

Der vorhandene `/webhooks/whatsapp` nimmt zusätzlich den separat freigegebenen
Materialkanal entgegen. Der empfangene Body wird vor jeder Verarbeitung mit
HMAC-SHA256 geprüft; fehlendes App-Secret ist kein Freifahrtschein. Maximal
256 KiB werden angenommen. Fotos gelangen nie in den bisherigen Fahrzeugchat.
Nachrichten an die für Material freigegebene Phone-Number-ID werden auch als Text
nicht an den zuletzt kontaktierten Fahrzeugauftrag weitergeleitet.

## Automatischer Eingang: lokale Implementierung

Die Verwaltungsseite im Bestellordner zeigt Rechnungsmonitor, Mitarbeiterzuordnung
und letzte Fotoeingänge. Alle Änderungen erfordern eine Adminsitzung und CSRF.
Import, Registrierung und Schemaanlage starten weder Netzwerk noch Threads.

Der Rechnungsmonitor (`werkstatt_einkaufsmonitor.py`) setzt auf dem bestehenden
freigegebenen Mailquellenimport auf. Er speichert je Konto einen Abrufstand,
UIDVALIDITY und eine begrenzte UIDNEXT-Spanne. Der Stand wird erst nach persistierter
Verarbeitung vorgerückt. Neue Anhänge werden dauerhaft für genau ihren
Rechnungsimport vorgemerkt. Er verarbeitet keine beliebige ältere offene
Lexware-Quelle. Identische Nachricht/Anhänge werden idempotent behandelt;
Message-ID allein genügt nicht als Duplikatnachweis. Wiederholungen nach Fehlern
verwenden gespeicherte Warteschlangen und befristete Prozesssperren. Artikel- und
Preisauslese bleibt prüfpflichtig, auch wenn der Abruf automatisch läuft.

Aktivierung: Konto im Adminbereich freigeben und entweder
`PURCHASE_MONITOR_WORKER_ENABLED=1` beim App-Start setzen oder einen dedizierten
Prozess `flask --app app werkstatt-einkaufsmonitor` betreiben. `--once` bearbeitet
einen begrenzten Lauf; `--status` zeigt den Stand. Ohne aktuellen gespeicherten
Heartbeat behauptet die Oberfläche keinen laufenden Hintergrundabruf.

Der Fotokanal (`werkstatt_materialkanal.py`) benötigt explizit
`MATERIAL_WHATSAPP_ENABLED=1`, die bestätigte CSV-Allowlist
`MATERIAL_WHATSAPP_PHONE_IDS`, bestehendes `WHATSAPP_APP_SECRET`,
`WHATSAPP_ACCESS_TOKEN` und die Graph-Version. Die persönliche Absendernummer
wird im Adminbereich einem aktiven Mitarbeiter mit Lese-/Einkaufsrecht zugeordnet.
Ein Widerruf oder eine Änderung der Mitarbeiterrechte sperrt auch bereits
eingegangene, aber noch nicht übernommene Nachrichten. Eine Nummer wird nicht
automatisch einer anderen Person zugewiesen.

Für die ausdrücklich gewählte gemeinsame Portalnummer wird zusätzlich
`MATERIAL_WHATSAPP_SHARE_PORTAL_NUMBER=1` gesetzt. Geteilt wird ausschließlich
die bestehende `WHATSAPP_PHONE_NUMBER_ID`, sofern sie in der Material-Allowlist
steht. Persönlich zugeordnete Mitarbeiter werden anhand ihrer Nummer zum
Materialkanal geleitet; andere Nachrichten durchlaufen weiter die vorhandenen
Prüfungen des Fahrzeugchats. Gespeicherte Materialabsender bleiben bei Pause,
Widerruf oder Rechteentzug reserviert und fallen nicht in den Fahrzeugchat zurück.
Andere Materialempfänger bleiben exklusiv. Ohne diese ausdrückliche Freigabe
wird eine doppelte Verwendung derselben Nummer als Einrichtungskonflikt angezeigt.

Der Webhook speichert nur das Ereignis. Ein separater Fotodienst lädt das
Original mit Größen-, Host-, MIME- und SHA256-Prüfung. Dafür entweder
`MATERIAL_WHATSAPP_WORKER_ENABLED=1` explizit setzen oder
`flask --app app werkstatt-materialeingang-worker` verwenden (`--once` verfügbar).
Originaleingang, persönliche Assistenten-Fotoliste und Ereignisabschluss werden
gemeinsam gespeichert. Ein Fehler hinterlässt keinen halben Vorgang. Weiterleitungen
nennen den ursprünglichen Fotografen nicht als bestätigt.

Der Materialdialog (`werkstatt_materialdialog.py`) verbindet den Eingang mit
Fotoauslese, Artikeltreffern, gebundenen Textantworten und dem vorhandenen
Bestellversand. Ein eigener Bedarf entsteht pro ursprünglicher Nachricht; eine
Antwort braucht entweder den zitierten Vorgang oder dessen Code und Revision.
Antworten werden niemals still einem zuletzt eingegangenen Foto zugeordnet.
Weitergeleitete Texte oder Bilder gelten nicht als persönliche Bestellfreigabe.

Im ausdrücklich eingerichteten Materialkanal gilt Foto plus „dringend“ als
Bestellwunsch. Die gewünschte Menge und Bestelleinheit müssen trotzdem belegt
sein; „VE 96“ ist weiterhin keine Bestellung von 96 Stück. Kandidaten und
historische Rechnungswerte bleiben Vorschläge. Die Werkstattleitung prüft
Artikelidentität, Bestelleinheit, aktuelle Bruttokosten samt Versand/Nebenkosten,
Bestellkontakt, Preisquelle und Gültigkeitsdatum im Materialeingang. Ein exakt
passender, noch gültiger Zuordnungssatz kann erneut verwendet werden.

Vollständige persönliche Anforderungen werden ohne zusätzliche pauschale
Freigaberunde an die bestehende Bestellwarteschlange übergeben. Dabei gelten
aktuelle Mitarbeiterrechte, persönliche Grenze, betriebliche Freigabe und
höchstens 250 Euro brutto. Es gibt keine simulierte Mitarbeiteranmeldung für den
Hintergrundprozess. Revidierte, abgebrochene oder bereits übergebene Vorgänge
können nicht unter einer neuen Revision noch einmal bestellt werden.

Rückfragen sind eine dauerhafte Warteschlange. Für deren produktiven Versand
muss zusätzlich `MATERIAL_WHATSAPP_REPLIES_ENABLED=1` gesetzt werden. Die
Rückfrage nutzt die ursprüngliche erlaubte Geschäftsnummer und den geprüften
persönlichen Empfänger innerhalb des Antwortfensters. Ein unbekannter
Sendestatus wird nicht blind erneut versendet. Konfigurationsfreigabe allein
beweist keine Zustellung oder laufenden Hintergrundprozess.

Die Verwaltungsansicht zeigt offene Angaben, Quellen, Artikelvorschläge und
Rückfragen. Eine Übergabe an den Bestellordner bedeutet noch nicht, dass die
Lieferantenmail versandt wurde; deren tatsächlicher Status bleibt im Bestellordner
nachvollziehbar. Die normale bestehende Gruppe wird nicht mitgelesen. Ein
zulässiger produktiver Eingangsweg und die Mitarbeiterzuordnung bleiben
Voraussetzung für den Livebetrieb.

Zusätzlich zu den fünf Monitor-/Kanaltabellen werden drei Dialogtabellen
mitgesichert. Ältere Pakete ohne `werkstatt_materialautomatik_v1` oder
`werkstatt_materialdialog_v1` bleiben importierbar; die Schemata werden nach
der Wiederherstellung ergänzt. Produktive Dienste vor einem Datenbank-Restore
anhalten und danach mit geprüfter Kontokonfiguration starten.

## Versandregel und weitere Integration

Reguläre Bestellungen: Montag 14 Uhr Europe/Berlin; dringend: sofort nach den
bestehenden Artikel-, Rechte-, Kontakt- und Budgetprüfungen. Die Sammelgrenze gilt
kumuliert je Lieferant und Berliner Montag, einschließlich noch vorhandener
Altanforderungen. Die Umstellung verschiebt belegte, noch nicht eingefrorene
12-Uhr-Anforderungen auf 14 Uhr desselben bereits vorgesehenen Montags.
Unveränderliche vorbereitete Mailpakete bleiben unverändert; eine separate
Versandsperrfrist verhindert den früheren Versand. Bereits gesendete Pakete
werden nie neu erzeugt. Sommer- und Winterzeit folgen Europe/Berlin. Dienstag ist der
vom Betrieb genannte Lieferrhythmus, keine Garantie für eine konkrete Lieferung.
Mister Bean als Kontakt, der bestellberechtigte Empfänger und ein E-Mail-Ersatz
bei Urlaub benötigen eine bestätigte Zuordnung. Ein unklarer WhatsApp-Versand
darf keinen automatischen E-Mail-Zweitversand auslösen. Historische Nachträge
werden nie automatisch nachgesendet.

Die neue Monitorstrecke ist lokal implementiert, aber nicht veröffentlicht oder
für das produktive Postfach aktiviert. Ein vollständiger automatischer Abgleich
zwischen Rechnung, tatsächlicher Bestellung und Wareneingang bleibt eine weitere
Integration; derzeit erfolgt die belegte Zuordnung im Materialeingang. Keine
Bank-/Lohnmails, Zahlungen oder automatischen Lieferantenreklamationen.

Technische Referenzen: [Meta Telefonnummernstatus](https://developers.facebook.com/documentation/business-messaging/whatsapp/business-phone-numbers/phone-numbers/),
[Meta Coexistence](https://developers.facebook.com/documentation/business-messaging/whatsapp/embedded-signup/onboarding-business-app-users),
[Meta Groups](https://developers.facebook.com/documentation/business-messaging/whatsapp/groups),
[Bild-Webhooks](https://developers.facebook.com/documentation/business-messaging/whatsapp/webhooks/reference/messages/image).
