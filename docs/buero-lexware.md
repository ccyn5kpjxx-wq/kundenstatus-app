# Büro: automatische Belegerkennung und Lexware-Übergabe

Nach einem Upload liest die vorhandene lokale Texterkennung das Original. `werkstatt_buero_analyse.py` extrahiert belegte Hauptdaten. Ist `OFFICE_AI_ANALYSIS_ENABLED` ausdrücklich aktiv und der vorhandene KI-Zugang eingerichtet, ergänzt `werkstatt_buero_ki.py` eine schema-begrenzte Analyse einschließlich Originalbildern. Es wird kein neuer Zugang angelegt. Ohne Konfiguration oder bei Ausfällen bleibt die lokale Erkennung mit sichtbarem Hinweis verfügbar.

Lieferant, Rechnungsnummer, Belegdatum, Fälligkeit und ausgewiesener Bruttogesamtbetrag werden ausschließlich in leere Entwurfsfelder übernommen. Manuelle Werte bleiben erhalten. Mehrdeutige Felder bleiben auch nach einem KI-Vorschlag offen. Art und Ordner sind Vorschläge, die beim Einreichen bestätigt werden. Die Erkennung erteilt weder eine Zahlungsfreigabe noch eine steuerliche Kontierung; Konten und Steuersätze werden nicht erfunden; es wird kein Zahlungsauftrag an eine Bank gesendet. Mehrere erkannte Umsatzsteuersätze erzeugen einen Prüfhinweis. Dokumentinhalt wird als untrusted Quelle verarbeitet.


## Überweisungsdaten am Beleg

`zahlungsempfaenger`, `iban`, `bic`, `verwendungszweck` und `faelligkeit` sind
innerhalb desselben geschützten Belegs sichtbar und korrigierbar. IBAN/BIC sind
neue Textspalten mit leerem Default; die idempotente SQLite-/PostgreSQL-Migration
erhält bestehende Entwürfe, Originale, IDs, Revisionen, Freigaben und Audit.
Backups enthalten alle Felder; der bestehende Office-Restoreguard schützt sie.

IBAN wird lokal ohne Leerzeichen in Großbuchstaben gespeichert und anhand
Länderformat/Länge sowie MOD97 geprüft. BIC wird auf das 8-/11-Zeichenformat
geprüft, bleibt bei fehlender Quelle leer und ist für einen Zahlungsauftrag
keine Pflichtangabe. Diese Prüfungen bestätigen weder Kontoinhaber noch
Kontobestand. Normquellen: [SWIFT IBAN Registry](https://www.swift.com/sites/default/files/files/iban-registry-v101.pdf),
[SWIFT BIC](https://www.swift.com/standards/data-standards/bic-business-identifier-code).

Nur explizit belegte Empfänger-/Kontoinhaber-, IBAN-, BIC- und Zweckangaben dürfen
leere Entwurfsfelder ergänzen. Mehrere mögliche Bankverbindungen oder nur ein
Lastschrift-/Mandatsbezug erzeugen keine automatische Überweisungsverbindung.
Manuelle Angaben bleiben erhalten; widersprechende oder nicht eindeutig zur
vorhandenen IBAN/BIC/Empfängerin gehörende Bankvorschläge werden gemeinsam
übersprungen. Ein Quellenname oder die Adresse eines Steuerberaters ersetzt
keinen Original-Steuerbescheid und begründet keine Zahlungsfreigabe.

Bei jedem Beleg stehen alle fünf Überweisungsangaben direkt sichtbar;
fehlende Werte heißen „Nicht angegeben“. Auf ausdrücklichen Nutzerwunsch
verhindern fehlende Empfänger-/Bankdaten, Fälligkeit oder Verwendungszweck
die Chef-Freigabe nicht. Eingegebene ungültige IBAN/BIC bleiben zu korrigieren.
Zahlungsaufträge benötigen weiterhin Originalgrundlage, positiven geprüften
Betrag, Auftragsart und ausdrückliche Chef-Zahlungsfreigabe; Einzelzahlungen
behalten die Begründungspflicht. Private Aufträge sind ausschließlich
begründete Einzelzahlungen mit bestätigter privater Zuordnung. Miete/Kaution
bleiben getrennt, monatliche Mietvorlagen sind nur im Ordner Miete zulässig.
Chefprüfung und Transferkorrektur verwenden eine gemeinsame CAS-Revision und
Transaktion. Ungültige Werte oder veraltete Revisionen speichern keine
Teilfreigabe. Fehlende Zahlungsdaten bleiben auch nach der Freigabe sichtbar.
Das Büro kann die Bearbeitung übernehmen; das ist keine Bankausführung.
„Zahlung dokumentieren“ verlangt weiterhin die ausdrückliche Bestätigung
einer extern ausgeführten Zahlung mit Datum und Referenz. Keine Freigabe
oder Bearbeitungsübernahme erzeugt selbst eine Überweisung oder „bezahlt“.

## Integration

`register_office_lexware(p)` registriert das Blueprint `office_lexware`, liefert den Service und setzt `p.office_lexware` sowie `p.office_lexware_init_schema`. Registrierung und Import greifen nicht auf Netzwerk oder Datenbank zu. `service.init_schema()` im bestehenden geschützten Initialisierungsablauf aufrufen. `TABLES` in die bestehende Sicherung/Wiederherstellung aufnehmen.

- `analyse_scan(key)` nach dem Originalupload aufrufen, für eigene geschützte Mail-Prüfkopien `analyse_scan(key, intake=True)`. Die revisionsgeprüfte Speicherung der Felder und Klassifizierung kann die Belegrevision erhöhen; anschließend den aktuellen Beleg neu laden.
- `send_approved(key, version)` erst nach ausdrücklicher Chefprüfung, Bestätigung als betrieblicher Zahlungsbeleg und zulässiger Zahlungsfreigabe aufrufen. Rechnungen sowie ausdrücklich zahlbare betriebliche Bescheide sind zulässig. Lieferscheine, sonstige Post, persönliche Einkommensteuer und erkannte Anhörungen/Zeugenfragebögen werden nicht automatisch übertragen.
- `file_for_review(key, version)` ist die ausdrücklich angeforderte Ablage über „In Lexware ablegen“. Sie ist für jeden sichtbaren Beleg unabhängig von Art, Ordner und Zahlungsstatus möglich, einschließlich privater Unterlagen und Zahlungsaufträgen. Sie gewährt keine Zahlungsfreigabe, keine betriebliche Zuordnung und keine steuerliche Buchung. Das ist ein eigener manueller Pfad; die obigen Automatikregeln bleiben unverändert.
- `sync_status(key)` liest ausschließlich den Lexware-Belegstatus. Selbst `paid` führt nicht zu einer lokalen Zahlungsbestätigung.

Die vorhandenen Werte `LEXWARE_API_KEY`, `LEXWARE_API_BASE_URL`, `get_requests()` und `lexware_request()` werden verwendet; keine neue Geheimnisquelle. Für externe Uploads muss `app.config['OFFICE_LEXWARE_UPLOAD_ENABLED'] is True` gesetzt sein. Das Modul allein ist standardmäßig deaktiviert. Die App aktiviert die autorisierte Übergabe nach ausdrücklicher Chef-Freigabe; mit `OFFICE_LEXWARE_UPLOAD_ENABLED=0` bleibt sie ausgeschaltet. Der API-Endpunkt wird auf die offizielle HTTPS-Domain begrenzt, Weiterleitungen werden beim Upload nicht verfolgt.

Blueprint-Endpunkte `office_lexware.analyse`, `.transfer`, `.filing`, `.status` benötigen persönliche Büroberechtigung bzw. Chefkontext und CSRF. Die appweite Bereichs-Allowlist begrenzt Mitarbeiterzugriffe auf den Bereich `belege`. `transfer`, `filing` und `status` haben zusätzliche `/admin/buero/...`-Routen; diese erweitern keine Rolle. Manuelle Ablage: `POST .../belege/<id>/lexware/ablegen` mit aktueller Revision. Auch bei privaten oder nicht zahlbaren Belegen führt die bestätigte Ablage anschließend zum konkreten Lexware-Link.

## Nachvollziehbare, wiederholbare Übertragung

Das Original bleibt unverändert. Für den Upload gilt eine konservative Grenze von 5.000.000 Byte. Der automatische Altpfad überträgt weiterhin Originalbytes und verändert zu große Dateien nicht. Die neue manuelle Ablage erstellt eine gekennzeichnete PDF-Prüfkopie: Deckblatt mit vorhandenen Portal-/Zahlungsangaben und sämtlichen Originalseiten in unveränderter Reihenfolge. Private Dokumente bleiben ausdrücklich privat gekennzeichnet; fehlende Angaben bleiben offen. Nur bei Bedarf werden alle Quellseiten als vollständige gut lesbare Rasterkopie komprimiert. Unter 150 dpi/JPEG65 wird nicht verkleinert, Seiten werden nicht weggelassen. Ist das Limit nicht lesbar einzuhalten, erscheint ein konkreter Fehler. Temporäre OCR-Dateien liegen in einem privaten Zufallsordner unter `DATA_DIR` und werden auch bei Fehlern entfernt.

Eine persistierte Übertragungsreferenz enthält Original-SHA, Quellrevision, kurze Lease, API-Konfigurationsfingerabdruck und zurückgegebene Datei-/Beleg-IDs. Vor dem ersten externen POST werden exakte Uploadbytes, eigener Upload-SHA, MIME, Dateiname und Kopiemetadaten eingefroren. Gleichzeitige Aufrufe beanspruchen denselben Datensatz atomar. Nach Netzwerkunterbrechung bleibt das Ergebnis unbestätigt. Ein erneuter Versuch verwendet dieselben eingefrorenen Bytes; ein bereits bestätigter Upload wird nur lokal verknüpft und nicht erneut gesendet. Alte unbestätigte Originalübertragungen werden nicht nachträglich durch eine andere Kopie ersetzt. Unvollständige Cachebytes/Metadaten stoppen den Wiederholungsversuch. Der API-Fingerprint wird auch bei verlinkten Wiederholungen geprüft. Ändert sich währenddessen der Belegstand, bleibt die externe Referenz erhalten, ohne neuere Belegdaten zu überschreiben. Der bestehende Restoreguard schützt sämtliche neuen Receipt-Spalten.

Miete und Kaution können dasselbe Vertragsoriginal verwenden. Ihre Prüfdeckblätter enthalten jedoch unterschiedliche stabile Portal-IDs und Zahlungszwecke; sie erhalten dadurch verschiedene Uploadbytes und können als getrennte Lexware-Prüfbelege angelegt werden. Die Portalaufgaben werden nie zusammengelegt. Es werden keine neuen Rechnungsnummern, Steuersätze, Raten oder Zahlungsbestätigungen erfunden.

Die [offizielle Files-Dokumentation](https://developers.lexware.io/docs/#files-endpoint) beschreibt Multipart-Upload, Antwortreferenzen und Deduplizierung identischer Dateien. Dieser Ablauf nutzt ausschließlich `POST /v1/files` mit `type=voucher`. Der hochgeladene Beleg benötigt anschließend die Prüfung in Lexware; das Modul finalisiert ihn nicht. Der erzeugte Link führt zum konkreten Lexware-Beleg.

Die Mitarbeiterin prüft den Beleg in Lexware und bereitet dort gegebenenfalls die Überweisung vor. Der zusätzliche Link „Überweisungen in Lexware“ öffnet den tatsächlich geprüften Bereich `https://app.lexware.de/fis/app/transfers`. Lexware erlaubt dort auch [vorbereitete Zahlungen ohne Rechnungsbezug](https://help.lexware.de/de-form/articles/665945-uberweisungen-ausfuhren-oder-vorbereiten), etwa Miete/Kaution. Die fünf Zahlungsangaben werden auf der Prüfkopie mitgegeben; es wird kein Bankauftrag per API erzeugt und keine fertige Überweisung vorgetäuscht. Private Ausgaben und [geleistete Kautionen](https://help.lexware.de/de-form/articles/548669-wie-erfasse-ich-kautionen) müssen in Lexware passend zugeordnet werden; die Ablage ersetzt diese Buchungsentscheidung nicht. Bankfreigabe/TAN bzw. Banking-App erfolgen persönlich entsprechend den [Lexware-Zahlungsschritten](https://help.lexware.de/de-form/articles/548712-belege-bezahlen). Das Portal ruft keine Zahlungs- oder Bank-API auf und speichert keine TAN.

## Isolierte Prüfung

`python scripts/test_buero_lexware.py`, `scripts/test_buero_analyse.py` und `scripts/test_buero_ki.py` verwenden synthetische Originale, temporäre SQLite-Dateien, OCR-Stubs und HTTP-Mocks. Geprüft werden zusätzlich Quellzitate, Bildherkunft, Mehrdeutigkeiten, Fehlerfallback und Transportgrenzen. Es werden keine echten Dateien oder Konten angesprochen.

Der KI-Transport nutzt den vorhandenen Modellnamen und Schlüssel, ausschließlich den offiziellen OpenAI-Endpunkt, keine Tools und keine Weiterleitungen. Es gibt einen Versuch mit begrenzter Laufzeit und Antwortgröße. Abgebrochene oder verweigerte Antworten werden nicht übernommen. `visual_evidence` ist eine vom Host gesetzte Information über tatsächlich beigefügte Bilder, niemals eine Modellbehauptung. Sie kennzeichnet Bildvorschläge ausdrücklich als nicht durch OCR bestätigt. Grundlage sind die offiziellen Dokumentationen zu [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=chat) und [Bildeingaben](https://developers.openai.com/api/docs/guides/images-vision?api-mode=chat).
