# Büro: Scanvorschlag und Lexware-Übergabe

Das Modul `werkstatt_buero_lexware.py` erkennt Dokumenttext ausschließlich mit der vorhandenen lokalen Texterkennung. Feste Regeln schlagen eine Büroablage und Dokumentart vor. Das ist keine steuerliche Kontierung: Es werden weder Konten-/Kategorie-IDs noch Steuersätze an Lexware gesendet. Mehrere erkannte Umsatzsteuersätze erzeugen einen Prüfhinweis. Aus Dokumenttext werden keine Anweisungen ausgeführt.

## Integration

`register_office_lexware(p)` registriert das Blueprint `office_lexware`, liefert den Service und setzt `p.office_lexware` sowie `p.office_lexware_init_schema`. Registrierung und Import greifen nicht auf Netzwerk oder Datenbank zu. `service.init_schema()` im bestehenden geschützten Initialisierungsablauf aufrufen. `TABLES` in die bestehende Sicherung/Wiederherstellung aufnehmen.

- `analyse_scan(key)` nach dem lokalen Originalupload aufrufen. Die revisionsgeprüfte Speicherung des Vorschlags kann die Belegrevision erhöhen; anschließend den aktuellen Beleg neu laden.
- `send_approved(key, version)` erst nach ausdrücklicher Chefprüfung, Bestätigung als betrieblicher Zahlungsbeleg und zulässiger Zahlungsfreigabe aufrufen. Rechnungen sowie ausdrücklich zahlbare betriebliche Bescheide sind zulässig. Lieferscheine, sonstige Post, persönliche Einkommensteuer und erkannte Anhörungen/Zeugenfragebögen werden nicht automatisch übertragen.
- `sync_status(key)` liest ausschließlich den Lexware-Belegstatus. Selbst `paid` führt nicht zu einer lokalen Zahlungsbestätigung.

Die vorhandenen Werte `LEXWARE_API_KEY`, `LEXWARE_API_BASE_URL`, `get_requests()` und `lexware_request()` werden verwendet; keine neue Geheimnisquelle. Für externe Uploads muss `app.config['OFFICE_LEXWARE_UPLOAD_ENABLED'] is True` gesetzt sein. Das Modul allein ist standardmäßig deaktiviert. Die App aktiviert die autorisierte Übergabe nach ausdrücklicher Chef-Freigabe; mit `OFFICE_LEXWARE_UPLOAD_ENABLED=0` bleibt sie ausgeschaltet. Der API-Endpunkt wird auf die offizielle HTTPS-Domain begrenzt, Weiterleitungen werden beim Upload nicht verfolgt.

Blueprint-Endpunkte `office_lexware.analyse`, `.transfer`, `.status` benötigen persönliche Büroberechtigung bzw. Chefkontext und CSRF. Die appweite Bereichs-Allowlist muss Mitarbeiterzugriffe auf den Bereich `belege` begrenzen. `transfer` und `status` haben zusätzliche `/admin/buero/...`-Routen; diese erweitern keine Rolle.

## Nachvollziehbare, wiederholbare Übertragung

Das Original bleibt unverändert. Für den Upload gilt eine konservative Grenze von 5.000.000 Byte, ausschließlich PDF/JPEG/PNG. Größere Originale werden nicht still komprimiert oder geteilt. Temporäre OCR-Dateien liegen in einem privaten Zufallsordner unter `DATA_DIR` und werden auch bei Fehlern entfernt.

Eine persistierte Übertragungsreferenz enthält Original-SHA, geprüfte Revision, kurze Lease, API-Konfigurationsfingerabdruck und zurückgegebene Datei-/Beleg-IDs. Gleichzeitige Aufrufe beanspruchen denselben Datensatz atomar. Nach Netzwerkunterbrechung bleibt das Ergebnis unbestätigt. Ein erneuter Versuch verwendet exakt dieselben Originalbytes; ein bereits bestätigter Upload wird nur lokal verknüpft und nicht erneut gesendet. Ändert sich währenddessen die Freigabe, bleibt die externe Referenz für den internen Abgleich erhalten, ohne neuere Belegdaten zu überschreiben.

Die [offizielle Files-Dokumentation](https://developers.lexware.io/docs/#files-endpoint) beschreibt Multipart-Upload, Antwortreferenzen und Deduplizierung identischer Dateien. Dieser Ablauf nutzt ausschließlich `POST /v1/files` mit `type=voucher`. Der hochgeladene Beleg benötigt anschließend die Prüfung in Lexware; das Modul finalisiert ihn nicht. Der erzeugte Link führt zum konkreten Lexware-Beleg.

Die Mitarbeiterin prüft den Beleg in Lexware und bereitet dort gegebenenfalls die Überweisung vor. Bankfreigabe/TAN bzw. Banking-App erfolgen persönlich entsprechend den [Lexware-Zahlungsschritten](https://help.lexware.de/de-form/articles/548712-belege-bezahlen). Das Portal ruft keine Zahlungs- oder Bank-API auf und speichert keine TAN.

## Isolierte Prüfung

`python scripts/test_buero_lexware.py` verwendet ausschließlich synthetische Originale, temporäre SQLite-Dateien, lokale OCR-Stubs und HTTP-Mocks. Keine produktive App wird importiert, keine echten Dateien oder Konten werden angesprochen.
