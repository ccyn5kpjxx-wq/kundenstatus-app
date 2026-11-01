# Chef-Posteingang: Abgleich vor der Beleganlage

Das Modul `werkstatt_buero_posteingang.py` enthält keinen Scheduler, keine
automatische Kontoauswahl und keine Bank- oder Lexware-Schreibaktion.

Ein Lauf benötigt einen ausdrücklichen Chefauftrag: im Request die Chefsession,
außerhalb davon einen bei der Registrierung injizierten Autorisierungscallback.
Die gespeicherte Einstellung `OFFICE_CEO_MAIL_ADDRESS` (alternativ der vom
Betreiber gesetzte gleichnamige `app.config`-Wert) und die angegebene Chefadresse
müssen übereinstimmen. Standardmäßig muss auch der konfigurierte IMAP-Benutzer
dieser Adresse entsprechen. Ein allgemeines Werkstattkonto wird niemals
ungefragt als Chefkonto angenommen.

## Konkrete Bedienung und Konfiguration

`werkstatt_buero_anbindung.py` verbindet die vorhandenen Adapter. Registrierung
über `register_office_inbox_bindings(p, authorize=None)` stellt
`p.office_inbox_bindings` und den Endpoint `office_inbox_bindings.admin` bereit.
Die Chefseite liegt unter `/admin/buero/posteingang-abgleich`. Ihr GET zeigt nur
Bereitschaft und Hinweise; erst POST mit CSRF, bewusster Bestätigung und Zeitraum
startet einen Lauf. Gleichlautende zentrale und seiteneigene CSRF-Token werden
akzeptiert; widersprüchliche Token oder mehrfach übermittelte Fachfelder nicht.
Eine normale Büro- oder Mitarbeiteranmeldung reicht für diese Seite nicht.

Die ausdrücklich bestätigte Quelle ist
`geschaeftsfuehrung@auto-lackierzentrum.de`. Die gespeicherte Einstellung
`OFFICE_CEO_MAIL_ADDRESS` hat Vorrang vor dem gleichnamigen `app.config`-Wert.
Ohne weitere Zuordnung muss der tatsächlich konfigurierte IMAP-Benutzer dieser
Adresse entsprechen. Bei einem ausdrücklich bestätigten gemeinsamen Postfach
kann `OFFICE_CEO_MAILBOX_USER` den erwarteten IMAP-Benutzer benennen. Für beide
Einstellungen gilt: gespeicherter Wert vor gleichnamigem `app.config`-Wert;
wenn beide Postfachwerte fehlen oder leer sind, gilt die strenge Chefadresse. Ungültige ausdrückliche Zuordnungen sperren den Lauf. Es gibt keinen
Fallback auf den zufällig vorhandenen IMAP-Benutzer und keine Übernahme aus
Formularfeldern. Die Chefadresse bleibt auch beim Alias unverändert.

Nach Bestätigung, dass Chefpost tatsächlich im gemeinsamen info-Posteingang
ankommt, ist die nicht geheime Konfiguration beispielsweise:

```text
OFFICE_CEO_MAIL_ADDRESS=geschaeftsfuehrung@auto-lackierzentrum.de
OFFICE_CEO_MAILBOX_USER=info@auto-lackierzentrum.de
```

Dies ändert keine IMAP-Zugangsdaten. Der vorhandene IMAP-Benutzer muss exakt der
bestätigten Postfachzuordnung entsprechen. Die Chefseite zeigt beide Adressen.
Ein gemeinsamer Lauf liest bewusst die **gesamte gemeinsame INBOX**, ohne
To-/Aliasfilter; Bcc/Weiterleitungen werden nicht über Headerannahmen getrennt.
Auch ein allgemeiner Büro-Mailboxgrant darf dieses zugeordnete Chefpostfach
nicht öffnen. Die ursprüngliche Chefadresse bleibt ebenfalls gesperrt.
Zuordnungen auf einen anderen physischen Postfachbenutzer erhalten getrennte
Abgleichsnachweise; bestehende strenge Zuordnungen behalten ihre bisherigen
Receiptreferenzen. Original-Dubletten werden weiterhin global wiederverwendet.

Die Bereitschaftsanzeige prüft nur Konfiguration, keinen Verbindungserfolg.
Bestehende IMAP-Konfiguration muss vollständig sein,
TLS verwenden und INBOX auswählen. Die Bindung beschafft weder neue Zugänge
noch Passwörter und nimmt kein allgemeines Werkstattkonto ersatzweise an.
Bei anderer oder fehlender Konfiguration bleibt der Lauf gesperrt.

Für den Geschäftsbeleg-Abgleich werden der bereits eingerichtete API-Zugang
mit offizieller Basis `https://api.lexware.io` und der geschützte
`office_documents`-Workflow benötigt. Ohne diese Quellen wird nicht gestartet.
Bestehende Zugangswerte erscheinen weder auf der Seite noch im Fehlerbericht.
Ein Zugriff des Büros auf die allgemeine Mailbox muss zusätzlich im Host
gesperrt bleiben, wenn diese mit dem Chefkonto verbunden ist.

Die konkrete Bindung verwendet `Mailbox.connect()` nur für das passende Konto,
`ReadonlyIMAP` für schreibgeschütztes Lesen und begrenzte GET-Downloads offizieller
Lexware-Dateien. Dateireferenzen sind geprüfte UUIDs; Downloads folgen keinen
Weiterleitungen. Der Lauf ruft keine Buchungs-, Zahlungs- oder Bankfunktion auf.

Der synchrone Lauf hat zusätzlich ein monotones Zeitbudget von 60 Sekunden.
Vor dem nächsten Iterator-Schritt, IMAP-Kommando, Bestands-/Originalzugriff,
lokaler Analyse und einer neuen Prüfbeleganlage wird die Deadline geprüft.
Eine bereits laufende begrenzte I/O-/Analyseoperation darf zurückkehren; danach
beginnt keine neue Arbeit. HTTP-GETs laufen ohne Wiederholung oder Redirect und
mit höchstens 30 Sekunden bzw. verbleibendem Budget als Timeout. IMAP verwendet
höchstens 20 Sekunden Timeout. Streams/Verbindungen werden beim Abbruch geschlossen.
Es gibt keinen automatischen Neustart. Der Abbruch nennt das Zeitbudget konkret;
bereits gespeicherte Prüfbelege, Receipts und Audit bleiben erhalten. Eine
Fortsetzung erfordert einen neuen bewussten Start, ggf. mit kleinerem Zeitraum.

## Einbindung der Pipeline

- `register_office_inbox(p, authorize=...)` liefert `p.office_inbox`.
- `init_schema()` erstellt nur Abgleichsnachweise und Audit, keine Freigaben.
- `BACKUP_TABLES`, `SCHEMA_FEATURE` und `ensure_office_inbox_for_import(...)`
  müssen in die vorhandene Sicherungs-/Restore-Routine aufgenommen werden.
- `run(address, mailbox=..., portal=..., lexware=..., metadata=...)` verarbeitet
  standardmäßig 14 Tage, höchstens 80 Mails/40 Anhänge/40 MiB. Größere Bereiche
  werden ausdrücklich blockiert. Das übergeordnete Heartbeat gehört nicht hierher.

Der IMAP-Adapter erhält eine ausdrücklich bereitgestellte Connection-Factory,
die eine authentifizierte Verbindung mit überprüfter TLS-Gegenstelle liefert.
Er öffnet INBOX schreibgeschützt, verwendet UID/SINCE/BEFORE und BODY.PEEK.
Auch bereits gelesene Nachrichten werden geprüft; Flags werden nie geändert.
PDF/JPEG/PNG werden begrenzt und anhand des Inhalts geprüft. Links, HTML und
Anhangstexte werden nicht als ausführbare Anweisungen behandelt.

## Bestandsadapter und alte Scans

`LegacyDocumentIndex` prüft im bestehenden Datenbestand `einkauf_belege`,
`dateien` und `datei_backups`. Er liest sichere direkte Dateinamen ausschließlich
aus dem konfigurierten `UPLOAD_DIR`, nutzt integritätsgeprüfte Datenbankoriginale
und berücksichtigt auch nicht zugeordnete direkte Uploads. Dateien werden weder
restauriert noch verändert. Fehlende Tabellen, Spalten oder Originale,
ungültige Sicherungen, Unterordner, Verknüpfungen, unbekannte Formate und
Prüflimits erscheinen ausdrücklich als Quellenlücke. Standardgrenzen: höchstens
3.000 Einträge je Altbestandsquelle/Ordner, 10 MiB pro Datei und 256 MiB gelesene
Originale. Ein Vergleich nur der neuesten 200 Büro-Belege genügt nicht.

`EvidencePortalInventory` verbindet diesen Index mit den geschützten Büro-Belegen.
Ein identischer SHA-256 kann sicher wiederverwendet werden. Eine andere Datei
kann jedoch Margittas Scan derselben E-Mail-Rechnung sein: vorhandener Alt- oder
Portalbestand ohne verifizierte vollständige Belegidentität sperrt deshalb eine
neue Prüfkopie, sofern kein eindeutiger vorhandener Beleg getroffen wird. Die
konkrete Datenlücke wird angezeigt. Ein vollständiger Dateihash-Index wird nicht
als sicherer inhaltlicher Dublettenabgleich ausgegeben. Solche Fälle benötigen
einen manuellen Vergleich mit Original und vorhandenen Belegangaben.

`LexwarePurchaseIndex(request, download=...)` verwendet ausschließlich GET:
Eingangsrechnungen mit Status `any`, aktive und archivierte Belege,
gezielte Belegdetails und bei Bedarf begrenzte Datei-Hashes. Ein abgebrochener
oder begrenzter Bestandslauf wird nie als vollständige Prüfung dargestellt.

Ein Metadatenabgleich erfordert vier belegte Werte: normalisierter Lieferant,
Belegnummer, positiver Centbetrag und Belegdatum. Nur ein ausdrücklich als
`identity_verified=True` gelieferter Datensatz wird so verglichen. Ein gleicher
Betrag oder ein Mailbetreff genügt nicht. Der Metadatencallback darf keinen
unsicheren OCR-Vorschlag als geprüfte Identität ausgeben.

Die konkrete Bindung prüft PDF/Bildintegrität vor der lokalen Texterkennung.
`extract_document_text_local` arbeitet im konfigurierten geschützten Datenordner.
Wie beim manuellen Upload prüft anschließend `analyse_document` die erkannten
Angaben. Die temporäre Originaldatei bleibt während dieser Analyse verfügbar
und wird danach entfernt. Nur bei ausdrücklich gesetztem
`OFFICE_AI_ANALYSIS_ENABLED=True` darf der bereits vorhandene Hostcallback
`office_extract_fields_ai` ergänzende Vorschläge liefern; ein fehlender oder
nicht verfügbarer Callback belässt die lokale Erkennung. Die Anbindung führt
keinen weiteren KI-Provider, keine neuen Zugangsdaten und keinen eigenen
KI-Transport ein.

Belegte Lieferanten-/Absenderangaben, Belegnummer, Belegdatum, Fälligkeit und
Bruttogesamtbetrag sowie ausdrücklich belegte Zahlungsempfänger, IBAN, BIC und
Verwendungszweck können leere Felder eigener Intakebelege vorbelegen. Bankdaten
werden lokal auf Format/Prüfziffer geprüft, nie ergänzt oder aus anderen Werten
abgeleitet. Bestehende manuelle Kontoverbindungen bleiben zusammenhängend erhalten. Belegart
und Ordner bleiben Vorschläge, unsichere Angaben bleiben offen. Die Quellen-
und Prüfhinweise werden mitgespeichert; der Rückgabewert ist niemals eine
verifizierte Belegidentität. Extraktion ersetzt deshalb weder einen vollständigen
Dublettenabgleich noch die Chefprüfung und bestimmt keine Steuern, Kontierung
oder betriebliche Zuordnung. Hinweise auf Privatpost aus OCR oder ausschließlich
den akzeptierten Quell-Evidenzen der erkannten Felder unterdrücken weiterhin
vorsorglich den GmbH-Lexware-Abgleich, bleiben ohne Chefentscheidung aber als
Bereich `unklar` gekennzeichnet. Bereits als privat zugeordnete Portaloriginale
bleiben getrennt. Nicht unterstützte/fehlbenannte oder Inline-Anhänge werden im
Bericht zur manuellen Prüfung genannt.

Nach dem Intake schreibt `record_extracted_fields(..., intake=True)` nur
revisionstreu in den eigenen automatischen Eingang. Erst mit dessen aktueller
Revision folgt die Klassifizierung. Gleichzeitige Änderungen werden nicht
überschrieben. Deadlineprüfungen vor und nach OCR/Analyse sowie zwischen den
Speicherschritten verhindern neue Arbeit nach Ablauf des Gesamtbudgets;
bereits gespeicherte Originale, Feldvorschläge und Abgleichsnachweise bleiben
bei Konflikten oder Abbruch erhalten. Keine Analyse erteilt eine Zahlungsfreigabe
oder legt einen zusätzlichen Lexware-Beleg an.

## Ergebnis und Freigabe

- Portal und Lexware vorhanden: wiederverwenden, keine neue Buchung.
- Nur Portal vorhanden: dessen Workflow fortsetzen, keinen zweiten Portalbeleg.
- Nur Lexware vorhanden: vorhandene Lexware-ID im Abgleichnachweis speichern;
  eine neue Portal-Prüfkopie darf nach vollständigem Portalcheck entstehen.
  Der vorhandene Lexware-Beleg wird nach Chefprüfung zugeordnet, nie neu hochgeladen.
- Beide fehlen und vollständiger Dublettenabgleich ist belegt: Portal-Prüfbeleg,
  keine sofortige Lexware-Anlage. Die vorstehende Scan-/Identitätslücke sperrt
  die Neuanlage weiterhin.
- Mehrdeutiger oder unvollständiger Bestand: blockieren und zur Prüfung melden.
- Belegt private Dokumente: Ordner/Bereich Privat, keine GmbH-Lexwareabfrage.
  Unklare Dokumente bleiben unklar und erhalten keine betriebliche Freigabe.

Der Portaladapter nutzt `create_from_intake`: eingereicht zur Chefprüfung,
`payable=0`, `business_confirmed=0`. Weder automatische Klassifizierung noch
ein Match-Vorschlag erteilt eine Buchungs- oder Zahlungsfreigabe. Margittas
Zahlungsaufgabe folgt erst aus dem getrennten Chef-Freigabeworkflow.

Die Tabelle `buero_posteingang_receipts` bindet Konto-Hash, UIDVALIDITY, UID
und Anhangs-Hash. Wiederholte Läufe gleichen den Bestand erneut ab und erzeugen
keine zusätzlichen Prüforiginale. Der Restore-Guard erhält diese Nachweise.

## Täglicher Ablauf und tatsächlicher Stand

Der tägliche 9-Uhr-Heartbeat ist im übergeordneten Chat geplant. Er ist kein
Backend-Cron und beweist keinen erfolgreichen Postfachlauf. Diese Implementierung
wurde ausschließlich mit synthetischen Dateien, isolierter Testdatenbank und
Fake-IMAP/HTTP geprüft. Ein erfolgreicher produktiver Abgleich wird damit nicht
behauptet. Vor einem tatsächlichen Start sind Kontoübereinstimmung,
Quellenbereitschaft und die konkreten Altbestandslücken auf der Chefseite zu
prüfen. Die fehlende oder abweichende Quelle wird zuerst geklärt.

Offline-Prüfung: `python scripts/test_buero_anbindung.py -v` und
`python scripts/test_buero_posteingang.py -v`. Der Host integriert die bestehenden
Inbox-Tabellen und Restore-Prüfung; die Bindung führt keine zusätzlichen
Tabellen und keinen automatischen Hintergrundlauf ein.

Primärquellen: [Python IMAP](https://docs.python.org/3/library/imaplib.html),
[Python E-Mail-Modell](https://docs.python.org/3/library/email.message.html),
[Lexware Voucherlist/Vouchers](https://developers.lexware.io/docs/#voucherlist-endpoint).
