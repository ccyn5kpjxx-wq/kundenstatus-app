# Lieferscheine zur Bestellung

Unter **Eingänge & Werkstatt → Bestellordner** die Bestellung öffnen und
**Lieferschein erfassen** wählen. **Lieferschein hochladen & automatisch
zuordnen** speichert das Original, analysiert den Beleg und übernimmt eindeutig
belegte Lieferpositionen. Vollständig gelieferte Bestellungen erhalten den
Lieferstatus **Vollständig geliefert**. Teilmengen bleiben als Teillieferung
offen. Nur unklare Belege benötigen die manuelle Originalprüfung.

Die Bestellübersicht zeigt offene, teilweise oder vollständige Lieferungen.
Mehrlieferungen und beschädigte beziehungsweise fehlende Nachweise erhalten
einen Prüfstatus. Der ursprüngliche Bestell- und Versandnachweis bleibt erhalten.

## Analyse und Mengen

Die Auslese läuft lokal. Bei Fotos werden OCR-Koordinaten verwendet, damit
Tabellenzeilen zusammenbleiben. Die konservative Tabellenerkennung unterstützt
den Top-Color-Lieferscheintyp; andere Layouts bleiben über Original und Auslesetext
manuell prüfbar. Automatische Buchungen erfolgen ausschließlich nach dem
positiven Abgleich unten; sonst bleiben OCR-Ergebnisse ungeprüfte Vorschläge.

Ein geliefertes Gebinde mit 0,5 Liter Inhalt bedeutet **1 Stück**, sofern dies
die gespeicherte Bestelleinheit ist. Inhalt, Gesamtmenge und gelieferte Gebinde
bleiben getrennt. Logistik- und Energiepauschalen erscheinen als Nebenkosten.
Gedruckte Positionen dürfen bei **0** beginnen. Eine Rechnungsposition oder ein
Lieferschein ohne Preise erzeugt keinen neuen Rechnungspreis.

Bei einer abweichenden OCR-Artikelnummer oder einer fälschlich erkannten
Nebenkostenposition kann der Admin die Auslese nach Originalprüfung ausdrücklich
mit Begründung korrigieren. Diese Begründung bleibt im Liefernachweis gespeichert.

## Daten und Schutz

Originale verwenden die vorhandene Belegsammlung
`order-receipts:<kanonischer Bestellschlüssel>`. Rechnungen und Lieferscheine
können dort gemeinsam liegen. Die neue Tabelle `assistent_bestelllieferungen`
speichert bestätigte Zuordnungen mit Bestellfingerprint, Originalhash, Seite,
gedruckter Position, Menge und Prüfvermerk.

Der Originalhash zusammen mit Seite und Position verhindert eine zweite Buchung
derselben Belegposition, auch über Bestell-Aliase und andere Uploadsammlungen.
Korrekturen bestehender Bestellidentitäten führen zum Prüfstatus; Liefernachweise
werden nicht auf eine veränderte Bestellung übertragen. Schreibaktionen sind
auf Admins beschränkt, CSRF-geschützt und mit dem Restore-Lock abgesichert.

Backups enthalten die neue Tabelle und Originale. Vorhandene bestätigte
Lieferungen einschließlich ihrer Originalzuordnung dürfen durch einen älteren
Import nicht verloren gehen. Alte Backups bleiben auf einem Ziel ohne bereits
bestätigte Liefernachweise kompatibel.

## Prüfung dieser Erweiterung

- 17 neue Liefer-/Analyseprüfungen, 15 Backupprüfungen, 25 bestehende
  Preisoberflächenprüfungen und 54 Einkaufseingangsprüfungen bestanden.
- Vollständiger Flow-Test, Python-Syntax und Diffprüfung bestanden.
- Echte Bildauslese des bereitgestellten Lieferscheins lokal geprüft;
  Belegnummer, Datum, Position 0, Artikel, ein Gebinde, 0,5-Liter-Inhalt und
  getrennte Logistikposition erkannt. Beschreibungen bleiben rohe OCR.
- Browseransicht bei Desktop, 390 und 320 Pixeln geprüft, ohne horizontalen
  Dokumentüberlauf. Breite Tabellen scrollen innerhalb ihres Bereichs.
- Allgemeiner Smoke-Test: 111 bestanden, zwei auch auf dem unveränderten
  Ausgangsstand vorhandene Fehler zu Cockpitmodulen und Mitarbeiter-Urlaub.
- PostgreSQL-Kompatibilität über den vorhandenen Adapter getestet; keine echte
  PostgreSQL-Testinstanz verwendet.

Die Grundfunktion wurde am 08.10.2026 mit `77f6e2eb` veröffentlicht. Ein echter
Originalupload wurde anschließend im Liveportal geprüft; der heruntergeladene
Beleg stimmt bytegenau mit der hochgeladenen Datei überein. Die Liveauslese
lieferte dabei keinen lesbaren Text; die manuelle Prüfung bleibt verfügbar.
Eine produktive Lieferbuchung erfolgte nicht.

## Upload unter Eingänge & Belege

Unter „Lieferung angekommen?“ kann der Admin den Lieferschein direkt als Foto oder PDF auswählen und einer gespeicherten Bestellung zuordnen. Die Bestellwahl hat keine Vorauswahl und bietet nur verifizierbare Bestellsnapshots; Alias-Schlüssel erscheinen einmal. Nach dem Speichern öffnet sich die Beleganalyse an der gewählten Bestellung. Upload und Analyse buchen weiterhin keine Lieferung. Fehler führen zurück zum Upload; eine neue Dateiauswahl ist dann erforderlich.

Nach einem Verbindungsabbruch kann der Browser auf der Aktionsadresse
`/admin/assistent-bestellungen/lieferung/beleg` stehen bleiben. Direkte GET- und
HEAD-Aufrufe dieser Adresse öffnen wieder das Uploadformular statt einer
405-Fehlerseite. Sie speichern keine Datei und wiederholen keine Buchung.
Analyse und Zuordnung sind weiterhin ausschließlich per CSRF-geschütztem POST
verfügbar. Die Wiederherstellung samt Adminschutz und unveränderten Daten ist
durch die nun 22 Lieferprüfungen abgedeckt. Eine 502-Antwort während eines
Serverausfalls wird dadurch nicht zu einem erfolgreichen Upload; erst der
sichtbar gespeicherte Beleg und sein Originaldownload bestätigen den Erfolg.
## Begrenzte Fotoauslese auf Render (08.10.2026)

Der Lieferschein-Upload speichert das Original; die Aktion „Beleg analysieren“
liest es anschließend als ungeprüften Vorschlag. Die Render-Standardkonfiguration
schaltet die allgemeine lokale OCR ab. Deshalb verwendet ausschließlich diese
explizite Adminaktion jetzt einen separaten lokalen Prozess. Es werden keine
Dokumente an externe KI-Dienste übertragen und keine Liefermengen automatisch
gebucht.

`werkstatt_belegauslese.py` importiert keine Portal-App. Der Prozess erhält keine
Portal-Zugangsdaten, höchstens 20 Sekunden Laufzeit, unter Linux 1 GiB Adressraum
und echte ONNX-SessionOptions mit je einem intra-/inter-op-Thread. Die gepinnte
RapidOCR-Version 1.2.3 ignoriert entsprechende Konstruktor-kwargs; ihre lokale
SessionOptions-Factory wird deshalb nur bei der Engineinitialisierung im
Wegwerfprozess angepasst. Fotos werden auf 1600 Pixel begrenzt; über 20 Megapixel
und PDFs über fünf Seiten bleiben zur manuellen Prüfung. Originalbytes bleiben
unverändert. Der Timeout beendet den Prozess und räumt ihn auf.

Nur eine Lieferscheinanalyse läuft gleichzeitig pro Portalworker. Weitere
Analyseanfragen erhalten sofort einen Hinweis. Nichtleere erfolgreiche Auslesen
werden wiederverwendet; Fehler, leere Ergebnisse und Zeitlimits bleiben erneut
versuchbar. „Zeitlimit“ wird sichtbar angezeigt. Ein Seitenlabel allein gilt
nicht als erfolgreiche PDF-Auslese. Der globale OriginalsLock schützt weiterhin
gegen paralleles Wiederherstellen; seine Wartezeit und Datenbankarbeit sind
nicht Teil des OCR-Prozesslimits.

Dateilisten, Upload-Rückgaben, Quellenprüfung und Analyse-Metadaten laden keine
Original-Base64-Daten mehr. Eine neue Analyse liest und verifiziert das Original
einmal. Downloads und bestätigte Lieferzuordnungen behalten die bisherige
Originalprüfung.

Verifikation: 26 Liefereingangtests, 54 Einkaufseingangtests und fünf
Prozesstests bestanden. Der echte Linuxlauf auf Render unter 1 GiB Adressraum
las den vorhandenen Beleg in 3,678 Sekunden (1289 Textzeichen, fünf Threads,
957808 KiB maximaler virtueller Adressraum). Der vollständige Aufruf mit
gefilterter Umgebung und Prozessbereinigung dauerte 3,876 Sekunden. Der Container hat 8 GiB RAM;
vorheriger Verbrauch etwa 1,4 GiB, keine OOM-Ereignisse. Dies ist ein Messwert
für diesen Beleg, keine Laufzeitgarantie für beliebige Dokumente.

## Automatische Zuordnung auf ausdrücklichen Nutzerauftrag

Die Aktion `lieferung/automatisch` kombiniert Upload, begrenzte Auslese und
Lieferzuordnung. Ohne Bestellauswahl bleibt das Original zunächst in einer
neutralen Eingangssammlung; eindeutig passende Positionen werden mitsamt
Original und Auslese bei den kanonischen Bestellungen abgelegt. Eine vorhandene
Bestellauswahl begrenzt den Abgleich auf diese Bestellung. Originale unklarer
Positionen bleiben erhalten. Unter **Eingänge & Belege → Lieferung angekommen?**
ist keine Bestellauswahl erforderlich: Foto/PDF wählen und einmal hochladen.
Eine optionale Bestellvorgabe bleibt unter einer aufklappbaren Zusatzoption.
Die Schaltfläche zeigt während des Aufrufs den laufenden Analysezustand.

Regelversion `automatic-v1` unterstützt die bekannte Top-Color-Tabelle mit
positiv belegtem Lieferantenkopf, Belegnummer und Datum, exakter Artikelnummer,
PPG-T/E-Produktcode, Variante/Farbe, Literinhalt und Bestelleinheit Stück/Gebinde.
Produkt- und Variantenangaben sowie die Beschreibung dürfen sich beim Inhalt
nicht widersprechen. Die gelieferte ganzzahlige Gebindezahl multipliziert mit
dem Gebindeinhalt muss der gedruckten Gesamtmenge entsprechen. Logistik wird
ausgeschlossen. Nur eine passende offene und nachweislich versandte Bestellung
wird gewählt; unklare Bestellungen, Mehrlieferungen und mehrdeutige Positionen
werden nicht automatisch gebucht.

Der Original-/Restore-Lock umfasst den abschließenden Abgleich, erneute
Restmengenprüfung und Buchung. `record(..., automatic=True)` prüft den positiven
Belegabgleich selbst erneut; ein HTTP-Formular kann diesen Modus nicht setzen.
Automatische Nachweise tragen `created_by=admin:auto`, Regelversion und
Dokumentidentität. SHA/Seite/Position und zusätzlich Lieferant/Belegnummer/Seite/
Position verhindern Wiederholungen, auch bei einem neuen Foto desselben
Papiers. Alte manuelle Nachweise ohne rekonstruierbare Belegnummer blockieren
eine unsichere automatische Folgebuchung. Teilweise zugeordnete Dateien
behalten die Aktionen für noch offene Positionen.

Es entstehen keine neuen Bestellungen, Rechnungsbuchungen oder Nachrichten.
Der unveränderliche Bestell-/Versandnachweis bleibt erhalten; abgehakt wird der
separate Lieferstatus. Rechnung und tatsächlicher Preis bleiben separat offen.
