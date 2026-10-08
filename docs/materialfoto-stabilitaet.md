# Begrenzte Verarbeitung von Materialfotos

Die Bildnormalisierung und lokale QR-/EAN-/UPC-Auslese laufen in einem eigenen, kurzlebigen Python-Prozess. Ein HTTP-Thread wartet höchstens drei Sekunden auf diese Bildphase; weitere Bildanfragen erhalten bei belegtem Prozessplatz sofort den bestehenden Fehlerpfad mit der Bitte, es erneut zu versuchen. Die Zulassung wartet nicht auf eine Semaphore. Bei der aktuellen Konfiguration mit einem Worker und vier HTTP-Threads beansprucht diese Bildphase damit höchstens einen HTTP-Thread gleichzeitig.

`werkstatt_fotoauslese.py` lädt im Kindprozess weder `app.py` noch andere Portalmodule. Nur eine kleine Liste benötigter Systemvariablen wird vererbt; Portal-, Datenbank- und API-Zugangsdaten werden nicht übernommen. Unter Linux gilt vor den nativen Imports zusätzlich eine Grenze von 1 GiB virtuellem Adressraum. OpenMP, OpenBLAS, MKL und OpenCV werden auf einen Rechenthread begrenzt.

## Verhalten bei Zeitüberschreitung und Fehlern

- Die bereits bestehenden Grenzen bleiben erhalten: maximal 8 MiB Upload, ein JPEG-/PNG-/WebP-Einzelbild mit höchstens 20 Millionen Pixeln, JPEG-Normalisierung mit längster Seite 2048 Pixel und Qualität 90. EXIF-Orientierung wird angewendet; Metadaten werden entfernt.
- Normalisierung und Codeauslese eines `stage`- oder `preview`-Aufrufs teilen sich einen Kindprozess und ein Zeitbudget. QR-/Barcode-Auslese verwendet weiterhin die Originalpixel, ausgerichtet und auf höchstens 2560 Pixel verkleinert.
- Das fertige JPEG wird vor der optionalen Codeauslese atomar geschrieben. Überschreitet erst der Decoder die Frist, bleibt dieses Bild nutzbar. Die Codeauslese meldet `nicht_verfuegbar`; der bestehende manuelle bzw. explizite Etikett-Fallback bleibt erhalten. Ist noch kein vollständiges JPEG vorhanden, wird die Anfrage ohne Speichern abgelehnt.
- Der Elternprozess beendet und reapet den Kindprozess vor Tempdatei-Bereinigung und Freigabe des Prozessplatzes. Ein Fehler im optionalen Decoder oder dessen Ergebnisdatei erzeugt keine Code-Evidenz.
- Fehler beim Prozessstart oder temporären Datei-I/O werden über den bestehenden Foto-Fehlerpfad abgefangen. Interne Betriebssystemdetails werden nicht an den Browser zurückgegeben; der Prozessplatz wird auch in diesen Fällen freigegeben.
- Bestehende serverseitige Regeln zu zulässigen Artikelcodes, GTIN-Prüfziffern, Mehrdeutigkeit, Quellenbindung, Rechten und Bestellfreigabe bleiben maßgeblich. Erkennung löst keine Bestellung aus und übernimmt keine Menge oder Preise.
- Die Originalbytes des Aufrufers werden nicht verändert. Der Materialbestellungsprozess speichert die Originalbelege weiterhin über seinen bestehenden Pfad; seine Original- und Replay-Prüfungen sind Teil der Regression.

Die Begrenzung gilt für die native Bildphase. Datenbankzugriffe, Katalogsuche und die optional ausdrücklich angeforderte Vision-Auslese haben ihre eigenen Laufzeiten und Schutzmechanismen. Bei mehreren Server-Workern existiert je Worker ein Prozessplatz. Die vorgelagerte Batch-Bildvalidierung ist ein eigener Normalisierungsaufruf; die Transaktions- und Lock-Grenzen der Batch-Übernahme werden gesondert bearbeitet.

## Reproduzierte Belastung

Ein vollständig synthetisches, gültiges PNG mit 256 QR-Feldern auf 2560 × 2560 Pixeln überschritt im bisherigen synchronen Decoder ein externes Limit von sechs Sekunden. Bereits 64 QR-Felder benötigten lokal rund 2,73 Sekunden für den Decoder; zufällige 2560-Pixel-Bilddaten rund 2,23 Sekunden. Das zeigt ein messbares Auslastungsrisiko der bisherigen Bildphase. Es belegt keine konkrete Ursache eines früheren Produktionsausfalls.

Der neue Regressionstest verarbeitet das dichte 256-QR-Bild mit dem realen Kindprozess und dem Drei-Sekunden-Budget. Weitere Tests erzwingen einen 30 Sekunden schlafenden Kindprozess, prüfen dessen Beendigung, das Aufräumen temporärer Dateien, die anschließende Wiederverwendung des Prozessplatzes und freie Kapazität in einem Pool mit vier Threads. Ein separater Fall schreibt zuerst ein vollständiges JPEG und bleibt danach hängen: Das JPEG bleibt erhalten, die Codeerkennung fällt sicher zurück.

## Prüfung ohne Produktivdaten

Im Repository-Root:

```text
python -m py_compile werkstatt_materialfoto.py werkstatt_fotoauslese.py scripts/test_materialfoto_bounds.py scripts/test_artikelscan_vorschau.py
python scripts/test_materialfoto_bounds.py -v
python scripts/test_artikelscan_vorschau.py -v
python scripts/test_materialfoto.py -v
python scripts/test_materialbestellung_portal.py -v
python scripts/test_materialfoto_codes.py -v
git diff --check
```

Die Tests verwenden synthetische Bilder und isolierte Datenbanken. Die Prozesssuite umfasst reale QR-Auslese, kaputte und übergroße Bilder, Timeout, Tempdateien, Prozess-Reaping, Parallelitätsgrenze und den Ausschluss von App-Importen und Zugangsdaten im Kindprozess. Der Endpoint-Test prüft zusätzlich die sofortige Ablehnung bei belegtem Prozessplatz ohne Lookup, Datenbankänderung oder Bestellung sowie die erfolgreiche nächste Anfrage.

Lokal am 8. Oktober 2026 bestanden neun Prozessgrenzen-Tests (6,680 s), 15 Artikelscan-Vorschautests einschließlich Browserharness (22,497 s), 17 Materialfototests (22,126 s) und 51 Materialportaltests (87,751 s), außerdem Syntax- und Whitespace-Prüfung.

Ein isolierter Test im echten Render-Linux mit der Python-Produktionsruntime und OpenCV 4.11 bestand ebenfalls: Der Kindprozess erkannte den synthetischen QR-Code `ART-2026-4711` unter der 1-GiB-Grenze und dem Drei-Sekunden-Budget in 0,667 Sekunden; das bereinigte JPEG umfasste 8395 Bytes. Dieser Test verwendete keine Kundendaten und änderte keinen Dienst. Der dichte QR-Fall wird zusätzlich bei der Integration unter Linux geprüft.
