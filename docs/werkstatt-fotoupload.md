# Werkstatt: Fotos und interne Auftragsnummer

Stand: 08.10.2026, TomorrowWorks-Aufgabe 119. Geprüft auf Basis `370573dd`
einschließlich der PostgreSQL-Korrektur für die persönliche Auftragssuche.

## Bedienung

Auf jeder Fahrzeugkarte der Werkstatt-Tafel und im Kopf des geöffneten Auftrags
steht rechts oben die **reine Nummer, groß, fett und schwarz**. Diese Nummer ist
die bestehende interne Datenbank-ID. Neue Aufträge erhalten aufsteigende IDs;
vorhandene Nummern bleiben dauerhaft gleich, auch nach Statuswechseln.
Es werden keine Aufträge neu angelegt oder umnummeriert. Eine vorhandene
Autohaus-Referenz bleibt davon getrennt.

Im persönlichen Mitarbeiterkonto unter **Aufträge** dieselbe Nummer eingeben.
Der bestehende Mitarbeiterzugang prüft weiterhin Rechte, Auftragsstatus und
gegebenenfalls die Versicherungsfreigabe. Arbeiten, Termine, Statusschritte und
interne Fotos bleiben an demselben Auftrag.

An der Werkstatt-Tafel können mehrere Fotos wie bisher gemeinsam ausgewählt
werden. Die Originaldateien werden nacheinander einzeln übertragen. Fortschritt
und bestätigte Speicherung werden angezeigt. Bei einer Unterbrechung stoppt die
Serie. Bereits bestätigte Fotos nicht erneut auswählen. Ist die Speicherung des
letzten Fotos unklar, zuerst **Auftrag aktualisieren** und den Bestand prüfen.
Es gibt keine automatische Wiederholung. Der Kioskwechsel pausiert während des
Uploads. Ein einzelnes Foto muss unter dem bestehenden 25-MiB-Anfragelimit
einschließlich Formular bleiben; größere Einzeldateien werden vorher abgewiesen.

Fotos aus der gemeinsamen Werkstatt-Tafel bleiben entsprechend der bestehenden
Route auf Kunden- und Partnerstatusseiten sichtbar. Persönliche Mitarbeiterfotos
bleiben intern; dort gelten weiterhin maximal sechs JPEG-/PNG-Fotos mit jeweils
8 MiB und die bestehende Bildprüfung/Metadatenbereinigung. Dieser persönliche
Upload bleibt eine atomare, wiederholungssichere Anfrage.

## Technische Grenze

Das allgemeine 25-MiB-Anfragelimit bleibt bestehen. Nur der persönliche
Foto-POST erhält vor der globalen CSRF-Formularauslese ein Limit von 49 MiB
(sechs mal acht plus ein MiB Formularreserve). Rechte, CSRF, Bildanzahl,
Einzeldateigröße, Original-Backups und Idempotenz bleiben unverändert.

Die gemeinsame Tafel bestätigt einen Einzelupload erst nach erfolgreicher
Antwort auf der richtigen Auftragsseite mit auftragsspezifischem
Speichermarker. Loginseiten, allgemeine HTTP-200-Antworten, Warnungen,
Serverfehler und verlorene Antworten gelten nicht als Speicherbestätigung.
Warnungen haben auch gegenüber einem alten Erfolgs-Flash Vorrang.

## Prüfung

- `node --test scripts/test_werkstatt_fotoupload.js`
- Offline-Python-Suite: `test_werkstatt_fotoupload`, `test_mitarbeiter_auftraege`
  mit temporärer Datenbank, synthetischen Bildern und gesperrtem Netzwerk.
- Browserprüfung auf separatem lokalen Testserver; keine Kundenaufträge ändern.

Die Tests prüfen unter anderem Serien über 25 MiB mit bytegleichen Originalen,
Teilabbrüche ohne Wiederholung, spezifische Bestätigungen, sechs persönliche
Fotos über 25 MiB mit genau einem Audit auch nach Wiederholung, sowie Rechte,
CSRF, Sichtbarkeit und unveränderte Limits anderer Routen.

## Lokale Monatsablage: noch gesondert umzusetzen

Das vorhandene lokale Dateiarchiv vom 03.10.2026 enthält 557 nach SHA und Größe
geprüfte Originaldateien. Es belegt keine vollständige Monatsablage: Metadaten
zu Rückgaben fehlen, und reine Datenbank-Originale sind darin nicht vollständig
enthalten. Bestehende Dateien bleiben unverändert und werden nicht gelöscht.

Für eine vollständige Septemberkopie braucht es einen kleinen admin-geschützten
Metadatenexport von Aufträgen, Statusereignissen, Dateizuordnung sowie
Backup-Hash/-Größe. Keine Binärdaten oder Secrets im Export. Rückgaben anhand
`status_log.status=5` und geparstem Ereignisdatum auswählen; Dateizeitstempel und
`geaendert_am` eignen sich dafür nicht. Das Archiviert-Kennzeichen allein
belegt keinen Archivierungsmonat.

Anschließend vorhandene lokale Originale anhand Hash/Größe wiederverwenden und
fehlende Dateien seriell über den bestehenden admin-geschützten Download
`/admin/datei/<id>/download` beziehen. Diese Route kann valide Datenbankkopien
direkt liefern, ohne neue Dateien auf Render anzulegen. Ein vollständiger
ZIP-Export auf dem kleinen Render-Datenträger wird dafür nicht verwendet.
Dieser Export ist als TomorrowWorks-Aufgabe 121 vorgemerkt. `app.py` ist während
Aufgabe 119 bereits durch Aufgabe 112 belegt.
