# Gärtner Mitarbeiter-App für iPhone

**Stand: 8. Oktober 2026. Entwicklungsprototyp, noch kein installierbares Release.** Ein signierter Gerätebuild, die iPhone-Abnahme und Apples Freigabe stehen noch aus. Es gibt noch keinen App-Store-Installationslink. Die vorhandene Web-App und das Live-Portal werden durch dieses Verzeichnis nicht verändert.

Die iOS-App verbindet eine native SwiftUI-Oberfläche mit dem bestehenden Mitarbeiterportal. Auftragsnummerneingabe und QR-Scan ergänzen den Portalzugang. Die bestehenden persönlichen Konten und serverseitigen Rechte bleiben maßgeblich; eine Auftragsnummer oder ein QR-Code erteilt keinen Zugriff.

Der Prototyp hält WebKit-Daten nur während der laufenden Sitzung im Speicher. Nach vollständigem Beenden oder einem Neustart der App ist eine erneute persönliche Anmeldung erforderlich. Ein dauerhaftes sicheres Anmelden ist vor einem Mitarbeiterrelease noch gesondert zu entscheiden und zu prüfen; es wird hier nicht durch eigenes Speichern von Passwörtern ersetzt.

## Was für die Veröffentlichung noch gebraucht wird

| Benötigt | Konkreter nächster Schritt |
| --- | --- |
| Apple Developer Program für die Firma | Vorhandene Mitgliedschaft und verfügbares Entwicklerteam bestätigen oder die Organisation registrieren. |
| Organisationsdaten | Rechtlicher Firmenname, D-U-N-S-Nummer, vertretungsberechtigte Person, geschäftliche E-Mail und Firmenwebsite bereithalten. |
| Team-ID und App-Kennung | Apples Team-ID bereitstellen; eine eindeutige Bundle-ID unter diesem Team registrieren und mit dem Projekt abgleichen. Keine Apple-Passwörter oder Signaturschlüssel ins Repository schreiben. |
| Mac mit Xcode | Für App-Store-Uploads mindestens Xcode 26 und iOS-26-SDK verwenden; XcodeGen installieren. Die Mindestversion der Mitarbeitergeräte bleibt iOS 17. |
| Signierung | In Xcode das bestätigte Team wählen, Provisionierung einrichten und zuerst einen signierten Build auf einem echten iPhone ausführen. |
| App Store Connect | App-Datensatz, Support-/Datenschutz-URL, App-Icon, Screenshots mit Beispieldaten, Datenschutzangaben und Review-Zugang vorbereiten. |
| Abnahme | Die unten genannten Geräteproben durchführen und mit Buildnummer, Gerät, iOS-Version und Ergebnis dokumentieren. |

Für die Registrierung als Organisation verlangt Apple unter anderem eine rechtlich selbstständige Firma, deren D-U-N-S-Nummer und eine vertretungsberechtigte Person. Ein vorhandenes privates Apple-Konto ersetzt die Firmenmitgliedschaft nicht. Siehe [Apple: Registrierung](https://developer.apple.com/programs/enroll/). Die passende Entwicklungsumgebung steht unter [Apple: Xcode-Systemanforderungen](https://developer.apple.com/xcode/system-requirements).

Seit dem 28. April 2026 verlangt Apple für neue Uploads Xcode 26 oder neuer mit dem iOS-26-SDK oder neuer. Ein erfolgreicher Simulatorbuild mit älteren Werkzeugen ist daher ausschließlich ein Entwicklungsnachweis, kein hochladbarer Releasebuild. [Apple: aktuelle Uploadvorgaben](https://developer.apple.com/news/upcoming-requirements/)

## Installationsweg für die Mitarbeiter

Vorgesehen ist eine **nicht gelistete App im App Store (Unlisted App)**. Das passt auch zu privaten Mitarbeiter-iPhones ohne Geräteverwaltung. Nach Apples Genehmigung wird der App-Store-Link verteilt; der Mitarbeiter öffnet ihn und bestätigt dort die Installation. Die App erscheint nicht in der App-Store-Suche. Jeder mit dem Link kann sie jedoch installieren: Zugang zu Firmendaten gibt weiterhin ausschließlich das eigene Mitarbeiterkonto. [Apple: Unlisted App Distribution](https://developer.apple.com/support/unlisted-app-distribution/)

Der Ablauf bis dahin:

1. Signierten Gerätebuild erstellen und abnehmen; eine separate TestFlight-Erprobung kann davor erfolgen.
2. Eine fertige Version zu App Review einreichen und in den Review Notes den geplanten Unlisted-Vertrieb erklären.
3. Den Unlisted-Antrag bei Apple stellen. Ein Entwicklungsprototyp oder eine Beta genügt dafür nicht.
4. Erst nach bestätigter Freigabe und Veröffentlichung den tatsächlichen App-Store-Link an die Mitarbeiter geben.

Eine reine Verpackung der Website garantiert keine App-Store-Zulassung. Native Auftragsnummerneingabe und QR-Scan müssen als nutzbare Funktionen geprüft und im Review erläutert werden; Apples Entscheidung bleibt offen. Für die Prüfung einen getrennten Testzugang mit Beispieldaten und Beispiel-QR-Code verwenden, keine echten Personalakten oder Mitarbeiterpasswörter. [Apple: Review Guidelines, insbesondere 2.1 und 4.2](https://developer.apple.com/app-store/review/guidelines/)

## Projekt auf einem Mac öffnen

Die Projektdefinition liegt in `project.yml`. Das Xcode-Projekt wird daraus erzeugt; die generierte Projektdatei ist kein veröffentlichter Gerätebuild. Auf einem Mac im Verzeichnis `native/ios`:

```sh
xcodegen generate --spec project.yml
```

Anschließend die erzeugte `.xcodeproj` in Xcode öffnen, das App-Scheme auswählen und unter **Signing & Capabilities** das eigene Team konfigurieren. Projekt-/Scheme-Name und Mindest-iOS-Version werden durch `project.yml` festgelegt. XcodeGen ist ein Entwicklungswerkzeug; [die Anleitung des Projekts](https://github.com/yonaskolb/XcodeGen) beschreibt Installation und Generierung.

Ein manueller GitHub-Workflow `iOS unsigned check` prüft Projektgenerierung und einen unsignierten Simulatorbuild, ohne Apple-Zertifikate, Portalzugänge oder Personaldaten zu erhalten. Er wird ausdrücklich manuell gestartet. Ein grüner Lauf ersetzt weder Code-Signing noch einen Gerätebuild, einen echten iPhone-Test oder App Review. Der Workflow veröffentlicht nichts.

Am 8. Oktober 2026 bestanden im [Mac-Prüflauf 37791327727](https://github.com/ccyn5kpjxx-wq/kundenstatus-app/actions/runs/37791327727) alle fünf Tests für Portalziele, erlaubte Links und Auftrags-/QR-Eingaben. Die App wurde unsigniert gebaut, im iPhone-16-Pro-Simulator gestartet und der generische persönliche Anmeldebildschirm visuell geprüft. Grundlage war Commit `56e8411f5c41d768f064debca4c1aff5a9c63fba`, Xcode 16.4 mit iOS-18.5-SDK. Dieser Lauf bestätigt den Prototyp, erfüllt aber nicht Apples aktuelle Uploadvorgabe. Er enthält weder eine Mitarbeiteranmeldung noch einen Mikrofon-, Kamera- oder Dokumenttest.

Der Workflow erzeugt außerdem einen Screenshot mit einer frischen Simulator-Sitzung ohne Mitarbeiteranmeldung. Dieser zeigt nur den generischen Einstieg und wird höchstens drei Tage als CI-Artefakt gespeichert. Es werden weder echte Mitarbeiterkonten noch Einrichtungslinks verwendet; Kamera und Mikrofon werden dabei nicht freigegeben.

Für ein Release danach in Xcode einen signierten Archivbuild erzeugen, validieren und nach App Store Connect hochladen. Siehe [Apple: Builds hochladen](https://developer.apple.com/help/app-store-connect/manage-builds/upload-builds/).

## Verbindliche Geräteproben vor Mitarbeiterfreigabe

Die folgende Liste ist eine **offene Abnahme**, kein bereits bestandenes Prüfprotokoll. Simulator- oder Quelltextprüfungen reichen für diese Fälle nicht.

Vor der Geräteabnahme sind zwei bekannte Funktionslücken zu schließen: Geschützte Attachment-Downloads (unter anderem Lohnzettel) werden im Prototyp mit einer sichtbaren Meldung blockiert; außerdem zeigt das bestehende Portal in der nativen App noch seine PWA-Installationskarte. Die native Ansicht darf später nicht erneut zur Installation derselben App auffordern. Ein geprüftes Dokumentverfahren und die bereinigte Ansicht stehen noch aus.

| Probe auf einem echten iPhone | Erwartetes Ergebnis |
| --- | --- |
| Anmeldung, Abmeldung, Benutzerwechsel | Zwei getrennte Testkonten nacheinander verwenden. Nach Abmeldung und Wechsel bleiben weder die vorherige Akte noch deren Dokumente oder Navigationsverlauf zugänglich; eine abgelaufene Sitzung verlangt eine Anmeldung. |
| Auftragsnummer | Einen erlaubten Auftrag öffnen. Ungültige, fremde und nicht vorhandene Nummern verständlich behandeln; keine Rechte umgehen. |
| Nativer QR-Scan | Kamera erlauben, ablehnen und später erneut aktivieren. Nur erlaubte Auftragsnummern bzw. bekannte Auftragslinks auswerten. Fremde URLs, abweichende Hosts, `javascript:`-/`file:`-Werte und andere QR-Inhalte dürfen keine Navigation auslösen. |
| Kamera und Upload | Neues Foto und vorhandenes Bild/Screenshot zu einem Testvorgang hinzufügen; Menge und Dringlichkeit bleiben dem jeweiligen Bild zugeordnet. Abbruch und erneuter Versuch erzeugen keine zweite Bestellung. Kein echter Testversand. |
| Mikrofon und Sprachgespräch | Berechtigung erlauben/ablehnen, Ton, WebRTC-Verbindungsaufbau, Unterbrechen, Neustart und Rückkehr aus dem Hintergrund prüfen. Ein funktionierender Safari-Test bestätigt die eingebettete Webansicht noch nicht. |
| Persönliche PDF-Anhänge | Einen eigenen Test-Lohnzettel mit Attachment-Antwort öffnen und eine fremde Dokument-ID ablehnen lassen. Keine privaten Dateien automatisch in Downloads, Dateien, Cache, Backups oder CI-Artefakten ablegen. Ist eine geschützte Anzeige noch nicht unterstützt, muss das erkennbar sein. |
| Hintergrund und App-Umschalter | Die App bei offener Test-Personalakte verlassen und das iPhone sperren. Der Vorschau-Screenshot darf keine privaten Inhalte zeigen; Kamera und Mikrofon dürfen nicht unbemerkt weiterlaufen. |
| Externe Links und neue Fenster | Erlaubte externe Links bewusst außerhalb des persönlichen Portalbereichs öffnen; fremde Seiten erhalten keine Portal-Cookies oder Zugangsdaten. Unbekannte Schemata und unbeabsichtigte Fenster blockieren. |
| Netzunterbrechung | Verständlichen Verbindungszustand zeigen. Nach Wiederkehr des Netzes keine Bestellung, Zeiterfassung oder andere Änderung ungefragt nachsenden. |

## Datenschutzgrenzen des Prototyps

- Keine privaten Offlinekopien: keine Personalunterlagen, Dokumentinhalte, Passwörter oder Einladungslinks in App-Dateien, Logs, Analytics, selbst erzeugte Screenshots, Keychain oder Build-Artefakte übernehmen. Für die Dokumentanzeige nur den geprüften authentifizierten Weg verwenden; kein automatischer Datei-Export.
- Authentifizierte Webinhalte dürfen nicht dauerhaft lokal zwischengespeichert werden. Eine Sitzung im Arbeitsspeicher ersetzt keine serverseitige Eigentümerprüfung. Abmeldung, Benutzerwechsel und Hintergrundverhalten müssen auf dem Gerät überprüft werden.
- QR-Codes sind ausschließlich Eingaben für die erlaubte Auftragssuche, keine Anweisung zum Öffnen beliebiger URLs. Kamera und Mikrofon werden nur für die ausdrücklich gestartete Funktion benötigt.
- Die App benötigt die vorhandenen Backend-Dienste und eine Internetverbindung. Es gibt keine Offline-Bestellwarteschlange und keine automatische Wiederholung von Schreibaktionen.

Diese Grenzen beschreiben die verlangte Freigabequalität. Solange Build und Geräteabnahme offen sind, ist keine vollständige Sicherheits- oder Funktionsfreigabe behauptet.
