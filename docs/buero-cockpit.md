# Büro-Cockpit und Chef-Freigaben

Der persönliche Bürozugang liegt unter `/werkstatt/buero`. Die Werkstattleitung
vergibt die Bereiche unter `/admin/buero-zugaenge` für einen bereits vorhandenen
persönlichen Mitarbeiterzugang. Es wird kein Mitarbeiter automatisch
freigeschaltet und keine allgemeine Admin-Session erzeugt. Die neue Rolle
schaltet dessen Bestellaktionen nicht frei. Eigene persönliche Arbeitszeit und
Urlaubsanträge sind über exakt erlaubte persönliche Endpunkte erreichbar;
fremde Konten und Chefentscheidungen bleiben gesperrt.

## Tagesgeschäft

Heute zeigt die offenen Arbeitszeitprüfungen, geplante Hol-/Bringfahrten,
Bürotermine und freigegebene Zahlungen. Fahrten stammen aus den bestehenden
Fahrzeugterminen; fehlende Adressen oder Uhrzeiten bleiben sichtbar. Bürotermine
werden im bestehenden Kalender als gemeinsame Büroeinträge gespeichert.
Arbeitszeiten werden geprüft oder mit einer Rückfrage markiert. Tatsächlich
abweichende Von-/Bis-Zeiten können mit Begründung als getrennte Korrektur
gespeichert werden. Rohbuchungen bleiben erhalten. Erst ausdrücklich geprüfte,
weiterhin zur Quelle passende Korrekturen beeinflussen berechnete Tages- und
Monatssummen; Pausen und Schichtlücken bleiben berücksichtigt. Neue Stempel
machen eine frühere Prüfung und Korrektur erneut prüfpflichtig.

Der vorhandene persönliche Urlaubsantrag bleibt die Quelle. Der Chef sieht
offene Anträge unter **Urlaub zur Freigabe**. Nach Genehmigung erscheint der
zugehörige Kalendereintrag im Büro mit bestätigtem Status und gespeicherten
Urlaubstagen. Ältere Kalendereinträge ohne Antrag werden entsprechend bezeichnet.

Abwesenheiten, Mahnungen, News, Bewerbungen, Lieferanten, Portalpost und
Autohauszugänge werden auf eigenen Seiten mit begrenzten Feldern dargestellt.
Bewerbungseinladungen werden als Entwurf für das Mailprogramm vorbereitet.
Das allgemeine geschäftliche Mailpostfach ist für den Bürozugang lesbar;
ein als Geschäftsführer-Postfach konfiguriertes Konto bleibt gesperrt.

Unter **Preise** steht die interne Nettopreisliste vom 30.10.2026 direkt als
Übersicht mit Neu-/Gebrauchtteilen, Partner-/Privatpreisen, Montage,
Beilackierung und Dreischichtzuschlag. Instandsetzungsrichtwerte sind getrennt
als noch abzustimmen gekennzeichnet. Die unveränderte Original-PDF wird über
eine geschützte Route bereitgestellt; sie liegt nicht im öffentlichen
Static-Verzeichnis. Der frühere Lexware-Hinweis im Original ist ein historischer
Dokumentstand, keine live abgefragte Bestandsanzeige. Der eigene Bereich
`preise` muss für den persönlichen Zugang freigegeben sein.

Die Übersicht enthält außerdem die bereits abgeglichenen 18 Reifenservice- und
zwei Achsvermessungspositionen mit ihrer jeweiligen Einheit, Netto- und
Bruttopreis. Reifenpreise behalten ihre führende Bruttobasis; Achsvermessung
ihre führende Nettobasis. Lackierpreis-Aufschläge werden darauf nicht angewandt.
Die eigene Reifen-/Achsvermessungs-PDF bleibt ebenfalls unverändert geschützt
abrufbar. Sprunglinks führen direkt zu Lackierung, Reifen und Achsvermessung.

## Rechnungen, Gebühren, private Belege und Zahlungsaufträge

Im Chef-Cockpit führt **Rechnungen zur Freigabe** nach
`/admin/buero/belege`. Der Bürozugang nutzt `/werkstatt/buero/belege`.

1. Datei auswählen und analysieren. Lokale Erkennung und optional der vorhandene
   KI-Zugang füllen belegte Hauptdaten vor; unklare Werte bleiben offen. Art und
   Ordner werden vorgeschlagen. Die kurze Prüfansicht zeigt die erkannten Daten;
   längere Hinweise und die manuelle Erfassung sind aufklappbar.
2. Zur Chefprüfung einreichen. Ungeklärte und private Originale bleiben bis zur
   Freigabe auf Chef und erfassende Person begrenzt.
3. Chef bestätigt die geschäftliche oder private Zuordnung ausdrücklich.
   Bescheide und Zahlungsaufträge benötigen eine eigene Zahlungsbestätigung.
   In seiner eigenen Ansicht kann er auch einen erfassten Scan direkt prüfen
   und freigeben. Belegart, Ordner und Originalprüfung werden dabei zusammen
   mit der Entscheidung gespeichert; eine Einreichung an sich selbst entfällt.
4. Das Büro erhält die freigegebene Zahlungsaufgabe. Betriebliche Rechnungen
   und passende Bescheide können mit ihrem Original an Lexware übergeben werden.
   Lexware bleibt für Erfassung, Kontierung und persönliche Bankfreigabe zuständig.
5. Eine tatsächlich extern ausgeführte Zahlung wird mit Datum und Referenz
   dokumentiert. Ein Lexware-Status allein schließt diese Aufgabe nicht.

Der Chef kann in `/admin/buero/belege/<id>` eine bereits freigegebene Zahlung
über **Zahlung als erledigt dokumentieren** nachtragen. Dafür prüft er die
tatsächliche Ausführung anhand eines Zahlungsnachweises und bestätigt Datum
und Referenz. Der Nachtrag ist direkt nach der Freigabe oder nach einer
Büro-Bearbeitungsübernahme möglich; eine erneute Freigabe ist nicht nötig.
Bereits dokumentierte Zahlungen bieten kein weiteres Zahlungsformular.
Der Änderungsverlauf erfasst den Chef als handelnde Person. Original,
Freigabe und Lexware-Verknüpfung bleiben erhalten; es wird keine Überweisung
ausgelöst. Der persönliche Bürozugang behält seinen eigenen Nachtragsweg.

**Neu**, **zur Freigabe offen** und **zur Zahlung offen** sind getrennt.
Das Öffnen eines Belegs entfernt nur dessen Neu-Markierung für diese Person.
Es erteilt keine Freigabe und bestätigt keine Zahlung.

Miete, Kaution und private Einzelzahlungen sind eigene Zahlungsaufträge auf
Basis einer geprüften Originalunterlage und ausdrücklichen Zahlungsabsicht.
Der positive Betrag und die Chefprüfung bleiben erforderlich. Alle fünf
Überweisungsangaben stehen bei jedem Beleg sichtbar, auch wenn sie fehlen.
„Nicht angegeben“ verhindert die Freigabe nicht; die Angaben können außerhalb
des Portals vor der tatsächlichen Überweisung geklärt werden. Vorhandene IBAN/BIC
werden weiterhin validiert und ausschließlich aus belegten Quellen übernommen;
es gibt keine Ableitung aus der IBAN. Die Chefansicht kann Überweisungsdaten und
Freigabe atomar mit derselben Revision speichern. Leere Rechnungsnummer und
Belegdatum sind bei Zahlungsaufträgen keine offenen Pflichtangaben.
Private Zahlungsaufträge bleiben privat und dürfen keine monatliche Mietvorlage
sein. Die Freigabe öffnet die Büro-Bearbeitungsaufgabe auch bei fehlenden
Überweisungsdaten; eine tatsächliche Zahlung erfordert weiterhin eine separate
Bestätigung mit Datum und Referenz. Dieselbe Mietunterlage darf beide unterschiedlichen Aufträge
belegen. Eine monatliche Mietvorlage erzeugt keine automatischen Folgezahlungen.
Private Belege und Kautionen werden nicht automatisch als GmbH-Rechnung gebucht.
Jeder sichtbare Beleg bietet zusätzlich „In Lexware ablegen“, unabhängig von
Art und Zahlungsfreigabe. Die ausdrückliche Ablage erstellt nur einen Prüfbeleg
mit gekennzeichneter Prüfkopie und erhält Original, Freigabe und Zahlungsstatus.
Anschließend öffnen „In Lexware öffnen“ und „Überweisungen in Lexware“ die
konkrete Lexware-Erfassung bzw. den Zahlungsbereich. Steuerliche Zuordnung und
tatsächliche Bankfreigabe erfolgen dort. Miete und Kaution bleiben auch mit
demselben Vertragsoriginal zwei getrennte Prüfbelege und Zahlungszwecke.

## Monatliche Löhne

**Löhne zur Freigabe** führt den Chef nach `/admin/buero/loehne`. Die
Monatsauswertung bleibt bis zur Freigabe Chef-only. Erst danach sieht ein
Bürozugang mit dem zusätzlichen Bereich **Löhne** die Zahlungsaufgabe.
Das Portal berechnet keine Löhne und führt keine Überweisung aus.

Einzellohnzettel werden als einzelne PDF aufgenommen, am Original geprüft und
mit einer stabilen Mitarbeiter-ID und dem Monat verbunden. Erst nach
Chef-Freigabe und ausdrücklicher Bereitstellung gelangen sie in das bestehende
persönliche Mitarbeiterkonto. Dort kann jeder nur seine eigenen Lohnzettel
herunterladen. Eine Gesamtauswertung wird nicht an Mitarbeiter verteilt.
Der Quellenname Katharina Seber ersetzt keine Prüfung der tatsächlichen
Absenderadresse oder der Originalauswertung.

## Mailabgleich und Betriebsstand

Die Chefseite `/admin/buero/posteingang-abgleich` prüft ihre Quellen vor einem
ausdrücklich gestarteten Lauf. Die bestätigte Geschäftsführer-Adresse muss mit
dem vorhandenen verschlüsselten IMAP-Zugang oder dessen ausdrücklich gesetzter
`OFFICE_CEO_MAILBOX_USER`-Zuordnung übereinstimmen. Ohne diese Zuordnung bleibt
die strenge Adressgleichheit bestehen. Ein bestätigtes gemeinsames Chefpostfach
bleibt für den allgemeinen Büro-Mailboxzugang gesperrt. Ein unvollständiger
Altbestand oder ein anderer Scan ohne verifizierte Rechnungsidentität sperrt
die automatische Neuanlage. Details stehen in [buero-posteingang.md](buero-posteingang.md).

Die im Chat eingerichteten täglichen und monatlichen Aufgaben ersetzen weder
die Bereitstellung dieses Codes noch den verifizierten Kontozugriff. Ein
erfolgreicher produktiver Mailabgleich oder Dokumentimport ist damit nicht
belegt. Echte Rechnungen, Lohnunterlagen, Mietwerte und Zahlungsempfänger sind
keine Testdaten und werden nicht mit dem Quellcode ausgeliefert.

Sicherungen enthalten Originale, Freigaben, Prüfnachweise und persönliche
Zuordnungen. Der Restoreguard verweigert deren Verlust, Rücksetzung oder eine
andere Person hinter derselben Mitarbeiter-ID. Ein aktueller vollständiger
Sicherungsstand ist für die Wiederherstellung erforderlich.

## Prüfung

`python -m unittest discover -s scripts -p 'test_buero_*.py' -q` prüft die neuen
Module und die vollständige App mit synthetischen Daten. Der Integrationstest
sperrt Provider, echte Datenbanken und Hintergrundarbeit vor dem App-Import.
Die bestehende Smoke- und Ablaufprüfung bleibt zusätzlich erforderlich.

Die lokale Demo verwendet ausschließlich erfundene Daten und einen sichtbaren
Vorschauhinweis. Sie ist kein Nachweis eines Liveimports oder einer Bankzahlung.
