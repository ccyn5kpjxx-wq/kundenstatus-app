# Büro-Cockpit und Chef-Freigaben

Der persönliche Bürozugang liegt unter `/werkstatt/buero`. Die Werkstattleitung
vergibt die Bereiche unter `/admin/buero-zugaenge` für einen bereits vorhandenen
persönlichen Mitarbeiterzugang. Es wird kein Mitarbeiter automatisch
freigeschaltet und keine allgemeine Admin-Session erzeugt. Die neue Rolle
schaltet den bisherigen Assistenten und dessen Bestellaktionen nicht frei.

## Tagesgeschäft

Heute zeigt die offenen Arbeitszeitprüfungen, geplante Hol-/Bringfahrten,
Bürotermine und freigegebene Zahlungen. Fahrten stammen aus den bestehenden
Fahrzeugterminen; fehlende Adressen oder Uhrzeiten bleiben sichtbar. Bürotermine
werden im bestehenden Kalender als gemeinsame Büroeinträge gespeichert.
Arbeitszeiten werden geprüft oder mit einer Rückfrage markiert. Eine Änderung
der ursprünglichen Zeitbuchungen macht die Prüfung erneut erforderlich.

Abwesenheiten, Mahnungen, News, Bewerbungen, Lieferanten, Portalpost und
Autohauszugänge werden auf eigenen Seiten mit begrenzten Feldern dargestellt.
Bewerbungseinladungen werden als Entwurf für das Mailprogramm vorbereitet.
Das allgemeine geschäftliche Mailpostfach ist für den Bürozugang lesbar;
ein als Geschäftsführer-Postfach konfiguriertes Konto bleibt gesperrt.

## Rechnungen, Gebühren, private Belege und Zahlungsaufträge

Im Chef-Cockpit führt **Rechnungen zur Freigabe** nach
`/admin/buero/belege`. Der Bürozugang nutzt `/werkstatt/buero/belege`.

1. Original erfassen. Lokale Texterkennung schlägt Dokumentart und Ordner vor.
   Lieferant, Nummer, Datum und Betrag werden am Original geprüft.
2. Zur Chefprüfung einreichen. Ungeklärte und private Originale bleiben bis zur
   Freigabe auf Chef und erfassende Person begrenzt.
3. Chef bestätigt die geschäftliche oder private Zuordnung ausdrücklich.
   Bescheide und Zahlungsaufträge benötigen eine eigene Zahlungsbestätigung.
4. Das Büro erhält die freigegebene Zahlungsaufgabe. Betriebliche Rechnungen
   und passende Bescheide können mit ihrem Original an Lexware übergeben werden.
   Lexware bleibt für Erfassung, Kontierung und persönliche Bankfreigabe zuständig.
5. Eine tatsächlich extern ausgeführte Zahlung wird mit Datum und Referenz
   dokumentiert. Ein Lexware-Status allein schließt diese Aufgabe nicht.

**Neu**, **zur Freigabe offen** und **zur Zahlung offen** sind getrennt.
Das Öffnen eines Belegs entfernt nur dessen Neu-Markierung für diese Person.
Es erteilt keine Freigabe und bestätigt keine Zahlung.

Miete und Kaution sind eigene Zahlungsaufträge auf Basis einer geprüften
Originalunterlage. Betrag, Zahlungsempfänger, Fälligkeit und Verwendungszweck
sind erforderlich. Dieselbe Mietunterlage darf beide unterschiedlichen Aufträge
belegen. Eine monatliche Mietvorlage erzeugt keine automatischen Folgezahlungen.
Private Belege und Kautionen werden nicht automatisch als GmbH-Rechnung gebucht.

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
ausdrücklich gestarteten Lauf. Das bestätigte Geschäftsführer-Konto muss mit
dem vorhandenen verschlüsselten IMAP-Zugang übereinstimmen. Ein unvollständiger
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
