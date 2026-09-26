# MOS: Aufbewahrung angefangener Online-Buchungen

Stand 24.09.2026. **Keine Löschfreigabe, kein produktiver Löschlauf.**

## Tatsächliche Speicherung

Schon nach der digitalen Unterschrift und vor der Zahlung enthält
`miet_checkout_holds.payload.quote.signature_png_base64` das gezeichnete PNG.
`signed_at` und die Hashes des unterschriebenen Vertragssnapshots stehen daneben.
Kundenname, E-Mail, Mietzeit und Tarif liegen ebenfalls im Hold-Payload. Erst bei
bezahlter und bestätigter Buchung wird zusätzlich ein dauerhaftes PDF samt
Unterschrift in `miet_checkout_contracts` gespeichert. Ein abgebrochener Hold
behält seinen Payload derzeit auch nach Status `released`.

Nach [Art. 5 Abs. 1 Buchst. e DSGVO](https://eur-lex.europa.eu/eli/reg/2016/679/)
ist die Aufbewahrung an den notwendigen Zweck zu binden. Die betroffene Person
muss über eine konkrete Frist oder die Kriterien ihrer Bestimmung informiert
werden ([Art. 13 Abs. 2 Buchst. a DSGVO](https://eur-lex.europa.eu/eli/reg/2016/679/)).
Die DSGVO gibt für diese abgebrochenen Buchungen keine pauschale feste Anzahl
von Tagen vor; der Betreiber muss eine begründete Regel für diesen Zweck
festlegen. **Vorschlag:** 30 Kalendertage ab der erstmaligen technischen
Feststellung eines bereits freigegebenen, vollständig providerunberührten
Holds. Das ist ein konfigurierbarer Prüfwert, keine gesetzliche Frist. Die
aktuelle Datenschutzerklärung nennt dafür noch keine konkrete Frist und wird
erst nach freigegebener und tatsächlich laufender Löschpraxis angepasst.

Vor einer Freigabe ist besonders zu klären, ob die elektronisch eingereichte
Unterschrift mit Buchungsdaten trotz ausbleibender Zahlung einen empfangenen
Handels-/Geschäftsbrief bildet. Für solche Unterlagen können die längeren
Pflichten aus [§ 257 HGB](https://www.gesetze-im-internet.de/hgb/__257.html)
und [§ 147 AO](https://www.gesetze-im-internet.de/ao_1977/__147.html)
gelten. Die technische Klassifizierung allein entscheidet diese Rechtsfrage
nicht. Auch Sicherungskopien und konkrete Rechtsstreitigkeiten sind zu regeln.

## Technische Vorprüfung und gesperrte Bereinigung

`mos_signature_retention.py` klassifiziert gespeicherte Unterschriften und gibt
nur aggregierte Fallzahlen zurück, keine Namen, Bilder oder Buchungsnummern.
`scripts/report_mos_signature_retention.py` kann **nur** eine ausdrücklich
angegebene vorhandene SQLite-Kopie schreibgeschützt lesen. Die zwei
Retention-Metadatentabellen müssen zuvor explizit und idempotent migriert
werden. Beispiel auf einer synthetischen oder kontrollierten Kopie:

```powershell
python scripts/run_mos_signature_retention.py --sqlite-db C:\sicher\mos-kopie.sqlite3 --migrate
python scripts/report_mos_signature_retention.py --sqlite-copy C:\sicher\mos-kopie.sqlite3 --signed-before-utc 2026-09-01T00:00:00Z
python scripts/run_mos_signature_retention.py --sqlite-db C:\sicher\mos-kopie.sqlite3
```

Ohne Stichtag weist der erste Bericht technisch isolierte Fälle gesondert aus,
aber nicht als altersmäßig geprüfte Löschfälle. Ein Stichtag ist eine Eingabe
für die Prüfung und noch keine Aufbewahrungsregel. Der zweite Befehl nach der
Migration ist ebenfalls ein **Trockenlauf**: Er nimmt den vorgeschlagenen
30-Tage-Wert, zeigt aggregierte Zahlen und ändert keine Daten. Die Migration
legt nur die Beobachtungs- und Sperrtabellen an. Fehlende Tabellen oder
ungültige Zeitstempel lassen die Prüfung scheitern beziehungsweise markieren
den Fall für manuelle Sichtung. Kein Bericht setzt Stripe-Schlüssel ein oder
gibt Namen, Bilder oder Buchungsnummern aus.

Nur Holds mit Status `released`, einer gültig datierten Unterschrift, ohne
Mietvorgang oder Zahlung und **ohne** Checkout-Session, Kautionsdatensatz,
Provider-Erzeugungsversuch, Webhook-Ereignis, Vertrag, Bestelleingangsbestätigung,
Vertragsversand, Schlüsselübergabe, Erstattung, Stornobuchung oder gesetzte
Rechtsstreit-Sperre gelten als
*technisch isoliert*. `released` allein genügt nicht:
ein späteres bezahltes Provider-Ereignis könnte eine manuelle Prüfung
auslösen. Bestätigte Buchungen, offene Reservierungen, `review`-Fälle und alle
Vorgänge mit Provider-/Finanzspur sind ausgenommen. Nur neue
Kreditkartenautorisierungs-Quotes sind technisch erfasst; historische oder
unlesbare Datensätze werden nicht automatisch bereinigt.

Die separate Bereinigungsfunktion unterstützt SQLite und PostgreSQL. Ein
echter Durchlauf benötigt gleichzeitig `--apply`, eine externe Policy-Datei
mit festgelegter Dauer, Geltungsbereich, Genehmigungszeit und
Freigabereferenz sowie `MOS_SIGNATURE_RETENTION_APPLY_ENABLED=1`. Ohne diese
Eingaben bleibt der Lauf lesend. Ein erster genehmigter Lauf markiert
geeignete Holds nur als erstmals freigegeben gesehen. Erst nach einer vollen
weiteren Policy-Frist und erneutem Prüfen aller Ausschlüsse innerhalb einer
Transaktion wird der gesamte isolierte Hold gelöscht. PostgreSQL sperrt die
betroffene Hold-Zeile; SQLite verwendet eine Schreibtransaktion. Eine
einzelfallbezogene Sperre wird in `miet_checkout_retention_blocks` gehalten und
bei jeder Entscheidung erneut geprüft. Der Operator kann sie ohne
Löschfreigabe über `--block-file` aus einer lokalen JSON-Datei mit `hold_id`
und begründetem `reason` eintragen; der Befehl gibt die Buchungsnummer nicht
aus. Solche Sperren müssen nach Abschluss des konkreten Falls manuell geprüft
und aufgehoben werden; sie laufen nicht automatisch ab. Es ist **kein**
produktiver Zeitplan oder Apply-Schalter konfiguriert und kein echter
Datensatz gelöscht worden.

Ein isolierter PostgreSQL-Test auf einer frisch erzeugten, synthetischen
Datenbank des lokalen MOS-Testclusters hat das Vormerken, die spätere Löschung
eines geeigneten Holds und den Erhalt von Provider-, Bestätigungs-, Übergabe- und Rechtsstreitfällen
geprüft (`scripts/check_mos_signature_retention_postgres.py`). Der Test
berührte keine bestehende Datenbank; der Cluster wurde danach gestoppt.

## Vor einer tatsächlichen Löschung erforderlich

1. Zweck und Frist rechtlich und betrieblich festlegen und vor allem die
   mögliche Handelsbrief-/Steuerpflicht des unterschriebenen, nicht bezahlten
   Angebots klären. Bei Pflichtaufbewahrung diese Datensätze aus dem
   30-Tage-Scope herausnehmen. Konkrete Rechtsfälle sperren; Sicherungskopien
   und Lösch-/Wiederherstellungsprozess dokumentieren.
2. Den vorgeschlagenen Wert und die Löschung des **gesamten** isolierten Holds
   freigeben oder anpassen; nur das PNG zu entfernen ließe weitere
   personenbezogene Daten zurück und veränderte den Snapshot.
3. Erst dann eine Policy-Datei außerhalb von Git dokumentieren, die
   Datenschutzerklärung präzisieren und den bereinigenden Lauf auf einer
   getrennten PostgreSQL-Testdatenbank mit synthetischen Fällen abnehmen.
4. Für produktiven Betrieb einen überwachten Zeitplan mit demselben
   autoritativen Datenbankziel einrichten. Ohne gemessene Laufhäufigkeit darf
   nicht zugesagt werden, dass jeder abgebrochene Datensatz genau am 30. Tag
   gelöscht ist.

## Nachtrag 26.09.2026 – rechtliche und technische Vorprüfung

**Die Zustimmung zu den Miet-Betriebsregeln ist keine Freigabe dieser
Aufbewahrungspolitik.** Der Vorschlag von 30 Tagen ist keine gesetzliche
Löschfrist. Bei einem genehmigten `--apply`-Lauf wird ein technisch geeigneter
`released`-Hold zunächst mit `first_seen_released_at` vorgemerkt. Ein späterer
Lauf löscht ihn nur, wenn **Unterschriftszeitpunkt, Hold-Ablauf und erstmalige
Beobachtung jeweils mindestens die gewählte Policy-Dauer zurückliegen** und
alle Provider-, Finanz- und Streitfall-Ausschlüsse in der Transaktion weiterhin
fehlen. Die erste Beobachtung setzt nicht voraus, dass die Unterschrift schon
30 Tage alt ist. Erfolgt sie erst 30 Tage nach Unterschrift und Hold-Ablauf,
entsteht faktisch ein weiterer 30-Tage-Zeitraum; bei zeitnaher Beobachtung
können beide Fristen weitgehend parallel laufen. Der tatsächliche Löschtermin
hängt zusätzlich vom genehmigten Laufplan ab. „Löschung nach 30 Tagen“ wäre
als pauschale Zusage daher falsch.

Das vor Stripe elektronisch eingereichte, unterschriebene Buchungsangebot
**könnte** trotz fehlender Zahlung ein empfangener Handels- oder Geschäftsbrief
sein. [§ 257 Abs. 1 Nr. 2, Abs. 2, 4 und 5 HGB](https://www.gesetze-im-internet.de/hgb/__257.html)
und [§ 147 Abs. 1 Nr. 2, Abs. 3 und 4 AO](https://www.gesetze-im-internet.de/ao_1977/__147.html)
sehen für solche Unterlagen grundsätzlich sechs Jahre ab Schluss des
Empfangsjahres vor. Das ist eine **offene rechtliche Einordnung des konkreten
Angebots**, keine Feststellung, dass jeder unbezahlte Hold sechs Jahre
aufzubewahren ist. Falls die Pflicht greift, darf der vollständige signierte
Nachweis nicht durch den derzeit vorbereiteten 30-Tage-Purge verloren gehen;
eine getrennte, unverändert lesbare Archivierung und das Löschen einer bloßen
Arbeitskopie müssten erst konzipiert und geprüft werden.

Stripe-berührte Vorgänge bleiben technisch vom isolierten Purge ausgeschlossen,
sind damit aber nicht automatisch unbegrenzt aufzubewahren. Für Angebote,
Kartenautorisierungen, Checkout-Sessions, Zahlungen, Erstattungen und
Stornierungen braucht es einen **eigenen** Fristen- und Löschplan nach ihrem
jeweiligen Zweck und ihrer rechtlichen Einordnung. Soweit Unterlagen tatsächlich
Buchungsbelege sind, nennen [§ 257 Abs. 1 Nr. 4 und Abs. 4 HGB](https://www.gesetze-im-internet.de/hgb/__257.html)
und [§ 147 Abs. 1 Nr. 4 und Abs. 3 AO](https://www.gesetze-im-internet.de/ao_1977/__147.html)
grundsätzlich acht Jahre. Ein bloßer Providerkontakt ist noch kein Nachweis
dafür, dass gerade ein Buchungsbeleg vorliegt.

Konkrete Rechtsstreitigkeiten müssen mit dokumentiertem Grund gezielt gesperrt
und nach Abschluss erneut geprüft werden; sie rechtfertigen keine pauschale
Langzeitspeicherung sämtlicher Fälle. Für Sicherungskopien fehlen noch eine
konkret dokumentierte Überschreibfrist und ein Verfahren, das Löschungen nach
einer Wiederherstellung erneut anwendet. Der [Europäische Datenschutzausschuss
im Bericht zur Löschung 2026](https://www.edpb.europa.eu/system/files/2026-02/edpb_cef-report_2025_right-to-erasure_en.pdf)
hebt getrennte, zweckbezogene Fristen und den Umgang mit wiederhergestellten
Backups hervor; [Art. 5 Abs. 1 Buchst. e und Art. 13 Abs. 2 Buchst. a
DSGVO](https://eur-lex.europa.eu/eli/reg/2016/679/oj/deu) verlangen
Speicherbegrenzung und transparente Fristen oder Bestimmungskriterien.

Vor einer produktiven Löschregel müssen eine qualifizierte rechtliche Prüfung
die Angebots- und Belegkategorien einordnen, der Betreiber daraufhin getrennte
Fristen sowie Backup- und Streitfallverfahren festlegen und die tatsächliche
Praxis mit dem Datenschutzhinweis abgleichen. **Dieser Nachtrag aktiviert weder
eine Löschung noch Live-Buchungen oder Zahlungen.**
