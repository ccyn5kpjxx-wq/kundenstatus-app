# MOS: Aufbewahrung angefangener Online-Buchungen

Stand 24.09.2026. Dieser Bericht ist **keine Löschfreigabe** und ändert keine Daten.

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
festlegen. Die aktuelle Datenschutzerklärung nennt dafür noch keine konkrete
Frist. Auch Sicherungskopien und etwaige rechtliche Sperrfälle sind zu regeln.

## Technische Vorprüfung ohne Löschung

`mos_signature_retention.py` klassifiziert gespeicherte Unterschriften und gibt
nur aggregierte Fallzahlen zurück, keine Namen, Bilder oder Buchungsnummern.
`scripts/report_mos_signature_retention.py` kann **nur** eine ausdrücklich
angegebene vorhandene SQLite-Kopie schreibgeschützt lesen. Beispiel nach
Festlegung eines fachlich begründeten Stichtags:

```powershell
python scripts/report_mos_signature_retention.py --sqlite-copy C:\sicher\mos-kopie.sqlite3 --signed-before-utc 2026-09-01T00:00:00Z
```

Ohne Stichtag weist der Bericht technisch isolierte Fälle gesondert aus, aber
nicht als altersmäßig geprüfte Löschfälle. Ein Stichtag ist eine Eingabe für
die Prüfung und noch keine Aufbewahrungsregel. Fehlende Tabellen oder ungültige
Zeitstempel lassen die Prüfung scheitern beziehungsweise markieren den Fall
für manuelle Sichtung. Der Bericht setzt keine Stripe-Schlüssel ein und führt
keine Datenbankänderung aus.

Nur Holds mit Status `released`, einer gültig datierten Unterschrift, ohne
Mietvorgang oder Zahlung und **ohne** Checkout-Session, Kautionsdatensatz,
Provider-Erzeugungsversuch, Webhook-Ereignis, Vertrag, Erstattung oder
Stornobuchung gelten als *technisch isoliert*. `released` allein genügt nicht:
ein späteres bezahltes Provider-Ereignis könnte eine manuelle Prüfung
auslösen. Bestätigte Buchungen, offene Reservierungen, `review`-Fälle und alle
Vorgänge mit Provider-/Finanzspur sind ausgenommen.

## Vor einer tatsächlichen Löschung erforderlich

1. Zweck und Frist für **nicht zustande gekommene** Buchungen rechtlich und
   betrieblich festlegen, inklusive Streit-/Nachweisfällen und Sicherungskopien;
   Datenschutzhinweis vor Livebetrieb entsprechend präzisieren.
2. Entscheiden, ob das gesamte isolierte Hold-Payload (auch Kontaktdaten) statt
   nur des Bildes zu löschen ist. Ein bloßes Entfernen des PNG lässt weitere
   personenbezogene Daten zurück und verändert den unterschriebenen Snapshot.
3. Für PostgreSQL eine transaktionale erneute Statusprüfung mit Zeilensperre
   und eine prüfbare Ausnahmekennzeichnung für konkrete Rechtsstreitigkeiten
   entwickeln. Kein Cleanup darf einen noch aktiven oder unklaren
   Karten-/Checkout-Vorgang berühren.
4. Erst nach dieser Entscheidung einen gesonderten, zunächst nur auf Testdaten
   erprobten Bereinigungsjob ergänzen und kontrolliert abnehmen. Der vorhandene
   Bericht hat absichtlich **keinen** `--apply`- oder Löschmodus.
