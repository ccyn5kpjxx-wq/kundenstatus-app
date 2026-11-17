# Google-Ads-Messung im Shop

Implementierungsstand: 18. November 2026. Diese Dokumentation beschreibt die
Integration und die im Google-Ads-Konto gespeicherten Ziele. Eine erfolgreiche
lokale Prüfung ist noch kein Nachweis einer von Google empfangenen Conversion.

## Ziele und Werte

Die beiden neuen Website-Aktionen verwenden das bestehende Google-Tag
`AW-18349808556`. Die folgenden Tagkennungen sind öffentliche Konfiguration,
keine Zugangsdaten.

| Aktion im Google-Ads-Konto | Ereignisziel (`send_to`) | Zählmethode | Wert |
| --- | --- | --- | --- |
| Gärtner Shop – Kauf bezahlt (Bruttoumsatz) | `AW-18349808556/6QSkCKmYvLodEKy3761E` | Jede | Tatsächlich vollständig eingezogener Bruttobetrag in EUR; Standardwert 0 |
| Gärtner Shop – Anfrage gesendet | `AW-18349808556/lupOCKyYvLodEKy3761E` | Eine | 0; Anfragen erhalten keinen erfundenen Umsatzwert |

Beide Aktionen sind als primäre Conversions und als Kontostandardziele
gespeichert, mit 90 Tagen Klick-Conversion-Zeitraum. Der neue Kaufstandard wurde
von Google für alle vier vorhandenen Kampagnen übernommen. Automatisches
Tagging ist bereits aktiviert und wurde nicht geändert. Erweiterte Conversions
wurden bei der Einrichtung nicht aktiviert. Kampagnenbudgets und Gebotsstrategien
werden durch diesen Code nicht verändert.

Der Kaufwert enthält Material, gewählte Leistungen, Versand und Mehrwertsteuer,
soweit diese im tatsächlich bezahlten Gesamtbetrag enthalten sind. Er ist
**Bruttoumsatz, kein Gewinn und keine Marge**. Die Integration sendet ein natives
Google-Ads-`conversion`-Ereignis; sie richtet kein zusätzliches GA4-Kaufereignis ein.
Die bisherigen Formular-, Telefon- und WhatsApp-Ziele bleiben bestehen.

## Wann ein Kauf zählt

`GET /shop/api/v2/orders/<order_id>/measurement` ist eine lesende,
kundenbezogene Projektion. Der Server prüft den eigenen Sitzungsinhaber und
konfigurierten HTTPS-Ursprung. Er verwendet ausschließlich den gespeicherten
Bestell- und Zahlungsstand. Vom Browser gelieferte Preise, Modi oder zusätzliche
Abfrageparameter werden nicht als Nachweis angenommen.

`shop_measurement.purchase_projection` gibt nur dann einen Kaufnachweis aus,
wenn alle Bedingungen zusammen erfüllt sind:

- Produktionsmodus `live`, Bestellstatus `paid` und Währung EUR;
- vollständig gebundener, bezahlter Zahlungsversuch;
- dauerhaft abgeschlossener Einzugsauftrag mit Zustand `captured`;
- exakt übereinstimmende Bindung von Kunde, Bestellung, Angebot, Betrag,
  Zahlungsversuch, Checkout-Sitzung und Payment Intent;
- gespeichertes, verifiziertes `paid`-Ereignis für denselben Zahlungsversuch.

Eine erfolgreiche Rückkehr von Stripe, eine Kartenautorisierung oder eine
abgeschlossene Checkout-Sitzung allein genügen nicht. Testmodus, ungeklärte
Zahlungen und unvollständige Nachweise ergeben keinen Kauf. Die Messungsroute
ruft weder Stripe noch Lieferanten auf und legt keine Bestellung, Zahlung oder
Warteschlangenaufgabe an. Antworten tragen `Cache-Control: no-store`.

Das Ergebnis enthält ausschließlich `transaction_id`, `value` und `currency`.
Die stabile Transaktionskennung ist ein Hash mit eigenem Namensraum; interne
Bestell-, Kunden- und Stripe-Kennungen werden nicht an Google übergeben.
Google kann wiederholte Meldungen derselben Transaktion innerhalb derselben
Conversion-Aktion deduplizieren.

Der Browser merkt einen begonnenen Checkout nur nach passender Einwilligung.
Auf der Bestellstatusseite fragt `static/shop_v2/conversions.js` höchstens
60-mal im Abstand von fünf Sekunden nach dem Zahlungsnachweis. Die Abfragen
lösen keinen Einzug und keine Lieferantenbestellung aus.

## Wann eine Anfrage zählt

Der bestehende Anfrageablauf erstellt eine zufällige, einmalige Ereigniskennung
erst nach erfolgreicher Verarbeitung der Anfrage. Sie wird sitzungsgebunden
über die Weiterleitung zur Bestätigungsseite transportiert und dort einmalig
verbraucht. Eine URL wie `?gesendet=1` erzeugt ohne diesen Servernachweis keine
Conversion. Erneutes Laden der Bestätigung, Validierungsfehler und abgewiesene
Anfragen ergeben keine weitere Meldung. Die Wiederverwendung einer bereits
gespeicherten Reifenangebotsanfrage erzeugt ebenfalls keine neue Kennung.

Shop-Gesamtangebotsanfragen verwenden das neue Shop-Anfrageziel. Gewöhnliche
Website-Anfragen verwenden weiterhin das vorhandene Formularziel. Es werden
weder Anfrageinhalt noch Kontakt- oder Fahrzeugdaten in das Messungsereignis
übernommen.

## Einwilligung und Datenbegrenzung

Die gemeinsame Datei `static/google_ads_tracking.js` verwendet Basic Consent
Mode: Vor einer Zustimmung werden keine Google-Ressourcen geladen und keine
Google-Messungsanfragen gesendet. DNT und Global Privacy Control unterbinden
die Messung ebenfalls. Lokale Vorschauadressen und unbekannte Hosts sind
gesperrt.

Die bisherige Auswahl bleibt unter `gaertner_google_ads_consent_v1` erhalten.
Eine frühere Zustimmung nur zur Anfragemessung aktiviert die neue Kaufmessung
nicht. Im Shop ist dafür die aktualisierte Auswahl erforderlich; ihre Reichweite
wird mit `gaertner_google_ads_scope_v2` gespeichert. Eine alte Auswahl aus einem
anderen Browser-Tab erweitert diese Reichweite nicht automatisch.

Bei Ablehnung oder Widerruf werden wartende lokale Ereignisse verworfen und
Checkout-Zuordnungen entfernt. Ein noch ladendes Google-Tag darf diese
verworfenen Ereignisse später nicht nachsenden. Bereits rechtmäßig gesendete
Ereignisse werden dadurch nicht rückwirkend gelöscht.

Nach Zustimmung verwenden begrenzte lokale Zuordnungen einen Abgleichszeitraum
von 30 Tagen. Ältere Einträge werden beim Lesen nicht mehr berücksichtigt.
Das ist keine Zusage einer automatischen physischen Löschung nach 30 Tagen:
Local Storage besitzt keine automatische Ablaufzeit; in einem danach nicht mehr
benutzten Browser können alte Einträge bis zum Überschreiben oder manuellen
Löschen verbleiben.

Übermittelte Seitenadressen werden von Formular- und Suchparametern, Fragmenten
und der internen Bestellkennung bereinigt. Nur validierte Google-Klickparameter
werden nach Zustimmung zur Anzeigenzuordnung erhalten. Der Referrer wird auf
seinen Ursprung reduziert. Personalisierte Werbung und die Übermittlung von
Nutzerdaten für erweiterte Conversions bleiben deaktiviert. Die ergänzte
Datenschutzerklärung erläutert Zweck, Bruttowert und pseudonyme Kennungen.

Die Shop-CSP erlaubt die dokumentierten Google-Ads-Endpunkte einschließlich
der deutschen Google-Domain gezielt. Sie benötigt weder `unsafe-inline` noch
`unsafe-eval`. Die Einwilligungsoberfläche verwendet eine eigene lokale CSS-Datei.

## Grenzen und Betriebskontrolle

- Diese Implementierung ist browserbasiert. Wird die Statusseite vor dem späteren
  Einzug geschlossen oder läuft das begrenzte Abfragefenster ab, kann der Kauf
  in Google Ads fehlen. Es gibt keinen zusätzlichen serverseitigen Google-Upload.
- Ohne Einwilligung, bei Tracking-Schutz, blockiertem Google-Tag oder fehlender
  Anzeigenzuordnung kann ein echter Abschluss ungemessen bleiben. Eine
  Conversion-Anzahl von null beweist deshalb nicht, dass es keine Anfragen oder
  Verkäufe gab.
- Automatische Korrekturen für Erstattungen, Retouren oder spätere Stornierungen
  sind nicht Teil dieser Integration. Gemeldeter Bruttoumsatz ist nicht automatisch
  um diese Vorgänge bereinigt.
- Ein Tag-Callback bedeutet lokale Verarbeitung des Ereignisses, keinen
  Zustellnachweis und keine bestätigte Anzeigenzuordnung bei Google.
- „Warten auf Conversions“ ist nach der Einrichtung erwartbar, solange noch kein
  echter zuordenbarer Abschluss erfasst wurde. Für eine Prüfung werden keine
  künstlichen Live-Conversions erzeugt.

Prüfungen erfolgen mit temporärer synthetischer Datenbank, blockiertem Netzwerk,
simuliertem Zahlungsjournal und einem lokalen Google-Tag-Doppel. Wichtige
Regressionen liegen in `scripts/test_shop_measurement.py`,
`scripts/test_shop_ads_lead.py` und `scripts/test_shop_google_ads.js`.

## Primärquellen

- [Google: Basic Consent Mode](https://developers.google.com/tag-platform/security/concepts/consent-mode#basic-consent-mode)
- [Google: Transaktionskennungen gegen Doppelzählungen](https://support.google.com/google-ads/answer/6386790?hl=de)
- [Google: Transaktionsspezifische Conversion-Werte](https://support.google.com/google-ads/answer/13064208?hl=de)
- [Google: Websiteweite Tags und Anzeigenzuordnung](https://support.google.com/google-ads/answer/9148089?hl=de)
- [Google: CSP für Google-Ads-Tags](https://developers.google.com/tag-platform/security/guides/csp#google_ads)
