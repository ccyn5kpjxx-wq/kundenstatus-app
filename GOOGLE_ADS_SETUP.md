# Google-Ads-API einrichten

Das Portal liest ausschließlich Berichtsdaten aus Google Ads und speichert die
Tageswerte in `google_ads_tageswerte`. Es verändert keine Kampagnen, Anzeigen
oder Budgets.

## Voraussetzungen

1. Die Google-Werbetreibendenprüfung ist abgeschlossen.
2. Ein Google-Ads-Verwaltungskonto (MCC) mit freigegebenem Developer Token ist
   vorhanden.
3. Im zugehörigen Google-Cloud-Projekt ist die Google Ads API aktiviert.
4. Ein OAuth-Client und ein Refresh-Token mit dem Scope
   `https://www.googleapis.com/auth/adwords` sind vorhanden.
5. Die zehnstellige Ads-Kundennummer des Kontos mit den drei Kampagnen ist
   bekannt. Bei Zugriff über ein MCC wird zusätzlich dessen Kundennummer als
   Login-Kundennummer benötigt.

## Geheimnisse hinterlegen

Lokal gehören die Werte nur in `.env.local`; live werden sie im geschützten
Render-Dienst als Secret-Umgebungsvariablen gepflegt:

```text
GOOGLE_ADS_DEVELOPER_TOKEN=
GOOGLE_ADS_CLIENT_ID=
GOOGLE_ADS_CLIENT_SECRET=
GOOGLE_ADS_REFRESH_TOKEN=
GOOGLE_ADS_CUSTOMER_ID=
GOOGLE_ADS_LOGIN_CUSTOMER_ID=
```

Bindestriche in den Kundennummern sind erlaubt und werden vor dem Abruf
entfernt. `GOOGLE_ADS_LOGIN_CUSTOMER_ID` bleibt leer, wenn kein MCC verwendet
wird. Zugangsdaten niemals in Git, Screenshots, Chat-Nachrichten oder die
öffentliche Homepage eintragen.

## Kampagnen zuordnen

Ohne weitere Konfiguration erwartet das Portal exakt diese Namen:

- `Lackierzentrum | Suche | Test 2026-07`
- `Tomorrowworks | Suche | Test 2026-07`
- `Autovermietung MOS | Suche | Test 2026-07`

Stabiler ist die Zuordnung über die numerischen Kampagnen-IDs:

```text
GOOGLE_ADS_CAMPAIGN_ID_AUTO_LACKIERZENTRUM=
GOOGLE_ADS_CAMPAIGN_ID_TOMORROWWORKS=
GOOGLE_ADS_CAMPAIGN_ID_AUTOVERMIETUNG_MOS=
```

## Prüfen

Nach einem Neustart zeigt `/admin/besucherstatistik` den Zustand
`Google-Ads-API bereit` oder bereits den Zeitpunkt des ersten erfolgreichen
Imports. Mit `Jetzt synchronisieren` kann der erste Abruf bewusst ausgelöst
werden. Danach prüft der Portalprozess einmal täglich die letzten 90 Tage
erneut, damit nachträglich zugeordnete Conversions aktualisiert werden.

Bei einem Fehler bleibt die manuelle Erfassung verfügbar. Die Fehlermeldung im
Admin-Dashboard enthält keine hinterlegten Tokens. Google-Request-IDs dürfen
für die Fehlersuche verwendet werden.
