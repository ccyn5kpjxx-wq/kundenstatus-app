# WhatsApp-Anbindung fuer den Auftragschat

Die App kann Portal-Chatnachrichten per WhatsApp an die Werkstatt senden und Antworten aus WhatsApp wieder als Werkstatt-Antwort im Auftragschat speichern.

## Ablauf

1. Autohaus schreibt im Portal-Chat eines Auftrags.
2. Die App sendet eine WhatsApp-Nachricht an die Werkstattnummer.
3. Die Werkstatt antwortet direkt in WhatsApp.
4. Der Webhook `/webhooks/whatsapp` nimmt die Antwort entgegen und speichert sie im passenden Auftragschat.

Die Zuordnung erfolgt zuerst ueber die WhatsApp-Antwortreferenz. Zusaetzlich steht in jeder Nachricht eine Kennung wie `#A38`; wenn diese Kennung in der Antwort enthalten ist, kann die App den Auftrag ebenfalls zuordnen. Ohne Antwortreferenz oder Kennung nutzt die App den letzten WhatsApp-Hinweis dieser Werkstattnummer innerhalb von 48 Stunden.

## Voraussetzungen

- WhatsApp Business Platform / Cloud API bei Meta.
- Eine oeffentlich erreichbare HTTPS-Adresse fuer diese Flask-App, zum Beispiel die Render-URL.
- Eine WhatsApp-Business-Telefonnummer in Meta.
- Einen permanenten System-User-Access-Token mit `whatsapp_business_messaging`.
- Optional, aber empfohlen: `WHATSAPP_APP_SECRET`, damit eingehende Webhooks signiert geprueft werden.

## `.env.local`

```env
WHATSAPP_ENABLED=true
WHATSAPP_ACCESS_TOKEN=EA...
WHATSAPP_PHONE_NUMBER_ID=123456789012345
WHATSAPP_VERIFY_TOKEN=ein-langes-geheimes-token
WHATSAPP_APP_SECRET=meta-app-secret
WHATSAPP_WORKSHOP_NUMBER=+491701234567
WHATSAPP_GRAPH_VERSION=v25.0
WHATSAPP_REPLY_WINDOW_HOURS=48
```

Mehrere Werkstattnummern sind moeglich:

```env
WHATSAPP_WORKSHOP_NUMBERS=+491701234567,+491721234567
```

## Webhook in Meta eintragen

Callback URL:

```text
https://deine-domain.de/webhooks/whatsapp
```

Verify Token:

```text
WHATSAPP_VERIFY_TOKEN aus .env.local
```

Abonnieren: `messages`

## Template fuer Benachrichtigungen

WhatsApp erlaubt freie Textnachrichten nur innerhalb des Kundenservice-Fensters. Fuer zuverlaessige Benachrichtigungen sollte in Meta ein Template angelegt und freigegeben werden.

Beispiel-Template mit vier Platzhaltern:

```text
Neue Portal-Nachricht {{1}}
Fahrzeug: {{2}}
Von: {{3}}
Nachricht: {{4}}

Bitte direkt auf diese WhatsApp antworten. Die Antwort wird im Portal-Chat gespeichert.
```

Danach ergaenzen:

```env
WHATSAPP_NOTIFICATION_TEMPLATE=portal_chat_hinweis
WHATSAPP_TEMPLATE_LANGUAGE=de
```

Wenn kein Template gesetzt ist, sendet die App freie Textnachrichten. Das ist gut fuer Tests, kann ausserhalb des 24-Stunden-Fensters aber von WhatsApp abgelehnt werden.
