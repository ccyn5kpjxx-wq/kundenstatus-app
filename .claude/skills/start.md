---
name: /start
description: Session-Briefing fuer das Autohaus-Terminportal erstellen.
type: workflow
---

# /start

Nutze diesen Ablauf am Anfang einer neuen Session.

1. Lies `CLAUDE.md`.
2. Pruefe den Git-Zustand:

```powershell
git status --short --branch
git log --oneline -5
```

3. Suche offene Code-Hinweise:

```powershell
rg "TODO|FIXME" .
```

4. Gib ein kurzes Briefing in genau zwei Saetzen aus:

```text
Wo wir stehen: ...
Als naechstes sinnvoll: ...
```

Wichtig: Secrets, Kundendaten, Datenbanken und Uploads niemals anzeigen oder committen.
