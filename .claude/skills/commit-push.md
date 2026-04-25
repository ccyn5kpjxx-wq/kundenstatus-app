---
name: /commit-push
description: Sicheren Commit mit vorheriger Diff-Pruefung erstellen und pushen.
type: workflow
---

# /commit-push

Nutze diesen Ablauf, wenn Aenderungen nach GitHub sollen.

1. Status und Diff zeigen:

```powershell
git status --short --branch
git diff --stat
git diff
```

2. Vor dem Commit auf Secrets achten:

```powershell
git diff --name-only
```

Warnen, wenn `.env`, `.env.local`, Service-Account-JSON, Datenbanken, Uploads oder API-Keys im Diff auftauchen.

3. Commit-Message im Conventional-Commits-Format vorschlagen, z. B.:

```text
feat: add offer approval workflow
fix: prevent upload analysis from overwriting dates
docs: update project memory
```

4. Auf ausdrueckliches OK warten.
5. Dann ausfuehren:

```powershell
git add .
git commit -m "<message>"
git push
```

Wichtig: Vor jedem Push muss der Diff geprueft und dem Nutzer erklaert werden.
