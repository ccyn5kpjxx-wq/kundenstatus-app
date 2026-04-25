---
name: /task-add
description: Neue Aufgabe mit Datum in TODO.md notieren.
type: workflow
---

# /task-add <text>

Nutze diesen Ablauf, wenn eine neue Aufgabe festgehalten werden soll.

1. Falls `TODO.md` fehlt, erstelle sie mit den Bereichen:

```markdown
# TODO

## Offen

## In Arbeit

## Erledigt
```

2. Haenge den neuen Eintrag unter `## Offen` an.
3. Format:

```markdown
- 2026-04-25: <text>
```

4. Wenn die Aufgabe bereits existiert, keinen doppelten Eintrag anlegen.
5. Danach kurz bestaetigen, wo der Eintrag steht.
