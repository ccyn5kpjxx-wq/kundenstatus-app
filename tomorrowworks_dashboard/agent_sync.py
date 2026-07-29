from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


STATUS = {
    "neu",
    "planung",
    "in_arbeit",
    "interne_pruefung",
    "kundenfreigabe",
    "aenderungen",
    "wartet_auf_kunde",
    "blockiert",
    "veroeffentlicht",
    "abgeschlossen",
    "pausiert",
}


def _git(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def git_payload() -> dict[str, object]:
    status = _git("status", "--porcelain")
    return {
        "git_branch": _git("branch", "--show-current"),
        "git_commit": _git("rev-parse", "HEAD"),
        "git_kurz": _git("rev-parse", "--short", "HEAD"),
        "git_author": _git("log", "-1", "--format=%an"),
        "git_author_email": _git("log", "-1", "--format=%ae"),
        "git_nachricht": _git("log", "-1", "--format=%s"),
        "git_geaendert_am": _git("log", "-1", "--format=%aI"),
        "git_dirty": len([line for line in status.splitlines() if line.strip()]),
    }


def sende_update(
    dashboard_url: str,
    projekt_id: int,
    token: str,
    payload: dict[str, object],
    timeout: int = 10,
) -> dict:
    url = f"{dashboard_url.rstrip('/')}/api/agent/projekte/{projekt_id}/update"
    request = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Codex-/Claude-Stand an das Tomorrow-Works-Dashboard senden.")
    parser.add_argument("--dashboard", default=os.getenv("TW_DASHBOARD_URL", "http://127.0.0.1:5070"))
    parser.add_argument("--project-id", type=int, required=True)
    parser.add_argument("--token", default=os.getenv("TW_PROJECT_TOKEN", ""))
    parser.add_argument("--agent", default=os.getenv("TW_AGENT_NAME", "Codex/Claude"))
    parser.add_argument("--status", choices=sorted(STATUS))
    parser.add_argument("--progress", type=int)
    parser.add_argument("--task")
    parser.add_argument("--note")
    parser.add_argument("--customer-update", help="Optionaler sichtbarer Eintrag im dauerhaften Kunden-Ticket.")
    parser.add_argument("--preview-url")
    args = parser.parse_args()
    if not args.token:
        parser.error("Projekt-Token fehlt. TW_PROJECT_TOKEN setzen oder --token verwenden.")

    payload: dict[str, object] = {"agent": args.agent, **git_payload()}
    if args.status:
        payload["status"] = args.status
    if args.progress is not None:
        payload["fortschritt"] = min(max(args.progress, 0), 100)
    if args.task is not None:
        payload["aktuelle_aufgabe"] = args.task
    if args.note is not None:
        payload["notiz"] = args.note
    if args.customer_update is not None:
        payload["kunden_update"] = args.customer_update
    if args.preview_url is not None:
        payload["vorschau_url"] = args.preview_url

    try:
        result = sende_update(args.dashboard, args.project_id, args.token, payload)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"Dashboard-Update abgelehnt ({exc.code}): {detail}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"Dashboard nicht erreichbar: {exc.reason}", file=sys.stderr)
        return 1
    print(result.get("message", "Dashboard wurde aktualisiert."))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
