from __future__ import annotations

import functools
import socket
import subprocess
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


PREVIEW_SERVERS: dict[int, tuple[ThreadingHTTPServer, threading.Thread, Path, int]] = {}
PREVIEW_LOCK = threading.Lock()


def _git(path: Path, *args: str) -> tuple[int, str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(path), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 1, str(exc)
    output = (result.stdout or result.stderr or "").strip()
    return result.returncode, output


def git_projektstand(pfad: str) -> dict[str, object]:
    info: dict[str, object] = {
        "git_status": "nicht_verbunden",
        "git_branch": "",
        "git_commit": "",
        "git_kurz": "",
        "git_author": "",
        "git_author_email": "",
        "git_nachricht": "",
        "git_geaendert_am": "",
        "git_dirty": 0,
        "git_ahead": 0,
        "git_behind": 0,
        "git_remote": "",
        "git_fehler": "",
    }
    if not pfad.strip():
        return info

    projektpfad = Path(pfad).expanduser()
    if not projektpfad.exists() or not projektpfad.is_dir():
        info.update(git_status="pfad_fehlt", git_fehler="Projektordner wurde nicht gefunden.")
        return info

    code, root_text = _git(projektpfad, "rev-parse", "--show-toplevel")
    if code != 0:
        info.update(git_status="kein_git", git_fehler="Der Ordner ist noch kein Git-Projekt.")
        return info
    git_root = Path(root_text)

    _, branch = _git(git_root, "branch", "--show-current")
    if not branch:
        _, branch = _git(git_root, "rev-parse", "--short", "HEAD")
    code, log_output = _git(
        git_root,
        "log",
        "-1",
        "--format=%H%x1f%h%x1f%an%x1f%ae%x1f%aI%x1f%s",
    )
    if code != 0:
        info.update(git_status="kein_commit", git_branch=branch, git_fehler="Noch kein Git-Commit vorhanden.")
        return info
    teile = log_output.split("\x1f", 5)
    while len(teile) < 6:
        teile.append("")
    commit, kurz, author, author_email, geaendert_am, nachricht = teile

    _, status_output = _git(git_root, "status", "--porcelain")
    dirty = len([line for line in status_output.splitlines() if line.strip()])
    _, remote = _git(git_root, "config", "--get", "remote.origin.url")
    upstream_code, _ = _git(git_root, "rev-parse", "--abbrev-ref", "@{upstream}")
    ahead = behind = 0
    if upstream_code == 0:
        counts_code, counts = _git(git_root, "rev-list", "--left-right", "--count", "@{upstream}...HEAD")
        if counts_code == 0:
            parts = counts.replace("\t", " ").split()
            if len(parts) == 2:
                try:
                    behind, ahead = int(parts[0]), int(parts[1])
                except ValueError:
                    ahead = behind = 0

    if dirty:
        status = "lokale_aenderungen"
    elif ahead and behind:
        status = "abweichung"
    elif ahead:
        status = "push_ausstehend"
    elif behind:
        status = "pull_ausstehend"
    elif upstream_code != 0:
        status = "kein_upstream"
    else:
        status = "aktuell"

    info.update(
        git_status=status,
        git_branch=branch,
        git_commit=commit,
        git_kurz=kurz,
        git_author=author,
        git_author_email=author_email,
        git_nachricht=nachricht,
        git_geaendert_am=geaendert_am,
        git_dirty=dirty,
        git_ahead=ahead,
        git_behind=behind,
        git_remote=remote,
        git_fehler="",
    )
    return info


def netzwerk_adressen(port: int) -> list[str]:
    adressen: set[str] = set()
    try:
        for result in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = result[4][0]
            if ip and not ip.startswith(("127.", "169.254.")) and ip != "0.0.0.0":
                adressen.add(ip)
    except OSError:
        pass
    return [f"http://{ip}:{port}" for ip in sorted(adressen)]


def _port_frei(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
        except OSError:
            return False
    return True


def _vorschau_port(projekt_id: int) -> int:
    basis = 8100 + (projekt_id % 500)
    for port in range(basis, 8999):
        if _port_frei(port):
            return port
    raise RuntimeError("Kein freier Vorschau-Port zwischen 8100 und 8998 gefunden.")


class _LeiserHandler(SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args) -> None:
        return


def vorschau_starten(projekt_id: int, projektordner: str, relativer_pfad: str = ".") -> dict[str, object]:
    basis = Path(projektordner).expanduser().resolve()
    if not basis.exists() or not basis.is_dir():
        raise ValueError("Der lokale Projektordner wurde nicht gefunden.")
    relativ = (relativer_pfad or ".").strip()
    ziel = (basis / relativ).resolve()
    if ziel != basis and basis not in ziel.parents:
        raise ValueError("Der Vorschauordner muss innerhalb des Projektordners liegen.")
    if not ziel.exists() or not ziel.is_dir():
        raise ValueError("Der angegebene Vorschauordner wurde nicht gefunden.")
    if not (ziel / "index.html").exists():
        raise ValueError("Im Vorschauordner fehlt eine index.html.")

    with PREVIEW_LOCK:
        vorhandene = PREVIEW_SERVERS.get(projekt_id)
        if vorhandene:
            server, thread, alter_pfad, port = vorhandene
            if thread.is_alive() and alter_pfad == ziel:
                return {"port": port, "pfad": str(ziel), "aktiv": True}
            server.shutdown()
            server.server_close()
            PREVIEW_SERVERS.pop(projekt_id, None)

        port = _vorschau_port(projekt_id)
        handler = functools.partial(_LeiserHandler, directory=str(ziel))
        server = ThreadingHTTPServer(("0.0.0.0", port), handler)
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, name=f"tw-preview-{projekt_id}", daemon=True)
        thread.start()
        PREVIEW_SERVERS[projekt_id] = (server, thread, ziel, port)
        return {"port": port, "pfad": str(ziel), "aktiv": True}


def vorschau_stoppen(projekt_id: int) -> bool:
    with PREVIEW_LOCK:
        vorhanden = PREVIEW_SERVERS.pop(projekt_id, None)
    if not vorhanden:
        return False
    server, thread, _pfad, _port = vorhanden
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    return True


def vorschau_aktiv(projekt_id: int) -> bool:
    with PREVIEW_LOCK:
        vorhanden = PREVIEW_SERVERS.get(projekt_id)
        return bool(vorhanden and vorhanden[1].is_alive())


def alle_vorschauen_stoppen() -> None:
    with PREVIEW_LOCK:
        ids = list(PREVIEW_SERVERS)
    for projekt_id in ids:
        vorschau_stoppen(projekt_id)
