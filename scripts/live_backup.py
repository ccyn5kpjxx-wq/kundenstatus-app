from __future__ import annotations

import argparse
from datetime import datetime
from html import unescape
import os
from pathlib import Path
import re
import sys
import zipfile

import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_URL = "https://kundenstatus-app.onrender.com"
DEFAULT_OUT_DIR = ROOT / "data" / "live_backups"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def csrf_from_html(html: str) -> str:
    match = re.search(
        r'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    )
    return unescape(match.group(1)) if match else ""


def filename_from_response(response: requests.Response) -> str:
    disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';]+)', disposition)
    if match:
        return Path(match.group(1)).name
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"kundenstatus-live-backup-{stamp}.zip"


def prune_backups(out_dir: Path, keep: int) -> None:
    if keep <= 0:
        return
    backups = sorted(out_dir.glob("kundenstatus-*.zip"), key=lambda path: path.stat().st_mtime, reverse=True)
    for old_backup in backups[keep:]:
        try:
            old_backup.unlink()
        except OSError:
            pass


def download_live_backup(base_url: str, password: str, out_dir: Path, keep: int) -> Path:
    base_url = base_url.rstrip("/")
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    login_response = session.get(f"{base_url}/login", timeout=30)
    login_response.raise_for_status()
    token = csrf_from_html(login_response.text)
    if not token:
        raise RuntimeError("Login-CSRF-Token wurde nicht gefunden.")

    auth_response = session.post(
        f"{base_url}/login",
        data={"passwort": password, "csrf_token": token},
        timeout=30,
        allow_redirects=True,
    )
    auth_response.raise_for_status()
    if "/login" in auth_response.url or "Falsches Passwort" in auth_response.text:
        raise RuntimeError("Live-Login fehlgeschlagen. ADMIN_PASS/LIVE_ADMIN_PASS prüfen.")

    token = csrf_from_html(auth_response.text)
    if not token:
        admin_response = session.get(f"{base_url}/admin", timeout=30)
        admin_response.raise_for_status()
        token = csrf_from_html(admin_response.text)
    if not token:
        raise RuntimeError("Admin-CSRF-Token wurde nicht gefunden.")

    backup_response = session.post(
        f"{base_url}/admin/backup/download",
        data={"csrf_token": token},
        timeout=180,
        allow_redirects=False,
    )
    backup_response.raise_for_status()
    content_type = backup_response.headers.get("Content-Type", "").lower()
    if "zip" not in content_type and not backup_response.content.startswith(b"PK"):
        raise RuntimeError("Backup-Download hat kein ZIP geliefert. Login oder Servermeldung prüfen.")

    target = out_dir / filename_from_response(backup_response)
    if target.exists():
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        target = out_dir / f"{target.stem}-{stamp}{target.suffix}"
    target.write_bytes(backup_response.content)

    with zipfile.ZipFile(target) as archive:
        names = set(archive.namelist())
        if "backup.json" not in names:
            raise RuntimeError(f"Backup unvollständig: backup.json fehlt in {target}")
        upload_count = sum(1 for name in names if name.startswith("uploads/") and not name.endswith("/"))
    prune_backups(out_dir, keep)
    print(f"Backup gespeichert: {target}")
    print(f"Upload-Dateien im ZIP: {upload_count}")
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="Live-Backup der Kundenstatus-App lokal speichern.")
    parser.add_argument("--base-url", default=os.environ.get("LIVE_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--output-dir", default=os.environ.get("LIVE_BACKUP_DIR", str(DEFAULT_OUT_DIR)))
    parser.add_argument("--keep", type=int, default=int(os.environ.get("LIVE_BACKUP_KEEP", "60")))
    parser.add_argument("--env-file", action="append", default=[])
    args = parser.parse_args()

    for env_path in (ROOT / ".env.local", ROOT / ".env"):
        load_env_file(env_path)
    for env_file in args.env_file:
        load_env_file(Path(env_file))

    password = os.environ.get("LIVE_ADMIN_PASS") or os.environ.get("ADMIN_PASS") or ""
    if not password:
        print("ADMIN_PASS oder LIVE_ADMIN_PASS fehlt. Bitte in .env.local setzen.", file=sys.stderr)
        return 2

    try:
        download_live_backup(args.base_url, password, Path(args.output_dir), args.keep)
    except Exception as exc:
        print(f"Backup fehlgeschlagen: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
