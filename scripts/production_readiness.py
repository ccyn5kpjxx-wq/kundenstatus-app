"""Fail-fast production configuration check without printing secret values."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def https_url(name: str) -> bool:
    value = os.environ.get(name, "").strip()
    parsed = urlparse(value)
    return parsed.scheme == "https" and bool(parsed.netloc)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=("public", "portal"), required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env.local")
    args = parser.parse_args()
    load_env(args.env_file)

    failures: list[str] = []
    warnings: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    require(https_url("PUBLIC_BASE_URL"), "PUBLIC_BASE_URL muss eine vollstaendige HTTPS-Adresse sein.")
    require(flag("SESSION_COOKIE_SECURE"), "SESSION_COOKIE_SECURE muss in Produktion 1 sein.")
    require(os.environ.get("FLASK_SECRET_KEY", "") not in {"", "change-me", "gaertner-autohaus-2026"}, "FLASK_SECRET_KEY fehlt oder ist unsicher.")

    if args.profile == "public":
        require(flag("PUBLIC_SITE_ONLY"), "PUBLIC_SITE_ONLY muss fuer den Homepage-Dienst 1 sein.")
        require(https_url("PORTAL_BASE_URL"), "PORTAL_BASE_URL muss auf den getrennten HTTPS-Portal-Dienst zeigen.")
        require(flag("PUBLIC_SITE_INDEXABLE"), "PUBLIC_SITE_INDEXABLE muss erst zum echten Livegang bewusst auf 1 gesetzt werden.")
    else:
        require(not flag("PUBLIC_SITE_ONLY"), "PUBLIC_SITE_ONLY muss fuer den Portal-Dienst 0 sein.")
        require(os.environ.get("DATABASE_URL", "").startswith(("postgres://", "postgresql://")), "DATABASE_URL muss auf PostgreSQL zeigen.")
        require(flag("REQUIRE_POSTGRES_ON_RENDER"), "REQUIRE_POSTGRES_ON_RENDER muss 1 sein.")
        require(os.environ.get("ADMIN_PASS", "") not in {"", "change-me", "gaertner2026"}, "ADMIN_PASS fehlt oder ist unsicher.")
        require(bool(os.environ.get("UPLOAD_DIR", "").strip()), "UPLOAD_DIR fuer dauerhafte Uploads fehlt.")
        require(bool(os.environ.get("BACKUP_DIR", "").strip()), "BACKUP_DIR fuer Sicherungen fehlt.")
        if not flag("DATENSCHUTZ_RECHTLICH_FREIGEGEBEN"):
            warnings.append("Datenschutzerklaerung ist weiterhin als juristische Prueffassung markiert.")
        if not flag("MIETVERTRAG_RECHTLICH_FREIGEGEBEN"):
            warnings.append("Mietvertrag bleibt zu Recht im gesperrten Entwurfsmodus.")

    for warning in warnings:
        print(f"[WARNUNG] {warning}")
    for failure in failures:
        print(f"[FEHLER] {failure}")
    if failures:
        print(f"Nicht produktionsbereit: {len(failures)} Pflichtpunkt(e) offen.")
        return 1
    print(f"Produktionsprofil '{args.profile}' ist konfigurationsseitig bereit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
