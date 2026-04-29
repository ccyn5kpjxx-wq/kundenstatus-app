from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def run_step(label, command):
    print(f"\n== {label} ==")
    result = subprocess.run(command, cwd=ROOT)
    if result.returncode != 0:
        print(f"[FEHLER] {label} ist fehlgeschlagen.")
        return False
    print(f"[OK] {label}")
    return True


def check_gitignore():
    print("\n== Gitignore-Schutz ==")
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8", errors="ignore")
    required = ["data/", ".env", ".env.*", "*.db", "*.sqlite", "*.sqlite3", "*.log"]
    ok = True
    for entry in required:
        present = entry in gitignore
        print(f"[OK] {entry}" if present else f"[FEHLER] {entry} fehlt")
        ok &= present
    return ok


def check_backup_config():
    print("\n== Backup-Konfiguration ==")
    sys.path.insert(0, str(ROOT))
    import app as portal  # noqa: E402

    checks = [
        ("Automatische Backups aktiv", portal.AUTO_BACKUP_ENABLED),
        ("Mindestens 24 Backups werden behalten", portal.AUTO_BACKUP_KEEP >= 24),
        ("Backup-Ordner ist gesetzt", bool(portal.BACKUP_DIR)),
        ("Upload-Papierkorb ist gesetzt", bool(portal.DELETED_UPLOAD_DIR)),
    ]
    ok = True
    for label, passed in checks:
        print(f"[OK] {label}" if passed else f"[FEHLER] {label}")
        ok &= bool(passed)
    return ok


def main():
    ok = True
    ok &= run_step("Python-Syntax", [sys.executable, "-m", "py_compile", "app.py"])
    ok &= check_gitignore()
    ok &= check_backup_config()
    ok &= run_step("Smoke-Test", [sys.executable, "scripts/smoke_test.py"])
    ok &= run_step("Ablauf-Test", [sys.executable, "scripts/flow_test.py"])

    if not ok:
        print("\nStabilitaets-Check fehlgeschlagen. Nicht an Kunden geben.")
        return 1
    print("\nStabilitaets-Check erfolgreich. Die bekannten Kernablaeufe sind gruen.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
