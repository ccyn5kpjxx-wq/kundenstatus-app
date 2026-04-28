from pathlib import Path
from datetime import datetime
import zipfile


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB = DATA_DIR / "auftraege.db"
UPLOAD_DIR = DATA_DIR / "uploads"
OUT_DIR = DATA_DIR / "exports"


def main():
    if not DB.exists():
        raise SystemExit(f"Lokale Datenbank nicht gefunden: {DB}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = OUT_DIR / f"kundenstatus-render-import-{stamp}.zip"
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.write(DB, "auftraege.db")
        if UPLOAD_DIR.exists():
            for path in UPLOAD_DIR.iterdir():
                if path.is_file():
                    archive.write(path, f"uploads/{path.name}")
    print(target)


if __name__ == "__main__":
    main()
