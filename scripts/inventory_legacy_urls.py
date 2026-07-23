"""Inventory URLs from the current WordPress sitemaps for the domain migration."""

from __future__ import annotations

import csv
from pathlib import Path
import sys
import time
import xml.etree.ElementTree as ET

import requests


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "migration" / "legacy_urls.csv"
SITEMAPS = (
    "https://auto-lackierzentrum.de/post-sitemap1.xml",
    "https://auto-lackierzentrum.de/page-sitemap1.xml",
)
SERVICE_PATHS = {
    "/kontakt/": "/homepage#kontakt",
    "/ueber-uns/": "/homepage#kontakt",
    "/industrielackierung/": "/leistungen",
    "/fahrzeugpflege/": "/leistungen",
    "/dellendoktor/": "/leistungen",
    "/unfallinstandsetzung-und-smart-repair/": "/leistungen",
    "/lackiererei-und-fahrzeuglackierung/": "/leistungen",
    "/impressum/": "/impressum",
    "/datenschutz/": "/datenschutz",
}


def main() -> int:
    rows: list[tuple[str, str, str]] = []
    session = requests.Session()
    session.headers["User-Agent"] = "Gaertner-Migrationsinventur/1.0 (+https://auto-lackierzentrum.de/)"
    for sitemap in SITEMAPS:
        response = None
        for attempt in range(3):
            response = session.get(sitemap, timeout=20)
            if response.status_code < 500:
                break
            time.sleep(1 + attempt)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        for node in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
            source = (node.findtext("{http://www.sitemaps.org/schemas/sitemap/0.9}loc") or "").strip()
            path = "/" + source.split("auto-lackierzentrum.de/", 1)[-1]
            if path == "/":
                target, action = "/", "keep"
            elif path in SERVICE_PATHS:
                target, action = SERVICE_PATHS[path], "301"
            elif sitemap.endswith("post-sitemap1.xml"):
                target, action = "", "review-regio"
            else:
                target, action = "", "review"
            rows.append((source, target, action))
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(("alte_url", "neues_ziel", "aktion"))
        writer.writerows(sorted(set(rows)))
    print(f"{len(rows)} URLs nach {OUTPUT} geschrieben.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"URL-Inventur fehlgeschlagen: {exc}", file=sys.stderr)
        raise SystemExit(1)
