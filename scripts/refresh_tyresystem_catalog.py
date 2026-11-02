"""Explicit one-shot private catalogue refresh. No dotenv or app import."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tyresystem_provider import TyreSystemCatalogCache


def refresh_catalog(cache, client):
    """Offline-testable boundary: failed fetch/write never changes old freshness."""
    try:
        snapshot = client.fetch()
        cache.replace(snapshot)
        return {"updated": True, "received_at": snapshot.abgerufen_am.isoformat(),
                "articles": len(snapshot.artikel), "public_release_changed": False}, 0
    except (Exception, KeyboardInterrupt):
        return {"updated": False, "cache_must_be_checked": True,
                "public_release_changed": False}, 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Authorize exactly one export GET and private cache update")
    parser.add_argument("--cache-dir")
    parser.add_argument("--private-root")
    args = parser.parse_args(argv)
    if args.run is not True or not args.cache_dir or not args.private_root:
        print(json.dumps({"updated": False, "run_authorized": False}))
        return 2
    try:
        cache = TyreSystemCatalogCache(args.cache_dir, private_root=args.private_root)
        from tyresystem_export import TyreSystemExportClient
        client = TyreSystemExportClient.from_env(authorize=lambda: True)
        result, code = refresh_catalog(cache, client)
    except (Exception, KeyboardInterrupt):
        result, code = {"updated": False, "public_release_changed": False}, 1
    print(json.dumps(result, ensure_ascii=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
