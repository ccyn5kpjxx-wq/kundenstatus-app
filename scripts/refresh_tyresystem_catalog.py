"""Explicit one-shot private catalogue refresh. No dotenv or app import.

Dual mode downloads once and validates both categories before publication.
Each cache file is atomic; a multi-file disk failure is reported, never hidden
behind a fabricated common receipt or an unsafe rollback of newer data.
"""
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import argparse
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tyresystem_provider import TyreSystemCatalogCache, TyreSystemCatalogError
from tyresystem_feed import parse_feed
from felgen_katalog import FelgenCatalogCache, FelgenKatalogError, parse_felgen_feed


MAX_REFRESH_SECONDS = 90.0
MAX_AGE = timedelta(minutes=15)


@contextmanager
def _refresh_locks(caches):
    """Nonblocking OS locks before HTTP; process death releases ownership.

    Lock files remain in place: unlinking advisory-lock files could let another
    process lock a different inode and bypass the still-running holder.
    """
    owned = []
    try:
        folders = {}
        for cache in caches:
            folder, root = cache.directory, cache.private_root
            if (folder.resolve() != folder or root.resolve() != root
                    or not folder.is_relative_to(root) or folder == root
                    or cache.path.is_symlink()):
                raise ValueError()
            folders[folder] = root
        for folder, root in sorted(folders.items(), key=lambda item: str(item[0])):
            folder.mkdir(parents=True, exist_ok=True, mode=0o700)
            if folder.resolve() != folder or root.resolve() != root:
                raise ValueError()
            path = folder / ".tyresystem-refresh.lock"
            if path.is_symlink():
                raise ValueError()
            fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
            try:
                info, current = os.fstat(fd), path.lstat()
                if (not stat.S_ISREG(info.st_mode) or path.is_symlink()
                        or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino)):
                    raise ValueError()
                if os.name == "nt":
                    import msvcrt
                    if info.st_size == 0:
                        os.write(fd, b"\0")
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                owned.append(fd)
            except BaseException:
                os.close(fd)
                raise
        yield
    finally:
        for fd in reversed(owned):
            try:
                os.close(fd)
            except OSError:
                pass


def _failure(partial=False, may_have_changed=False):
    return {"updated": False, "partially_updated": partial, "cache_must_be_checked": True,
            "cache_may_have_changed": may_have_changed or partial,
            "public_release_changed": False}, 1


def refresh_catalog(cache, client):
    """Legacy tire-only mode; failed fetch never changes old freshness."""
    writing = False
    try:
        with _refresh_locks((cache,)):
            snapshot = client.fetch()
            writing = True
            cache.replace(snapshot)
        return {"updated": True, "received_at": snapshot.abgerufen_am.isoformat(),
                "articles": len(snapshot.artikel), "public_release_changed": False}, 0
    except (Exception, KeyboardInterrupt):
        return _failure(may_have_changed=writing)


def refresh_catalogs(cache, felgen_cache, client, *, clock=None, monotonic=None):
    """One explicit private GET, two real parsers, original receipt/15m TTL.

    ``client.fetch_raw`` must implement the trusted bounded export transport.
    Production uses TyreSystemExportClient; offline tests inject fake HTTP only.
    No raw CSV, net purchase prices, private paths or credentials are returned.
    """
    timer = monotonic or time.monotonic
    utc_now = clock or (lambda: datetime.now(timezone.utc))
    published = 0
    writing = False
    try:
        deadline = timer() + MAX_REFRESH_SECONDS
        with _refresh_locks((cache, felgen_cache)):
            if timer() >= deadline:
                raise ValueError()
            raw, received = client.fetch_raw()
            if (not isinstance(received, datetime) or received.tzinfo is None or received.utcoffset() is None
                    or timer() >= deadline):
                raise ValueError()
            received = received.astimezone(timezone.utc)
            tires = parse_feed(raw, abgerufen_am=received)
            rims = parse_felgen_feed(raw, abgerufen_am=received)
            if (not tires.artikel or not rims.artikel or tires.sha256 != rims.sha256
                    or tires.abgerufen_am != rims.abgerufen_am or timer() >= deadline):
                raise ValueError()
            current = utc_now()
            if (not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() is None
                    or not current - MAX_AGE < received <= current + timedelta(seconds=30)):
                raise ValueError()
            # Preflight both destinations before changing either. A slow refresh
            # must not overwrite a newer receipt in the other category.
            for target in (cache, felgen_cache):
                if target.path.exists():
                    try:
                        prior = target.load()
                    except (TyreSystemCatalogError, FelgenKatalogError) as error:
                        if error.code in {"cache_missing", "cache_invalid"}:
                            continue  # A fully validated export can repair a damaged cache.
                        raise
                    old_receipt = prior.snapshot.abgerufen_am if hasattr(prior, "snapshot") else prior.abgerufen_am
                    if old_receipt > received:
                        raise ValueError()
            if timer() >= deadline:
                raise ValueError()
            writing = True
            cache.replace(tires)
            published += 1
            if timer() >= deadline:
                raise ValueError()
            felgen_cache.replace(rims)
            published += 1
            if timer() >= deadline:
                raise ValueError()
        return {"updated": True, "received_at": received.isoformat(),
                "valid_until": (received+MAX_AGE).isoformat(), "articles": len(tires.artikel),
                "rim_articles": len(rims.artikel), "source_sha256":tires.sha256, "public_release_changed": False}, 0
    except (Exception, KeyboardInterrupt):
        # A signal can arrive after os.replace but before replace() returns.
        # Never infer untouched files merely from the completed-call counter.
        return _failure(partial=published == 1, may_have_changed=writing)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="Authorize exactly one export GET and private cache update")
    parser.add_argument("--cache-dir")
    parser.add_argument("--private-root")
    parser.add_argument("--felgen-cache-dir", help="Also refresh this private rim cache from the same download")
    parser.add_argument("--felgen-private-root", help="Rim private root; defaults to --private-root")
    args = parser.parse_args(argv)
    if (args.run is not True or not args.cache_dir or not args.private_root
            or (args.felgen_private_root and not args.felgen_cache_dir)):
        print(json.dumps({"updated": False, "run_authorized": False}))
        return 2
    previous_term = None
    term_installed = False
    try:
        # A normal manager SIGTERM must unwind HTTP/cache finally blocks.
        # SIGKILL or a power loss can still leave a cache writer's own lock;
        # that requires operator inspection, never a blind stale-lock delete.
        previous_term = signal.signal(signal.SIGTERM, _terminate)
        term_installed = True
        cache = TyreSystemCatalogCache(args.cache_dir, private_root=args.private_root)
        rims = (FelgenCatalogCache(args.felgen_cache_dir, private_root=args.felgen_private_root or args.private_root)
                if args.felgen_cache_dir else None)
        from tyresystem_export import TyreSystemExportClient
        client = TyreSystemExportClient.from_env(authorize=lambda: True)
        result, code = refresh_catalogs(cache, rims, client) if rims is not None else refresh_catalog(cache, client)
    except (Exception, KeyboardInterrupt):
        result, code = _failure()
    finally:
        if term_installed:
            signal.signal(signal.SIGTERM, previous_term)
    print(json.dumps(result, ensure_ascii=True))
    return code


def _terminate(signum, frame):
    raise KeyboardInterrupt()


if __name__ == "__main__":
    raise SystemExit(main())
