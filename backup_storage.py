"""Bounded storage for complete application backups; never manages source files.

Callers serialize prepare, creation and finish with their backup lock. Unknown,
damaged or linked files are deliberately left for an operator to inspect.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
import os
from pathlib import Path
import re
import shutil
import stat
import zipfile
import zlib


BACKUP_NAME = re.compile(r"kundenstatus-backup-\d{8}-\d{6}(?:-\d{6})?\.zip\Z")


class BackupStorageError(OSError):
    """A backup cannot be made without exceeding its storage allowance."""


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _regular_unlinked(info):
    return (
        stat.S_ISREG(info.st_mode)
        and info.st_nlink == 1
        and not getattr(info, "st_file_attributes", 0) & 0x400
    )


@lru_cache(maxsize=512)
def _valid_archive(path_text, identity):
    path = Path(path_text)
    try:
        before = path.lstat()
        if not _regular_unlinked(before) or _identity(before) != identity:
            return False
        with path.open("rb") as source:
            # Windows exposes creation/change time differently through fstat and
            # lstat; device, inode, size and mtime identify the opened file there.
            if _identity(os.fstat(source.fileno()))[:4] != identity[:4]:
                return False
            with zipfile.ZipFile(source) as archive:
                names = archive.namelist()
                if len(names) != len(set(names)) or "backup.json" not in names:
                    return False
                info = archive.getinfo("manifest.json")
                if info.file_size > 64 * 1024:
                    return False
                manifest = json.loads(archive.read(info))
                if not isinstance(manifest, dict) or manifest.get("backup_file") != path.name:
                    return False
                if manifest.get("format_version") not in {1, 2, 3, 4}:
                    return False
                backup_info = archive.getinfo("backup.json")
                if backup_info.file_size > 128 * 1024 * 1024:
                    return False
                export = json.loads(archive.read(backup_info))
                if not isinstance(export, dict) or not isinstance(export.get("tables"), dict):
                    return False
                if not {"auftraege", "autohaeuser"}.issubset(export["tables"]):
                    return False
                if export.get("format_version") != manifest["format_version"]:
                    return False
                uploads = [name for name in names if name.startswith("uploads/") and not name.endswith("/")]
                if manifest.get("upload_count") != len(uploads):
                    return False
                if archive.testzip() is not None:
                    return False
        return _identity(path.lstat()) == identity
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, EOFError, zipfile.BadZipFile, zlib.error):
        return False


@dataclass(frozen=True)
class Archive:
    path: Path
    identity: tuple

    @property
    def size(self):
        return self.identity[2]


class LimitedBackupWriter:
    """Seekable ZIP target with a hard physical-size limit and free-space check."""

    def __init__(self, raw, directory, max_bytes, reserve_bytes):
        self.raw = raw
        self.directory = directory
        self.max_bytes = max_bytes
        self.reserve_bytes = reserve_bytes
        self.high_water = 0

    def write(self, data):
        end = self.raw.tell() + len(data)
        growth = max(0, end - self.high_water)
        if end > self.max_bytes:
            raise BackupStorageError("Backup abgebrochen: Archiv-Groessenlimit erreicht.")
        if growth and shutil.disk_usage(self.directory).free < self.reserve_bytes + growth:
            raise BackupStorageError("Backup abgebrochen: freier Speicher fuer Betriebsreserve fehlt.")
        written = self.raw.write(data)
        self.high_water = max(self.high_water, self.raw.tell())
        return written

    def tell(self):
        return self.raw.tell()

    def seek(self, *args):
        return self.raw.seek(*args)

    def flush(self):
        return self.raw.flush()


class BackupStorage:
    def __init__(self, directory, *, keep, max_bytes=0, reserve_bytes=0):
        self.directory = Path(directory).absolute()
        self.keep = max(1, int(keep))
        self.max_bytes = max(0, int(max_bytes))
        self.reserve_bytes = max(0, int(reserve_bytes))
        self.protected_count = 2 if self.max_bytes else min(2, self.keep)
        self.keep = max(self.keep, self.protected_count)

    def _scan(self):
        # A configured symlink/junction must never turn cleanup into a source-file operation.
        for path in (self.directory, *self.directory.parents):
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                raise BackupStorageError("Backup-Verzeichnis darf keinen verlinkten Pfad enthalten.")
        archives = []
        used = 0
        for path in self.directory.iterdir():
            info = path.lstat()
            if self.max_bytes and stat.S_ISDIR(info.st_mode):
                raise BackupStorageError("Unbekannter Unterordner im Backup-Verzeichnis; Speicherpruefung erforderlich.")
            if stat.S_ISREG(info.st_mode):
                # Also account for corrupt/unknown/partial files, without deleting them.
                used += info.st_size
            if not BACKUP_NAME.fullmatch(path.name) or not _regular_unlinked(info):
                continue
            identity = _identity(info)
            if _valid_archive(str(path), identity):
                archives.append(Archive(path, identity))
        archives.sort(key=lambda item: (item.path.name, item.identity[3]), reverse=True)
        return archives, used

    def _prune(self, *, next_bytes=0, creating=False):
        archives, used = self._scan()
        free = shutil.disk_usage(self.directory).free
        count_limit = max(self.protected_count, self.keep - int(creating))
        count = len(archives)
        removable = list(reversed(archives[self.protected_count:]))
        deletions = []

        def fits():
            return (
                count <= count_limit
                and (not self.max_bytes or used + next_bytes <= self.max_bytes)
                and free >= self.reserve_bytes + next_bytes
            )

        for item in removable:
            if fits():
                break
            deletions.append(item)
            used -= item.size
            free += item.size
            count -= 1
        if not fits():
            # Validate feasibility first: never consume recovery copies chasing an impossible target.
            raise BackupStorageError("Backup-Speicher reicht mit zwei geschuetzten Sicherungen und Reserve nicht aus.")
        for item in deletions:
            info = item.path.lstat()
            if not _regular_unlinked(info) or _identity(info) != item.identity:
                raise BackupStorageError("Backup-Datei wurde waehrend der Speicherpruefung veraendert.")
            item.path.unlink()
        return used, len(deletions)

    def prepare(self):
        """Reserve space for a new archive without sacrificing the two newest valid copies."""
        # Three complete archives must be possible: two recovery copies plus the new one.
        next_limit = self.max_bytes // 3 if self.max_bytes else 0
        used, removed = self._prune(next_bytes=next_limit, creating=True)
        free = shutil.disk_usage(self.directory).free
        limit = free - self.reserve_bytes
        if self.max_bytes:
            limit = min(limit, next_limit, self.max_bytes - used)
        if limit <= 0:
            raise BackupStorageError("Kein freier Backup-Speicher oberhalb der Betriebsreserve.")
        print(
            f"BACKUP_STORAGE event=prepared path={self.directory} bytes={used} "
            f"removed={removed} archive_limit_bytes={limit} free_bytes={free}", flush=True,
        )
        return limit

    def writer(self, raw, limit):
        return LimitedBackupWriter(raw, self.directory, limit, self.reserve_bytes)

    def validate_created(self, path):
        path = Path(path)
        if path.parent.absolute() != self.directory or not BACKUP_NAME.fullmatch(path.name):
            raise BackupStorageError("Neue Sicherung liegt ausserhalb des Backup-Verzeichnisses.")
        info = path.lstat()
        if not _regular_unlinked(info) or not _valid_archive(str(path), _identity(info)):
            raise BackupStorageError("Neue Sicherung hat die Archivpruefung nicht bestanden.")

    def finish(self):
        used, removed = self._prune()
        print(
            f"BACKUP_STORAGE event=retained path={self.directory} bytes={used} "
            f"removed={removed} free_bytes={shutil.disk_usage(self.directory).free}", flush=True,
        )
