"""Explicit private SQLite provisioning for shop orders and buyer sessions.

No filesystem/database action occurs on import or construction. The hosting
bootstrap must deliberately provision below its persistent DATA_DIR; errors
never fall back to ephemeral storage or the portal's PostgreSQL connection.
"""
from contextlib import closing
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
import tempfile
import time

from shop_buyer import init_buyer_schema, SQLiteBuyerBackend
from shop_order_store import init_schema, OrderStore

MAX_SNAPSHOT = 64 * 1024 * 1024


class StorageError(ValueError):
    pass


class ShopStorage:
    def __init__(self, persistent_data_dir, *, clock=None):
        root = Path(persistent_data_dir)
        if not root.is_absolute():
            raise StorageError('persistent_absolute_root_required')
        self.root = root
        self.directory = root / 'shop-v2'
        self.path = self.directory / 'shop.sqlite3'
        self.marker = self.directory / 'initialized'
        self.clock = clock
        self.ready = False

    def _paths(self):
        if (self.root.is_symlink() or self.directory.is_symlink() or self.path.is_symlink() or self.marker.is_symlink()
                or self.directory.resolve().parent != self.root.resolve()
                or self.path.resolve().parent != self.directory.resolve()):
            raise StorageError('unsafe_shop_storage_path')

    def provision(self, *, create=False):
        self.ready = False
        self._paths()
        if not self.root.is_dir():
            raise StorageError('persistent_root_unavailable')
        if type(create) is not bool:
            raise StorageError('explicit_storage_creation_required')
        initialized = self.marker.is_file()
        if initialized:
            if (self.marker.read_bytes() != b'shop_storage_v2\n' or not self.path.is_file()
                    or self.path.stat().st_size == 0):
                raise StorageError('initialized_shop_database_missing_or_invalid')
        elif not create or self.directory.exists():
            # A pre-existing unmarked directory may be an interrupted setup or
            # lost database. Never announce a fresh empty shop after a restart.
            raise StorageError('explicit_storage_creation_required')
        try:
            self.directory.mkdir(mode=0o700, exist_ok=True)
            self._paths()
            if self.path.exists() and not self.path.is_file():
                raise StorageError('unsafe_shop_storage_path')
            # Exclusive creation avoids inheriting a broad mode for a new file.
            if not self.path.exists():
                descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(descriptor)
            os.chmod(self.path, 0o600)
            with closing(sqlite3.connect(self.path, timeout=5)) as connection:
                if initialized:
                    objects = {(row[0], row[1]) for row in connection.execute('SELECT type,name FROM sqlite_master')}
                    required = {('table', name) for name in ('shop_quotes_v2', 'shop_orders_v2',
                        'shop_payment_attempts_v2', 'shop_payment_events_v2', 'shop_buyer_sessions_v2')}
                    required |= {('trigger', 'shop_quotes_v2_immutable'), ('trigger', 'shop_orders_v2_immutable')}
                    if not required <= objects:
                        raise StorageError('initialized_shop_database_missing_or_invalid')
                connection.execute('PRAGMA journal_mode=WAL')
                connection.execute('PRAGMA synchronous=FULL')
                init_schema(connection)
                init_buyer_schema(connection)
                connection.commit()
                if connection.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise StorageError('shop_storage_unavailable')
            if not initialized:
                descriptor = os.open(self.marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                with os.fdopen(descriptor, 'wb') as marker_file:
                    marker_file.write(b'shop_storage_v2\n')
                    marker_file.flush()
                    os.fsync(marker_file.fileno())
            self.ready = True
        except StorageError:
            raise
        except Exception:
            raise StorageError('shop_storage_unavailable') from None
        return self

    def connect(self):
        if self.ready is not True:
            raise StorageError('shop_storage_not_provisioned')
        self._paths()
        try:
            # mode=rw never silently recreates an empty lost database.
            connection = sqlite3.connect(self.path.as_uri() + '?mode=rw', uri=True, timeout=5)
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA synchronous=FULL')
            return connection
        except Exception:
            raise StorageError('shop_storage_unavailable') from None

    def order_store(self):
        return OrderStore(self.connect, clock=self.clock)

    def buyer_backend(self):
        return SQLiteBuyerBackend(self.connect, clock=self.clock)

    def snapshot(self):
        """Consistent SQLite backup bytes, including WAL-committed records.

        Private customer data: caller may only place this inside the existing
        authenticated backup package, never static assets or a public route.
        """
        self._paths()
        if not self.ready or not self.path.is_file() or self.path.stat().st_size > MAX_SNAPSHOT:
            raise StorageError('shop_backup_unavailable')
        temporary = None
        try:
            descriptor, name = tempfile.mkstemp(prefix='.snapshot-', suffix='.sqlite3', dir=self.directory)
            temporary = Path(name)
            os.close(descriptor)
            os.chmod(temporary, 0o600)
            deadline = time.monotonic() + 15

            def progress(status, remaining, total):
                if time.monotonic() > deadline or total * 4096 > MAX_SNAPSHOT:
                    raise StorageError('shop_backup_unavailable')

            with closing(self.connect()) as source, closing(sqlite3.connect(temporary)) as target:
                source.backup(target, pages=128, progress=progress, sleep=0.01)
                if target.execute('PRAGMA quick_check').fetchone() != ('ok',):
                    raise StorageError('shop_backup_unavailable')
            if temporary.stat().st_size > MAX_SNAPSHOT:
                raise StorageError('shop_backup_unavailable')
            return temporary.read_bytes()
        except StorageError:
            raise
        except Exception:
            raise StorageError('shop_backup_unavailable') from None
        finally:
            if temporary is not None and temporary.parent == self.directory:
                temporary.unlink(missing_ok=True)

    def write_backup(self, archive):
        data = self.snapshot()
        name = 'shop-v2/shop.sqlite3'
        archive.writestr(name, data)
        return {'format': 'shop_sqlite_v2', 'path': name, 'bytes': len(data), 'sha256': sha256(data).hexdigest()}
