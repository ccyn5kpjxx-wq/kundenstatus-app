"""Bounded retention for new marked live-display GET run directories only."""
from pathlib import Path
import re
import threading

MARKER = '.shop-live-display-run'
MARKER_BYTES = b'shop-live-display-read-run-v1\n'
RUN = re.compile(r'[a-f0-9]{32}')
FILE = re.compile(r'[0-9]{2}-(?:tyres|alloys|products|wearparts)-[A-Za-z0-9_]+\.(?:body|meta\.json)')


class DisplayStorageError(ValueError):
    pass


class LiveReadRetention:
    def __init__(self, roots, *, max_bytes=64*1024*1024, max_runs=256):
        if (not isinstance(roots,(tuple,list)) or not 1 <= len(roots) <= 2
                or type(max_bytes) is not int or not 1024 <= max_bytes <= 64*1024*1024
                or type(max_runs) is not int or not 1 <= max_runs <= 256):
            raise DisplayStorageError('invalid_live_read_storage')
        self.roots=tuple(dict.fromkeys(Path(root).resolve() for root in roots))
        if any(not root.is_absolute() for root in self.roots):
            raise DisplayStorageError('invalid_live_read_storage')
        self.max_bytes,self.max_runs=max_bytes,max_runs
        self.lock=threading.RLock()

    def _target(self, output_dir):
        path=Path(output_dir)
        if (not path.is_absolute() or not RUN.fullmatch(path.name)
                or path.parent not in self.roots or path.is_symlink()
                or path.parent.is_symlink() or path.resolve()!=path):
            raise DisplayStorageError('unsafe_live_read_storage')
        return path

    def create(self, output_dir):
        with self.lock:
            path=self._target(output_dir)
            path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
            # Never take ownership of a pre-existing directory, even if empty.
            path.mkdir(mode=0o700)
            try:
                with (path/MARKER).open('xb') as stream:
                    stream.write(MARKER_BYTES)
            except Exception:
                # This is our new empty directory only. No recursive removal.
                try:path.rmdir()
                except OSError:pass
                raise DisplayStorageError('live_read_storage_unavailable') from None
            return path

    def _runs(self):
        entries=[]
        for root in self.roots:
            if root.is_symlink() or root.resolve()!=root:
                raise DisplayStorageError('unsafe_live_read_storage')
            if not root.exists():continue
            for path in root.iterdir():
                self._target(path)
                if not path.is_dir():raise DisplayStorageError('unsafe_live_read_storage')
                marker=path/MARKER
                if (marker.is_symlink() or not marker.is_file() or marker.stat().st_size!=len(MARKER_BYTES)
                        or marker.read_bytes()!=MARKER_BYTES):
                    raise DisplayStorageError('unreviewed_live_read_storage')
                files=[]
                for file in path.iterdir():
                    files.append(file)
                    if len(files)>70:raise DisplayStorageError('unreviewed_live_read_storage')
                size=0
                for file in files:
                    if (file.is_symlink() or not file.is_file()
                            or file.name!=MARKER and not FILE.fullmatch(file.name)):
                        raise DisplayStorageError('unreviewed_live_read_storage')
                    size+=file.stat().st_size
                entries.append((marker.stat().st_mtime_ns,path,size,files))
                if len(entries)>1024:raise DisplayStorageError('live_read_storage_limit')
        return sorted(entries,key=lambda entry:(entry[0],str(entry[1])))

    def prepare(self, output_dir, reserve_bytes):
        """Reserve before a bounded GET. Called under lock through its finish."""
        path=self._target(output_dir)
        if type(reserve_bytes) is not int or not 0 <= reserve_bytes <= self.max_bytes:
            raise DisplayStorageError('live_read_storage_limit')
        with self.lock:
            runs=self._runs()
            if not any(row[1]==path for row in runs):
                raise DisplayStorageError('unreviewed_live_read_storage')
            total,count=sum(row[2] for row in runs),len(runs)
            for _,old,size,files in runs:
                if total+reserve_bytes<=self.max_bytes and count<=self.max_runs:break
                if old==path:continue
                # One-level known files only, with absolute target rechecked.
                self._target(old)
                for file in files:
                    if file.parent!=old or file.is_symlink():
                        raise DisplayStorageError('unsafe_live_read_storage')
                    file.unlink()
                old.rmdir()
                total-=size;count-=1
            if total+reserve_bytes>self.max_bytes or count>self.max_runs:
                raise DisplayStorageError('live_read_storage_limit')

    def reader_factory(self, native_factory, *, reserve_bytes):
        retention=self
        def factory(*args,**kwargs):
            path=retention.create(kwargs['output_dir'])
            client=native_factory(*args,**kwargs)
            class BoundedReader:
                def read(self,area,operation,params=None):
                    with retention.lock:
                        retention.prepare(path,reserve_bytes)
                        result=client.read(area,operation,params)
                        retention.prepare(path,0)
                        return result
            return BoundedReader()
        return factory
