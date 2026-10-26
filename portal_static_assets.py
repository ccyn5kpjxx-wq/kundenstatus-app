"""Browser caching for an explicit set of public, content-versioned UI assets."""
from functools import lru_cache
from hashlib import sha256
from hmac import compare_digest
from pathlib import Path


PUBLIC_UI_ASSETS = frozenset({
    'admin-sidebar.css', 'admin-sidebar.js', 'werkstatt_theme.css',
    'inspektionscheckliste.css', 'logo-nav-192.webp',
})


@lru_cache(maxsize=64)
def _content_version(path, modified_ns, size):
    return sha256(Path(path).read_bytes()).hexdigest()[:20]


def asset_version(static_folder, filename):
    if filename not in PUBLIC_UI_ASSETS:
        return None
    path = Path(static_folder) / filename
    info = path.stat()
    return _content_version(str(path), info.st_mtime_ns, info.st_size)


def is_versioned_ui_asset(static_folder, endpoint, method, values, args):
    if endpoint != 'static' or method not in {'GET', 'HEAD'}:
        return False
    filename = (values or {}).get('filename')
    if filename not in PUBLIC_UI_ASSETS or len(args.getlist('v')) != 1:
        return False
    try:
        return compare_digest(args['v'], asset_version(static_folder, filename))
    except (OSError, TypeError):
        return False
