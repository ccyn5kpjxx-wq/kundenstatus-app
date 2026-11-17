"""Pure static media URL validation; no image downloader or HTTP."""
from urllib.parse import unquote,urlsplit

def safe_image_url(value):
    if not isinstance(value,str) or len(value)>2000:
        return None
    value=value.replace('%s-%s','h800-w800')
    try:
        parsed=urlsplit(value)
        if (parsed.scheme!='https' or parsed.hostname not in {'media1.tyre-shopping.com','cdn01.alzura.com'}
                or parsed.username or parsed.password or parsed.port is not None
                or parsed.query or parsed.fragment or '%' in unquote(parsed.path)
                or '\\' in unquote(parsed.path) or '..' in unquote(parsed.path).split('/')):
            return None
        if parsed.hostname=='media1.tyre-shopping.com' and not parsed.path.startswith('/mediamanagement/'):
            return None
        if not parsed.path.lower().endswith(('.jpg','.jpeg','.png','.webp')):
            return None
        return value
    except ValueError:
        return None
