"""Disposable, bounded image processing. The child imports no portal modules."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading


PHOTO_TIMEOUT_SECONDS = 3
MAX_BYTES = 8 * 1024 * 1024
MAX_PIXELS = 20_000_000
MAX_ADDRESS_SPACE = 1024 * 1024 * 1024
MAX_METADATA_BYTES = 16384
_PHOTO_SLOTS = threading.BoundedSemaphore(1)


class PhotoProcessingBusy(ValueError):
    """Native image processing is occupied; never queue HTTP threads."""


def _unavailable():
    return {'available': False, 'decoded': [], 'incomplete': False}


def read_photo(raw, *, decode=False, codes_only=False, timeout=PHOTO_TIMEOUT_SECONDS):
    """Return a clean JPEG and optional raw code hints within one process budget.

    A completed JPEG survives a decoder timeout. An incomplete normalization
    fails without persisting anything. The caller retains the original bytes.
    """
    if not isinstance(raw, bytes) or not raw or len(raw) > MAX_BYTES:
        raise ValueError('Foto leer oder größer als 8 MB.')
    if not _PHOTO_SLOTS.acquire(blocking=False):
        raise PhotoProcessingBusy('Die Fotoverarbeitung ist gerade belegt. Bitte kurz erneut versuchen.')
    try:
        with tempfile.TemporaryDirectory(prefix='werkstatt-foto-') as directory:
            root = Path(directory)
            original, clean, metadata = root / 'original', root / 'clean.jpg', root / 'result.json'
            original.write_bytes(raw)
            env = {key: value for key, value in os.environ.items()
                   if key in {'PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'LD_LIBRARY_PATH'}}
            env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
                       PYTHONIOENCODING='utf-8')
            mode = 'codes' if codes_only else 'prepare' if decode else 'normalize'
            try:
                subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker',
                                str(original), str(clean), str(metadata), mode],
                               check=True, timeout=timeout, env=env, cwd=directory,
                               stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
                # subprocess.run kills AND reaps a timed-out child before the
                # temporary directory or the process admission slot is released.
                pass
            result = _unavailable()
            if metadata.exists() and metadata.stat().st_size <= MAX_METADATA_BYTES:
                try:
                    candidate = json.loads(metadata.read_text(encoding='utf-8'))
                    if isinstance(candidate, dict):
                        result = candidate
                except (ValueError, OSError):
                    # A failed optional decoder cannot invalidate an already
                    # completed normalization or manufacture code evidence.
                    pass
                if result.get('invalid_image'):
                    raise ValueError('Kein gültiges Materialfoto. Bitte ein einzelnes JPEG, PNG oder WebP mit höchstens 20 Megapixeln auswählen.')
            if codes_only:
                return None, result
            if not clean.exists() or not 0 < clean.stat().st_size <= MAX_BYTES:
                raise ValueError('Das Foto konnte nicht rechtzeitig verarbeitet werden. Bitte erneut versuchen oder ein kleineres Foto wählen.')
            normalized = clean.read_bytes()
            if not normalized.startswith(b'\xff\xd8\xff') or not normalized.endswith(b'\xff\xd9'):
                raise ValueError('Das Foto konnte nicht vollständig verarbeitet werden. Bitte erneut versuchen.')
            return normalized, result
    except OSError as exc:
        raise ValueError('Das Foto konnte nicht verarbeitet werden. Bitte erneut versuchen.') from exc
    finally:
        _PHOTO_SLOTS.release()


def _load_image(raw):
    from PIL import Image, ImageOps
    with Image.open(io.BytesIO(raw)) as image:
        if (image.format not in ('JPEG', 'PNG', 'WEBP') or image.width * image.height > MAX_PIXELS
                or getattr(image, 'n_frames', 1) != 1):
            raise ValueError('invalid image')
        image.verify()
    with Image.open(io.BytesIO(raw)) as image:
        return ImageOps.exif_transpose(image).convert('RGB')


def _normalize(raw):
    with _load_image(raw) as image:
        image.thumbnail((2048, 2048))
        buffer = io.BytesIO()
        image.save(buffer, format='JPEG', quality=90)
        return buffer.getvalue()


def _scan_codes(raw):
    """Original-pixel QR/EAN/UPC hints; business validation stays in the parent."""
    try:
        import cv2
        import numpy as np
        cv2.setNumThreads(1)
        with _load_image(raw) as image:
            image.thumbnail((2560, 2560))
            pixels = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2GRAY)
        detector = cv2.QRCodeDetector()
        decoded = []
        okay, values, regions, _ = detector.detectAndDecodeMulti(pixels)
        incomplete = regions is not None and len(regions) > 1 and len(regions) > len(values)
        if okay:
            incomplete |= len(values) > 8 or len(values) > 1 and any(not value for value in values)
            decoded.extend(('qr_code', value) for value in values[:8] if value)
        if not decoded:
            value, _, _ = detector.detectAndDecode(pixels)
            if value:
                decoded.append(('qr_code', value))
        try:
            barcode = cv2.barcode_BarcodeDetector()
            okay, values, formats, _ = barcode.detectAndDecodeWithType(pixels)
            if okay:
                incomplete |= len(values) > 8 or len(values) > 1 and any(not value for value in values)
                aliases = {'EAN_8': 'ean_8', 'EAN_13': 'ean_13', 'UPC_A': 'upc_a', 'UPC_E': 'upc_e'}
                decoded.extend((aliases[kind], value) for value, kind in zip(values[:8], formats[:8]) if kind in aliases)
        except (AttributeError, cv2.error):
            pass
        # Values over 60 characters cannot become product identifiers. Retain
        # their invalidity/ambiguity without transferring an unbounded QR body.
        return {'available': True, 'decoded': [[kind, str(value)[:61]] for kind, value in decoded],
                'incomplete': bool(incomplete)}
    except Exception:
        return _unavailable()


def _write_atomic(path, raw):
    pending = path.with_suffix(path.suffix + '.tmp')
    pending.write_bytes(raw)
    os.replace(pending, path)


def _worker(original, clean, metadata, mode):
    if sys.platform == 'linux':
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (MAX_ADDRESS_SPACE, MAX_ADDRESS_SPACE))
    raw = original.read_bytes()
    result = _unavailable()
    if mode != 'codes':
        try:
            _write_atomic(clean, _normalize(raw))
        except Exception:
            _write_atomic(metadata, b'{"invalid_image":true}')
            return
    if mode != 'normalize':
        result = _scan_codes(raw)
    _write_atomic(metadata, json.dumps(result, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    if len(sys.argv) != 6 or sys.argv[1] != '--worker' or sys.argv[5] not in {'normalize', 'prepare', 'codes'}:
        raise SystemExit(2)
    _worker(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5])
