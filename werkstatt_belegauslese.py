"""Bounded local receipt OCR in a disposable process, without importing app.py."""
import os
from pathlib import Path
import subprocess
import sys


ANALYSIS_TIMEOUT_SECONDS = 20
MAX_PAGES = 5
MAX_DIMENSION = 1600
MAX_TEXT = 24000
MAX_ADDRESS_SPACE = 1024 * 1024 * 1024


def read_receipt(path, timeout=ANALYSIS_TIMEOUT_SECONDS):
    path = Path(path).resolve()
    output = path.with_name('auslese.txt')
    # OCR needs no portal secrets or network credentials.
    env = {key: value for key, value in os.environ.items()
           if key in {'PATH', 'SystemRoot', 'WINDIR', 'TEMP', 'TMP', 'LD_LIBRARY_PATH'}}
    env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
               PYTHONIOENCODING='utf-8')
    try:
        subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker',
                        str(path), str(output)], check=True, timeout=timeout, env=env,
                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError('Belegauslese nach 20 Sekunden beendet.') from exc
    except subprocess.CalledProcessError as exc:
        raise ValueError('Lokale Belegauslese fehlgeschlagen.') from exc
    return output.read_text(encoding='utf-8')[:MAX_TEXT]


def _extract(path):
    if sys.platform == 'linux':
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (MAX_ADDRESS_SPACE, MAX_ADDRESS_SPACE))
    from PIL import Image, ImageOps
    import numpy as np
    from werkstatt_liefereingang import delivery_ocr_rows

    engine = None

    def image_text(image):
        nonlocal engine
        image = ImageOps.exif_transpose(image)
        image.thumbnail((MAX_DIMENSION, MAX_DIMENSION))
        image = image.convert('RGB')
        if engine is None:
            from rapidocr_onnxruntime import RapidOCR
            import rapidocr_onnxruntime.utils as rapid_utils
            # Version 1.2.3 ignores thread-count kwargs. Set the real ONNX
            # session options inside this disposable process only.
            original_options = rapid_utils.SessionOptions

            def bounded_options():
                options = original_options()
                options.intra_op_num_threads = 1
                options.inter_op_num_threads = 1
                return options

            rapid_utils.SessionOptions = bounded_options
            try:
                engine = RapidOCR()
            finally:
                rapid_utils.SessionOptions = original_options
        rows, _ = engine(np.asarray(image))
        return delivery_ocr_rows(rows)

    if path.suffix.lower() != '.pdf':
        with Image.open(path) as image:
            if image.width * image.height > 20_000_000:
                raise ValueError('Foto für automatische Auslese zu groß.')
            image.draft('RGB', (MAX_DIMENSION, MAX_DIMENSION))
            return image_text(image)[:MAX_TEXT]
    import fitz
    parts = []
    with fitz.open(path) as document:
        if document.page_count > MAX_PAGES:
            raise ValueError('Für mehr als fünf Seiten das Original manuell prüfen.')
        for number, page in enumerate(document, 1):
            text = page.get_text()
            if not text.strip():
                scale = min(2, MAX_DIMENSION / max(page.rect.width, page.rect.height))
                pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
                with Image.frombytes('RGB', (pixmap.width, pixmap.height), pixmap.samples) as image:
                    text = image_text(image)
            if text.strip():
                parts.append('Seite ' + str(number) + '\n' + text)
            if sum(map(len, parts)) >= MAX_TEXT:
                break
    return '\n'.join(parts)[:MAX_TEXT]


if __name__ == '__main__':
    if len(sys.argv) != 4 or sys.argv[1] != '--worker':
        raise SystemExit(2)
    Path(sys.argv[3]).write_text(_extract(Path(sys.argv[2])), encoding='utf-8')
