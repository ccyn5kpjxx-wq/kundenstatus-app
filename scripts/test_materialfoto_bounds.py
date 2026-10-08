"""Synthetic image/process regression: no portal app, network or live records."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from PIL import Image, ImageOps
import werkstatt_fotoauslese as reader
from werkstatt_materialfoto import _decode_codes, _image


def picture(image=None, kind='PNG'):
    buffer = io.BytesIO()
    (image or Image.new('RGB', (80, 60), 'green')).save(buffer, kind)
    return buffer.getvalue()


def qr_image(value='SKU-123456', size=420):
    import cv2
    image = ImageOps.expand(Image.fromarray(cv2.QRCodeEncoder_create().encode(value)), border=4, fill=255)
    return image.resize((size, size), Image.Resampling.NEAREST).convert('RGB')


class PhotoBoundsTests(unittest.TestCase):
    def test_real_prepare_normalizes_and_decodes_original_in_one_child(self):
        raw = picture(qr_image().rotate(90))
        run = subprocess.run
        with patch.object(subprocess, 'run', wraps=run) as calls:
            clean, decoded = _image(raw, with_codes=True)
            evidence = _decode_codes(raw, hashlib.sha256(clean).hexdigest(), decoded=decoded)
        self.assertEqual(calls.call_count, 1)
        self.assertEqual(evidence['status'], 'erkannt')
        self.assertEqual(evidence['codes'][0]['suchwert'], 'SKU-123456')
        with Image.open(io.BytesIO(clean)) as image:
            self.assertEqual(image.format, 'JPEG')
            self.assertFalse(image.getexif())
        self.assertEqual(raw, picture(qr_image().rotate(90)))

    def test_corrupt_and_oversized_images_fail_without_clean_output(self):
        for raw in (b'not an image', picture(Image.new('RGB', (4500, 4500), 'white'))):
            with self.subTest(size=len(raw)), self.assertRaises(ValueError):
                reader.read_photo(raw, decode=True)
        self.assertTrue(reader.read_photo(picture())[0].startswith(b'\xff\xd8\xff'))

    def test_timeout_kills_reaps_cleans_temp_and_releases_slot(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'slow.py'
            worker.write_text('import time; time.sleep(30)', encoding='utf-8')
            children, paths = [], []
            popen = subprocess.Popen
            def start(command, **kwargs):
                paths.append(Path(command[3]).parent)
                child = popen(command, **kwargs)
                children.append(child)
                return child
            with patch.object(reader, '__file__', str(worker)), patch.object(subprocess, 'Popen', side_effect=start):
                started = time.perf_counter()
                with self.assertRaises(ValueError):
                    reader.read_photo(picture(), timeout=.3)
                self.assertLess(time.perf_counter() - started, 2)
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].poll())
            self.assertTrue(all(not path.exists() for path in paths))
        self.assertIsNotNone(reader.read_photo(picture())[0])

    def test_decoder_timeout_preserves_completed_jpeg_and_falls_back(self):
        jpeg = picture(kind='JPEG')
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'slow_decoder.py'
            worker.write_text('import pathlib,sys,time\npathlib.Path(sys.argv[3]).write_bytes(' + repr(jpeg) + ')\ntime.sleep(30)', encoding='utf-8')
            with patch.object(reader, '__file__', str(worker)):
                clean, decoded = reader.read_photo(picture(), decode=True, timeout=.7)
        self.assertEqual(clean, jpeg)
        evidence = _decode_codes(picture(), hashlib.sha256(clean).hexdigest(), decoded=decoded)
        self.assertEqual(evidence['status'], 'nicht_verfuegbar')
        self.assertEqual(evidence['codes'], [])

    def test_invalid_decoder_metadata_cannot_invalidate_completed_normalization(self):
        jpeg = picture(kind='JPEG')
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'invalid_metadata.py'
            worker.write_text('import pathlib,sys\npathlib.Path(sys.argv[3]).write_bytes(' + repr(jpeg) + ')\n'
                              'pathlib.Path(sys.argv[4]).write_text("{unfinished")', encoding='utf-8')
            with patch.object(reader, '__file__', str(worker)):
                clean, decoded = reader.read_photo(picture(), decode=True)
        self.assertEqual(clean, jpeg)
        self.assertFalse(decoded['available'])

    def test_temp_io_and_process_start_errors_are_safe_and_release_slot(self):
        raw = picture()
        for target in ('werkstatt_fotoauslese.tempfile.TemporaryDirectory',
                       'werkstatt_fotoauslese.Path.write_bytes',
                       'werkstatt_fotoauslese.subprocess.Popen'):
            with self.subTest(target=target), patch(target, side_effect=OSError('synthetic private OS detail')):
                with self.assertRaisesRegex(ValueError, 'Bitte erneut versuchen') as error:
                    reader.read_photo(raw, decode=True)
                self.assertNotIn('private OS detail', str(error.exception))
            self.assertTrue(reader._PHOTO_SLOTS.acquire(blocking=False))
            reader._PHOTO_SLOTS.release()
        self.assertIsNotNone(reader.read_photo(raw)[0])

    def test_nonblocking_slot_leaves_pool_available_for_health_work(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'slow.py'
            worker.write_text('import time; time.sleep(30)', encoding='utf-8')
            started = threading.Event()
            popen = subprocess.Popen
            def start(*args, **kwargs):
                child = popen(*args, **kwargs)
                started.set()
                return child
            with patch.object(reader, '__file__', str(worker)), patch.object(subprocess, 'Popen', side_effect=start), ThreadPoolExecutor(max_workers=4) as pool:
                first = pool.submit(reader.read_photo, picture(), timeout=.7)
                self.assertTrue(started.wait(2))
                others = [pool.submit(reader.read_photo, picture()) for _ in range(3)]
                for future in others:
                    with self.assertRaises(reader.PhotoProcessingBusy):
                        future.result(timeout=.3)
                self.assertEqual(pool.submit(lambda: 'healthy').result(timeout=.3), 'healthy')
                with self.assertRaises(ValueError):
                    first.result(timeout=2)
        self.assertIsNotNone(reader.read_photo(picture())[0])

    def test_real_child_does_not_import_app_or_receive_credentials(self):
        actual = str(Path(reader.__file__).resolve())
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'inspect_worker.py'
            worker.write_text('import runpy,os,sys,json,pathlib\nrunpy.run_path(' + repr(actual) + ',run_name="__main__")\n'
                'path=pathlib.Path(sys.argv[4])\nresult=json.loads(path.read_text())\n'
                'result["app_loaded"]="app" in sys.modules\n'
                'result["credentials"]=[key for key in os.environ if key in ("OPENAI_API_KEY","DATABASE_URL","FLASK_SECRET_KEY")]\n'
                'path.write_text(json.dumps(result))', encoding='utf-8')
            with patch.dict('os.environ', OPENAI_API_KEY='synthetic', DATABASE_URL='synthetic', FLASK_SECRET_KEY='synthetic'):
                with patch.object(reader, '__file__', str(worker)):
                    _, result = reader.read_photo(picture(), decode=True)
        self.assertFalse(result['app_loaded'])
        self.assertEqual(result['credentials'], [])

    def test_dense_real_qr_photo_has_bounded_runtime_and_usable_normalization(self):
        image = Image.new('RGB', (2560, 2560), 'white')
        code = qr_image(size=144)
        for y in range(16):
            for x in range(16):
                image.paste(code, (x * 160 + 8, y * 160 + 8))
        raw = picture(image)
        self.assertLess(len(raw), reader.MAX_BYTES)
        started = time.perf_counter()
        clean, result = reader.read_photo(raw, decode=True)
        self.assertLess(time.perf_counter() - started, reader.PHOTO_TIMEOUT_SECONDS + 2)
        self.assertTrue(clean.startswith(b'\xff\xd8\xff'))
        if result['available']:
            self.assertTrue(result['incomplete'])
        else:
            self.assertEqual(_decode_codes(raw, 'test', decoded=result)['status'], 'nicht_verfuegbar')


if __name__ == '__main__':
    unittest.main()
