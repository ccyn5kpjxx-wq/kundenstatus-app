"""Real process timeout/cleanup and receipt extraction boundary checks."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import werkstatt_belegauslese as ocr


class ReceiptProcessTests(unittest.TestCase):
    def test_timeout_kills_and_reaps_child(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / 'slow_worker.py'
            worker.write_text('import time; time.sleep(30)', encoding='utf-8')
            original = worker.with_name('original.png')
            original.write_bytes(b'test')
            children = []
            real_popen = subprocess.Popen

            def start(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                children.append(process)
                return process

            with patch.object(ocr, '__file__', str(worker)), patch.object(subprocess, 'Popen', side_effect=start):
                with self.assertRaises(TimeoutError):
                    ocr.read_receipt(original, timeout=.3)
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].poll())

    def test_child_receives_no_credentials_and_output_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / 'original.png'
            original.write_bytes(b'test')

            def run(command, **kwargs):
                self.assertNotIn('OPENAI_API_KEY', kwargs['env'])
                self.assertNotIn('DATABASE_URL', kwargs['env'])
                self.assertEqual(kwargs['env']['OMP_NUM_THREADS'], '1')
                self.assertEqual(kwargs['timeout'], 20)
                Path(command[-1]).write_text('ü' * 30000, encoding='utf-8')

            with patch.dict('os.environ', OPENAI_API_KEY='test-secret', DATABASE_URL='test-secret'):
                with patch.object(subprocess, 'run', side_effect=run):
                    self.assertEqual(len(ocr.read_receipt(original)), ocr.MAX_TEXT)

    def test_digital_pdf_uses_real_disposable_reader(self):
        import fitz
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / 'original.pdf'
            with fitz.open() as document:
                page = document.new_page()
                page.insert_text((72, 72), 'LIEFERSCHEIN Test Artikel 123456')
                document.save(original)
            text = ocr.read_receipt(original)
            self.assertIn('Seite 1', text)
            self.assertIn('LIEFERSCHEIN', text)

    def test_too_many_pdf_pages_fail_without_partial_result(self):
        import fitz
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / 'original.pdf'
            with fitz.open() as document:
                for _ in range(ocr.MAX_PAGES + 1):
                    document.new_page()
                document.save(original)
            with self.assertRaises(ValueError):
                ocr.read_receipt(original)

    def test_empty_pdf_does_not_become_success_from_page_label(self):
        import fitz
        with tempfile.TemporaryDirectory() as directory:
            original = Path(directory) / 'original.pdf'
            with fitz.open() as document:
                document.new_page(width=300, height=300)
                document.save(original)
            self.assertEqual(ocr.read_receipt(original), '')


if __name__ == '__main__':
    unittest.main()
