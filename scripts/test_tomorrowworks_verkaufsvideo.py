"""Fokustest für den internen Verkaufsvideo-Generator."""

from __future__ import annotations

import io
import re
import sqlite3
import sys
import tempfile
import wave
from array import array
from pathlib import Path

from PIL import Image, ImageDraw

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import tomorrowworks_dashboard.app as dashboard_module
import tomorrowworks_dashboard.sales_video as sales_video_module
from tomorrowworks_dashboard import create_app


def csrf(client, path: str) -> str:
    response = client.get(path, follow_redirects=True)
    assert response.status_code == 200
    match = re.search(rb'name="_csrf_token" value="([^"]+)"', response.data)
    assert match, f"Kein CSRF-Token auf {path}"
    return match.group(1).decode("utf-8")


def post(client, path: str, data: dict, *, follow_redirects: bool = True, content_type: str | None = None):
    values = dict(data)
    values["_csrf_token"] = csrf(client, path)
    return client.post(path, data=values, follow_redirects=follow_redirects, content_type=content_type)


def png_bytes() -> bytes:
    image = Image.new("RGB", (960, 540), "#f2eadc")
    draw = ImageDraw.Draw(image)
    draw.rectangle((40, 40, 920, 500), fill="#173f36")
    draw.rectangle((95, 95, 865, 445), fill="#e8b35d")
    draw.text((140, 225), "KUNDEN-HOMEPAGE", fill="#171915")
    stream = io.BytesIO()
    image.save(stream, "PNG")
    return stream.getvalue()


def speech_wav_bytes(duration: float = 0.8, sample_rate: int = 16_000) -> bytes:
    stream = io.BytesIO()
    samples = array("h", [0] * int(duration * sample_rate))
    with wave.open(stream, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(sample_rate)
        audio.writeframes(samples.tobytes())
    return stream.getvalue()


class FakeSpeechResponse:
    def __init__(self, payload: bytes, status_code: int = 200):
        self.payload = payload
        self.status_code = status_code
        self.headers = {"Content-Type": "audio/wav"}
        self.closed = False

    def iter_content(self, chunk_size: int):
        for start in range(0, len(self.payload), chunk_size):
            yield self.payload[start:start + chunk_size]

    def close(self):
        self.closed = True


def run() -> None:
    with tempfile.TemporaryDirectory(prefix="tw-sales-video-test-") as temp_name:
        root = Path(temp_name)
        database = root / "dashboard.db"
        uploads = root / "uploads"
        app = create_app(
            {
                "TESTING": True,
                "DATABASE": str(database),
                "UPLOAD_FOLDER": str(uploads),
                "SECRET_KEY": "sales-video-test-secret",
                "GIT_MONITOR_ENABLED": False,
                "TTS_API_KEY": "",
            }
        )
        client = app.test_client()
        setup = post(
            client,
            "/einrichtung",
            {"name": "Test Admin", "email": "admin@example.test", "passwort": "sicher-test-123"},
        )
        assert setup.status_code == 200

        customer = post(
            client,
            "/kunden/neu",
            {
                "firma": "Kreativatelier Beispiel",
                "ansprechpartner": "Testperson Beispiel",
                "email": "kontakt@example.test",
                "telefon": "+49 171 1234567",
                "adresse": "Musterstraße 12, 74821 Mosbach",
                "website": "https://example.test",
                "status": "interessent",
            },
            follow_redirects=False,
        )
        assert customer.status_code == 302
        customer_id = int(customer.headers["Location"].rstrip("/").split("/")[-1])

        project = post(
            client,
            "/projekte/neu",
            {
                "kunde_id": str(customer_id),
                "titel": "Website, Logo & Visitenkarten",
                "typ": "website",
                "beschreibung": "Kundenvorschau für den neuen Auftritt.",
                "status": "interne_pruefung",
                "prioritaet": "normal",
                "fortschritt": "80",
                "aktuelle_aufgabe": "Präsentation vorbereiten",
                "vorschau_url": "",
                "repo_url": "",
                "lokaler_pfad": "",
                "preview_pfad": ".",
            },
            follow_redirects=False,
        )
        assert project.status_code == 302
        project_id = int(project.headers["Location"].rstrip("/").split("/")[-1])

        customer_page = client.get(f"/kunden/{customer_id}").get_data(as_text=True)
        project_page = client.get(f"/projekte/{project_id}").get_data(as_text=True)
        video_form = client.get(f"/projekte/{project_id}/verkaufsvideo").get_data(as_text=True)
        assert "Verkaufsvideo generieren" in customer_page
        assert "Verkaufsvideo" in project_page
        assert "KI-generierte Stimme" in video_form
        assert "Nur dieser Sprechertext wird an OpenAI" in video_form

        anonymous = app.test_client()
        denied = anonymous.get(f"/projekte/{project_id}/verkaufsvideo", follow_redirects=False)
        assert denied.status_code == 302
        assert "/anmelden" in denied.headers["Location"]
        no_csrf = client.post(f"/projekte/{project_id}/verkaufsvideo", data={"datei_ids": "1"})
        assert no_csrf.status_code == 400

        too_many = post(
            client,
            f"/projekte/{project_id}/verkaufsvideo",
            {
                "datei_ids": [str(index) for index in range(1, 10)],
                "headline": "Zu viele Quellen",
                "subtitle": "Die Auswahl muss verständlich begrenzt werden.",
                "kapitel": "Startseite",
                "potenziale": "Google-Sichtbarkeit ausbauen",
                "cta": "Auswahl anpassen.",
                "foliendauer": "2",
            },
        )
        assert "höchstens acht Quelldateien" in too_many.get_data(as_text=True)

        generic = post(
            client,
            f"/projekte/{project_id}/verkaufsvideo",
            {
                "headline": "Ihr neuer digitaler Auftritt",
                "subtitle": "Ein Projekt- und Potenzialfilm funktioniert auch vor dem ersten Screenshot.",
                "kapitel": "",
                "potenziale": "Google-Sichtbarkeit ausbauen\nOnline-Termine vereinfachen",
                "cta": "Jetzt den visuellen Rundgang ergänzen.",
                "foliendauer": "2",
            },
        )
        assert generic.status_code == 200
        generic_body = generic.get_data(as_text=True)
        assert "mit dezenter Musik intern erzeugt" in generic_body, generic_body[:2000]

        upload = client.post(
            f"/projekte/{project_id}/datei",
            data={
                "_csrf_token": csrf(client, f"/projekte/{project_id}"),
                "datei": (io.BytesIO(png_bytes()), "homepage-vorschau.png"),
            },
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert upload.status_code == 200

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        source = connection.execute(
            "SELECT * FROM projekt_dateien WHERE projekt_id = ? AND original_name = 'homepage-vorschau.png'",
            (project_id,),
        ).fetchone()
        assert source is not None
        old_path = uploads / "old-generated.mp4"
        old_path.write_bytes(b"old-internal-video")
        connection.execute(
            """
            INSERT INTO projekt_dateien
              (projekt_id, original_name, gespeichert_name, mimetype, hochgeladen_von, created_at)
            VALUES (?, 'Verkaufsvideo_alt.mp4', 'old-generated.mp4', 'video/mp4', 1, '2026-08-01 10:00:00')
            """,
            (project_id,),
        )
        before_messages = connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0]
        before_notifications = connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0]
        connection.commit()
        connection.close()

        original_mail = dashboard_module.mail_senden
        original_speech_post = sales_video_module.requests.post
        mail_called = False

        def forbidden_mail(**_kwargs):
            nonlocal mail_called
            mail_called = True
            raise AssertionError("Verkaufsvideo darf keine Kundenmail auslösen")

        dashboard_module.mail_senden = forbidden_mail
        sales_video_module.requests.post = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("Film ohne Sprecherstimme darf keinen externen Sprachdienst aufrufen")
        )
        try:
            generated = post(
                client,
                f"/projekte/{project_id}/verkaufsvideo",
                {
                    "datei_ids": str(source["id"]),
                    "headline": "Ihr neuer digitaler Auftritt",
                    "subtitle": "Ein kompakter Rundgang durch den aktuellen Entwurf.",
                    "kapitel": "Startseite & Positionierung",
                    "potenziale": "Google-Sichtbarkeit ausbauen\nOnline-Termine vereinfachen",
                    "cta": "Den Entwurf gemeinsam finalisieren.",
                    "foliendauer": "2",
                },
                follow_redirects=True,
            )
        finally:
            dashboard_module.mail_senden = original_mail
            sales_video_module.requests.post = original_speech_post

        body = generated.get_data(as_text=True)
        assert generated.status_code == 200
        assert "mit dezenter Musik intern erzeugt" in body
        assert "<video" in body
        assert "kein Kundenversand" in body
        assert mail_called is False
        assert not old_path.exists()

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        videos = connection.execute(
            "SELECT * FROM projekt_dateien WHERE projekt_id = ? AND mimetype = 'video/mp4'",
            (project_id,),
        ).fetchall()
        assert len(videos) == 1
        assert connection.execute("SELECT COUNT(*) FROM portal_nachrichten").fetchone()[0] == before_messages
        assert connection.execute("SELECT COUNT(*) FROM portal_benachrichtigungen").fetchone()[0] == before_notifications
        activity = connection.execute(
            "SELECT * FROM aktivitaeten WHERE projekt_id = ? AND aktion LIKE 'Verkaufsvideo generiert%'",
            (project_id,),
        ).fetchone()
        connection.close()
        assert activity is not None
        assert "kein Kundenversand" in activity["text"]

        video_path = uploads / videos[0]["gespeichert_name"]
        assert video_path.exists()
        assert video_path.stat().st_size < 20 * 1024 * 1024
        assert video_path.read_bytes()[4:8] == b"ftyp"
        video_response = client.get(f"/dateien/{videos[0]['id']}")
        assert video_response.status_code == 200
        assert video_response.content_type.startswith("video/mp4")
        video_response.close()

        without_key = post(
            client,
            f"/projekte/{project_id}/verkaufsvideo",
            {
                "datei_ids": str(source["id"]),
                "headline": "Sprecherin ohne Konfiguration",
                "subtitle": "Die bestehende Datei muss bei einem Konfigurationsfehler erhalten bleiben.",
                "kapitel": "Startseite & Positionierung",
                "servicepunkte": "Laufende Betreuung\nTransparentes Kundenportal",
                "potenziale": "Google-Sichtbarkeit ausbauen",
                "mit_sprecher": "1",
                "sprechertext": "Dieser ausreichend lange Sprechertext darf ohne sichere API-Konfiguration nicht verarbeitet werden.",
                "cta": "Konfiguration prüfen.",
                "foliendauer": "2",
            },
        )
        assert "noch nicht eingerichtet" in without_key.get_data(as_text=True)
        assert video_path.exists()

        captured_request: dict = {}

        def fake_speech_post(url, *, headers, json, stream, timeout):
            captured_request.update(
                {"url": url, "headers": headers, "json": json, "stream": stream, "timeout": timeout}
            )
            return FakeSpeechResponse(speech_wav_bytes())

        app.config["TTS_API_KEY"] = "test-secret-not-for-output"
        sales_video_module.requests.post = fake_speech_post
        try:
            narrated = post(
                client,
                f"/projekte/{project_id}/verkaufsvideo",
                {
                    "datei_ids": str(source["id"]),
                    "headline": "Ihr neuer betreuter Auftritt",
                    "subtitle": "Website, Betreuung und Sichtbarkeit überzeugend erklärt.",
                    "kapitel": "Startseite & Positionierung",
                    "servicepunkte": "Laufende Betreuung im Abo\nGoogle-Auswertung nach Freigabe\nTransparentes Kundenportal",
                    "potenziale": "Google-Sichtbarkeit ausbauen\nOnline-Termine vereinfachen",
                    "mit_sprecher": "1",
                    "sprechertext": "Willkommen zu Ihrem neuen digitalen Auftritt. Wir zeigen die Website und entwickeln Sichtbarkeit und Betreuung transparent gemeinsam weiter.",
                    "cta": "Den Entwurf gemeinsam finalisieren.",
                    "foliendauer": "2",
                },
                follow_redirects=True,
            )
        finally:
            sales_video_module.requests.post = original_speech_post

        narrated_body = narrated.get_data(as_text=True)
        assert narrated.status_code == 200
        assert "mit KI-generierter Sprecherstimme" in narrated_body
        assert captured_request["url"] == sales_video_module.OPENAI_SPEECH_URL
        assert captured_request["stream"] is True
        assert captured_request["timeout"] == (10, 60)
        assert captured_request["json"]["response_format"] == "wav"
        assert captured_request["json"]["input"].startswith("Willkommen zu Ihrem neuen digitalen Auftritt")
        assert "homepage-vorschau.png" not in repr(captured_request["json"])
        assert captured_request["headers"]["Authorization"].startswith("Bearer ")
        assert mail_called is False

        connection = sqlite3.connect(database)
        connection.row_factory = sqlite3.Row
        videos = connection.execute(
            "SELECT * FROM projekt_dateien WHERE projekt_id = ? AND mimetype = 'video/mp4'",
            (project_id,),
        ).fetchall()
        latest_activity = connection.execute(
            "SELECT * FROM aktivitaeten WHERE projekt_id = ? AND aktion LIKE 'Verkaufsvideo generiert%' ORDER BY id DESC LIMIT 1",
            (project_id,),
        ).fetchone()
        connection.close()
        assert len(videos) == 1
        assert latest_activity is not None
        assert "KI-Sprecherstimme" in latest_activity["text"]
        assert "kein Kundenversand" in latest_activity["text"]
        video_path = uploads / videos[0]["gespeichert_name"]
        assert video_path.exists()
        assert video_path.stat().st_size < 20 * 1024 * 1024

        files_before_failure = {path.name for path in uploads.glob(f"{project_id}_verkaufsvideo_*.mp4")}
        original_create = dashboard_module.create_sales_video

        def broken_generator(_sources, output_path, **_kwargs):
            output_path.write_bytes(b"partial-video")
            raise RuntimeError("simulierter FFmpeg-Abbruch")

        dashboard_module.create_sales_video = broken_generator
        try:
            failed = post(
                client,
                f"/projekte/{project_id}/verkaufsvideo",
                {
                    "headline": "Fehlerfall",
                    "subtitle": "Ein technischer Fehler darf keine halbe Datei hinterlassen.",
                    "kapitel": "",
                    "potenziale": "Google-Sichtbarkeit ausbauen",
                    "cta": "Später erneut versuchen.",
                    "foliendauer": "2",
                },
            )
        finally:
            dashboard_module.create_sales_video = original_create
        assert failed.status_code == 200
        assert "unerwartet nicht erzeugt" in failed.get_data(as_text=True)
        assert {path.name for path in uploads.glob(f"{project_id}_verkaufsvideo_*.mp4")} == files_before_failure

        connection = sqlite3.connect(database)
        assert connection.execute(
            "SELECT COUNT(*) FROM projekt_dateien WHERE projekt_id = ? AND mimetype = 'video/mp4'",
            (project_id,),
        ).fetchone()[0] == 1
        connection.close()

    print("Tomorrow Works Verkaufsvideo: intern, geschützt und ohne Kundenversand erfolgreich getestet.")


if __name__ == "__main__":
    run()
