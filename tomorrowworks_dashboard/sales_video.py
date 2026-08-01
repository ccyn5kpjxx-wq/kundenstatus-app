"""Erzeugt kurze, intern freizugebende Verkaufsvideos aus Projektdateien.

Die Bildsprache und die leise Tonspur werden vollständig lokal erzeugt. Es werden
keine Kundendaten an einen externen Video- oder KI-Dienst übertragen.
"""

from __future__ import annotations

import math
import os
import shutil
import subprocess
import tempfile
import wave
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import fitz
import imageio_ffmpeg
from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps


VIDEO_WIDTH = 1280
VIDEO_HEIGHT = 720
VIDEO_FPS = 25
MAX_CONTENT_SLIDES = 10


class SalesVideoError(RuntimeError):
    """Verständlicher Fehler für die Cockpit-Oberfläche."""


@dataclass(frozen=True)
class SalesVideoSource:
    path: Path
    display_name: str


@dataclass(frozen=True)
class SalesVideoResult:
    slide_count: int
    duration_seconds: int
    source_count: int


@dataclass
class _Frame:
    image: Image.Image
    source_name: str
    page_number: int | None = None


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    linux_name = "DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"
    candidates = [
        Path("/usr/share/fonts/truetype/dejavu") / linux_name,
        Path("/usr/share/fonts/truetype/liberation2")
        / ("LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf"),
        Path("C:/Windows/Fonts") / ("segoeuib.ttf" if bold else "segoeui.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return ImageFont.truetype(str(candidate), size=size)
            except OSError:
                pass
    # Pillow >= 10 bringt eine skalierbare Standardschrift mit. Damit bleibt die
    # Generierung auch auf schlanken Render-Images ohne Systemschrift lauffähig.
    return ImageFont.load_default(size=size)


def _text_width(draw: ImageDraw.ImageDraw, text: str, font) -> int:
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0]


def _wrap(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> list[str]:
    words = (text or "").split()
    if not words:
        return []
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        candidate = f"{current} {word}"
        if _text_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


def _draw_wrapped(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int],
    text: str,
    font,
    fill: str | tuple[int, ...],
    max_width: int,
    *,
    line_gap: int = 8,
    max_lines: int | None = None,
) -> int:
    lines = _wrap(draw, text, font, max_width)
    if max_lines:
        lines = lines[:max_lines]
    x, y = xy
    line_height = draw.textbbox((0, 0), "Ag", font=font)[3] + line_gap
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        y += line_height
    return y


def _human_name(filename: str) -> str:
    stem = Path(filename).stem
    for fragment in ("Homepage-Vorschau_", "Verkaufsvideo_", "Visitenkartenentwurf_"):
        stem = stem.replace(fragment, "")
    stem = stem.replace("_", " ").replace("-", " ")
    return " ".join(stem.split()).strip() or "Projektansicht"


def _load_frames(sources: Sequence[SalesVideoSource]) -> list[_Frame]:
    frames: list[_Frame] = []
    for source in sources:
        if len(frames) >= MAX_CONTENT_SLIDES:
            break
        suffix = source.path.suffix.lower()
        if suffix == ".pdf":
            try:
                document = fitz.open(source.path)
                with document:
                    for index, page in enumerate(document):
                        if len(frames) >= MAX_CONTENT_SLIDES:
                            break
                        longest_edge = max(float(page.rect.width), float(page.rect.height), 1.0)
                        scale = min(1.55, 1800.0 / longest_edge)
                        pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
                        image = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
                        frames.append(_Frame(image=image, source_name=source.display_name, page_number=index + 1))
            except Exception as exc:  # beschädigte PDFs sollen nicht den gesamten Projektstand zerstören
                raise SalesVideoError(f"PDF ‚{source.display_name}‘ konnte nicht gelesen werden.") from exc
        elif suffix in {".png", ".jpg", ".jpeg", ".webp"}:
            try:
                with Image.open(source.path) as opened:
                    if opened.width * opened.height > 24_000_000:
                        raise SalesVideoError(
                            f"Bild ‚{source.display_name}‘ ist für die sichere Videoerzeugung zu groß."
                        )
                    oriented = ImageOps.exif_transpose(opened)
                    oriented.thumbnail((2400, 1800), Image.Resampling.LANCZOS)
                    prepared = oriented.convert("RGBA")
                    background = Image.new("RGBA", prepared.size, "#f3eee4")
                    image = Image.alpha_composite(background, prepared).convert("RGB")
                    frames.append(_Frame(image=image.copy(), source_name=source.display_name))
            except SalesVideoError:
                raise
            except Exception as exc:
                raise SalesVideoError(f"Bild ‚{source.display_name}‘ konnte nicht gelesen werden.") from exc
    return frames


def _background_from(image: Image.Image) -> Image.Image:
    background = ImageOps.fit(image, (VIDEO_WIDTH, VIDEO_HEIGHT), method=Image.Resampling.LANCZOS)
    background = background.filter(ImageFilter.GaussianBlur(radius=28))
    background = ImageEnhance.Brightness(background).enhance(0.38)
    overlay = Image.new("RGBA", (VIDEO_WIDTH, VIDEO_HEIGHT), (4, 12, 11, 72))
    return Image.alpha_composite(background.convert("RGBA"), overlay)


def _rounded_image(image: Image.Image, size: tuple[int, int], radius: int = 22) -> Image.Image:
    contained = ImageOps.contain(image.convert("RGB"), size, method=Image.Resampling.LANCZOS)
    mask = Image.new("L", contained.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, contained.width, contained.height), radius=radius, fill=255)
    result = Image.new("RGBA", contained.size, (255, 255, 255, 0))
    result.paste(contained.convert("RGBA"), (0, 0), mask)
    return result


def _chapter_description(chapter: str) -> str:
    value = chapter.lower()
    descriptions = (
        (("start", "position"), "Ein klarer erster Eindruck bringt Persönlichkeit, Angebot und nächsten Schritt sofort zusammen."),
        (("kurs", "leistung", "angebot"), "Die Angebote werden verständlich, hochwertig und mit einem direkten Weg zur Anfrage präsentiert."),
        (("termin", "buch"), "Interessierte können vom ersten Impuls ohne Umweg zur passenden Termin- oder Kursanfrage wechseln."),
        (("galerie", "referenz", "arbeit"), "Ausgewählte Arbeiten schaffen Vertrauen und zeigen auf einen Blick die eigene Handschrift."),
        (("über", "person", "vertrauen", "team"), "Die persönliche Geschichte macht aus einer Website eine glaubwürdige Einladung."),
        (("kontakt", "anfrage", "whatsapp"), "Kontaktwege und Handlungsaufforderungen sind sichtbar, mobil und leicht erreichbar."),
        (("visiten", "qr", "print"), "Print und Website greifen ineinander: Der QR-Code führt direkt vom persönlichen Kontakt zur Buchung."),
        (("logo", "marke", "branding"), "Ein wiedererkennbarer Auftritt verbindet Website, Print und künftige Werbemaßnahmen."),
    )
    for keywords, description in descriptions:
        if any(keyword in value for keyword in keywords):
            return description
    return "Dieser Baustein zeigt den aktuellen Entwurf und macht den Nutzen für Interessierte schnell erfassbar."


def _title_slide(customer: str, headline: str, subtitle: str, project_title: str, progress: int) -> Image.Image:
    image = Image.new("RGB", (VIDEO_WIDTH, VIDEO_HEIGHT), "#efe8dc")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, 34, VIDEO_HEIGHT), fill="#ff641a")
    draw.ellipse((865, -230, 1385, 290), fill="#d8b168")
    draw.ellipse((1025, 400, 1375, 750), fill="#173f36")
    draw.text((88, 72), "TOMORROW WORKS  ·  PROJEKTVORSCHAU", font=_font(22, bold=True), fill="#d54c0d")
    y = _draw_wrapped(draw, (88, 154), headline, _font(72, bold=True), "#171915", 820, line_gap=5, max_lines=3)
    y = _draw_wrapped(draw, (92, y + 28), subtitle, _font(29), "#4d514c", 760, line_gap=7, max_lines=3)
    draw.rounded_rectangle((88, 605, 750, 660), radius=27, fill="#ffffff")
    draw.text((116, 620), customer, font=_font(22, bold=True), fill="#171915")
    draw.text((920, 178), f"{max(0, min(progress, 100))}%", font=_font(78, bold=True), fill="#171915")
    draw.text((922, 265), "PROJEKTSTAND", font=_font(18, bold=True), fill="#6c5b3b")
    draw.rounded_rectangle((870, 310, 1218, 540), radius=28, fill="#173f36")
    project = project_title if len(project_title) <= 34 else project_title[:31].rstrip() + "…"
    _draw_wrapped(draw, (900, 342), project, _font(27, bold=True), "#ffffff", 285, line_gap=5, max_lines=4)
    return image


def _content_slide(frame: _Frame, chapter: str, number: int) -> Image.Image:
    canvas = _background_from(frame.image)
    draw = ImageDraw.Draw(canvas, "RGBA")
    visual = _rounded_image(frame.image, (1110, 585), radius=22)
    x = (VIDEO_WIDTH - visual.width) // 2
    y = (VIDEO_HEIGHT - visual.height) // 2 + 4
    draw.rounded_rectangle((x + 12, y + 15, x + visual.width + 12, y + visual.height + 15), radius=24, fill=(0, 0, 0, 115))
    canvas.alpha_composite(visual, (x, y))
    draw.rounded_rectangle((54, 44, 760, 180), radius=22, fill=(12, 17, 15, 225), outline=(255, 255, 255, 38), width=1)
    draw.text((80, 64), f"{number:02d}  ·  PROJEKTRUNDGANG", font=_font(17, bold=True), fill="#e8b35d")
    draw.text((80, 94), chapter, font=_font(35, bold=True), fill="#ffffff")
    _draw_wrapped(draw, (80, 139), _chapter_description(chapter), _font(18), "#e8ece9", 645, line_gap=4, max_lines=2)
    label = _human_name(frame.source_name)
    if frame.page_number is not None:
        label += f"  ·  Seite {frame.page_number}"
    width = min(620, _text_width(draw, label, _font(15, bold=True)) + 36)
    draw.rounded_rectangle((VIDEO_WIDTH - width - 50, 654, VIDEO_WIDTH - 50, 691), radius=18, fill=(255, 255, 255, 225))
    draw.text((VIDEO_WIDTH - width - 32, 663), label, font=_font(15, bold=True), fill="#20231f")
    return canvas.convert("RGB")


def _project_overview_slide(customer: str, project_title: str, progress: int) -> Image.Image:
    image = Image.new("RGB", (VIDEO_WIDTH, VIDEO_HEIGHT), "#f2eee5")
    draw = ImageDraw.Draw(image)
    draw.text((82, 68), "AKTUELLER PROJEKTSTAND", font=_font(20, bold=True), fill="#d54c0d")
    _draw_wrapped(draw, (82, 125), project_title, _font(58, bold=True), "#171915", 850, line_gap=5, max_lines=3)
    draw.rounded_rectangle((82, 385, 1185, 424), radius=19, fill="#ddd8ce")
    progress_width = int(1103 * max(0, min(progress, 100)) / 100)
    if progress_width:
        draw.rounded_rectangle((82, 385, 82 + progress_width, 424), radius=19, fill="#ff641a")
    draw.text((82, 455), f"{progress}% ausgearbeitet", font=_font(32, bold=True), fill="#173f36")
    _draw_wrapped(
        draw,
        (82, 515),
        "Für einen vollständigen Rundgang können jederzeit Screenshots, Entwürfe oder PDFs im Projekt ergänzt werden.",
        _font(24),
        "#555b55",
        900,
        line_gap=8,
        max_lines=3,
    )
    draw.text((82, 647), customer, font=_font(18, bold=True), fill="#746b5e")
    return image


def _potential_slide(potentials: Sequence[str]) -> Image.Image:
    image = Image.new("RGB", (VIDEO_WIDTH, VIDEO_HEIGHT), "#101512")
    draw = ImageDraw.Draw(image)
    draw.ellipse((930, -190, 1370, 250), fill="#163f35")
    draw.text((78, 65), "NÄCHSTER HEBEL", font=_font(20, bold=True), fill="#ff7a38")
    draw.text((78, 112), "Potenzial für mehr Anfragen", font=_font(52, bold=True), fill="#fffaf1")
    draw.text((80, 179), "Die Website ist die Basis. Sichtbarkeit und einfache Kontaktwege machen daraus Kundengewinnung.", font=_font(22), fill="#bfc8c2")
    items = list(potentials)[:4] or ["Google-Sichtbarkeit gezielt ausbauen", "Kontakt und Terminbuchung messbar vereinfachen"]
    y = 260
    for index, item in enumerate(items, start=1):
        draw.rounded_rectangle((78, y, 1202, y + 80), radius=18, fill="#1c241f", outline="#334139", width=1)
        draw.ellipse((102, y + 20, 142, y + 60), fill="#e8b35d")
        number_text = str(index)
        draw.text((122 - _text_width(draw, number_text, _font(18, bold=True)) // 2, y + 28), number_text, font=_font(18, bold=True), fill="#151812")
        _draw_wrapped(draw, (166, y + 20), item, _font(23, bold=True), "#ffffff", 990, line_gap=4, max_lines=2)
        y += 96
    draw.text((80, 664), "Potenziale werden vor einer Kampagne gemeinsam priorisiert und freigegeben.", font=_font(17), fill="#8f9c94")
    return image


def _cta_slide(customer: str, cta: str) -> Image.Image:
    image = Image.new("RGB", (VIDEO_WIDTH, VIDEO_HEIGHT), "#e2b866")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 565, VIDEO_WIDTH, VIDEO_HEIGHT), fill="#173f36")
    draw.text((76, 65), "GEMEINSAM WEITERENTWICKELN", font=_font(20, bold=True), fill="#5c461d")
    _draw_wrapped(draw, (76, 132), cta, _font(65, bold=True), "#171915", 1070, line_gap=5, max_lines=3)
    draw.rounded_rectangle((76, 432, 735, 505), radius=36, fill="#fffaf1")
    draw.text((113, 452), "Vorschau ansehen  ·  Wünsche besprechen", font=_font(22, bold=True), fill="#173f36")
    draw.text((76, 611), customer, font=_font(28, bold=True), fill="#ffffff")
    draw.text((932, 620), "TOMORROW WORKS", font=_font(19, bold=True), fill="#b8d0c6")
    return image


def _write_ambient_wav(path: Path, duration_seconds: int, *, sample_rate: int = 32_000) -> None:
    chords = (
        (220.00, 277.18, 329.63),
        (196.00, 246.94, 293.66),
        (174.61, 220.00, 261.63),
        (196.00, 246.94, 329.63),
    )
    total = duration_seconds * sample_rate
    chunk_size = 16_000
    with wave.open(str(path), "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(sample_rate)
        for start in range(0, total, chunk_size):
            samples = array("h")
            for offset in range(min(chunk_size, total - start)):
                index = start + offset
                second = index / sample_rate
                chord = chords[int(second // 8) % len(chords)]
                global_fade = min(1.0, second / 1.8, max(0.0, (duration_seconds - second) / 2.2))
                chord_position = (second % 8) / 8
                pulse = 0.72 + 0.28 * math.sin(math.pi * chord_position)
                value = sum(math.sin(2 * math.pi * frequency * second) for frequency in chord) / 3
                samples.append(int(520 * global_fade * pulse * value))
            audio.writeframes(samples.tobytes())


def _concat_line(path: Path) -> str:
    escaped = str(path.resolve()).replace("\\", "/").replace("'", "'\\''")
    return f"file '{escaped}'"


def _run_ffmpeg(command: list[str], *, timeout: int, message: str) -> subprocess.CompletedProcess[str]:
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise SalesVideoError(f"{message} Das Zeitlimit wurde überschritten.") from exc
    except OSError as exc:
        raise SalesVideoError(f"{message} FFmpeg konnte nicht gestartet werden.") from exc
    if completed.returncode:
        raise SalesVideoError(message) from RuntimeError(completed.stderr[-4000:])
    return completed


def create_sales_video(
    sources: Sequence[SalesVideoSource],
    output_path: Path,
    *,
    customer_name: str,
    project_title: str,
    headline: str,
    subtitle: str,
    chapters: Sequence[str],
    potentials: Sequence[str],
    cta: str,
    progress: int,
    seconds_per_slide: int = 4,
) -> SalesVideoResult:
    """Erzeugt eine H.264-MP4-Datei und liefert Eckdaten für die Aktivität."""

    seconds_per_slide = max(2, min(int(seconds_per_slide), 5))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frames = _load_frames(sources)
    slides: list[Image.Image] = [
        _title_slide(customer_name, headline, subtitle, project_title, progress)
    ]
    if frames:
        fallback_chapters = (
            "Startseite & Positionierung",
            "Angebote & Leistungen",
            "Termine & Buchung",
            "Referenzen & Galerie",
            "Persönlichkeit & Vertrauen",
            "Kontakt & Anfrage",
            "Visitenkarte & QR-Code",
            "Marke & Wiedererkennung",
        )
        requested = [item.strip() for item in chapters if item.strip()]
        for index, frame in enumerate(frames):
            chapter = requested[index] if index < len(requested) else fallback_chapters[index % len(fallback_chapters)]
            slides.append(_content_slide(frame, chapter, index + 1))
    else:
        slides.append(_project_overview_slide(customer_name, project_title, progress))
    slides.extend((_potential_slide(potentials), _cta_slide(customer_name, cta)))

    duration = len(slides) * seconds_per_slide
    with tempfile.TemporaryDirectory(prefix="tw-sales-video-") as temp_name:
        temp_dir = Path(temp_name)
        slide_paths: list[Path] = []
        for index, slide in enumerate(slides):
            slide_path = temp_dir / f"slide-{index:02d}.png"
            slide.save(slide_path, format="PNG", optimize=True)
            slide_paths.append(slide_path)

        silent_path = temp_dir / "silent.mp4"
        audio_path = temp_dir / "ambient.wav"
        _write_ambient_wav(audio_path, duration)
        try:
            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception as exc:
            raise SalesVideoError("Die lokale Video-Engine ist derzeit nicht verfügbar.") from exc
        segment_paths: list[Path] = []
        fade_out_start = max(0.25, seconds_per_slide - 0.28)
        for index, slide_path in enumerate(slide_paths):
            segment_path = temp_dir / f"segment-{index:02d}.mp4"
            fade_in = "" if index == 0 else ",fade=t=in:st=0:d=0.24"
            _run_ffmpeg(
                [
                    ffmpeg,
                    "-y",
                    "-loop",
                    "1",
                    "-framerate",
                    str(VIDEO_FPS),
                    "-i",
                    str(slide_path),
                    "-t",
                    str(seconds_per_slide),
                    "-vf",
                    (
                        "zoompan=z='min(zoom+0.00022,1.025)':"
                        "x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:"
                        f"s={VIDEO_WIDTH}x{VIDEO_HEIGHT}:fps={VIDEO_FPS},"
                        f"format=yuv420p{fade_in},fade=t=out:st={fade_out_start:.2f}:d=0.28"
                    ),
                    "-an",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-crf",
                    "22",
                    "-pix_fmt",
                    "yuv420p",
                    str(segment_path),
                ],
                timeout=45,
                message="Eine Filmsequenz konnte technisch nicht erzeugt werden.",
            )
            segment_paths.append(segment_path)

        concat_path = temp_dir / "segments.txt"
        concat_path.write_text("\n".join(_concat_line(path) for path in segment_paths) + "\n", encoding="utf-8")
        _run_ffmpeg(
            [
                ffmpeg,
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_path),
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                str(silent_path),
            ],
            timeout=60,
            message="Das Video konnte technisch nicht zusammengesetzt werden.",
        )
        rendered_path = temp_dir / "rendered.mp4"
        _run_ffmpeg(
            [
                ffmpeg,
                "-y",
                "-i",
                str(silent_path),
                "-i",
                str(audio_path),
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-b:a",
                "96k",
                "-shortest",
                "-movflags",
                "+faststart",
                str(rendered_path),
            ],
            timeout=90,
            message="Die fertige MP4-Datei konnte nicht gespeichert werden.",
        )
        if not rendered_path.exists() or rendered_path.stat().st_size < 10_000:
            raise SalesVideoError("Die fertige MP4-Datei konnte nicht gespeichert werden.")

        partial_path = output_path.with_suffix(output_path.suffix + ".partial")
        try:
            partial_path.unlink(missing_ok=True)
            shutil.copyfile(rendered_path, partial_path)
            os.replace(partial_path, output_path)
        except OSError as exc:
            try:
                partial_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise SalesVideoError("Die fertige MP4-Datei konnte nicht sicher abgelegt werden.") from exc

    return SalesVideoResult(
        slide_count=len(slides),
        duration_seconds=duration,
        source_count=len(frames),
    )
