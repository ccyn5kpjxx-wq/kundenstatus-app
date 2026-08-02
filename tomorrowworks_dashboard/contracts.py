from __future__ import annotations

import io
from datetime import date
from html import escape
from typing import Mapping, Sequence

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


TEXT_VERSION = "tw-agenturvertrag-2026-08-02-v2"

INK = colors.HexColor("#151815")
MUTED = colors.HexColor("#626861")
ORANGE = colors.HexColor("#FF641A")
GREEN = colors.HexColor("#164E3F")
PAPER = colors.HexColor("#F7F3ED")
LINE = colors.HexColor("#D9D4CC")
WHITE = colors.white


PACKAGE_CATALOG: dict[str, dict[str, object]] = {
    "website_start": {
        "name": "Website Start",
        "setup_cent": 250_000,
        "monthly_cent": 0,
        "duration_months": 0,
        "notice_months": 0,
        "public": True,
        "tagline": "Individueller, mobil optimierter Webauftritt mit klarer Kontaktstrecke.",
        "services": (
            "Konzeption, individuelles Webdesign und technische Umsetzung",
            "Responsive Darstellung für Smartphone, Tablet und Desktop",
            "Kontaktweg, Basis-Suchmaschinenoptimierung und technische Veröffentlichung",
            "Zwei gebündelte Korrekturrunden vor der Abnahme",
        ),
    },
    "website_growth": {
        "name": "Website & Wachstum",
        "setup_cent": 250_000,
        "monthly_cent": 30_000,
        "duration_months": 12,
        "notice_months": 1,
        "public": True,
        "tagline": "Website, laufende Betreuung und verständliche Auswertung in einem Paket.",
        "services": (
            "Alle Leistungen aus Website Start",
            "Technische Betreuung, Wartung, Backups und Verfügbarkeitskontrolle",
            "Messkonzept für Kontakt-, Anfrage- oder Buchungsziele nach Freigabe",
            "Persönlicher Projektraum mit Fortschritt, Nachrichten, Dateien und Verträgen",
            "Monatliche Auswertung und bis zu 60 Minuten kleinere Inhaltsänderungen",
        ),
    },
    "digital_cockpit": {
        "name": "Digitales Cockpit",
        "setup_cent": 390_000,
        "monthly_cent": 49_900,
        "duration_months": 12,
        "notice_months": 1,
        "public": True,
        "tagline": "Zentrale Arbeitsoberfläche für Anfragen, Projekte, Dokumente und Kennzahlen.",
        "services": (
            "Alle Leistungen aus Website & Wachstum",
            "Business-Cockpit für Anfragen, Aufgaben, Projektstatus und Dokumente",
            "Vorbereitete E-Mail- und Werbekennzahlen-Anbindung nach technischer Prüfung",
            "Verträge und Rechnungen zentral ablegen, ansehen und exportieren",
            "Bis zu drei Nutzer sowie eine vereinbarte Schnittstelle oder Automatisierung",
            "Rechtssichere E-Rechnungserstellung nur bei gesondert beauftragter Integration",
        ),
    },
    "founder_pilot": {
        "name": "Founder-Pilot",
        "setup_cent": 99_000,
        "monthly_cent": 39_900,
        "duration_months": 6,
        "notice_months": 1,
        "public": False,
        "tagline": "Begrenztes Pilotangebot mit reduziertem Startpreis und enger Betreuung.",
        "services": (
            "Individueller digitaler Grundauftritt inklusive gemeinsamer Finalisierung",
            "Technischer Betrieb, Support und kleinere laufende Weiterentwicklung",
            "Persönlicher Projektraum mit Dateien, Fortschritt und Vertragsübersicht",
            "Bis zu drei Nutzer und eine erste gemeinsam definierte Automatisierung",
            "Preisgarantie für zwölf Monate ab Leistungsbeginn",
            "Strukturierte Rückmeldung zum Pilotbetrieb; Referenznutzung nur separat freigegeben",
        ),
    },
}


ADDON_CATALOG: dict[str, dict[str, object]] = {
    "technical_care": {
        "name": "Technische Website-Pflege",
        "setup_cent": 0,
        "monthly_cent": 9_900,
        "description": "Updates, Verfügbarkeitskontrolle, Backups und technische Fehlerbehebung im vereinbarten Rahmen.",
    },
    "email_setup": {
        "name": "Geschäfts-E-Mail bis drei Postfächer",
        "setup_cent": 19_000,
        "monthly_cent": 0,
        "description": "Domainprüfung, Postfächer, Weiterleitungen sowie SPF, DKIM und DMARC; Anbieter-Lizenzen separat.",
    },
    "google_business": {
        "name": "Google-Unternehmensprofil einrichten oder korrigieren",
        "setup_cent": 29_000,
        "monthly_cent": 0,
        "description": "Betriebsdaten, Kategorie, Bilder, Website und Öffnungszeiten nach bestätigter Inhaberschaft pflegen.",
    },
    "google_business_care": {
        "name": "Google-Unternehmensprofil laufend pflegen",
        "setup_cent": 0,
        "monthly_cent": 7_900,
        "description": "Monatliche Datenkontrolle und vereinbarte Aktualisierungen; keine Bewertungs- oder Rankinggarantie.",
    },
    "google_ads": {
        "name": "Google Ads",
        "setup_cent": 49_000,
        "monthly_cent": 24_900,
        "media_fee_percent": 15,
        "description": "Lokale Suchkampagne, Keyword- und Suchbegriffsprüfung, Anzeigentexte, Conversion-Messung, Budgetkontrolle und Monatsbericht; Betreuung 249,00 EUR monatlich oder 15 Prozent des Medienbudgets, falls höher.",
    },
    "meta_ads": {
        "name": "Instagram & Facebook Ads",
        "setup_cent": 49_000,
        "monthly_cent": 24_900,
        "media_fee_percent": 15,
        "description": "Kampagnen für Bekanntheit, Videoaufrufe, Nachrichten oder Leads mit Zielgruppen-, Creative- und Budgetoptimierung; Betreuung 249,00 EUR monatlich oder 15 Prozent des Medienbudgets, falls höher.",
    },
    "google_meta_ads": {
        "name": "Google + Meta Ads",
        "setup_cent": 79_000,
        "monthly_cent": 39_900,
        "media_fee_percent": 15,
        "description": "Verknüpfte Such- und Social-Kampagnen mit gemeinsamer Zieldefinition und kanalgetrennter Auswertung; Betreuung 399,00 EUR monatlich oder 15 Prozent des kombinierten Medienbudgets, falls höher.",
    },
    "dashboard_extension": {
        "name": "Business-Dashboard einrichten",
        "setup_cent": 149_000,
        "monthly_cent": 14_900,
        "description": "Projekt-, Anfrage-, Datei- und Kennzahlenübersicht; Schnittstellen und Sondermodule nach Leistungsanlage.",
    },
    "booking": {
        "name": "Termin- oder Buchungssystem",
        "setup_cent": 49_000,
        "monthly_cent": 0,
        "description": "Buchungsablauf, Kalender und Bestätigungslogik; Anbieter- und Zahlungsgebühren separat.",
    },
    "sales_video": {
        "name": "Verkaufsfilm bis 90 Sekunden",
        "setup_cent": 69_000,
        "monthly_cent": 0,
        "description": "Konzept, Schnitt und kundenspezifischer Erklärtext; Sprecher-, Musik-, Stock- und Darstellerrechte nach Angebot.",
    },
    "short_clips": {
        "name": "Drei kurze Werbeclips",
        "setup_cent": 69_000,
        "monthly_cent": 0,
        "description": "Drei kurze Social-Media-Clips aus freigegebenem Material; externe Produktionskosten separat.",
    },
    "flyer": {
        "name": "Flyer-Gestaltung",
        "setup_cent": 29_000,
        "monthly_cent": 0,
        "description": "Druckfertiger Gestaltungsentwurf nach Format- und Inhaltsfreigabe; Druck und Versand separat.",
    },
    "business_card": {
        "name": "Visitenkarten-Gestaltung",
        "setup_cent": 19_000,
        "monthly_cent": 0,
        "description": "Vorder- und Rückseite inklusive QR-Ziel; Druck, Papier und Veredelung separat.",
    },
    "print_bundle": {
        "name": "Flyer + Visitenkarte",
        "setup_cent": 39_000,
        "monthly_cent": 0,
        "description": "Abgestimmtes Printpaket mit zwei Druckvorlagen; Produktion erst nach gesonderter Druckfreigabe.",
    },
    "social_content": {
        "name": "Social-Media-Redaktionspaket - vier Beiträge",
        "setup_cent": 0,
        "monthly_cent": 29_900,
        "description": "Vier abgestimmte Beiträge pro Monat; Foto-/Videoproduktion, Community-Management und Anzeigenbudget separat.",
    },
}


AD_ADDONS = {"google_ads", "meta_ads", "google_meta_ads"}
PRINT_ADDONS = {"flyer", "business_card", "print_bundle"}


def _row_value(row: Mapping[str, object], key: str, default: object = "") -> object:
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def euro(cent: int) -> str:
    value = int(cent or 0) / 100
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " EUR"


def validate_selection(package_code: str, addon_codes: Sequence[str]) -> tuple[dict[str, object], list[dict[str, object]]]:
    if package_code not in PACKAGE_CATALOG:
        raise ValueError("Bitte ein gültiges Leistungspaket auswählen.")
    clean_codes = list(dict.fromkeys(code for code in addon_codes if code))
    unknown = [code for code in clean_codes if code not in ADDON_CATALOG]
    if unknown:
        raise ValueError("Mindestens ein Zusatzmodul ist nicht bekannt.")
    if len(AD_ADDONS.intersection(clean_codes)) > 1:
        raise ValueError("Bitte nur eine Werbekombination für Google/Meta auswählen.")
    if "print_bundle" in clean_codes and len(PRINT_ADDONS.intersection(clean_codes)) > 1:
        raise ValueError("Das Printpaket ersetzt die einzelnen Flyer- und Visitenkartenmodule.")
    return PACKAGE_CATALOG[package_code], [ADDON_CATALOG[code] | {"code": code} for code in clean_codes]


def build_contract_snapshot(
    *,
    project: Mapping[str, object],
    customer: Mapping[str, object],
    provider: Mapping[str, str],
    package_code: str,
    addon_codes: Sequence[str],
    media_budget_cent: int,
    start_date: str,
    notes: str,
    version: int,
    contract_id: int | None = None,
    created_at: str,
    legal_approved: bool,
) -> dict[str, object]:
    package, addons = validate_selection(package_code, addon_codes)
    media_budget_cent = max(int(media_budget_cent or 0), 0)
    selected_addon_codes = {str(item["code"]) for item in addons}
    if media_budget_cent > 0 and not AD_ADDONS.intersection(selected_addon_codes):
        raise ValueError(
            "Ein Medienbudget kann nur zusammen mit Google Ads, Instagram/Facebook Ads oder Google + Meta Ads gespeichert werden."
        )
    berechnete_addons: list[dict[str, object]] = []
    for addon in addons:
        effektiver_monatspreis = int(addon["monthly_cent"])
        if addon.get("media_fee_percent"):
            anteil = int(round(media_budget_cent * int(addon["media_fee_percent"]) / 100))
            effektiver_monatspreis = max(effektiver_monatspreis, anteil)
        berechnete_addons.append(addon | {"effective_monthly_cent": effektiver_monatspreis})
    addons = berechnete_addons
    selected_addons = {str(item["code"]): item for item in addons}
    selection_manifest = {
        "packages": [
            {
                "code": code,
                "name": str(catalog_package["name"]),
                "selected": code == package_code,
                "setup_cent": int(catalog_package["setup_cent"]),
                "monthly_cent": int(catalog_package["monthly_cent"]),
            }
            for code, catalog_package in PACKAGE_CATALOG.items()
        ],
        "addons": [
            {
                "code": code,
                "name": str(catalog_addon["name"]),
                "selected": code in selected_addons,
                "setup_cent": int(catalog_addon["setup_cent"]),
                "monthly_cent": int(
                    selected_addons.get(code, {}).get(
                        "effective_monthly_cent", catalog_addon["monthly_cent"]
                    )
                ),
            }
            for code, catalog_addon in ADDON_CATALOG.items()
        ],
    }
    setup_cent = int(package["setup_cent"]) + sum(int(item["setup_cent"]) for item in addons)
    monthly_cent = int(package["monthly_cent"]) + sum(int(item["effective_monthly_cent"]) for item in addons)
    duration = int(package["duration_months"])
    notice_months = int(package["notice_months"])
    if monthly_cent > 0 and notice_months == 0:
        notice_months = 1
    project_id = int(_row_value(project, "id", 0) or 0)
    contract_number = (
        f"TW-{date.fromisoformat(created_at[:10]).year}-{project_id:04d}"
        f"-{int(contract_id or 0):03d}-V{version:02d}"
    )
    return {
        "text_version": TEXT_VERSION,
        "contract_number": contract_number,
        "version": version,
        "created_at": created_at,
        "legal_approved": bool(legal_approved),
        "provider": {
            "name": provider.get("name", "").strip(),
            "address": provider.get("address", "").strip(),
            "representative": provider.get("representative", "").strip(),
            "email": provider.get("email", "").strip(),
        },
        "customer": {
            "company": str(_row_value(customer, "firma", "")).strip(),
            "contact": str(_row_value(customer, "ansprechpartner", "")).strip(),
            "address": str(_row_value(customer, "adresse", "")).strip(),
            "email": str(_row_value(customer, "email", "")).strip(),
        },
        "project": {
            "id": project_id,
            "title": str(_row_value(project, "titel", "")).strip(),
            "description": str(_row_value(project, "beschreibung", "")).strip(),
        },
        "package": {
            "code": package_code,
            "name": str(package["name"]),
            "tagline": str(package["tagline"]),
            "services": list(package["services"]),
        },
        "addons": addons,
        "selection_manifest": selection_manifest,
        "pricing": {
            "setup_cent": setup_cent,
            "monthly_cent": monthly_cent,
            "media_budget_cent": media_budget_cent,
            "duration_months": duration,
            "notice_months": notice_months,
            "first_term_cent": setup_cent + monthly_cent * duration,
            "vat_rate": 19,
        },
        "start_date": start_date or "nach gemeinsamer Freigabe",
        "notes": (notes or "").strip()[:3000],
    }


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "body": ParagraphStyle(
            "TWBody",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=12.1,
            textColor=INK,
            spaceAfter=5,
        ),
        "small": ParagraphStyle(
            "TWSmall",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=7.5,
            leading=9.5,
            textColor=MUTED,
            spaceAfter=4,
        ),
        "kicker": ParagraphStyle(
            "TWKicker",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8,
            leading=10,
            textColor=ORANGE,
            spaceAfter=5,
        ),
        "title": ParagraphStyle(
            "TWTitle",
            parent=base["Title"],
            fontName="Helvetica-Bold",
            fontSize=24,
            leading=26,
            textColor=INK,
            alignment=TA_LEFT,
            spaceAfter=10,
        ),
        "subtitle": ParagraphStyle(
            "TWSubtitle",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=10.8,
            leading=14,
            textColor=MUTED,
            spaceAfter=12,
        ),
        "h1": ParagraphStyle(
            "TWH1",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=15.5,
            textColor=GREEN,
            spaceBefore=9,
            spaceAfter=5,
            keepWithNext=True,
        ),
        "h2": ParagraphStyle(
            "TWH2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=10.5,
            leading=13,
            textColor=INK,
            spaceBefore=6,
            spaceAfter=3,
            keepWithNext=True,
        ),
        "table": ParagraphStyle(
            "TWTable",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=8.2,
            leading=10.5,
            textColor=INK,
        ),
        "table_bold": ParagraphStyle(
            "TWTableBold",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.2,
            leading=10.5,
            textColor=INK,
        ),
        "table_header": ParagraphStyle(
            "TWTableHeader",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.2,
            leading=10.5,
            textColor=WHITE,
        ),
        "callout": ParagraphStyle(
            "TWCallout",
            parent=base["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.7,
            leading=12,
            textColor=GREEN,
            alignment=TA_CENTER,
        ),
    }


def _p(text: object, style: ParagraphStyle) -> Paragraph:
    return Paragraph(escape(str(text or "")).replace("\n", "<br/>"), style)


def _rich(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def _bullet(text: object, styles: dict[str, ParagraphStyle]) -> Paragraph:
    return _p(f"- {text}", styles["body"])


def _table(data: list[list[object]], widths: list[float], *, header: bool = False) -> Table:
    table = Table(data, colWidths=widths, repeatRows=1 if header else 0, hAlign="LEFT")
    commands: list[tuple] = [
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("GRID", (0, 0), (-1, -1), 0.45, LINE),
    ]
    if header:
        commands.extend(
            [
                ("BACKGROUND", (0, 0), (-1, 0), GREEN),
                ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ]
        )
    table.setStyle(TableStyle(commands))
    return table


def _selection_table(
    items: Sequence[Mapping[str, object]],
    styles: dict[str, ParagraphStyle],
    *,
    selected_label: str,
    unselected_label: str,
) -> Table:
    rows: list[list[object]] = [
        [
            _rich("Auswahl", styles["table_header"]),
            _rich("Leistung", styles["table_header"]),
            _rich("Einmalig", styles["table_header"]),
            _rich("Monatlich", styles["table_header"]),
        ]
    ]
    for item in items:
        selected = bool(item.get("selected"))
        monthly_text = euro(int(item.get("monthly_cent", 0) or 0))
        if not selected and str(item.get("code", "")) in AD_ADDONS:
            monthly_text = f"ab {monthly_text}\noder 15 % Budget"
        rows.append(
            [
                _p(
                    f"[X]\n{selected_label}" if selected else f"[ ]\n{unselected_label}",
                    styles["table_bold"] if selected else styles["table"],
                ),
                _p(item.get("name", ""), styles["table_bold"] if selected else styles["table"]),
                _p(euro(int(item.get("setup_cent", 0) or 0)), styles["table"]),
                _p(monthly_text, styles["table"]),
            ]
        )
    table = Table(rows, colWidths=[30 * mm, 74 * mm, 35 * mm, 35 * mm], repeatRows=1, hAlign="LEFT")
    commands: list[tuple] = [
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("GRID", (0, 0), (-1, -1), 0.45, LINE),
        ("BACKGROUND", (0, 0), (-1, 0), GREEN),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("ALIGN", (0, 1), (0, -1), "CENTER"),
    ]
    for index, item in enumerate(items, start=1):
        if item.get("selected"):
            commands.extend(
                [
                    ("BACKGROUND", (0, index), (-1, index), colors.HexColor("#E8F3EF")),
                    ("TEXTCOLOR", (0, index), (0, index), GREEN),
                ]
            )
        else:
            commands.append(("TEXTCOLOR", (0, index), (0, index), MUTED))
    table.setStyle(TableStyle(commands))
    return table


def _page_decorator(snapshot: Mapping[str, object]):
    draft = not bool(snapshot.get("legal_approved"))
    number = str(snapshot.get("contract_number", ""))
    is_price_overview = number.startswith("TW-LEISTUNGEN")

    def draw(canvas, doc):
        canvas.saveState()
        width, height = A4
        canvas.setFont("Helvetica-Bold", 7.2)
        canvas.setFillColor(INK)
        canvas.drawString(
            18 * mm,
            height - 13 * mm,
            "TOMORROW WORKS  |  PREISÜBERSICHT" if is_price_overview else "TOMORROW WORKS  |  VERTRAGSCENTER",
        )
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTED)
        canvas.drawRightString(width - 18 * mm, height - 13 * mm, number)
        canvas.setStrokeColor(LINE)
        canvas.setLineWidth(0.5)
        canvas.line(18 * mm, height - 16 * mm, width - 18 * mm, height - 16 * mm)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTED)
        canvas.drawString(
            18 * mm,
            11 * mm,
            "Interne Preisfassung - vor externer Nutzung freigeben"
            if is_price_overview
            else "Interne Vertragsfassung - keine automatische Kundenübermittlung",
        )
        canvas.drawRightString(width - 18 * mm, 11 * mm, f"Seite {doc.page}")
        if draft:
            canvas.setFillColor(ORANGE)
            canvas.setFont("Helvetica-Bold", 7.2)
            canvas.drawCentredString(
                width / 2,
                height - 20 * mm,
                "PREISENTWURF - INTERN" if is_price_overview else "VERTRAGSENTWURF - NICHT RECHTLICH GEPRÜFT",
            )
        canvas.restoreState()

    return draw


def _commercial_summary(snapshot: Mapping[str, object], styles: dict[str, ParagraphStyle]) -> Table:
    pricing = snapshot["pricing"]
    duration = int(pricing["duration_months"])
    monthly_cent = int(pricing["monthly_cent"])
    duration_text = "Projektleistung ohne Mindestlaufzeit" if duration == 0 else f"{duration} Monate Mindestlaufzeit"
    if monthly_cent <= 0:
        notice_text = "Nicht anwendbar auf die einmalige Projektleistung"
    elif duration == 0:
        notice_text = f"{pricing['notice_months']} Monat(e) zum Monatsende"
    else:
        notice_text = f"{pricing['notice_months']} Monat(e) zum Laufzeitende; danach monatlich"
    data = [
        [_p("Einmalige Agenturleistung", styles["table_bold"]), _p(euro(pricing["setup_cent"]), styles["table"])],
        [_p("Laufende Agenturbetreuung", styles["table_bold"]), _p(f"{euro(pricing['monthly_cent'])} pro Monat", styles["table"])],
        [_p("Werbebudget", styles["table_bold"]), _p(f"{euro(pricing['media_budget_cent'])} pro Monat - separat an Plattform", styles["table"])],
        [_p("Laufzeit", styles["table_bold"]), _p(duration_text, styles["table"])],
        [_p("Kündigung", styles["table_bold"]), _p(notice_text, styles["table"])],
        [_p("Umsatzsteuer", styles["table_bold"]), _p("Alle Beträge netto zuzüglich gesetzlicher Umsatzsteuer", styles["table"])],
    ]
    return _table(data, [59 * mm, 115 * mm])


def create_contract_pdf(snapshot: Mapping[str, object]) -> bytes:
    styles = _styles()
    stream = io.BytesIO()
    doc = SimpleDocTemplate(
        stream,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=27 * mm,
        bottomMargin=20 * mm,
        title=f"{snapshot['contract_number']} - {snapshot['customer']['company']}",
        author="Tomorrow Works",
        subject="Interner Rahmen- und Betreuungsvertrag",
    )
    story: list[object] = []
    provider = snapshot["provider"]
    customer = snapshot["customer"]
    project = snapshot["project"]
    package = snapshot["package"]
    pricing = snapshot["pricing"]

    story.extend(
        [
            _p("RAHMENVERTRAG · B2B-ENTWURF", styles["kicker"]),
            _rich("Rahmen- und<br/>Betreuungsvertrag", styles["title"]),
            _p(f"Projekt: {project['title']} | Paket: {package['name']}", styles["subtitle"]),
        ]
    )
    status_text = (
        "Rechtlich freigegebene Vertragsfassung. Vertragsparteien und Projektangaben vor Unterzeichnung dennoch prüfen."
        if snapshot["legal_approved"]
        else "Nicht rechtsverbindlicher Vertragsentwurf. Firmierung, Preise, Leistungsumfang, Datenschutzrollen, Haftung und B2B-Status müssen vor Verwendung anwaltlich beziehungsweise steuerlich geprüft werden."
    )
    status_table = Table([[_p(status_text, styles["callout"])]], colWidths=[174 * mm])
    status_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFF0E6") if not snapshot["legal_approved"] else colors.HexColor("#E8F3EF")),
                ("BOX", (0, 0), (-1, -1), 0.8, ORANGE if not snapshot["legal_approved"] else GREEN),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
            ]
        )
    )
    story.extend([status_table, Spacer(1, 8 * mm)])
    parties = [
        [_p("Auftragnehmer", styles["table_bold"]), _p(f"{provider['name']}\n{provider['address']}\n{provider['representative']}\n{provider['email']}", styles["table"])],
        [_p("Auftraggeber", styles["table_bold"]), _p(f"{customer['company']}\n{customer['address']}\nAnsprechpartner: {customer['contact'] or 'vor Unterzeichnung ergänzen'}\n{customer['email']}", styles["table"])],
        [_p("Vertragsnummer", styles["table_bold"]), _p(snapshot["contract_number"], styles["table"])],
        [_p("Leistungsbeginn", styles["table_bold"]), _p(snapshot["start_date"], styles["table"])],
    ]
    story.extend([_table(parties, [42 * mm, 132 * mm]), Spacer(1, 7 * mm), _commercial_summary(snapshot, styles)])

    story.extend(
        [
            _p("1. Vertragsgegenstand", styles["h1"]),
            _p(
                f"Tomorrow Works unterstützt den Auftraggeber beim Projekt {project['title']}. Maßgeblich sind dieser Rahmenvertrag, die nachstehende Leistungs- und Preisanlage sowie dokumentierte spätere Freigaben. Nicht ausdrücklich aufgeführte Leistungen sind nicht geschuldet.",
                styles["body"],
            ),
            _p(
                "Konzeption, Gestaltung, Programmierung, Logo-, Print- oder Videowerke sind nach dem vereinbarten Ergebnis und einer dokumentierten Abnahme zu erbringen. Laufende Betreuung, Beratung, Werbeoptimierung, Reporting und Support sind Dienstleistungen; ein bestimmter wirtschaftlicher Erfolg wird nicht geschuldet.",
                styles["body"],
            ),
            _p("2. Paket und Leistungsumfang", styles["h1"]),
            _p(f"{package['name']}: {package['tagline']}", styles["body"]),
        ]
    )
    for service in package["services"]:
        story.append(_bullet(service, styles))
    if snapshot["addons"]:
        story.append(_p("Zusatzmodule", styles["h2"]))
        for addon in snapshot["addons"]:
            story.append(_bullet(f"{addon['name']}: {addon['description']}", styles))

    story.extend(
        [
            _p("3. Projektablauf, Korrekturen und Abnahme", styles["h1"]),
            _p(
                "Tomorrow Works arbeitet in nachvollziehbaren Zwischenständen. Der Auftraggeber prüft Entwürfe und erteilt Freigaben in Textform. Soweit die Leistungsanlage nichts anderes bestimmt, sind zwei gebündelte Korrekturrunden enthalten. Zusätzliche Änderungen erfolgen erst nach Freigabe zum aktuellen Zusatzstundensatz von 95,00 EUR netto.",
                styles["body"],
            ),
            _p(
                "Abnahmefähige Werkleistungen werden mit der Bitte um Prüfung bereitgestellt. Erkannte Mängel sind konkret zu beschreiben. Schweigen, Nutzung oder Veröffentlichung ersetzen eine rechtlich erforderliche Abnahme nur, soweit dies im konkreten Vertrag wirksam vereinbart und rechtlich geprüft wurde.",
                styles["body"],
            ),
            _p("4. Mitwirkung des Auftraggebers", styles["h1"]),
            _p(
                "Der Auftraggeber liefert rechtzeitig richtige Texte, Preise, Kontaktdaten, Bild- und Markenfreigaben, Zugänge sowie fachliche Entscheidungen. Er sichert zu, die nötigen Rechte an bereitgestellten Materialien und Personenabbildungen zu besitzen. Verzögerte Mitwirkung verschiebt vereinbarte Zeitpläne angemessen.",
                styles["body"],
            ),
            _p("5. Preise, Fälligkeit und Fremdkosten", styles["h1"]),
            _p(
                f"Die einmalige Agenturleistung beträgt {euro(pricing['setup_cent'])} netto. Die laufende Agenturbetreuung beträgt {euro(pricing['monthly_cent'])} netto pro Monat. Projektleistungen werden, sofern im Auftragsblatt nichts anderes vereinbart ist, zu 50 Prozent bei Beauftragung und zu 50 Prozent vor Veröffentlichung fällig. Monatsleistungen werden monatlich im Voraus berechnet.",
                styles["body"],
            ),
            _p(
                "Werbebudgets sowie Domain-, Hosting-, E-Mail-, Plattform-, API-, Stock-, Druck-, Versand-, Zahlungs- oder sonstige Drittanbietergebühren sind nicht im Agenturhonorar enthalten, sofern sie nicht ausdrücklich mit Betrag genannt sind. Fremdkosten werden nur nach dokumentierter Freigabe ausgelöst.",
                styles["body"],
            ),
            _p("6. Google Ads, Instagram und Facebook", styles["h1"]),
            _p(
                "Google Ads soll Menschen erreichen, die bereits aktiv nach passenden Leistungen suchen. Mögliche Ziele sind qualifizierte Anfragen, Anrufe, Buchungen, Routenaufrufe oder Verkäufe. Tomorrow Works kann Zielgebiet, Keywords, Anzeigen, Landingpage, Ausschlussbegriffe, Conversion-Messung, Budgetkontrolle und Berichte betreuen.",
                styles["body"],
            ),
            _p(
                "Instagram- und Facebook-Kampagnen können Bekanntheit, Videoaufrufe, Interaktionen, Nachrichten, Leads oder Verkäufe unterstützen. Ziel, Zielgruppe, Laufzeit, Creatives und Budget werden je Kampagne freigegeben. Plattformabrechnungen und Agenturhonorar werden getrennt ausgewiesen.",
                styles["body"],
            ),
            _p(
                "Es besteht keine Garantie für Anzeigenrang, Reichweite, Klicks, Leads, Buchungen, Umsatz oder Return on Advertising Spend. Ergebnisse hängen unter anderem von Auktion, Wettbewerb, Budget, Angebot, Saison, Website, Tracking-Einwilligungen, Kundenfreigaben und Plattformregeln ab.",
                styles["body"],
            ),
            _p("7. Konten, Zugriffe und Transparenz", styles["h1"]),
            _p(
                "Domain, Werbekonten, Unternehmensprofile und Geschäftsdaten sollen grundsätzlich dem Auftraggeber gehören. Tomorrow Works erhält die für die Leistung nötigen Rollen- oder Managerzugriffe. Passwörter werden nicht im Vertrag gespeichert. Das Dashboard zeigt nur Daten, die technisch verfügbar, rechtmäßig erhoben und der jeweiligen Quelle zuordenbar sind.",
                styles["body"],
            ),
            _p("8. E-Mail und Dashboard", styles["h1"]),
            _p(
                "Geschäftliche E-Mail-Adressen benötigen eine Domain und einen Mailanbieter. Einrichtung und Sicherheitskonfiguration können beauftragt werden; Lizenzen bleiben Drittkosten. Im Projektraum können Fortschritt, Nachrichten, Dateien, Angebote und Verträge dargestellt werden. Ein erweitertes Business-Cockpit kann Anfragen, E-Mail-Anbindung, Werbekennzahlen, Aufgaben und Dokumente zusammenführen, soweit die jeweilige Schnittstelle beauftragt und verfügbar ist.",
                styles["body"],
            ),
            _p(
                "Verträge und Rechnungen können zentral abgelegt, angesehen und exportiert werden. Eine steuerlich und technisch vollständige Rechnungserstellung, insbesondere XRechnung oder ZUGFeRD, ist nur geschuldet, wenn das entsprechende Rechnungs- oder Buchhaltungssystem ausdrücklich integriert wurde.",
                styles["body"],
            ),
            _p("9. Datenschutz und Vertraulichkeit", styles["h1"]),
            _p(
                "Beide Parteien behandeln nicht öffentliche Geschäfts- und Zugangsdaten vertraulich. Verarbeitet Tomorrow Works personenbezogene Daten weisungsgebunden im Auftrag, schließen die Parteien vor Produktionsbetrieb einen gesonderten Auftragsverarbeitungsvertrag nach Art. 28 DSGVO. Tracking, Werbepixel und nicht notwendige Cookies werden erst nach rechtlicher Prüfung und erforderlicher Einwilligung aktiviert.",
                styles["body"],
            ),
            _p("10. Nutzungsrechte", styles["h1"]),
            _p(
                "Nach vollständiger Zahlung erhält der Auftraggeber die im Auftragsblatt bestimmten Nutzungsrechte an den final freigegebenen Ergebnissen. Rechte an Standardkomponenten, Bibliotheken, vorbestehendem Know-how und Drittmaterial richten sich nach den jeweiligen Lizenzen. Rohdateien, offene Produktionsdateien und Quellmaterial sind nur enthalten, wenn dies ausdrücklich vereinbart ist.",
                styles["body"],
            ),
            _p("11. Laufzeit und Vertragsende", styles["h1"]),
            _p(
                (
                    f"Die laufende Betreuung hat eine Mindestlaufzeit von {pricing['duration_months']} Monat(en). Sie kann mit einer Frist von {pricing['notice_months']} Monat(en) zum Ende der Mindestlaufzeit und danach monatlich gekündigt werden."
                    if int(pricing["duration_months"]) > 0
                    else (
                        f"Laufende Zusatzmodule haben keine Mindestlaufzeit und können mit einer Frist von {pricing['notice_months']} Monat(en) zum Monatsende gekündigt werden. Die einmalige Projektleistung endet mit ihrer vollständigen Erbringung und Abnahme."
                        if int(pricing["monthly_cent"]) > 0
                        else "Die einmalige Projektleistung endet mit ihrer vollständigen Erbringung und Abnahme; eine laufende Betreuung ist in dieser Fassung nicht vereinbart."
                    )
                )
                + " Das Recht zur außerordentlichen Kündigung aus wichtigem Grund bleibt unberührt.",
                styles["body"],
            ),
            _p(
                "Nach Vertragsende werden vereinbarte Zugänge, Exporte und Dateien geordnet übergeben. Offene Vergütung und freigegebene Drittkosten bleiben fällig. Löschung oder Rückgabe personenbezogener Daten richtet sich nach Datenschutzvereinbarung und gesetzlichen Aufbewahrungspflichten.",
                styles["body"],
            ),
            _p("12. Haftung, Plattformen und Schlussprüfung", styles["h1"]),
            _p(
                "Es gelten die gesetzlichen Haftungsregeln, solange keine anwaltlich geprüfte, wirksam einbezogene Haftungsregel vereinbart wurde. Tomorrow Works haftet nicht für eigenständige Entscheidungen, Sperrungen, Preisänderungen oder Ausfälle von Google, Meta, Hosting-, Mail- oder anderen Drittanbietern, soweit diese nicht von Tomorrow Works zu vertreten sind.",
                styles["body"],
            ),
        ]
    )
    if snapshot["notes"]:
        story.extend([_p("Projektbezogene Hinweise", styles["h2"]), _p(snapshot["notes"], styles["body"])])

    signature_table = Table(
        [
            ["", ""],
            [_p("Ort, Datum / Auftragnehmer", styles["small"]), _p("Ort, Datum / Auftraggeber", styles["small"])],
            ["", ""],
            [_p("Unterschrift", styles["small"]), _p("Unterschrift", styles["small"])],
        ],
        colWidths=[84 * mm, 84 * mm],
        rowHeights=[9 * mm, 5 * mm, 9 * mm, 5 * mm],
    )
    signature_table.setStyle(
        TableStyle(
            [
                ("LINEBELOW", (0, 0), (-1, 0), 0.7, INK),
                ("LINEBELOW", (0, 2), (-1, 2), 0.7, INK),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
            ]
        )
    )
    closing_section = KeepTogether(
        [
            _p("Freigaben vor Livegang", styles["h1"]),
            _bullet("Firmierung, B2B-Status, Geschäftsanschriften und vertretungsberechtigte Personen", styles),
            _bullet("Finale Texte, Preise, Termine, Bilder, Marken, KI-Kennzeichnungen und Nutzungsrechte", styles),
            _bullet("Werbekonten, Conversion-Ziele, Medienbudget, Einwilligungs- und Datenschutzkonzept", styles),
            _bullet("Auftragsverarbeitung, Unterauftragnehmer, Lösch- und Übergaberegeln", styles),
            Spacer(1, 5 * mm),
            _p("Auswahl bestätigt", styles["h1"]),
            _p(
                "Mit ihrer Unterschrift bestätigen die Parteien die vorstehend mit [X] dokumentierte Leistungswahl sowie ausschließlich die nach rechtlicher Endprüfung vorgelegte Fassung. Dieser Entwurf darf nicht ungeprüft zur Online-Annahme freigeschaltet werden.",
                styles["small"],
            ),
            signature_table,
        ]
    )

    manifest = snapshot.get("selection_manifest") or {}
    package_options = list(manifest.get("packages") or [])
    addon_options = list(manifest.get("addons") or [])
    if not package_options:
        package_options = [
            {
                "code": code,
                "name": catalog_package["name"],
                "selected": code == package["code"],
                "setup_cent": catalog_package["setup_cent"],
                "monthly_cent": catalog_package["monthly_cent"],
            }
            for code, catalog_package in PACKAGE_CATALOG.items()
        ]
    if not addon_options:
        selected_by_code = {str(item["code"]): item for item in snapshot["addons"]}
        addon_options = [
            {
                "code": code,
                "name": catalog_addon["name"],
                "selected": code in selected_by_code,
                "setup_cent": catalog_addon["setup_cent"],
                "monthly_cent": selected_by_code.get(code, {}).get(
                    "effective_monthly_cent", catalog_addon["monthly_cent"]
                ),
            }
            for code, catalog_addon in ADDON_CATALOG.items()
        ]
    package_options = [
        item
        for item in package_options
        if bool(PACKAGE_CATALOG.get(str(item.get("code", "")), {}).get("public", True))
        or bool(item.get("selected"))
    ]

    story.extend(
        [
            PageBreak(),
            _p("LEISTUNGS- UND PREISANLAGE", styles["kicker"]),
            _p("Leistungsauswahl", styles["title"]),
            _p(
                "Genau ein Grundpaket ist mit [X] als gewählt markiert. Bei den Zusatzmodulen werden nur mit [X] als zusätzlich gebucht markierte Positionen separat berechnet. Leistungen des Grundpakets und projektbezogene Inklusivleistungen gelten unabhängig von den Zusatzmodul-Kästchen.",
                styles["callout"],
            ),
            Spacer(1, 5 * mm),
            _p("Grundpaket - genau eine Auswahl", styles["h1"]),
            _selection_table(
                package_options,
                styles,
                selected_label="gewählt",
                unselected_label="nicht gewählt",
            ),
            _p("Zusatzmodule - Mehrfachauswahl möglich", styles["h1"]),
            _selection_table(
                addon_options,
                styles,
                selected_label="zusätzlich gebucht",
                unselected_label="nicht zusätzlich gebucht",
            ),
            Spacer(1, 6 * mm),
            _p("Preiszusammenfassung der angekreuzten Positionen", styles["h1"]),
        ]
    )
    selected_package = next(
        (item for item in package_options if item.get("selected")),
        {
            "setup_cent": PACKAGE_CATALOG[package["code"]]["setup_cent"],
            "monthly_cent": PACKAGE_CATALOG[package["code"]]["monthly_cent"],
        },
    )
    rows: list[list[object]] = [
        [_rich("Leistung", styles["table_header"]), _rich("Einmalig", styles["table_header"]), _rich("Monatlich", styles["table_header"])],
        [_p(package["name"], styles["table_bold"]), _p(euro(selected_package["setup_cent"]), styles["table"]), _p(euro(selected_package["monthly_cent"]), styles["table"])],
    ]
    for addon in snapshot["addons"]:
        rows.append([_p(addon["name"], styles["table"]), _p(euro(addon["setup_cent"]), styles["table"]), _p(euro(addon["effective_monthly_cent"]), styles["table"])])
    rows.append([_p("Agenturhonorar gesamt", styles["table_bold"]), _p(euro(pricing["setup_cent"]), styles["table_bold"]), _p(euro(pricing["monthly_cent"]), styles["table_bold"])])
    story.extend([_table(rows, [104 * mm, 35 * mm, 35 * mm], header=True), Spacer(1, 6 * mm)])
    if int(pricing["duration_months"]) > 0:
        story.append(
            _p(
                f"Rechnerische Agenturvergütung der ersten Mindestlaufzeit: {euro(pricing['first_term_cent'])} netto. Werbebudget und Drittkosten sind darin nicht enthalten.",
                styles["callout"],
            )
        )
    story.extend(
        [
            _p("Getrennte Budgets", styles["h1"]),
            _bullet(f"Freigegebenes Medienbudget: {euro(pricing['media_budget_cent'])} pro Monat; Abrechnung möglichst direkt durch Google/Meta beim Auftraggeber.", styles),
            _bullet("Domain, Hosting-Sonderleistungen, E-Mail-Lizenzen, Stockmedien, API-, SMS-, Termin-, Zahlungs- und Druckkosten nur nach Freigabe.", styles),
            _bullet("Zusatzarbeit außerhalb des Umfangs: 95,00 EUR netto pro Stunde, ausschließlich nach vorheriger Freigabe.", styles),
            _p("Abgrenzung des Dashboards", styles["h1"]),
            _p(
                "Der Projektraum dient der transparenten Zusammenarbeit. E-Mail-Empfang, Werbekonten-Steuerung, Rechnungsstellung, Buchhaltung und weitere Automatisierungen sind nur enthalten, wenn sie in dieser Anlage oder einer späteren versionierten Ergänzung ausdrücklich genannt sind.",
                styles["body"],
            ),
            closing_section,
        ]
    )
    doc.build(story, onFirstPage=_page_decorator(snapshot), onLaterPages=_page_decorator(snapshot))
    return stream.getvalue()


def create_price_overview_pdf(*, legal_approved: bool = False) -> bytes:
    styles = _styles()
    stream = io.BytesIO()
    snapshot = {
        "legal_approved": legal_approved,
        "contract_number": "TW-LEISTUNGEN-2026",
    }
    doc = SimpleDocTemplate(
        stream,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=27 * mm,
        bottomMargin=20 * mm,
        title="Tomorrow Works - Leistungen und Preise",
        author="Tomorrow Works",
    )
    story: list[object] = [
        _p("LEISTUNGSÜBERSICHT · B2B", styles["kicker"]),
        _rich("Ein digitaler Auftritt,<br/>der mitarbeitet.", styles["title"]),
        _p("Website, Sichtbarkeit und Anfragen - persönlich betreut von Tomorrow Works.", styles["subtitle"]),
    ]
    if not legal_approved:
        story.append(_p("Interne Preisvorlage. Konditionen und Vertragsunterlagen vor externer Verwendung freigeben.", styles["callout"]))
        story.append(Spacer(1, 5 * mm))
    story.append(_p("Pakete", styles["h1"]))
    package_rows: list[list[object]] = [[_rich("Paket", styles["table_header"]), _rich("Einmalig", styles["table_header"]), _rich("Monatlich", styles["table_header"])]]
    for package in PACKAGE_CATALOG.values():
        if not package.get("public", True):
            continue
        package_rows.append(
            [
                _rich(f"<b>{escape(str(package['name']))}</b><br/>{escape(str(package['tagline']))}", styles["table"]),
                _p(euro(package["setup_cent"]), styles["table"]),
                _p(euro(package["monthly_cent"]), styles["table"]),
            ]
        )
    story.extend([_table(package_rows, [104 * mm, 35 * mm, 35 * mm], header=True), Spacer(1, 5 * mm)])
    story.append(_p("Zusatzmodule", styles["h1"]))
    addon_rows: list[list[object]] = [[_rich("Modul", styles["table_header"]), _rich("Einmalig", styles["table_header"]), _rich("Monatlich", styles["table_header"])]]
    for addon in ADDON_CATALOG.values():
        addon_rows.append(
            [
                _rich(f"<b>{escape(str(addon['name']))}</b><br/>{escape(str(addon['description']))}", styles["table"]),
                _p(euro(addon["setup_cent"]), styles["table"]),
                _p(euro(addon["monthly_cent"]), styles["table"]),
            ]
        )
    story.extend([_table(addon_rows, [104 * mm, 35 * mm, 35 * mm], header=True), Spacer(1, 6 * mm)])
    story.extend(
        [
            _p("Was Google Ads konkret erreichen soll", styles["h1"]),
            _p(
                "Google Ads richtet sich an Menschen, die bereits aktiv nach einer passenden Leistung suchen. Ziel sind messbare Anfragen, Anrufe, Buchungen, Routenaufrufe oder Verkäufe - nicht bloß möglichst viele Klicks. Tomorrow Works kann regionale Ausrichtung, Keywords, Anzeigentexte, Landingpage, Conversion-Messung, Ausschlussbegriffe, Budgetkontrolle und Optimierung übernehmen.",
                styles["body"],
            ),
            _p("Was Instagram und Facebook leisten", styles["h1"]),
            _p(
                "Meta-Kampagnen schaffen visuelle Aufmerksamkeit und können Videoaufrufe, Nachrichten, Leads oder Verkäufe unterstützen. Sie eignen sich besonders für erklärungsbedürftige Angebote, Kurse, Events, Vorher-nachher-Inhalte und Wiederansprache. Ziel, Laufzeit, Creatives und Budget werden vor dem Start freigegeben.",
                styles["body"],
            ),
            _p("Das Dashboard", styles["h1"]),
            _p(
                "Der Projektraum bündelt Fortschritt, Nachrichten, Dateien, Angebote und Verträge. Das erweiterte Business-Cockpit kann Anfragen, E-Mail-Anbindung, Werbekennzahlen, Aufgaben, Dokumente und Schnittstellen ergänzen. Verträge und Rechnungen können abgelegt, angesehen und exportiert werden. Rechtssichere E-Rechnung und Buchhaltung erfordern eine gesonderte Integration.",
                styles["body"],
            ),
            _p("Klare Kostentrennung", styles["h1"]),
            _bullet("Agenturhonorar: Einrichtung, Betreuung, Optimierung, Reporting und vereinbarte Inhalte.", styles),
            _bullet("Medienbudget: tatsächliche Ausspielung bei Google oder Meta, separat freizugeben.", styles),
            _bullet("Drittkosten: Domain, E-Mail-Lizenzen, Hosting-Sonderleistungen, APIs, Stock, Druck und Zahlungsanbieter.", styles),
            _bullet("Keine Garantie für Rang, Reichweite, Leads, Buchungen, Umsatz oder ROAS.", styles),
            _p("Preisstatus", styles["h1"]),
            _p(
                "Alle Preise sind Nettopreise für Gewerbekunden zuzüglich gesetzlicher Umsatzsteuer. Founder-Pilot-Konditionen sind ein begrenztes individuelles Angebot und kein allgemeiner Listenpreis. Eine verbindliche Beauftragung entsteht erst durch eine rechtlich geprüfte, kundenspezifische Vertragsfassung.",
                styles["body"],
            ),
        ]
    )
    doc.build(story, onFirstPage=_page_decorator(snapshot), onLaterPages=_page_decorator(snapshot))
    return stream.getvalue()
