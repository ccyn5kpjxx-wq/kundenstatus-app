from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


FEHLER = []


def check(label, ok):
    print(f"[{'OK' if ok else 'FEHLER'}] {label}")
    if not ok:
        FEHLER.append(label)


def lead_row(**updates):
    row = {
        "id": 2,
        "quelle": "website",
        "status": "neu",
        "kunde_name": "Kundin Muster",
        "kontakt_telefon": "0151 23456789",
        "kunde_email": "kunde@example.test",
        "fahrzeug": "Testfahrzeug",
        "kennzeichen": "",
        "schadenart": "unbekannt",
        "beschreibung": "Farbton- und Lackberatung",
        "naechste_aktion": "Farbtonprüfung abstimmen",
        "naechster_kontakt_am": "",
        "geschaetzter_wert": 0,
        "notiz": "",
        "verloren_grund": "",
        "autohaus_id": 0,
        "auftrag_id": 0,
        "source_email_id": 0,
    }
    row.update(updates)
    return row


def main():
    with portal.app.test_request_context("/admin/leads/2"):
        website_lead = portal.hydrate_lead(lead_row())
        check(
            "Website-Lead mit Mobilnummer bietet WhatsApp-Antwort",
            website_lead["can_whatsapp"]
            and website_lead["whatsapp_url"].startswith("https://wa.me/4915123456789?text="),
        )
        check(
            "Lead mit E-Mail bietet E-Mail-Antwort",
            website_lead["mailto_url"].startswith("mailto:kunde@example.test?"),
        )

        landline_lead = portal.hydrate_lead(lead_row(kontakt_telefon="06261 12345"))
        check(
            "Festnetznummer wird nicht als WhatsApp-Kontakt angeboten",
            not landline_lead["can_whatsapp"] and not landline_lead["whatsapp_url"],
        )

        phone_only_lead = portal.hydrate_lead(lead_row(kunde_email=""))
        check(
            "Fehlende E-Mail blendet E-Mail-Antwort aus",
            not phone_only_lead["mailto_url"],
        )

    template = (ROOT / "templates" / "lead_detail.html").read_text(encoding="utf-8")
    check("WhatsApp-Aktion ist eindeutig beschriftet", "Per WhatsApp antworten" in template)
    check("E-Mail-Aktion ist eindeutig beschriftet", "Per E-Mail antworten" in template)

    if FEHLER:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
