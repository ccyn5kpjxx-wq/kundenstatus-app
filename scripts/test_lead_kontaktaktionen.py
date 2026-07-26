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
            "Lead mit E-Mail bietet interne E-Mail-Antwort",
            website_lead["can_email"],
        )

        landline_lead = portal.hydrate_lead(lead_row(kontakt_telefon="06261 12345"))
        check(
            "Festnetznummer wird nicht als WhatsApp-Kontakt angeboten",
            not landline_lead["can_whatsapp"] and not landline_lead["whatsapp_url"],
        )

        phone_only_lead = portal.hydrate_lead(lead_row(kunde_email=""))
        check(
            "Fehlende E-Mail blendet E-Mail-Antwort aus",
            not phone_only_lead["can_email"],
        )

        rendered = portal.render_template(
            "lead_detail.html",
            lead=website_lead,
            lead_status=portal.LEAD_STATUS,
            lead_quellen=portal.LEAD_QUELLEN,
            schadenarten=portal.SCHADENARTEN,
            autohaeuser=[],
            lead_mail={
                "address": "jonas@example.test",
                "display_name": "Jonas",
                "configured": True,
            },
            lead_mail_entwurf={
                "betreff": "Ihre Anfrage",
                "nachricht": "Guten Tag,\n\nvielen Dank für Ihre Anfrage.",
            },
        )
        check("Gerenderter Button führt zum Portal-Composer", 'href="#email-antwort"' in rendered)
        check("Gerenderte Lead-Seite enthält keinen mailto-Aufruf", "mailto:" not in rendered)
        check("Gerenderte Antwort zeigt Jonas als Absender", "Jonas &lt;jonas@example.test&gt;" in rendered)

        original_config = portal.get_schaden_mail_config
        original_testing = portal.app.config.get("TESTING")
        try:
            portal.get_schaden_mail_config = lambda: {
                "from_address": "jonas@example.test",
                "display_name": "Jonas",
                "reply_to": "jonas@example.test",
                "smtp_configured": True,
                "smtp_host": "smtp.example.test",
                "smtp_port": 465,
                "smtp_user": "jonas@example.test",
                "smtp_ssl": True,
                "smtp_tls": False,
                "_smtp_password": "nur-testwert",
            }
            portal.app.config["TESTING"] = True
            portal.LEAD_MAIL_TESTLOG.clear()
            absender = portal.send_lead_email(
                website_lead,
                website_lead["kunde_email"],
                "Ihre Anfrage",
                "Guten Tag, vielen Dank für Ihre Anfrage.",
            )
            check("Interner Versand nutzt Jonas als Absender", absender == "jonas@example.test")
            check(
                "Interner Versand adressiert genau die Lead-E-Mail",
                len(portal.LEAD_MAIL_TESTLOG) == 1
                and portal.LEAD_MAIL_TESTLOG[0]["empfaenger"] == "kunde@example.test",
            )
        finally:
            portal.get_schaden_mail_config = original_config
            portal.app.config["TESTING"] = original_testing

    template = (ROOT / "templates" / "lead_detail.html").read_text(encoding="utf-8")
    check("WhatsApp-Aktion ist eindeutig beschriftet", "Per WhatsApp antworten" in template)
    check("E-Mail-Aktion ist eindeutig beschriftet", "Per E-Mail antworten" in template)
    check("E-Mail-Aktion öffnet den internen Composer", 'href="#email-antwort"' in template)
    check("Lead-Antwort verwendet keinen mailto-Link", "lead['mailto_url']" not in template)

    if FEHLER:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
