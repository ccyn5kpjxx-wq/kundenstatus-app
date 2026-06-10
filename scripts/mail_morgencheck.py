import pathlib
import sys
from collections import Counter


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as portal  # noqa: E402


def print_email_line(email_item):
    target = email_item.get("ziel_modul_label") or email_item.get("kategorie_label") or "Allgemein"
    recipient = email_item.get("empfaenger") or "ohne Empfaenger"
    sender = email_item.get("absender_display") or "Unbekannter Absender"
    subject = email_item.get("betreff") or "(ohne Betreff)"
    print(f"- #{email_item['id']} [{target}] {subject} | {sender} -> {recipient}")


def list_open_invoice_markers(limit=12):
    db = portal.get_db()
    try:
        rows = db.execute(
            """
            SELECT id, richtung, status, voucher_number, contact_name, total_amount, open_amount, due_date, source_email_id
            FROM lexware_rechnungen
            WHERE status IN ('pruefen', 'offen')
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    finally:
        db.close()
    return [dict(row) for row in rows]


def main():
    print(f"Mail-Morgencheck {portal.now_str()}")

    status = portal.werkstatt_imap_status()
    if not status.get("configured"):
        print("IMAP ist noch nicht voll konfiguriert. Bitte MAIL_IMAP_HOST, MAIL_IMAP_USER und MAIL_IMAP_PASS in .env.local setzen.")
    else:
        try:
            summary = portal.sync_werkstatt_imap()
            print(
                "IMAP-Sync: "
                f"{summary.get('checked', 0)} geprueft, "
                f"{summary.get('created', 0)} neu, "
                f"{summary.get('skipped', 0)} uebersprungen."
            )
            for error in summary.get("errors") or []:
                print(f"Fehler: {error}")
        except Exception as exc:
            print(f"IMAP-Sync fehlgeschlagen: {exc}")

    new_emails = portal.list_werkstatt_emails("neu", limit=40)
    if not new_emails:
        print("Keine neuen E-Mail-Aufgaben.")
    else:
        counts = Counter(email.get("ziel_modul") or email.get("kategorie") or "allgemein" for email in new_emails)
        print("Neue E-Mail-Aufgaben: " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
        for email_item in new_emails[:20]:
            print_email_line(email_item)

    invoices = list_open_invoice_markers()
    if invoices:
        print("Offene Rechnungskontrolle:")
        for invoice in invoices:
            number = invoice.get("voucher_number") or "ohne Nummer"
            contact = invoice.get("contact_name") or "unbekannt"
            amount = portal.format_euro(invoice.get("open_amount") or invoice.get("total_amount") or 0)
            source = f" E-Mail #{invoice['source_email_id']}" if invoice.get("source_email_id") else ""
            print(f"- #{invoice['id']} [{invoice.get('richtung') or 'offen'}] {number} | {contact} | {amount}{source}")


if __name__ == "__main__":
    main()
