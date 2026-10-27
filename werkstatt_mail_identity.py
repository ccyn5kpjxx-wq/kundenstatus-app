"""Current sender details for newly composed Gärtner mail, never stored mail."""

import html
import re
from werkstatt_kontakt import BUSINESS_ADDRESS, CITY, PHONE, STREET


FOOTER_TEXT = (
    "Gärtner GmbH Karosserie + Lack\n"
    + BUSINESS_ADDRESS
    + f"\nTelefon: {PHONE}\nE-Mail: info@auto-lackierzentrum.de"
)


def append_workshop_footer(text):
    text = str(text or "")
    if STREET in text and CITY in text:
        return text
    return text + "\n\n--\n" + FOOTER_TEXT + "\n"


def add_workshop_mail_footer(message):
    """Append current sender details to the main bodies, retaining attachments.

    Call only for newly composed Gärtner/company mail before its first persistence
    or dispatch. Tenant-specific callers must explicitly exclude other companies.
    Quotes and contractual/pickup snapshots are preserved verbatim.
    """
    for kind in ("plain", "html"):
        part = message.get_body(preferencelist=(kind,))
        if part is None:
            continue
        body = part.get_content()
        if not isinstance(body, str):
            continue
        visible = html.unescape(re.sub(r"<[^>]*>", " ", body)) if kind == "html" else body
        if STREET in visible and CITY in visible:
            continue
        if kind == "plain":
            updated = append_workshop_footer(body)
        else:
            footer = '<div data-gaertner-mail-footer="1" style="margin-top:20px;font-size:13px;line-height:1.5">' + html.escape(FOOTER_TEXT).replace("\n", "<br>") + "</div>"
            if re.search(r"</body\s*>", body, re.I):
                updated = re.sub(r"</body\s*>", lambda match: footer + match.group(), body, count=1, flags=re.I)
            elif re.search(r"</html\s*>", body, re.I):
                updated = re.sub(r"</html\s*>", lambda match: footer + match.group(), body, count=1, flags=re.I)
            else:
                updated = body + footer
        retained = {name: part[name] for name in ("Content-ID", "Content-Disposition") if part[name]}
        part.set_content(updated, subtype=kind, charset=part.get_content_charset() or "utf-8")
        for name, value in retained.items():
            part[name] = value
    return message
