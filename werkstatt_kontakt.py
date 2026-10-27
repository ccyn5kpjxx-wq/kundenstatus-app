"""Current public workshop contact, independent of WhatsApp and legal snapshots."""

from urllib.parse import quote


NAME = "Gärtner GmbH Karosserie + Lack"
STREET = "Alte Neckarelzer Straße 18/1"
POSTCODE = "74821"
LOCALITY = "Mosbach"
CITY = f"{POSTCODE} {LOCALITY}"
BUSINESS_ADDRESS = f"{STREET}, {CITY}"
# Confirmed by the user from the business card on 27 October 2026.
PHONE = "06261 1809260"
PHONE_E164 = "+4962611809260"
PHONE_URL = f"tel:{PHONE_E164}"
DIRECTIONS_URL = "https://www.google.com/maps/dir/?api=1&destination=" + quote(BUSINESS_ADDRESS)


def public_workshop_contact():
    """A fresh mapping for rendered pages; no mutable settings or network calls."""
    return {
        "name": NAME,
        "strasse": STREET,
        "plz": POSTCODE,
        "ort": LOCALITY,
        "plz_ort": CITY,
        "adresse": BUSINESS_ADDRESS,
        "telefon": PHONE,
        "telefon_e164": PHONE_E164,
        "telefon_url": PHONE_URL,
        "anfahrt_url": DIRECTIONS_URL,
    }
