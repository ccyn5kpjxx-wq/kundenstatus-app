"""Pure EUR tire-price calculation; no provider, HTTP, database or fallback prices.

Call only after confirming a supplier's current net unit price and the actual
extra shipping/procurement charge for the entire quote. Known free shipping is
the explicit value "0.00"; missing/unknown shipping is never silently zero.
The unit purchase price excludes separately charged procurement costs. A charge
already included in that unit price must not be passed a second time.

The tire markup is exactly 10 percent, before VAT. Published gross unit prices
are rounded to cents (HALF_UP); tire totals are those unit prices times quantity.
Shipping is added once without markup. An optional approved-catalogue mounting
snapshot retains its gross price and its own VAT, even if tire VAT differs.
Amounts are JSON-safe decimal strings and integer cents, never float. The result
is internal: it contains purchase-price calculation bases. A future public route
must whitelist customer-facing selling totals instead of serializing it whole.
This module alone does not make a live supplier price available.
"""
from collections.abc import Mapping
from decimal import Decimal, ROUND_HALF_UP, localcontext
import re

from reifenservice import MWST_SATZ, _gepruefter_snapshot


AUFSCHLAG_PROZENT = Decimal("10.00")
MAX_MENGE = 8
MAX_EINKAUF_NETTO = Decimal("10000")
MAX_BEZUGSKOSTEN_NETTO = Decimal("10000")
MAX_GESAMT_BRUTTO = Decimal("100000")
_CENT = Decimal("0.01")
_HUNDERT = Decimal("100")
_MONTAGE_IDS = {"montage_13_16", "montage_17_18", "montage_19_20"}
_ERLAUBTE_ZUSAETZE = {"runflat", "entsorgung", "metallventil", "einlagerung", "raederwaesche"}


def _betrag(value, label, *, maximum, stellen=2, null_erlaubt=True):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError(f"{label} muss als bestätigter Dezimalbetrag vorliegen; kein Float oder fehlender Wert.")
    if isinstance(value, Decimal):
        amount = value
    else:
        text = str(value)
        if len(text) > 30 or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", text):
            raise ValueError(f"{label} ist kein gültiger Dezimalbetrag (Punkt als Dezimaltrennzeichen).")
        amount = Decimal(text)
    if not amount.is_finite() or amount < 0 or amount > maximum:
        raise ValueError(f"{label} muss zwischen 0 und {maximum} liegen.")
    if not null_erlaubt and amount == 0:
        raise ValueError(f"{label} muss positiv sein.")
    if amount.as_tuple().exponent < -stellen:
        raise ValueError(f"{label} darf höchstens {stellen} Nachkommastellen haben.")
    return amount


def _menge(value):
    if isinstance(value, bool) or not isinstance(value, (int, str)) or not re.fullmatch(r"[1-8]", str(value)):
        raise ValueError(f"Bitte eine ganze Reifenmenge von 1 bis {MAX_MENGE} angeben.")
    return int(value)


def _cent(amount):
    return int(amount.quantize(_CENT, rounding=ROUND_HALF_UP) * _HUNDERT)


def _euro(cents):
    return f"{Decimal(cents) / _HUNDERT:.2f}"


def _montage(snapshot, menge):
    if snapshot is None:
        return {"brutto_cent": 0, "netto_cent": 0, "mwst_cent": 0, "mwst_satz": None}
    if not isinstance(snapshot, Mapping) or snapshot.get("quelle") == "CHECK24-Buchung":
        raise ValueError("Montage muss aus einer geprüften eigenen Reifenservice-Kalkulation stammen.")
    checked = _gepruefter_snapshot(snapshot)
    positions = checked["positionen"]
    if any(p["preis_id"] not in _MONTAGE_IDS | _ERLAUBTE_ZUSAETZE for p in positions):
        raise ValueError("Die Montage-Kalkulation darf nur Montage und passende Zusatzleistungen enthalten.")
    montage_menge = sum(p["menge"] for p in positions if p["preis_id"] in _MONTAGE_IDS)
    if montage_menge != menge:
        raise ValueError("Die Montagezahl muss zur angefragten Reifenmenge passen (höchstens vier Reifen je Montagevorgang).")
    return {**checked["gesamt"], "mwst_satz": checked["mwst_satz"]}


def berechne_reifenpreis(*, einkauf_netto, menge, bezugskosten_netto,
                        preise_bestaetigt, montage_kalkulation=None,
                        mwst_satz=MWST_SATZ):
    """Calculate one tire article, adding one order-level procurement charge.

    Required inputs: confirmed net EUR per tire (up to four decimal places),
    integer quantity 1..8, confirmed extra net shipping/procurement for the whole
    order (cent precision, including explicit zero), and preise_bestaetigt=True.
    Floats, missing prices, negative amounts and non-finite values are rejected.
    The optional mounting argument must be the exact current snapshot returned
    by reifenservice.berechne_reifenservice, with a matching mounting quantity.
    It cannot be a CHECK24 booking or arbitrary client-supplied gross amount.

    The caller must perform stock, delivery, age and fitment checks separately.
    For different front/rear articles, aggregate separate article lines in the
    future quote adapter and add procurement exactly once for that whole order;
    do not charge this function's shipping amount once per axle or per tire.
    """
    if preise_bestaetigt is not True:
        raise ValueError("Ohne bestätigte Einkaufs- und Bezugskosten kann kein Verkaufspreis berechnet werden.")
    anzahl = _menge(menge)
    ek = _betrag(einkauf_netto, "Netto-Einkaufspreis", maximum=MAX_EINKAUF_NETTO,
                 stellen=4, null_erlaubt=False)
    bezug = _betrag(bezugskosten_netto, "Netto-Bezugskosten", maximum=MAX_BEZUGSKOSTEN_NETTO)
    steuer = _betrag(mwst_satz, "Mehrwertsteuersatz", maximum=Decimal("30"))
    with localcontext() as context:
        context.prec = 36
        montage = _montage(montage_kalkulation, anzahl)
        faktor = Decimal("1") + steuer / _HUNDERT
        vk_netto_basis = ek * (Decimal("1") + AUFSCHLAG_PROZENT / _HUNDERT)
        stueck_brutto = _cent(vk_netto_basis * faktor)
        if not stueck_brutto:
            raise ValueError("Der bestätigte Einkaufspreis ergibt keinen positiven Cent-Verkaufspreis.")
        reifen_brutto = stueck_brutto * anzahl
        reifen_netto = _cent(Decimal(reifen_brutto) / _HUNDERT / faktor)
        versand_brutto = _cent(bezug * faktor)
        versand_netto = _cent(bezug)
        gesamt_brutto = reifen_brutto + versand_brutto + montage["brutto_cent"]
        if gesamt_brutto > int(MAX_GESAMT_BRUTTO * _HUNDERT):
            raise ValueError("Der Gesamtbruttopreis überschreitet die Angebotsgrenze von 100.000 EUR.")
        reifen_mwst = reifen_brutto - reifen_netto
        versand_mwst = versand_brutto - versand_netto
        result = {
            "schema_version": 1, "waehrung": "EUR", "menge": anzahl,
            "preise_bestaetigt": True, "aufschlag_prozent": "10.00",
            "mwst_satz": f"{steuer:.2f}", "montage_mwst_satz": montage["mwst_satz"],
            "einkauf_einzel_netto": format(ek, "f"),
            "reifen_kalkulationsbasis_einzel_netto": format(vk_netto_basis, "f"),
            "einzelpreis_brutto_reifen_cent": stueck_brutto,
            "gesamt_brutto_reifen_cent": reifen_brutto,
            "versand_brutto_cent": versand_brutto,
            "montage_brutto_cent": montage["brutto_cent"],
            "gesamt_brutto_cent": gesamt_brutto,
            "reifen_mwst_cent": reifen_mwst, "versand_mwst_cent": versand_mwst,
            "montage_mwst_cent": montage["mwst_cent"],
            "gesamt_mwst_cent": reifen_mwst + versand_mwst + montage["mwst_cent"],
            "gesamt_netto_cent": reifen_netto + versand_netto + montage["netto_cent"],
            "bezugskosten_einmal_pro_angebot": True,
        }
        for key in ("einzelpreis_brutto_reifen", "gesamt_brutto_reifen", "versand_brutto", "montage_brutto", "gesamt_brutto"):
            result[key] = _euro(result[key + "_cent"])
        return result
