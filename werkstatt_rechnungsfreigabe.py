"""Pure metadata gate before reading any invoice for workshop product import.

Call ``classify_invoice_source(source, allowed_suppliers=())`` before enqueueing
and immediately before file access. ``source`` can be a supplier string or a
mapping using supplier/lieferant/contact_name/contactName. Only explicitly named
material suppliers are allowed. Obvious banking, insurance, contributions, tax,
medical and debt-collection sources are blocked. Every unknown supplier requires
classification (review), without reading its files to decide its purpose.

``allowed_suppliers`` must come from an explicitly maintained server-side list,
never invoice text or client input. Entries are exact company-name matches after
case/punctuation normalization, not substrings or regular expressions. Sensitive
source rules always override that list. Existing supplier spelling is retained;
classification never merges records, invents supplier identities or authorizes
payments. Invoice type/existence still require validation by the source reader.

Returns {decision: allow|block|review, allowed, requires_review, supplier,
normalized_supplier, reason, rule}. No file, database, network or app access.
"""

from collections.abc import Mapping
import re
import unicodedata


_SUPPLIER_KEYS = ("supplier", "lieferant", "contact_name", "contactName")
_REFERENCE_KEYS = ("reference", "original_name", "voucher_number", "voucherNumber", "subject", "betreff", "filename")
_LEGAL_SUFFIXES = ("", "gmbh", "ag", "kg", "gmbhcokg", "gmbhundcokg", "ohg", "ek", "eg")


def normalize_supplier(value):
    """Normalize matching only; never use this as a replacement supplier name."""
    if not isinstance(value, str) or len(value) > 500:
        return ""
    value = unicodedata.normalize("NFKC", value).casefold()
    value = value.translate(str.maketrans({"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss"}))
    value = "".join(char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value).split())


def _compact(value):
    return normalize_supplier(value).replace(" ", "")


def _names(roots, suffixes=_LEGAL_SUFFIXES):
    return {root + suffix for root in roots for suffix in suffixes}


# Deliberately narrow company-name families. "Tech", "Parts", "Color", a
# product keyword or an arbitrary name containing a brand do not grant access.
_KNOWN = {
    "topcolor": _names(("topcolor", "topcolour", "topcolorautolackierbedarf", "topcolorlackierbedarf", "topcolorautolacke")) | {"topcolorgmbhhermannstr"},
    "carparts": _names(("carparts", "carpartsautomotive", "carpartsdeutschland")),
    "techmasters": _names(("techmasters", "techmastersdeutschland")),
}

_BLOCK_RULES = (
    ("payment_metadata", re.compile(
        r"\b(?:ueberweisung[a-z0-9]*|doppel\s?zahlung[a-z0-9]*|ueber\s?zahlung[a-z0-9]*|"
        r"sepa(?:lastschrift)?(?:mandat)?[0-9]*|lastschrift[a-z0-9]*|mandat(?:e|serteilung)?|mandatsreferenz|mandatsaenderung|"
        r"einzugsermaechtigung[a-z0-9]*|abbuchung[a-z0-9]*|mahnung[a-z0-9]*|doppelbelastung[a-z0-9]*|"
        r"zahlungs\s?(?:avis[a-z0-9]*|erinnerung[a-z0-9]*|bestaetigung[a-z0-9]*|abgleich|eingang|ausgang|verkehr|mitteilung|differenz|aufforderung)|"
        r"kontoabstimmung|kontenabstimmung|saldenbestaetigung|rueckzahlung[a-z]*|rueckerstattung[a-z]*|"
        r"remittance|payment\s+(?:advice|confirmation|reminder)|direct\s+debit)\b"),
     "Zahlungsmitteilungen, Überweisungen und Lastschriftunterlagen sind vom Artikelimport ausgeschlossen."),
    ("banking", re.compile(r"\b(?:[a-z]*bank[a-z]*|sparkass[a-z]*|raiffeisen[a-z]*|commerzbank|postbank|hypovereinsbank|dkb|ing(?:\s*diba)?|banking|konto(?:auszug|fuehrung|gebuehr[a-z]*|stand|nummer)|kreditkarte|darlehen|zinsen|zahlungsdienstleister|paypal|klarna)\b"),
     "Bank-, Konto- und Zahlungsdaten sind vom Artikelimport ausgeschlossen."),
    ("insurance", re.compile(r"\b(?:[a-z]*versicherung[a-z]*|assekuranz|allianz|huk(?:\s*coburg)?|devk|debeka|axa|generali|signal\s*iduna|r\s*v)\b"),
     "Versicherungsbelege sind vom Artikelimport ausgeschlossen."),
    ("contributions", re.compile(r"\b(?:[a-z]*beitra[e]?g[a-z]*|renten[a-z]*|sozialkass[a-z]*|krankenkass[a-z]*|aok|barmer|dak|tk|berufsgenossenschaft[a-z]*|bghm|bgetem|ihk|hwk|handwerkskammer|industrie\s*und\s*handelskammer|rundfunk[a-z]*|gez|innung[a-z]*)\b"),
     "Beiträge, Sozialkassen und Kammerbelege sind vom Artikelimport ausgeschlossen."),
    ("tax_authority", re.compile(r"\b(?:[a-z]*steuer[a-z]*|finanzamt|finanzkasse|zoll[a-z]*|behoerde[a-z]*|bundeskasse|landeskasse|stadtkasse|gemeindekasse|bussgeld[a-z]*|ordnungsamt|landratsamt|amtsgericht|justizkasse)\b"),
     "Steuer-, Behörden- und Finanzbelege sind vom Artikelimport ausgeschlossen."),
    ("medical", re.compile(r"\b(?:[a-z]*arzt[a-z]*|aerzt[a-z]*|praxis|klinikum|klinik|krankenhaus|medizin[a-z]*|apothek[a-z]*|zahnarzt[a-z]*|physio[a-z]*|therapie[a-z]*|patient[a-z]*|labor(?:rechnung)?)\b"),
     "Medizinische Belege sind vom Artikelimport ausgeschlossen."),
    ("collection", re.compile(r"\b(?:inkasso[a-z]*|forderungsmanagement|mahn(?:bescheid|gebuehr[a-z]*|verfahren)|schuldner[a-z]*|gerichtsvollzieher)\b"),
     "Inkasso- und Forderungsbelege sind vom Artikelimport ausgeschlossen."),
)
_MISSING = {"", "unbekannt", "lieferantoffen", "sammellieferant", "sonstige", "sonstiges", "unknown"}


def _answer(decision, supplier, reason, rule):
    return {
        "decision": decision, "allowed": decision == "allow", "requires_review": decision == "review",
        "supplier": supplier, "normalized_supplier": normalize_supplier(supplier),
        "reason": reason, "rule": rule,
    }


def classify_invoice_source(source, allowed_suppliers=()):
    """Allow only known/explicit workshop suppliers; unknown means no file read."""
    if isinstance(source, str):
        source = {"supplier": source}
    if not isinstance(source, Mapping):
        return _answer("review", "", "Die Rechnungsquelle muss zuerst einem Lieferanten zugeordnet werden.", "missing_supplier")
    names = [source[key].strip() for key in _SUPPLIER_KEYS if isinstance(source.get(key), str) and source[key].strip()]
    supplier = names[0][:500] if names else ""
    references = [value for key in _REFERENCE_KEYS if isinstance((value := source.get(key)), str)]
    if any(len(value) > 500 for value in names + references):
        return _answer("review", supplier, "Die Quellenangabe ist zu lang für eine sichere Zuordnung; vor dem Lesen prüfen.", "invalid_metadata")
    # Only metadata is inspected, never raw_json, amounts, invoice text or files.
    normalized = " ".join(normalize_supplier(value) for value in names + references)
    for rule, pattern, reason in _BLOCK_RULES:
        if pattern.search(normalized):
            return _answer("block", supplier, reason, rule)
    for key in ("voucher_type", "voucherType"):
        if key in source and source[key] != "purchaseinvoice":
            return _answer("block", supplier, "Die Quelle ist keine Lieferantenrechnung.", "not_purchase_invoice")
    if "beleg_typ" in source and source["beleg_typ"] != "rechnung":
        return _answer("block", supplier, "Die Quelle ist kein Rechnungsbeleg.", "not_invoice")
    keys = {_compact(name) for name in names}
    if len(keys) > 1:
        return _answer("review", supplier, "Die Lieferantenzuordnung der Quelle ist widersprüchlich und muss geprüft werden.", "conflicting_supplier")
    key = _compact(supplier)
    if key in _MISSING:
        return _answer("review", supplier, "Ein eindeutiger Materiallieferant fehlt; vor dem Lesen zuordnen.", "missing_supplier")
    for family, variants in _KNOWN.items():
        if key in variants:
            return _answer("allow", supplier, "Bekannter Material- oder Werkstattlieferant; Artikelvorschläge dürfen ausgelesen werden.", "known_supplier:" + family)
    if isinstance(allowed_suppliers, (list, tuple, set, frozenset)):
        allowed = {_compact(name) for name in allowed_suppliers
                   if isinstance(name, str) and len(name) <= 500 and not any(mark in name for mark in ("*", "?", "[", "]"))}
        if key in allowed:
            return _answer("allow", supplier, "Dieser Lieferant wurde serverseitig ausdrücklich für Materialrechnungen freigegeben.", "explicit_supplier")
    return _answer("review", supplier, "Unbekannter Lieferant: erst als Material- oder Werkstattlieferant einordnen; bis dahin keine Datei lesen.", "unclassified_supplier")
