"""Project the saved work description onto the workshop printout."""
import re


_BULLETS = re.compile(r"[;•●▪☐]")
_LIST_PREFIX = re.compile(r"^(?:[-*–—]\s+|\d{1,2}[.)]\s+)")
_HEADINGS = {"arbeiten", "arbeitsumfang", "arbeitspositionen", "bestellte artikel", "reparaturumfang"}
# Only use explicit part descriptions. "Stoßfänger neu lackieren" describes a
# paint operation on an unspecified part and is not a new-part declaration.
_PART = (r"(?:(?:[a-zäöüß-]*)(?:stoßfänger|stossfänger|stoßstange|stossstange|kotflügel|"
         r"motorhaube|heckklappe|tür(?:en|e)?|leiste[n]?|blende[n]?|gitter|spiegel|scheinwerfer|"
         r"leuchte[n]?|schweller|schürze|verkleidung|seitenwand|blech)|(?:bau|ersatz|anbau)?teile?)")
_EXPLICIT_NEW = re.compile(
    rf"\b(?:neu[\s-]?teile?(?:s|en)?|neuteil[\s-]?lackierung|neu(?:e|er|es|en|em)\s+{_PART})\b"
    rf"|\b{_PART}\s*\(\s*neu\s*\)", re.I)
_UNCERTAIN_NEW = re.compile(
    r"\?|\b(?:kein\w*|nicht|ohne|nein|entfällt|entfallen|statt|anstatt|gebraucht\w*|"
    r"evtl|eventuell|ggf|gegebenenfalls|falls|wenn|optional|alternativ\w*|vermutlich|"
    r"vielleicht|prüfen|unklar|anfragen|angebot|kostenvoranschlag|vormals|damals|früher|historisch\w*|"
    r"ehemals|zuletzt|ursprünglich|seinerzeit|war(?:en)?|hatte(?:n)?|vorjahr|letztes\s+jahr|"
    r"vor\s+[\w\s-]{1,25}\s+jahren|(?:19|20)\d{2}\s+(?:als\s+)?neuteil)\b", re.I)
_PAST = re.compile(r"\b(?:wurde(?:n)?|bereits|schon)\b", re.I)
_DELIVERED = re.compile(r"\b(?:angeliefert(?:e[nrms]?)?|beigestellt(?:e[nrms]?)?)\b", re.I)
_DOORS = re.compile(
    r"(?P<door>tür(?:e|en)?)\s+(?P<front>vorn(?:e)?)(?P<side1>\s+(?:links|rechts))?"
    r"\s+und\s+(?:tür(?:e|en)?\s+)?(?P<rear>hinten)(?P<side2>\s+(?:links|rechts))?"
    r"\s+(?P<work>lackieren|beilackieren|instand\s*setzen|demontieren|montieren)(?P<end>[.!]?)", re.I)


def _positions(text):
    doors = _DOORS.fullmatch(text)
    if not doors or bool(doors['side1']) != bool(doors['side2']):
        return [text]
    door = doors['door'] if not doors['door'].lower().endswith('en') else doors['door'][:-2]
    work = doors['work'] + doors['end']
    return [f"{door} {doors[where]}{doors[side] or ''} {work}"
            for where, side in (('front', 'side1'), ('rear', 'side2'))]


def checklist_rows(scope_text):
    """Keep each saved position intact except for explicit lists and unambiguous door pairs."""
    rows = []
    for line in (scope_text or '').splitlines():
        for item in _BULLETS.split(line):
            text = _LIST_PREFIX.sub('', item.strip()).strip()
            if not text or text.rstrip(':').lower() in _HEADINGS:
                continue
            for position in _positions(text):
                new_part = bool(_EXPLICIT_NEW.search(position) and not _UNCERTAIN_NEW.search(position)
                                and (not _PAST.search(position) or _DELIVERED.search(position)))
                rows.append({'text': position, 'new_part': new_part})
    return rows
