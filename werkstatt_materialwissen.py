"""Pure search/ranking of already scope-filtered product evidence.

No invoice text, network, writes or automatic purchase decisions. Similar search
terms can match a product; they never merge different supplier/SKU/variant data.
Invoice quantities are observations, not consumption or package contents.
"""
from collections import Counter, OrderedDict
from decimal import Decimal, InvalidOperation
from functools import lru_cache
import hashlib
import json
import re
import unicodedata


_ALIASES = {
    'anklebeband': 'klebeband', 'abklebeband': 'klebeband', 'abdeckband': 'klebeband',
    'lackierband': 'klebeband', 'maskierband': 'klebeband', 'tape': 'klebeband',
    'klebebaender': 'klebeband', 'abklebebaender': 'klebeband',
    'millimeter': 'mm', 'millimetern': 'mm', 'zentimeter': 'cm', 'meter': 'm', 'mtr': 'm',
    'rollen': 'rolle', 'kartons': 'karton', 'stueck': 'stueck', 'stk': 'stueck', 'stck': 'stueck',
    # Packaging can use the English color while the supplier invoice uses German.
    # This only broadens search; original names and variant identity stay intact.
    'silver': 'silber',
}
_COLORS = ('gruen', 'blau', 'rot', 'gelb', 'weiss', 'schwarz', 'grau', 'orange', 'violett', 'transparent', 'braun', 'silber')
_TOKEN_ALIASES = dict(_ALIASES, **{color+suffix:color for color in _COLORS for suffix in ('','e','en','er','es','em')})
_STOP = frozenset(('ich moechte will brauche brauchen bitte bestelle bestellen nachbestellen kauf kaufen '
                  'haben habt wir ihr du mir mich uns ein eine einen einem einer eines der die das den '
                  'dem des und oder von vom beim fuer zum zur mit ohne in im am an ist sind wird '
                  'wurde es noch mal wieder nur jetzt heute welche welches welcher was wie viel viele '
                  'gibt hatten zuletzt ueblich uebliche normale menge mengen breite breit lieferant '
                  'lieferanten artikel material produkt produkte ja nein leer nachfuellen aufbrauchen '
                  'kann kannst koennen koennt hast habe hat haetten haette hatten da zeigen zeige '
                  'suche suchen finden finde nach nachsehen nachschauen sehen schauen denn eigentlich '
                  'dafuer passend passende passenden welches normalerweise bisher bestellt').split())
_UNITS = frozenset(('mm', 'cm', 'm', 'ml', 'l', 'kg', 'g', 'rolle', 'stueck', 'pack', 'karton', 'gebinde', 'dose', 'set'))
_BANK = re.compile(r'\b(?:iban|bic|swift|konto(?:nummer|stand|auszug|belastung)|bankverbindung|sepa|lastschrift)\b', re.I)
_COLLOQUIAL_SIZE = re.compile(r'\b(?:(?:zwanzig|dreissig|vierzig|fuenfzig|sechzig|siebzig|achtzig|neunzig|hundert)(?:er|e|en|es)?|\d{1,3}er)\b')


def _text(value, limit=1000):
    return value.strip()[:limit] if isinstance(value, str) and not _BANK.search(value) else ''


def _normal(value):
    return ' '.join(unicodedata.normalize('NFKC', _text(value)).casefold().split())


def _ascii(value):
    return _normal(value).translate(str.maketrans({'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss'}))


@lru_cache(maxsize=4096)
def _tokens(value):
    # Pure string normalization only. Catalog rows, scope decisions and search
    # results are never cached, so revocation/import changes apply immediately.
    value = re.sub(r'(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)', ' ', _ascii(value))
    # Recorded trade names can be concatenated by PDF extraction. These are
    # search aliases only; the persisted name and variant identity stay exact.
    value = re.sub(r'\btapehydrogreen\b', 'tape hydrogreen', value)
    value = re.sub(r'\btop[ -]+colou?r\b', 'topcolor', value)
    value = re.sub(r'\bcar[ -]+parts\b', 'carparts', value)
    value = re.sub(r'\btech[ -]+masters\b', 'techmasters', value)
    return tuple(_TOKEN_ALIASES.get(x,x) for x in re.findall(r'[a-z]+|\d+(?:[.,]\d+)?', value))


def tokens(value):
    return _tokens(_text(value))


def _measurements(value):
    # The trademark 3M is not a stated three-metre dimension.
    value = re.sub(r'\b3M\b', '', _text(value))
    value = re.sub(r'(\d)\s*(m)(?=Rolle)', r'\1 \2 ', value)
    normalized = ' '.join(tokens(value)).replace(',', '.')
    result=set(re.findall(r'\b(\d+(?:\.\d+)?)\s+(mm|cm|m|ml|l|kg|g)\b', normalized))
    for first,second,unit in re.findall(r'\b(\d+(?:\.\d+)?)\s+x\s+(\d+(?:\.\d+)?)\s+(mm|cm|m)\b',normalized):
        result.update(((first,unit),(second,unit)))
    return result


def _colors(value):
    return set(tokens(value)) & set(_COLORS)


def query_notes(query):
    ambiguous = bool(_COLLOQUIAL_SIZE.search(_ascii(query)))
    if not _measurements(query) and any(t.isdigit() for t in tokens(query)):
        ambiguous = True
    return (['Größenangabe ohne eindeutige Maßeinheit: Varianten nennen und Einheit gezielt klären; nicht cm oder mm unterstellen.']
            if ambiguous else [])


def _search_text(row):
    return ' '.join(_text(row.get(k)) for k in
                    ('produkt_name', 'produkt_beschreibung', 'artikelnummer', 'lieferant', 'groesse', 'farbe', 'gebinde', 've'))


def rank_records(records, query=''):
    """Return references to matching rows ranked by query tokens, never merged."""
    search_query = _COLLOQUIAL_SIZE.sub(' ', _ascii(query))
    terms = list(dict.fromkeys(t for t in tokens(search_query) if t not in _STOP))
    # Packing wishes such as "einen Karton" do not imply a known pack size and
    # must not hide matching tape sold by the roll.
    required = [t for t in terms if t not in {'karton', 'pack', 'gebinde', 'rolle', 'stueck'}]
    families=[t for t in required if re.fullmatch(r'klebeband|hydrogreen|schleif[a-z]+|handschuh[a-z]*|politur[a-z]*|verduenn[a-z]*|silikonentferner',t)]
    if families:
        # Natural questions contain grammar that cannot occur on product rows.
        # Keep explicit variant/SKU/supplier constraints; other words can rank
        # a match but cannot erase an identified product family.
        required=list(dict.fromkeys(families+[t for t in required if re.fullmatch(r'\d+(?:[.,]\d+)?',t)
                        or t in _UNITS or t in _COLORS or t in {'topcolor','carparts','techmasters'}]))
    dimensions, colors = _measurements(query), _colors(query)
    ranked = []
    for index, row in enumerate(records):
        text = _search_text(row)
        hay = set(tokens(text))
        # This is a search alias for a recorded trade name, not a new fact in
        # the color field or a license to merge distinct product variants.
        if 'hydrogreen' in hay:
            hay.add('gruen')
        if dimensions and not dimensions.issubset(_measurements(text)):
            continue
        if colors and not colors.issubset(hay & set(_COLORS)):
            continue
        matches = [term for term in required if term in hay or
                   (len(term) >= 5 and any(word.startswith(term) for word in hay))]
        if required and len(matches) != len(required):
            continue
        name = set(tokens(row.get('produkt_name', '')))
        score = sum(5 if term in name else 2 for term in matches)
        score += sum(3 for term in terms if term not in required and term in hay)
        if query and _normal(query) == _normal(row.get('artikelnummer')):
            score += 100
        ranked.append((score, -index, row))
    return [row for _, _, row in sorted(ranked, key=lambda item: item[:2], reverse=True)]


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        return None
    text = str(value).strip()
    if not re.fullmatch(r'\d+(?:[.,]\d{1,4})?', text):
        return None
    try:
        number = Decimal(text.replace(',', '.'))
        return format(number.normalize(), 'f') if 0 < number <= 1000000 else None
    except InvalidOperation:
        return None


def _source(row):
    raw = row.get('quelle') if isinstance(row.get('quelle'), dict) else {}
    result = {}
    for key in ('art', 'beleg_id', 'artikel_id', 'seite', 'position', 'zeile', 'extraktionsindex', 'datum', 'methode'):
        value = raw.get(key)
        if type(value) is int and value >= 0:
            result[key] = value
        elif isinstance(value, str) and len(value) <= 100 and not _BANK.search(value):
            result[key] = value
    pages = raw.get('seiten')
    if isinstance(pages, list):
        result['seiten'] = list(dict.fromkeys(x for x in pages[:30] if type(x) is int and x > 0))
    digest = raw.get('datei_sha256')
    if isinstance(digest, str) and re.fullmatch('[a-fA-F0-9]{64}', digest):
        result['datei_sha256'] = digest.lower()
    return result


def _variant_fields(row):
    product = ' '.join(_text(row.get(k)) for k in ('produkt_name', 'produkt_beschreibung'))
    size = _text(row.get('groesse')) or ' / '.join(n + ' ' + u for n, u in sorted(_measurements(product)))
    color = _text(row.get('farbe')) or ' / '.join(sorted(_colors(product)))
    return size, color, _text(row.get('gebinde')), _text(row.get('ve'))


def _quantity(row):
    evidence = row.get('quantity_evidence') if isinstance(row.get('quantity_evidence'), dict) else {}
    if evidence.get('basis') != 'invoice_line':
        return None
    value = _number(evidence.get('value'))
    unit = _text(evidence.get('unit'), 60)
    source = _source(row)
    # No numeric defaults and no provenance-free assertion of a purchase.
    if not value or not source.get('beleg_id') or not any(k in source for k in ('seite', 'position', 'zeile')):
        return None
    return {'menge': value, 'einheit': unit, 'quelle': source, 'pruefen': True,
            'basis': 'Historische Rechnungsposition; keine Verbrauchs- oder Bestellzusage.'}


def _invoice_key(source):
    return source.get('datei_sha256') or str(source.get('art')) + ':' + str(source.get('beleg_id'))


def _package(row):
    evidence = row.get('package_evidence') if isinstance(row.get('package_evidence'), dict) else {}
    # Keep only structured explicit evidence supplied by the import service.
    value = _number(evidence.get('value', evidence.get('content')))
    unit = _text(evidence.get('unit'), 40)
    container = _text(evidence.get('per_unit'), 40)
    if value and unit and container and evidence.get('basis') == 'explicit_description':
        return {'menge': value, 'einheit': unit, 'pro': container, 'quelle': _source(row), 'pruefen': True}
    # Legacy parser-generated packaging is not independent evidence of content.
    return None


def band_auskunft(text, context):
    """Return a short factual answer for a narrow band question, else None.

    ``context`` must be freshly loaded, scope-filtered material_context data.
    No writes, historical defaults, stock claims or order recommendations.
    Only HydroGreen is recognized here: other tapes can have diameters instead
    of widths. Any unsupported intent, incomplete result or conflict falls back
    to the normal assistant. This does not replace authorization at the caller.
    """
    if not isinstance(text, str) or len(text) > 350 or not isinstance(context, dict):
        return None
    question = re.sub(r'^nur auskunft(?:,?\s*keine bestellung)?\s*:\s*', '', _ascii(text))
    terms = tokens(question)
    fields = r'(?:breiten?|verpackungseinheiten|packmengen|packinhalte?)'
    sentence = (rf'welche {fields}(?: und {fields})? (?:sind|ist) '
                r'(?:(?:bei|von) (?:unserem|unseren|dem|den) |beim |unserem |unser )?'
                r'(?:gruen )?(?:klebeband|hydrogreen) (?:belegt|hinterlegt)')
    if ('?' in question.rstrip('?').strip() or not re.fullmatch(sentence, ' '.join(terms))):
        return None
    wants_width = bool({'breiten', 'breite'} & set(terms))
    wants_pack = bool({'verpackungseinheiten', 'packmengen', 'packinhalt', 'packinhalte'} & set(terms))
    if not (wants_width or wants_pack):
        return None
    coverage = context.get('abdeckung') or {}
    if (context.get('suchstatus') != 'treffer' or context.get('varianten_gekuerzt')
            or context.get('suchhinweise') or coverage.get('begrenzt') or coverage.get('positionen_begrenzt')):
        return None
    variants = context.get('varianten')
    if not isinstance(variants, list) or not 1 <= len(variants) <= 20:
        return None

    def has_source(source):
        return (isinstance(source, dict) and source.get('art') in ('einkauf', 'lexware')
                and bool(source.get('beleg_id'))
                and any(type(source.get(key)) is int and source[key] > 0 for key in ('seite', 'position', 'zeile')))

    groups = {}
    color_unconfirmed = False
    for item in variants:
        if not isinstance(item, dict) or 'hydrogreen' not in tokens(item.get('produkt_name', '')):
            return None
        source_rows = item.get('quellen') or []
        if not any(has_source(source) for source in source_rows):
            return None
        dimensions = _measurements(item.get('groesse', ''))
        name_dimensions = _measurements(item.get('produkt_name', ''))
        for dimension_unit in ('mm', 'm'):
            stored = {number for number, unit in dimensions if unit == dimension_unit}
            named = {number for number, unit in name_dimensions if unit == dimension_unit}
            if stored and named and stored != named:
                return None
        dimensions |= name_dimensions
        widths = {number for number, unit in dimensions if unit == 'mm'}
        if (len(widths) != 1 or len({number for number, unit in dimensions if unit == 'm'}) > 1
                or any(unit not in ('mm', 'm') for _, unit in dimensions)):
            return None
        width = _number(next(iter(widths)))
        if not width or Decimal(width) > 1000:
            return None
        supplier, sku = _normal(item.get('lieferant')), _normal(item.get('artikelnummer'))
        if not supplier or not sku:
            return None
        colors = _colors(item.get('farbe', ''))
        if colors - {'gruen'}:
            return None
        if 'gruen' in terms and not colors:
            match = item.get('farbabgleich') or {}
            if match.get('basis') != 'produktname_alias' or match.get('namenshinweis') != 'HydroGreen':
                return None
            color_unconfirmed = True
        # Presentation only: old and new descriptions of the same SKU may
        # supply compatible evidence. Their stored variants remain separate.
        # Only formatting and explicit /VE contents may differ between old
        # and new names. Meaningful suffixes such as Premium stay distinct.
        name = re.sub(r'\b\d+(?:[.,]\d+)?\s*(?:stueck|stk\.?|rollen?)\s*/\s*ve\b', '', _ascii(item.get('produkt_name')))
        name = re.sub(r'(?<=\d)\s*mrolle\b', ' m rolle', name)
        packaging = tokens(item.get('gebinde', ''))
        if packaging == ('1', 'stueck'):
            packaging = ()  # Known legacy default; not evidence of pack size.
        key = (supplier, sku, tuple(sorted(dimensions)), tokens(name), packaging, tokens(item.get('ve', '')))
        group = groups.setdefault(key, {'width': width, 'packs': set()})
        pack = item.get('packinhalt')
        if pack is not None:
            if not isinstance(pack, dict) or not has_source(pack.get('quelle')):
                return None
            amount = _number(pack.get('menge'))
            unit, per = _ascii(pack.get('einheit')), _ascii(pack.get('pro'))
            units = {'stueck': 'Stück', 'rolle': 'Rollen', 'rollen': 'Rollen'}
            containers = {'ve': 'Verkaufseinheit', 'pack': 'Packung', 'karton': 'Karton', 'gebinde': 'Gebinde'}
            if not amount or unit not in units or per not in containers:
                return None
            group['packs'].add((amount, units[unit], containers[per]))
            if len(group['packs']) > 1:
                return None
    rows = sorted(groups.values(), key=lambda group: Decimal(group['width']))
    if (not 1 <= len(rows) <= 4 or len({key[0] for key in groups}) != 1
            or len({row['width'] for row in rows}) != len(rows)):
        return None

    def join(parts):
        return ' und '.join(parts) if len(parts) < 3 else ', '.join(parts[:-1]) + ' und ' + parts[-1]

    packs = [next(iter(row['packs'])) if row['packs'] else None for row in rows]
    if wants_pack and all(packs) and len({pack[1:] for pack in packs}) == 1:
        details = join([f"{row['width']} mm mit {pack[0].replace('.', ',')}" for row, pack in zip(rows, packs)])
        details += f' {packs[0][1]} je {packs[0][2]}'
    elif wants_pack:
        details = join([f"{row['width']} mm" + (f" mit {pack[0].replace('.', ',')} {pack[1]} je {pack[2]}" if pack else '')
                        for row, pack in zip(rows, packs)])
        missing = [f"{row['width']} mm" for row, pack in zip(rows, packs) if not pack]
        if missing:
            details += '. Packinhalt unbekannt bei ' + join(missing)
    else:
        details = join([f"{row['width']} mm" for row in rows])
    qualification = 'Die Auslese und Farbzuordnung sind ungeprüft.' if color_unconfirmed else 'Die Auslese ist ungeprüft.'
    return f'Laut bisheriger Belegauslese: HydroGreen {details}. {qualification}'


def build_variants(records, query='', limit=30):
    """Group exact operational variants; every amount remains a review proposal."""
    groups = OrderedDict()
    query_colors = _colors(query)
    seen_sources, seen_quantities = {}, {}
    for row in rank_records(records, query):
        size, color, packaging, unit = _variant_fields(row)
        sku = _text(row.get('artikelnummer'))
        keys = [_normal(row.get('lieferant')), _normal(sku), _normal(row.get('produkt_name')),
                _normal(size), _normal(color), _normal(packaging), _normal(unit)]
        package = _package(row)
        keys.append([package[k] for k in ('menge', 'einheit', 'pro')] if package else None)
        key = json.dumps(keys, ensure_ascii=False)
        if key not in groups:
            seen_sources[key],seen_quantities[key]=set(),set()
            groups[key] = {'variante_id': hashlib.sha256(key.encode()).hexdigest()[:20],
                           'produkt_name': _text(row.get('produkt_name')), 'lieferant': _text(row.get('lieferant')),
                           'artikelnummer': sku, 'groesse': size, 'farbe': color, 'gebinde': packaging, 've': unit,
                           'quellen': [], 'mengenhistorie': [], 'packinhalt': None,
                           'uebliche_menge': None, 'letzte_belegte_menge': None,
                           'historischer_preishinweis': _text(row.get('historischer_preishinweis')),
                           'preis_geprueft': False, 'bestellbar': False, 'pruefen': True}
        group = groups[key]
        source = _source(row)
        source_key=json.dumps(source,sort_keys=True)
        if source and source_key not in seen_sources[key]:
            seen_sources[key].add(source_key)
            group['quellen'].append(source)
        quantity = _quantity(row)
        if quantity:
            identity = (_invoice_key(source), source.get('position'), source.get('zeile'), quantity['menge'], quantity['einheit'])
            if identity not in seen_quantities[key]:
                seen_quantities[key].add(identity)
                group['mengenhistorie'].append(quantity)
        if package and group['packinhalt'] is None:
            group['packinhalt'] = package
    results = []
    for group in groups.values():
        history = group['mengenhistorie']
        history.sort(key=lambda item: str(item['quelle'].get('datum') or ''), reverse=True)
        if history and history[0]['quelle'].get('datum'):
            group['letzte_belegte_menge'] = history[0]
        # Count each invoice at most once; conflicting quantities on one invoice
        # do not count as repeated orders, nor do duplicate PDF page imports.
        per_invoice = {}
        for item in history:
            if item['einheit']:
                per_invoice.setdefault(_invoice_key(item['quelle']), set()).add((item['menge'], _normal(item['einheit'])))
        observations = [next(iter(values)) for values in per_invoice.values() if len(values) == 1]
        counts = Counter(observations)
        if counts:
            (value, unit), count = counts.most_common(1)[0]
            if count >= 2 and count > len(observations) / 2:
                group['uebliche_menge'] = {'menge': value, 'einheit': unit, 'belege': count,
                                          'basis': 'Häufigste belegte Rechnungsmenge dieser Variante; nur Vorschlag.', 'pruefen': True}
        group['fehlende_angaben'] = [name for name, present in
                                    (('Artikelnummer', group['artikelnummer']), ('Einheit', group['ve']),
                                     ('Packinhalt', group['packinhalt']), ('belegte Menge', history)) if not present]
        group['farbabgleich'] = None
        if query_colors and not group['farbe']:
            group['fehlende_angaben'].append('Farbe separat bestätigen')
            name_match = query_colors == {'gruen'} and 'hydrogreen' in tokens(group['produkt_name'])
            group['farbabgleich'] = {
                'angefragte_farben': sorted(query_colors), 'bestaetigt': False,
                'basis': 'produktname_alias' if name_match else 'unbekannt',
                'namenshinweis': 'HydroGreen' if name_match else '',
                'hinweis': 'Nur die Farbzuordnung ist unbestätigt. Vorhandene Breiten und Packmengen dieser gefundenen Variante bleiben belegte Angaben und können genannt werden.'}
        if any(not item['einheit'] for item in history):
            group['fehlende_angaben'].append('Einheit der Rechnungsmenge')
        group['belege_anzahl'] = len({_invoice_key(source) for source in group['quellen'] if source.get('beleg_id')})
        group['historie_gekuerzt'] = len(group['mengenhistorie']) > 50 or len(group['quellen']) > 50
        group['mengenhistorie'] = group['mengenhistorie'][:50]
        group['quellen'] = group['quellen'][:50]
        group['hinweis'] = 'Varianten und Rechnungsangaben prüfen. Keine Freigabe oder Bestellung durch Vorlesen.'
        results.append(group)
    if not query.strip():
        results.sort(key=lambda item: item['belege_anzahl'], reverse=True)
    return results[:max(1, min(int(limit), 101))]
