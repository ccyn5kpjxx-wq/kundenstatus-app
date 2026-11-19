"""Versioned sales terms for one checkout covering multiple released articles.

Apply this operational amendment only to the exact private pack it was written
against. Past acceptances retain their frozen documents in the order store.
"""
from copy import deepcopy
from hashlib import sha256

from shop_consumer import _freeze_pack
from shop_complete_wheels import NOTICE as MOUNTED_NOTICE


BASE_HASH = '4c8b3750c1120b1f01f400c523aa28f606fb8c8c225d128f65048028de65697f'
PACK_VERSION = 'shop-verbraucher-2026-11-19-gemeinsamer-warenkorb-v1'
PACK_HASH = '3ecaea67e832152ee17dba70be43a91ea4cde86c885dc656e5a594c4da19d596'
COMBINED_RULE = (
    'Gemeinsam als bestellbar angezeigte Artikel können Sie in einer Bestellung kaufen. '
    'Vor Abschluss sehen Sie den Gesamtpreis einschließlich Versand. Wir reservieren den '
    'Betrag einmal und ziehen ihn erst ein, wenn alle Positionen zum vereinbarten Preis '
    'bestätigt sind.'
)
INQUIRY_RULE = (
    'Als Anfrage gekennzeichnete Positionen können Sie gesondert anfragen. Sie sind im '
    'Zahlbetrag der gemeinsamen Bestellung nicht enthalten. Vor dem Abschluss zeigt der '
    'Shop die Positionen und Lieferziele der Bestellung und ihre Versandkosten an.'
)


def _amend(base):
    pack = deepcopy(base)
    pack['version'] = PACK_VERSION
    texts = pack['texts']
    old_inquiry = (
        'Andere Zusammenstellungen mit mehreren Lieferantenpositionen oder mehreren Lieferzielen '
        'werden zunächst als unverbindliche Anfrage gespeichert, soweit der Shop sie nicht '
        'ausdrücklich als Komplettrad-Zusammenstellung mit Montage für den oben beschriebenen '
        'Reservierungs- und Prüfweg freigibt.'
    )
    for key in ('agb', 'lieferung'):
        texts[key] = texts[key].replace('Stand: 18. November 2026', 'Stand: 19. November 2026')
        texts[key] = texts[key].replace(old_inquiry, INQUIRY_RULE)
    old_authorization = (
        'Für den ausdrücklich angebotenen Einzelartikel mit gewählter Menge und einem Lieferziel '
        'wird zunächst der angezeigte Kartenbetrag autorisiert.'
    )
    texts['agb'] = texts['agb'].replace(old_authorization, COMBINED_RULE)
    texts['lieferung'] = texts['lieferung'].replace('Mehrere Artikel oder Ziele\n',
        'Mehrere Artikel oder Ziele\n' + COMBINED_RULE + ' ')
    notice = texts['bestellhinweis'].replace(MOUNTED_NOTICE,
        'Für Kompletträder mit ausgewählter Montage gilt zusätzlich: ' + MOUNTED_NOTICE)
    texts['bestellhinweis'] = COMBINED_RULE + ' ' + notice
    amended_attachments = {'01-AGB.txt': 'agb', '04-Lieferung.txt': 'lieferung'}
    for item in pack['attachments']:
        key = amended_attachments.get(item['filename'])
        if key:
            item['content'] = texts[key].encode('utf-8')
    return pack


def checkout_terms(pack, profile_hash):
    """Return (pack, combined checkout approved) after verifying both releases."""
    original = _freeze_pack(pack)
    if not original:
        return pack, False
    original_hash = sha256(original.encode('utf-8')).hexdigest()
    if original_hash != profile_hash:
        return pack, False
    if original_hash == PACK_HASH:
        return pack, True
    if original_hash != BASE_HASH:
        return pack, False
    amended = _amend(pack)
    frozen = _freeze_pack(amended)
    if frozen is None or sha256(frozen.encode('utf-8')).hexdigest() != PACK_HASH:
        raise ValueError('combined_cart_terms_mismatch')
    return amended, True
