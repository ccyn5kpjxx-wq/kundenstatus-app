"""Public model names and inventory identity shared by booking and handoff."""
import re
import unicodedata


LISTINGS = {"kona": "Hyundai KONA N Line X", "i10": "Hyundai i10", "doblo-xl": "Fiat Doblò XL"}
REQUIRED_MODELS = {"kona", "i10"}


def valid_booking_fleet(fleet):
    """Keep mandatory models and accept only distinct, explicitly configured inventory IDs."""
    if not isinstance(fleet, dict) or not REQUIRED_MODELS <= fleet.keys() <= LISTINGS.keys():
        return False
    ids = [vehicle.get("id") if isinstance(vehicle, dict) else None for vehicle in fleet.values()]
    return all(type(vid) is int and vid > 0 for vid in ids) and len(set(ids)) == len(ids)


def is_doblo_xl(name):
    if not isinstance(name, str):
        return False
    normalized = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().casefold()
    return bool(re.search(r"\bfiat doblo\b.*\b(?:xl|maxi)\b", normalized))
