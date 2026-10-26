"""Explicitly enabled, bounded CHECK24 mailbox sync for a dedicated process."""

from __future__ import annotations

import argparse
from contextlib import redirect_stderr, redirect_stdout
import importlib
import os
from pathlib import Path
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
MIN_INTERVAL_SECONDS = 60
DEFAULT_INTERVAL_SECONDS = 300
MAX_BATCH_MESSAGES = 50

# These overrides exist only in this CLI process. Importing the web application
# must not also start its unrelated workers, backups or accounting sync.
CLI_ENVIRONMENT = {
    "PUBLIC_SITE_ONLY": "1",
    "CHECK24_WORKER_ENABLED": "0",
    "AUTO_BACKUP_ON_STARTUP": "0",
    "AUTO_BACKUP_ENABLED": "0",
    "AUTO_CHANGE_BACKUP_ENABLED": "0",
    "GOOGLE_ADS_AUTO_SYNC_ENABLED": "0",
    "LEXWARE_API_KEY": "",
    "ASSISTANT_ORDER_WORKER_ENABLED": "0",
    "ASSISTANT_ORDER_SEND_ENABLED": "0",
    "PURCHASE_MONITOR_WORKER_ENABLED": "0",
    "MATERIAL_WHATSAPP_WORKER_ENABLED": "0",
    "MATERIAL_WHATSAPP_ENABLED": "0",
    "MAILBOX_SEND_ENABLED": "0",
    "MAILBOX_WRITE_ENABLED": "0",
    "MAILBOX_MOVE_ENABLED": "0",
    "MAILBOX_FLAGS_ENABLED": "0",
}


def _interval(value):
    try:
        seconds = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("Intervall muss eine ganze Zahl sein.") from exc
    if seconds < MIN_INTERVAL_SECONDS:
        raise argparse.ArgumentTypeError("Intervall muss mindestens 60 Sekunden betragen.")
    return seconds


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--run", action="store_true", help="Abgleich ausführen; verlangt zusätzlich CHECK24_SYNC_ENABLED=1.")
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Genau einen begrenzten Durchlauf ausführen (Standard).")
    mode.add_argument("--loop", action="store_true", help="Als eigener Worker wiederholt abgleichen; kein Webserver-Thread.")
    result.add_argument("--interval", type=_interval, default=DEFAULT_INTERVAL_SECONDS,
                        metavar="SEKUNDEN", help="Pause zwischen Worker-Durchläufen, mindestens 60 (Standard: 300).")
    return result


def load_service():
    """Import the real application only after both explicit execution gates."""
    os.environ.update(CLI_ENVIRONMENT)
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    # The CLI has a counts-only output contract; startup messages are not a
    # useful report and may originate from independently configured modules.
    with open(os.devnull, "w", encoding="utf-8") as sink:
        with redirect_stdout(sink), redirect_stderr(sink):
            portal = importlib.import_module("app")
    return portal.check24_bookings


def main(argv=None, *, environment=None, service_loader=load_service, sleeper=time.sleep):
    args = parser().parse_args(argv)
    if not args.run:
        print("CHECK24-Abgleich inaktiv: kein Appimport, kein Postfach- oder Datenbankzugriff. "
              "Start nur mit --run und CHECK24_SYNC_ENABLED=1.")
        return 0
    runtime = os.environ if environment is None else environment
    if runtime.get("CHECK24_SYNC_ENABLED") != "1":
        print("CHECK24-Abgleich nicht gestartet: CHECK24_SYNC_ENABLED=1 fehlt.", file=sys.stderr)
        return 2
    try:
        service = service_loader()
        while True:
            count = service.sync(auto_apply=True)
            if type(count) is not int or not 0 <= count <= MAX_BATCH_MESSAGES:
                raise ValueError("Unexpected sync result")
            # This is the number of new mailbox messages examined, not the
            # number of orders created. Some messages require manual review.
            print(f"CHECK24-Abgleich erfolgreich: {count} neue Nachricht(en) geprüft.", flush=True)
            if not args.loop:
                return 0
            sleeper(args.interval)
    except KeyboardInterrupt:
        print("CHECK24-Abgleich beendet.", file=sys.stderr)
        return 130
    except Exception:
        # Mail and database exceptions can include credentials or customer
        # details. Do not emit exception text or traceback in worker output.
        print("CHECK24-Abgleich fehlgeschlagen. Verbindung und Prüfbereich kontrollieren; "
              "keine automatische Wiederholung in diesem Prozess.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
