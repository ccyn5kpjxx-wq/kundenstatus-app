"""Continuous Render worker for MOS contract and deposit-reminder outboxes.

The per-cycle starter validates the same live secrets and starts an isolated
Flask CLI process. A failed cycle terminates this worker with a non-zero code
so Render service-failure notification can alert an operator. Do not run this
on a web instance or in a test environment.

The shared ``mos-contract-delivery`` CLI also queues and sends one scoped-link
deposit reminder during the twelve hours before an advance booking's pickup.
Missed or uncertain reminders surface as failures; the worker never authorizes
a card or contacts Stripe. Existing explicit live-mail preflight remains required.
"""

import os
import sys
import time

from run_mos_render_contract_delivery import DeliveryPreflightError, preflight, run


POLL_SECONDS = 10


def work(environment=None, *, one_cycle=run, sleeper=time.sleep, max_cycles=None):
    runtime = dict(os.environ if environment is None else environment)
    try:
        preflight(runtime)
    except DeliveryPreflightError as exc:
        print(f'MOS-Vertragsworker nicht gestartet: {exc}', file=sys.stderr, flush=True)
        return 2
    cycles = 0
    while True:
        code = one_cycle(runtime)
        cycles += 1
        if code:
            print(f'MOS-Vertragsworker gestoppt: Versandprüfung fehlgeschlagen (Code {code}).',
                  file=sys.stderr, flush=True)
            return code
        if max_cycles is not None and cycles >= max_cycles:
            return 0  # deterministic unit-test seam; production never sets it
        sleeper(POLL_SECONDS)


if __name__ == '__main__':
    raise SystemExit(work())
