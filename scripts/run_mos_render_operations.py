"""Run existing MOS payment reconciliation and an optional termination alarm.

Uses the already configured reconciliation cron; creates no service and sends no
termination mail. Reconciliation keeps its bounded Flask subprocess; the optional
alarm is a direct read-only database count with no app/SMTP import. A failed
check never skips the other enabled check. No nested process layer or mailworker.
"""

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))  # Direct scripts/ entry must resolve mos_booking too.

if __package__:
    from . import run_mos_render_reconcile as reconciliation
    from . import run_mos_render_termination_monitor as termination_monitor
else:
    import run_mos_render_reconcile as reconciliation
    import run_mos_render_termination_monitor as termination_monitor


def run(environment=None, *, reconcile_runner=None, termination_runner=None):
    runtime = dict(os.environ if environment is None else environment)
    reconciliation_runtime = dict(runtime)
    reconciliation_runtime['MOS_TERMINATION_ENABLED'] = '0'  # Only the web process mounts public intake.
    jobs = [('Zahlungsabgleich', reconcile_runner or reconciliation.run, reconciliation_runtime)]
    if runtime.get('MOS_TERMINATION_MONITOR_ENABLED') == '1':
        jobs.append(('Kündigungsalarm', termination_runner or termination_monitor.check_read_only, runtime))
    failures = []
    for label, starter, job_runtime in jobs:
        try:
            code = starter(dict(job_runtime))
        except subprocess.TimeoutExpired:
            code = 124
            print(f'MOS {label}: maximale Laufzeit überschritten.', file=sys.stderr)
        except OSError:
            code = 127
            print(f'MOS {label}: Starter konnte nicht ausgeführt werden.', file=sys.stderr)
        except Exception:
            code = 1
            print(f'MOS {label}: unerwarteter Prüffehler.', file=sys.stderr)
        if code:
            failures.append(code)
            print(f'MOS {label}: Prüfung fehlgeschlagen (Code {code}).', file=sys.stderr)
    return failures[0] if failures else 0


if __name__ == '__main__':
    raise SystemExit(run())
