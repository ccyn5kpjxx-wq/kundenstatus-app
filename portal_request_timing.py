"""Request-local diagnostics; never retain SQL, parameters or customer data."""
from functools import wraps
from time import perf_counter
from flask import g, has_request_context


def start_request_timing():
    g.portal_request_timing = {'start':perf_counter(), 'db_ms':0.0, 'connect_ms':0.0, 'sql':0}


def timed_database_call(kind):
    def decorate(function):
        @wraps(function)
        def call(*args, **kwargs):
            timing = getattr(g, 'portal_request_timing', None) if has_request_context() else None
            if timing is None:
                return function(*args, **kwargs)
            started = perf_counter()
            try:
                return function(*args, **kwargs)
            finally:
                timing[kind + '_ms'] += (perf_counter() - started) * 1000
                if kind == 'db':
                    timing['sql'] += 1
        return call
    return decorate


def server_timing_header():
    timing = getattr(g, 'portal_request_timing', None)
    if timing is None:
        return None
    elapsed = (perf_counter() - timing['start']) * 1000
    return (f'app;dur={elapsed:.2f}, db;dur={timing["db_ms"]:.2f}, '
            f'connect;dur={timing["connect_ms"]:.2f}, sql;desc="{timing["sql"]} statements"')
