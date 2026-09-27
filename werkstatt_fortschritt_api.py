"""Narrow bearer-only API for internal workshop progress.

After app.init_db(), call register_progress_api(portal). In the application's
protect_csrf(), before accessing request.form, add the exact-endpoint guard:

    if progress_csrf_exempt(portal):
        return None

The guard only recognizes a currently valid ASSISTANT_API_GRANT bearer on this
POST endpoint. It does not recognize admin cookies or the portal's other API
tokens. The handler independently requires the new auftraege:fortschritt scope;
existing read grants are never upgraded. Grant creation/rotation is not exposed.
"""

import hashlib
import hmac
import json
import re

from flask import Blueprint, jsonify, request
from werkzeug.exceptions import HTTPException, RequestEntityTooLarge

from werkstatt_fortschritt import ProgressError, WorkshopProgress


MAX_BODY_BYTES = 4096
ENDPOINT = "werkstatt_fortschritt_api.progress"
READ_SCOPE = "auftraege:lesen"
WRITE_SCOPE = "auftraege:fortschritt"
POST_FIELDS = frozenset({"action", "expected_status", "expected_changed_at", "request_id"})


def _grant(portal):
    raw = request.headers.get("Authorization", "")
    if not raw.startswith("Bearer "):
        return None
    token = raw[7:]
    if not token or len(token) > 512 or any(character.isspace() for character in token):
        return None
    try:
        grant = json.loads(portal.get_app_setting("ASSISTANT_API_GRANT", "") or "{}")
    except (ValueError, TypeError):
        return None
    if not isinstance(grant, dict):
        return None
    stored = grant.get("hash")
    if not isinstance(stored, str) or not re.fullmatch(r"[0-9a-f]{64}", stored):
        return None
    scopes = grant.get("scopes", [])
    if not isinstance(scopes, list) or any(not isinstance(scope, str) for scope in scopes):
        return None
    digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
    if not hmac.compare_digest(digest, stored):
        return None
    return {"hash": stored, "scopes": scopes}


def _limit_body():
    # For chunked input allow one probe byte: Flask's LimitedStream can stop
    # exactly at its limit without raising. The handler rejects that extra byte.
    request.max_content_length = MAX_BODY_BYTES + (request.content_length is None)
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        raise RequestEntityTooLarge()


def progress_csrf_exempt(portal):
    """Exact-route CSRF exception only when the explicit bearer is valid.

    Read-only grants can reach the handler's explicit 403 response but never
    gain write access. This helper also places the body limit before the global
    CSRF handler might parse request.form. No other endpoint is affected.
    """
    if request.method != "POST" or request.endpoint != ENDPOINT:
        return False
    _limit_body()
    return _grant(portal) is not None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("Non-finite JSON number")


def register_progress_api(portal):
    """Initialize the progress schema and register the one GET/POST endpoint."""
    service = WorkshopProgress(portal)
    bp = Blueprint("werkstatt_fortschritt_api", __name__, url_prefix="/api/werkstatt/v1")

    @bp.after_request
    def no_store(response):
        response.headers["Cache-Control"] = "no-store"
        return response

    @bp.errorhandler(ProgressError)
    def progress_error(error):
        return jsonify(error=str(error), code=error.code), error.status_code

    @bp.errorhandler(HTTPException)
    def http_error(error):
        messages = {400: "Ungültige JSON-Anfrage.", 413: "Die Anfrage darf höchstens 4 KiB enthalten.",
                    415: "Content-Type application/json ist erforderlich."}
        return jsonify(error=messages.get(error.code, "API-Anfrage nicht möglich."),
                       code=f"http_{error.code}"), error.code

    @bp.route("/auftraege/<int:order_id>/fortschritt", methods=["GET", "POST"], endpoint="progress")
    def progress(order_id):
        grant = _grant(portal)
        if grant is None:
            raise ProgressError("unauthorized", "Ein gültiger API-Bearer-Zugang ist erforderlich.", 401)
        reading = request.method in {"GET", "HEAD"}
        scope = READ_SCOPE if reading else WRITE_SCOPE
        if scope not in grant["scopes"]:
            raise ProgressError("scope_missing", "Die API-Berechtigung für diese Aktion fehlt.", 403)
        if reading:
            return jsonify(service.read(order_id))
        _limit_body()
        if request.mimetype != "application/json":
            raise ProgressError("json_required", "Content-Type application/json ist erforderlich.", 415)
        raw = request.get_data(cache=False)
        if len(raw) > MAX_BODY_BYTES:
            raise RequestEntityTooLarge()
        try:
            data = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        except (ValueError, UnicodeError):
            raise ProgressError("invalid_json", "Die JSON-Anfrage ist ungültig oder enthält doppelte Felder.", 400)
        if not isinstance(data, dict) or set(data) != POST_FIELDS:
            raise ProgressError("invalid_fields", "Erforderlich sind nur action, expected_status, expected_changed_at und request_id.", 400)
        # A separate digest is a non-secret stable identity for this grant. The
        # actual bearer and its authentication hash never enter the audit actor.
        identity = hashlib.sha256(("avatar-actor-v1:" + grant["hash"]).encode("ascii")).hexdigest()[:20]
        result = service.update(order_id, data["action"], data["expected_status"],
                                data["expected_changed_at"], "avatar:" + identity, data["request_id"])
        return jsonify(result)

    portal.app.register_blueprint(bp)
    return service
