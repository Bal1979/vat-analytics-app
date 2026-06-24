"""
Central BALAI-brugerstyring for VAT Analytics (FastAPI).

VAT Analytics brugte tidligere sin egen SQLite-baserede login. Den delegerer nu
til den centrale tjeneste på auth.balai.dk:

- Login/admin/invitationer sker KUN centralt.
- Dette modul læser den delte Flask-session-cookie (sat på .balai.dk af den
  centrale app), validerer den mod den fælles Postgres, og håndhæver adgang til
  værktøjets slug ("vat").
- Ingen lokal brugerdatabase mere.

Cookie-format: den centrale app er Flask og signerer sin session-cookie med
itsdangerous (salt "cookie-session", HMAC-SHA1). Vores payload (uid, tv, iat) er
ren JSON, så vi kan afkode den med en almindelig JSON-serializer uden at
importere Flask. Samme SECRET_KEY på begge sider er det der binder dem sammen.

Værktøjets EGEN Starlette-session (CSRF) bruger en ANDEN cookie ("vat_local"),
så de to ikke kolliderer på .balai.dk.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TOOL_SLUG = "vat"
SESSION_MAX_HOURS = 12
CENTRAL_COOKIE_NAME = "session"  # den centrale Flask-cookie på .balai.dk

# API-ruter får 401 JSON ved manglende login; HTML-ruter redirectes til central login.
_JSON_PREFIXES = ("/analyze", "/preview", "/status", "/result")


def _secret() -> str:
    return os.environ.get("SECRET_KEY", "")


def _auth_base() -> str:
    return os.environ.get("AUTH_BASE_URL", "https://auth.balai.dk").rstrip("/")


# ---------------------------------------------------------------------------
# Afkodning af den centrale Flask-session-cookie
# ---------------------------------------------------------------------------

class _PlainJSON:
    """Payload er ren JSON (uid/tv/iat/_csrf) — ingen Flask-tags nødvendige."""

    def dumps(self, obj):
        return json.dumps(obj, separators=(",", ":"))

    def loads(self, s):
        return json.loads(s)


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(
        _secret(),
        salt="cookie-session",
        serializer=_PlainJSON(),
        signer_kwargs={"key_derivation": "hmac", "digest_method": hashlib.sha1},
    )


def _decode_session(request: Request):
    raw = request.cookies.get(CENTRAL_COOKIE_NAME)
    if not raw or not _secret():
        return None
    try:
        return _serializer().loads(raw, max_age=SESSION_MAX_HOURS * 3600)
    except (BadSignature, SignatureExpired):
        return None


# ---------------------------------------------------------------------------
# Postgres (delt database)
# ---------------------------------------------------------------------------

_engine = None


def _db():
    global _engine
    if _engine is None:
        url = os.environ.get("DATABASE_URL", "").strip()
        if url.startswith("postgres://"):
            url = "postgresql+psycopg://" + url[len("postgres://"):]
        elif url.startswith("postgresql://"):
            url = "postgresql+psycopg://" + url[len("postgresql://"):]
        _engine = create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=3)
    return _engine


def _load_user_and_access(uid):
    """Returnér (user_dict, has_access) eller None hvis brugeren ikke findes."""
    with _db().connect() as c:
        row = c.execute(
            text("SELECT id, email, status, all_access, is_admin, token_version "
                 "FROM users WHERE id = :id"),
            {"id": uid},
        ).mappings().first()
        if row is None:
            return None
        if row["all_access"]:
            has = True
        else:
            has = c.execute(
                text("SELECT 1 FROM entitlements "
                     "WHERE user_id = :id AND tool_slug = :slug"),
                {"id": uid, "slug": TOOL_SLUG},
            ).first() is not None
        return dict(row), has


def current_user(request: Request):
    """Den aktuelle, validerede bruger — eller None."""
    data = _decode_session(request)
    if not data:
        return None
    uid = data.get("uid")
    if uid is None:
        return None
    loaded = _load_user_and_access(uid)
    if loaded is None:
        return None
    user, has_access = loaded
    if user["status"] != "active":
        return None
    # token_version skal matche (så blokering/revoke virker øjeblikkeligt).
    if data.get("tv") != user["token_version"]:
        return None
    # iat-grænsen (12t) håndhæves allerede via max_age i serializer.
    user["username"] = user["email"]   # alias: main.py logger user["username"]
    user["_has_access"] = has_access
    return user


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------

def require_tool(request: Request):
    """
    Kræv login + adgang til VAT Analytics. API-ruter får 401; HTML-ruter
    redirectes til den centrale login med et next-link tilbage hertil.
    """
    user = current_user(request)
    if user is None:
        if request.url.path.startswith(_JSON_PREFIXES):
            raise HTTPException(401, "Login krævet. Genindlæs siden og log ind.")
        nxt = quote(str(request.url), safe="")
        raise HTTPException(303, headers={"Location": f"{_auth_base()}/login?next={nxt}"})
    if not user["_has_access"]:
        if request.url.path.startswith(_JSON_PREFIXES):
            raise HTTPException(403, "Ingen adgang til VAT Analytics.")
        raise HTTPException(403, "Du har ikke adgang til dette værktøj.")
    return user


# ---------------------------------------------------------------------------
# Logout (rydder den delte cookie -> logger ud overalt)
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/logout")
@router.post("/logout")
def logout():
    resp = RedirectResponse(f"{_auth_base()}/login", status_code=303)
    # Ryd den delte .balai.dk-session-cookie, så brugeren logges ud på tværs.
    resp.delete_cookie("session", domain=".balai.dk", path="/")
    return resp
