"""
Central auth — cookie-håndtering.

Regression for bug'en hvor værktøjets egen Starlette-session brugte cookie-navnet
"session" (default) og dermed kolliderede med den centrale BALAI-cookie: en stray
host-only "session"-cookie kunne skygge for den gyldige, så API-kald fejlede med
401 selvom siden loadede. Fix: (A) Starlette-cookien hedder nu "vat_local"
(main.py), og (B) central_auth prøver ALLE "session"-cookies og bruger den gyldige.
"""

import types

import central_auth as ca


def _req(cookie_header: str):
    # Minimal request-stub: central_auth læser kun headers.get("cookie") + cookies.get.
    return types.SimpleNamespace(headers={"cookie": cookie_header}, cookies={})


def _sign(payload: dict) -> str:
    return ca._serializer().dumps(payload)


def test_all_session_cookies_extracts_every_value():
    req = _req("session=aaa; foo=bar; session=bbb; vat_local=xyz")
    assert ca._all_session_cookies(req) == ["aaa", "bbb"]


def test_decode_picks_valid_among_multiple(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-fixed")
    payload = {"uid": 42, "tv": 1, "iat": 0}
    valid = _sign(payload)
    # Stray/ugyldig host-only cookie står FØRST — må ikke skygge for den gyldige.
    req = _req(f"session=ugyldig-gammel-cookie; session={valid}")
    assert ca._decode_session(req) == payload


def test_decode_returns_none_when_all_invalid(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-fixed")
    req = _req("session=helt-forkert; session=ogsaa-forkert")
    assert ca._decode_session(req) is None


def test_decode_ignores_vat_local_cookie(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-fixed")
    valid = _sign({"uid": 7, "tv": 0, "iat": 0})
    # Kun værktøjets egen cookie + en gyldig central "session".
    req = _req(f"vat_local=noget; session={valid}")
    assert ca._decode_session(req) == {"uid": 7, "tv": 0, "iat": 0}
