"""
Tests for den porterede session-auth: adgangskontrol, setup-flow, CSRF og login.
Ingen netværksafhængighed — kører mod FastAPI TestClient med en frisk db pr. test.
"""

import re


def _csrf_from(html: str) -> str:
    m = re.search(r'name="_csrf" value="([^"]+)"', html)
    assert m, "CSRF-token ikke fundet på siden"
    return m.group(1)


def test_index_requires_auth(client):
    # Uden login skal forsiden redirecte (ikke returnere analyse-UI'et).
    resp = client.get("/")
    assert resp.status_code in (302, 303, 307)


def test_health_is_public(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_setup_offered_when_no_users(client):
    # Med 0 brugere sendes man til /setup, og siden vises.
    resp = client.get("/login")
    assert resp.status_code in (302, 303, 307)
    assert "/setup" in resp.headers.get("location", "")
    page = client.get("/setup")
    assert page.status_code == 200
    assert "administrator" in page.text.lower()


def test_logout_requires_csrf(client):
    # POST uden CSRF-token skal afvises (400) — beviser CSRF-beskyttelsen.
    resp = client.post("/logout")
    assert resp.status_code == 400


def test_setup_creates_admin_and_grants_access(client):
    page = client.get("/setup")
    token = _csrf_from(page.text)
    resp = client.post("/setup", data={
        "_csrf": token,
        "username": "admin",
        "password": "et-langt-password",
        "password2": "et-langt-password",
    })
    assert resp.status_code in (302, 303)
    # Efter setup er man logget ind → forsiden svarer 200.
    home = client.get("/")
    assert home.status_code == 200


def test_setup_then_logout_then_login(client):
    # Opret admin
    token = _csrf_from(client.get("/setup").text)
    client.post("/setup", data={
        "_csrf": token, "username": "admin",
        "password": "et-langt-password", "password2": "et-langt-password",
    })
    # Log ud (med CSRF fra en autentificeret side)
    logout_token = _csrf_from(client.get("/admin/users").text)
    out = client.post("/logout", data={"_csrf": logout_token})
    assert out.status_code in (302, 303)
    # Forsiden kræver login igen
    assert client.get("/").status_code in (302, 303, 307)
    # Log ind igen
    login_token = _csrf_from(client.get("/login").text)
    login = client.post("/login", data={
        "_csrf": login_token, "username": "admin", "password": "et-langt-password",
    })
    assert login.status_code in (302, 303)
    assert client.get("/").status_code == 200


def test_login_wrong_password_fails(client):
    token = _csrf_from(client.get("/setup").text)
    client.post("/setup", data={
        "_csrf": token, "username": "admin",
        "password": "et-langt-password", "password2": "et-langt-password",
    })
    # Log ud
    logout_token = _csrf_from(client.get("/admin/users").text)
    client.post("/logout", data={"_csrf": logout_token})
    # Forkert password → ingen adgang
    login_token = _csrf_from(client.get("/login").text)
    resp = client.post("/login", data={
        "_csrf": login_token, "username": "admin", "password": "forkert-password",
    })
    assert resp.status_code == 200  # login-siden vises igen med fejl
    assert "forkert" in resp.text.lower()
    assert client.get("/").status_code in (302, 303, 307)


def test_api_route_requires_login_returns_401(client):
    # API-ruter får 401 JSON (ikke redirect) når der findes brugere men ingen session.
    token = _csrf_from(client.get("/setup").text)
    client.post("/setup", data={
        "_csrf": token, "username": "admin",
        "password": "et-langt-password", "password2": "et-langt-password",
    })
    logout_token = _csrf_from(client.get("/admin/users").text)
    client.post("/logout", data={"_csrf": logout_token})
    resp = client.get("/status/does-not-exist")
    assert resp.status_code == 401
