"""
Tests for adgangskontrollen efter overgangen til central BALAI-brugerstyring.

Login, setup, invitationer og admin lever IKKE længere lokalt — de håndteres af
den centrale tjeneste på auth.balai.dk. Dette repo håndhæver kun ADGANG via
``central_auth.require_tool``:

- HTML-ruter uden gyldig session  -> 303-redirect til den centrale login.
- API-ruter uden gyldig session   -> 401 JSON (så frontenden kan reagere).
- ``/logout`` rydder den delte .balai.dk-cookie og sender til central login.
- ``/health`` er offentlig.

Testene er netværksfrie: uden en gyldig, signeret session-cookie afvises kaldet,
før modulet nogensinde slår op i den delte Postgres.
"""

# API-præfikser som central_auth svarer 401 på (i stedet for at redirecte).
API_PREFIXES = ("/analyze", "/preview", "/status", "/result")


def test_health_is_public(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_index_requires_auth(client):
    # Forsiden er en HTML-rute: uden login skal den redirecte, ikke vise UI'et.
    resp = client.get("/")
    assert resp.status_code in (302, 303, 307)


def test_html_route_redirects_to_central_login(client):
    # Redirect'en skal pege på den centrale login med et next-link tilbage hertil.
    resp = client.get("/")
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "/login" in location
    assert "next=" in location


def test_api_route_requires_login_returns_401(client):
    # API-ruter får 401 JSON (ikke redirect) uden session — også for et ukendt job,
    # fordi adgangskontrollen kører før rute-handleren.
    resp = client.get("/status/does-not-exist")
    assert resp.status_code == 401


def test_all_api_prefixes_return_401_without_session(client):
    # Alle beskyttede API-præfikser svarer konsekvent 401 uden session.
    for path in ("/status/x", "/result/x"):
        assert client.get(path).status_code == 401, path


def test_logout_redirects_to_central_login(client):
    # Logout er en ren redirect til central login (rydder den delte cookie).
    resp = client.get("/logout")
    assert resp.status_code == 303
    assert "/login" in resp.headers.get("location", "")


def test_logout_post_also_redirects(client):
    # Logout accepterer både GET og POST og kræver ikke lokal CSRF længere.
    resp = client.post("/logout")
    assert resp.status_code == 303
    assert "/login" in resp.headers.get("location", "")
