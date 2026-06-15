"""
Verificerer at HTTP-sikkerhedsheaders sættes på alle svar, og at CSP er stram
(ingen CDN'er / 'unsafe-*').
"""


def test_security_headers_present(client):
    h = client.get("/login").headers
    assert "content-security-policy" in h
    assert h.get("x-content-type-options") == "nosniff"
    assert h.get("x-frame-options") == "DENY"
    assert h.get("referrer-policy") == "strict-origin-when-cross-origin"
    assert "strict-transport-security" in h
    assert "permissions-policy" in h


def test_csp_is_strict(client):
    csp = client.get("/login").headers["content-security-policy"]
    assert "object-src 'none'" in csp
    assert "frame-ancestors 'none'" in csp
    assert "base-uri 'self'" in csp
    assert "form-action 'self'" in csp
    # Ingen CDN'er og ingen usikre direktiver.
    assert "unsafe-inline" not in csp
    assert "unsafe-eval" not in csp
    assert "cdn" not in csp.lower()
