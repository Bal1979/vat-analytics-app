"""
Materialitet: de engagement-kalibrerbare tærskler har defaults = hidtidig adfærd
og kan overstyres via MATERIALITY_*-miljøvariabler.
"""

import importlib

from analytics import materiality


def test_defaults_match_legacy_behaviour():
    assert materiality.SEVERITY_WEIGHTS == {"critical": 25, "high": 15, "medium": 8, "low": 3}
    assert materiality.CASH_LIMIT == 20000.0
    assert materiality.LARGE_VAT_NO_DOCUMENT == 5000.0
    assert materiality.INPUT_OUTPUT_RATIO == 3.0
    assert materiality.INVOICE_POSTING_LAG_DAYS == 30
    assert materiality.DISTANCE_SELLING_THRESHOLD_DKK == 74500.0
    assert materiality.APPROVAL_THRESHOLDS[0] == 10000


def test_env_override(monkeypatch):
    monkeypatch.setenv("MATERIALITY_CASH_LIMIT", "5000")
    monkeypatch.setenv("MATERIALITY_INVOICE_POSTING_LAG_DAYS", "7")
    monkeypatch.setenv("MATERIALITY_WEIGHT_CRITICAL", "40")
    try:
        m = importlib.reload(materiality)
        assert m.CASH_LIMIT == 5000.0
        assert m.INVOICE_POSTING_LAG_DAYS == 7
        assert m.SEVERITY_WEIGHTS["critical"] == 40
    finally:
        monkeypatch.undo()
        importlib.reload(materiality)  # gendan defaults for de øvrige tests
