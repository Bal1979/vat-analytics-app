"""
Fase D: dedikerede tests for de nyligt aktiverede kontroller 36 og 46.
Kalder kontrolfunktionerne direkte med minimale datastrukturer (intet netværk).
"""

from analytics.categories import cat04_cross_border_eu as cat04
from analytics.categories import cat05_timing_period as cat05

# Kald via modulet, så pytest ikke opsamler de importerede test_NN-funktioner som tests.
run_36 = cat04.test_36_triangulation
run_46 = cat05.test_46_invoice_posting_lag

CTX = {"suppliers": {}, "customers": {}}


def _line(**kw):
    d = {
        "account_id": "1000", "description": "", "debit_amount": 0.0,
        "credit_amount": 0.0, "tax_amount": 0.0, "tax_base": 0.0, "country": "",
        "ship_from_country": "", "ship_to_country": "", "vat_number": "",
        "supplier_id": "", "customer_id": "",
    }
    d.update(kw)
    return d


def _txn(line, **kw):
    t = {
        "transaction_id": "T1", "journal_id": "J", "date": "2024-01-10",
        "document_date": "", "description": "",
        "total_debit": line["debit_amount"], "total_credit": line["credit_amount"],
        "lines": [line],
    }
    t.update(kw)
    return t


# --- Kontrol 36: place-of-supply / trekantshandel ---

def test_36_domestic_goods_zero_rated_is_flagged():
    # Vare forlader ikke DK (DK->DK), men sælges til DE uden moms -> uberettiget nulsats.
    line = _line(credit_amount=10000.0, country="DE",
                 ship_from_country="DK", ship_to_country="DK", tax_amount=0.0)
    findings = run_36({"transactions": [_txn(line)]}, CTX)
    assert len(findings) == 1
    f = findings[0]
    assert f["test_id"] == 36
    assert f["severity"] == "high"
    assert f["impact_type"] == "economic"
    assert f["estimated_amount"] == 2500.0  # 10000 * 25%


def test_36_delivery_country_mismatch_is_flagged():
    # Varen leveres til SE, men modparten er i DE -> mulig trekantshandel.
    line = _line(credit_amount=5000.0, country="DE", ship_to_country="SE", tax_amount=0.0)
    findings = run_36({"transactions": [_txn(line)]}, CTX)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 36
    assert findings[0]["severity"] == "medium"


def test_36_no_ship_data_is_silent():
    # Uden vareflow må kontrollen ikke fyre (ingen falske alarmer på flade udtræk).
    line = _line(credit_amount=5000.0, country="DE", tax_amount=0.0)
    assert run_36({"transactions": [_txn(line)]}, CTX) == []


# --- Kontrol 46: faktura/bogførings-lag ---

def test_46_large_lag_is_flagged():
    data = {"transactions": [_txn(_line(debit_amount=1000.0),
                                  date="2024-03-20", document_date="2024-02-01")]}
    findings = run_46(data)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 46


def test_46_small_lag_is_ok():
    data = {"transactions": [_txn(_line(debit_amount=1000.0),
                                  date="2024-02-10", document_date="2024-02-01")]}
    assert run_46(data) == []


def test_46_missing_document_date_is_silent():
    data = {"transactions": [_txn(_line(debit_amount=1000.0),
                                  date="2024-03-20", document_date="")]}
    assert run_46(data) == []
