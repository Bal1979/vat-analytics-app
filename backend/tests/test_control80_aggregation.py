"""
Kontrol 80 (byggetrin ~9, Del B, Bal-godkendt 2026-09-17): aggregeret PR.
KONTO i stedet for pr. postering.

Diagnose (verificeret manuelt): 24.152 per-posterings-fund fordelt på 77
konti; top-10 konti = 97%. Kontrollen udsteder nu ÉT fund pr. konto med
antal posteringer, sum af grundlag, andel af kontoens posteringer uden
momskode, og et udsnit (maks. CONTROL_80_MAX_REFS) af transaktionsreferencer.
Bevidst granularitetsændring på tværs af alle input-veje.
"""

from analytics.categories import cat10_vat_reconciliation as cat10
from analytics import materiality
from validation.builders import mk_data, mk_txn, mk_line

run_80 = cat10.test_80_revenue_without_output_vat


def _qualifying_line(amount, account_id="4000"):
    return mk_line(account_id=account_id, credit_amount=amount, country="DK",
                    tax_code="", tax_amount=0.0)


def test_multiple_qualifying_lines_same_account_aggregate_to_one_finding():
    txns = [mk_txn(_qualifying_line(10000.0), transaction_id=f"T{i}") for i in range(5)]
    findings = run_80(mk_data(txns))
    assert len(findings) == 1
    f = findings[0]
    assert f["test_id"] == 80
    assert "4000" in f["description"]
    assert "5 posteringer" in f["description"]


def test_different_accounts_produce_separate_findings():
    txns = [
        mk_txn(_qualifying_line(10000.0, account_id="4000"), transaction_id="T1"),
        mk_txn(_qualifying_line(20000.0, account_id="5000"), transaction_id="T2"),
    ]
    findings = run_80(mk_data(txns))
    assert len(findings) == 2
    accounts = {f["transactions"][0]["account_id"] for f in findings}
    assert accounts == {"4000", "5000"}


def test_account_name_included_when_chart_of_accounts_available():
    txn = mk_txn(_qualifying_line(10000.0, account_id="4000"))
    data = mk_data(txn, accounts=[{"account_id": "4000", "description": "Diverse indtægter"}])
    findings = run_80(data)
    assert len(findings) == 1
    assert "4000 (Diverse indtægter)" in findings[0]["description"]


def test_no_account_name_when_chart_of_accounts_missing():
    txn = mk_txn(_qualifying_line(10000.0, account_id="4000"))
    findings = run_80(mk_data(txn))
    assert len(findings) == 1
    assert "4000 (" not in findings[0]["description"]


def test_severity_high_above_high_threshold():
    txn = mk_txn(_qualifying_line(materiality.CONTROL_80_HIGH_THRESHOLD + 1))
    findings = run_80(mk_data(txn))
    assert findings[0]["severity"] == "high"


def test_severity_medium_between_thresholds():
    amt = (materiality.CONTROL_80_MEDIUM_THRESHOLD + materiality.CONTROL_80_HIGH_THRESHOLD) / 2
    txn = mk_txn(_qualifying_line(amt))
    findings = run_80(mk_data(txn))
    assert findings[0]["severity"] == "medium"


def test_severity_low_below_medium_threshold():
    txn = mk_txn(_qualifying_line(5000.0))  # over 5000-minimum, under medium-loft
    findings = run_80(mk_data(txn))
    assert findings[0]["severity"] == "low"


def test_drilldown_capped_at_max_refs():
    n = materiality.CONTROL_80_MAX_REFS + 7
    txns = [mk_txn(_qualifying_line(10000.0), transaction_id=f"T{i}") for i in range(n)]
    findings = run_80(mk_data(txns))
    assert len(findings) == 1
    assert len(findings[0]["transactions"]) == materiality.CONTROL_80_MAX_REFS
    assert f"{n} posteringer" in findings[0]["description"]
    assert "flere posteringer" in findings[0]["description"]


def test_no_code_share_reflects_all_lines_on_account_not_just_qualifying():
    """Andelen "uden momskode" tæller ALLE linjer på kontoen (ikke kun de
    kvalificerende) -- inkl. en linje MED momskode, som sænker andelen."""
    txn1 = mk_txn(_qualifying_line(10000.0))  # uden momskode
    txn2 = mk_txn(mk_line(account_id="4000", credit_amount=5000.0, country="DK",
                           tax_code="U25", tax_amount=1250.0))  # med momskode
    findings = run_80(mk_data([txn1, txn2]))
    assert len(findings) == 1
    assert "50%" in findings[0]["description"]


def test_suppressed_below_5000_minimum():
    txn = mk_txn(_qualifying_line(4999.0))
    assert run_80(mk_data(txn)) == []


def test_still_suppressed_on_balance_account():
    line = mk_line(account_id="6900", credit_amount=10000.0, country="DK",
                    tax_code="", tax_amount=0.0, account_type="Liability")
    assert run_80(mk_data(mk_txn(line))) == []
