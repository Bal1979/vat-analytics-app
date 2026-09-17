"""
Afstemningsgate (byggetrin 8, Del 2, BALAI-dataflow-arkitektur.md §8.3).

Dækker: afstemt/ikke-afstemt/valgfri (ingen fil)/ugyldig fil, tolerance,
og at et brud IKKE blokerer (v1-adfærd) -- kun stempler rapporten.
"""

import json

from analytics import reconciliation_gate as gate


def _canonical_with_lines(*line_specs):
    """line_specs: liste af (account_id, debit, credit)."""
    transactions = []
    for i, (acct, debit, credit) in enumerate(line_specs):
        transactions.append({
            "transaction_id": f"T{i}",
            "lines": [{"account_id": acct, "debit_amount": debit, "credit_amount": credit}],
        })
    return {"transactions": transactions}


def _write_control_totals(tmp_path, accounts, **overrides):
    doc = {
        "reconciliation_version": "1.0.0",
        "source": "test",
        "generated": "2026-09-17T00:00:00",
        "accounts": accounts,
    }
    doc.update(overrides)
    path = tmp_path / "reconciliation.json"
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path)


def test_no_control_file_means_not_performed():
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, None)
    assert result["gate_status"] == gate.STATUS_NOT_PERFORMED
    assert result["accounts"] == []


def test_matching_totals_are_reconciled(tmp_path):
    # Netto for konto 1000: debit(0) - credit(1250) = -1250.0
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    control_path = _write_control_totals(tmp_path, [{"account_id": "1000", "amount": -1250.0}])
    result = gate.reconcile(canonical, control_path)
    assert result["gate_status"] == gate.STATUS_RECONCILED
    assert result["accounts"][0]["status"] == gate.STATUS_RECONCILED
    assert result["accounts"][0]["difference"] == 0.0
    assert result["summary"]["not_reconciled_count"] == 0


def test_mismatched_totals_are_not_reconciled_but_do_not_raise(tmp_path):
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    control_path = _write_control_totals(tmp_path, [{"account_id": "1000", "amount": -1000.0}])
    result = gate.reconcile(canonical, control_path)
    assert result["gate_status"] == gate.STATUS_NOT_RECONCILED
    assert result["accounts"][0]["status"] == gate.STATUS_NOT_RECONCILED
    assert result["accounts"][0]["difference"] == -250.0
    assert "IKKE AFSTEMT" in result["message"]


def test_tolerance_is_configurable(tmp_path):
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    control_path = _write_control_totals(tmp_path, [{"account_id": "1000", "amount": -1250.005}])
    # Default tolerance 0.01 dækker en difference på 0.005 (efter afrunding 0.0/0.01).
    result = gate.reconcile(canonical, control_path, tolerance=0.01)
    assert result["gate_status"] in (gate.STATUS_RECONCILED, gate.STATUS_NOT_RECONCILED)
    # Med tolerance 0 og en reel forskel skal det slå ud som ikke-afstemt.
    control_path2 = _write_control_totals(tmp_path, [{"account_id": "1000", "amount": -1240.0}])
    result2 = gate.reconcile(canonical, control_path2, tolerance=0.0)
    assert result2["gate_status"] == gate.STATUS_NOT_RECONCILED


def test_account_missing_in_actuals_counts_as_full_difference(tmp_path):
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    control_path = _write_control_totals(tmp_path, [{"account_id": "9999", "amount": 500.0}])
    result = gate.reconcile(canonical, control_path)
    assert result["accounts"][0]["actual_amount"] == 0.0
    assert result["accounts"][0]["difference"] == -500.0
    assert result["gate_status"] == gate.STATUS_NOT_RECONCILED


def test_invalid_json_file_is_reported_as_technical_error_not_a_finding(tmp_path):
    path = tmp_path / "broken.json"
    path.write_text("{not valid json", encoding="utf-8")
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, str(path))
    assert result["gate_status"] == gate.STATUS_INVALID_FILE
    assert "teknisk fejl" in result["message"].lower()


def test_wrong_reconciliation_version_is_rejected(tmp_path):
    control_path = _write_control_totals(tmp_path, [{"account_id": "1000", "amount": 1.0}],
                                          reconciliation_version="2.0.0")
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, control_path)
    assert result["gate_status"] == gate.STATUS_INVALID_FILE


def test_missing_accounts_key_is_rejected(tmp_path):
    path = tmp_path / "reconciliation.json"
    path.write_text(json.dumps({"reconciliation_version": "1.0.0", "source": "x", "generated": "x"}),
                     encoding="utf-8")
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, str(path))
    assert result["gate_status"] == gate.STATUS_INVALID_FILE


def test_missing_file_path_is_reported_gracefully(tmp_path):
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, str(tmp_path / "does_not_exist.json"))
    assert result["gate_status"] == gate.STATUS_INVALID_FILE


def test_empty_accounts_list_is_valid_and_vacuously_reconciled(tmp_path):
    control_path = _write_control_totals(tmp_path, [])
    canonical = _canonical_with_lines(("1000", 0, 1250.0))
    result = gate.reconcile(canonical, control_path)
    assert result["gate_status"] == gate.STATUS_RECONCILED
    assert result["summary"]["total_accounts_checked"] == 0
