"""
Aggregat-korrekthed: de økonomiske/renterisiko-totaler må ikke dobbelttælle samme
transaktion, når flere kontroller flager den. Tester dedup-hjælperen direkte +
invarianten distinkt ≤ brutto via build_report.
"""

from analytics.engine import distinct_amount, build_report, run_all_tests
from analytics.models import make_finding


def _f(test_id, amount, txn_ids, direction="negative", impact="economic"):
    txns = [{"transaction_id": t, "highlighted_field": "x"} for t in txn_ids]
    return make_finding(test_id=test_id, test_name=f"T{test_id}", impact_type=impact,
                        direction=direction, severity="high", description="d",
                        estimated_amount=amount, transactions=txns)


def test_distinct_dedups_same_transaction():
    # To kontroller flager SAMME transaktion med 2500 hver.
    total, n = distinct_amount([_f(31, 2500.0, ["T1"]), _f(21, 2500.0, ["T1"])])
    assert total == 2500.0   # talt én gang (ikke 5000)
    assert n == 1


def test_distinct_takes_max_per_transaction():
    # Forskellige beløb på samme transaktion -> det største bidrag (defensivt).
    total, n = distinct_amount([_f(31, 2500.0, ["T1"]), _f(21, 1000.0, ["T1"])])
    assert total == 2500.0
    assert n == 1


def test_distinct_spreads_multi_transaction_finding():
    # Et fund med to transaktioner: beløbet fordeles ligeligt.
    total, n = distinct_amount([_f(11, 1000.0, ["T1", "T2"])])
    assert total == 1000.0
    assert n == 2


def test_distinct_counts_aggregate_findings_without_txn_id_whole():
    # Aggregeret fund uden transaktions-id (fx ratio) tælles helt.
    f = make_finding(test_id=76, test_name="ratio", impact_type="economic",
                     direction="negative", severity="medium", description="d",
                     estimated_amount=500.0, transactions=[{"input_vat": 1, "highlighted_field": "x"}])
    total, n = distinct_amount([f])
    assert total == 500.0
    assert n == 0


def test_build_report_exposes_gross_and_distinct():
    data = {"header": {"currency": "DKK"}, "transactions": [], "summary": {}}
    findings = [_f(31, 2500.0, ["T1"]), _f(21, 2500.0, ["T1"])]
    econ = build_report(data, findings)["impact_summary"]["economic"]
    assert econ["negative_amount"] == 5000.0           # brutto (kan overlappe)
    assert econ["negative_amount_distinct"] == 2500.0  # dedupliceret
    assert econ["distinct_transactions"] == 1


def test_distinct_never_exceeds_gross_on_real_run():
    # Invariant: på en rigtig kørsel må distinkt aldrig overstige brutto.
    txn = {
        "transaction_id": "T001", "date": "2024-01-15", "description": "x",
        "journal_id": "IMPORT", "period": "01", "period_year": "2024",
        "total_debit": 10000.0, "total_credit": 0,
        "lines": [{"record_id": "L1", "account_id": "4000", "description": "",
                   "debit_amount": 10000.0, "credit_amount": 0, "tax_code": "I25",
                   "tax_percentage": 25.0, "tax_base": 10000.0, "tax_amount": 100.0,
                   "currency": "DKK", "supplier_id": "S1", "supplier_name": "ABC",
                   "customer_id": "", "customer_name": "", "source_document_id": "INV-1",
                   "country": "", "ship_from_country": "", "ship_to_country": "", "vat_number": ""}],
    }
    data = {"header": {"currency": "DKK", "period": {"start": "1", "start_year": "2024",
            "end": "12", "end_year": "2024"}}, "accounts": [], "tax_table": [],
            "transactions": [txn], "suppliers": [], "customers": [], "summary": {}}
    rep = build_report(data, run_all_tests(data)["all_findings"])
    for kind in ("economic", "interest_risk"):
        s = rep["impact_summary"][kind]
        assert s["negative_amount_distinct"] <= s["negative_amount"] + 0.01
        assert s["positive_amount_distinct"] <= s["positive_amount"] + 0.01
