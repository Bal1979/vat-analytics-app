"""
build_declaration_reconciliation_table (byggetrin ~9, Del C, Bal-godkendt
2026-09-17) — den fulde periode-/rubrik-afstemningstabel til kundedialog-
rapportens tillidsanker (backend/tools/generate_report.py), som modsat
test_82_period_declaration viser ALLE perioder x rubrikker, ikke kun
afvigelserne.
"""

from analytics.categories import cat10_vat_reconciliation as cat10
from analytics.engine import run_all_tests
from validation.builders import mk_data, mk_line, mk_txn


def _decl(*periods):
    return {
        "declarations_version": "1.0.0", "source": "test", "generated": "2024-01-01",
        "periods": [
            {"period": p, "output_vat": o, "input_vat": i, "rc_services": rc,
             "rc_goods": 0.0, "energy_taxes": 0.0, "total": o + i + rc}
            for (p, o, i, rc) in periods
        ],
    }


def test_returns_none_without_declarations():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0)))
    assert cat10.build_declaration_reconciliation_table(data, None) is None
    assert cat10.build_declaration_reconciliation_table(data, {}) is None
    assert cat10.build_declaration_reconciliation_table(data, {"periods": []}) is None


def test_matching_period_is_status_match_for_all_three_rubrics():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0),
                          period="03", period_year="2024"))
    table = cat10.build_declaration_reconciliation_table(data, _decl(("2024-03", 250.0, 0.0, 0.0)))
    assert table is not None
    p = table["perioder"][0]
    assert p["periode"] == "2024-03"
    for r in ("output_vat", "rc_services", "input_vat"):
        assert p["rubrikker"][r]["status"] == "match"
    assert set(table["rubrik_labels"]) == {"output_vat", "rc_services", "input_vat"}


def test_real_difference_is_status_afvigelse():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0),
                          period="03", period_year="2024"))
    table = cat10.build_declaration_reconciliation_table(data, _decl(("2024-03", 999999.0, 0.0, 0.0)))
    assert table["perioder"][0]["rubrikker"]["output_vat"]["status"] == "afvigelse"


def test_timing_difference_resolved_by_annual_total_is_status_timing():
    data = mk_data([
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=100.0, tax_code="STANDARD|I25"),
               transaction_id="T1", period="01", period_year="2024"),
        mk_txn(mk_line(debit_amount=2000.0, tax_amount=200.0, tax_code="STANDARD|I25"),
               transaction_id="T2", period="02", period_year="2024"),
    ])
    declarations = _decl(("2024-01", 0.0, 150.0, 0.0), ("2024-02", 0.0, 150.0, 0.0))
    table = cat10.build_declaration_reconciliation_table(data, declarations)
    statuses = [p["rubrikker"]["input_vat"]["status"] for p in table["perioder"]]
    assert statuses == ["timing", "timing"]
    assert table["aarstotaler"]["input_vat"]["beregnet"] == 300.0
    assert table["aarstotaler"]["input_vat"]["angivet"] == 300.0


def test_period_without_declared_value_is_ingen_angivelse():
    data = mk_data([
        mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0), period="03", period_year="2024"),
        mk_txn(mk_line(credit_amount=2000.0, tax_amount=-500.0), transaction_id="T2",
               period="04", period_year="2024"),
    ])
    table = cat10.build_declaration_reconciliation_table(data, _decl(("2024-03", 250.0, 0.0, 0.0)))
    by_period = {p["periode"]: p for p in table["perioder"]}
    assert by_period["2024-04"]["rubrikker"]["output_vat"]["status"] == "ingen_angivelse"
    assert by_period["2024-04"]["rubrikker"]["output_vat"]["angivet"] is None


def test_wired_into_engine_report():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0),
                          period="03", period_year="2024"))
    report = run_all_tests(data, declarations=_decl(("2024-03", 250.0, 0.0, 0.0)))
    assert report["declaration_reconciliation"] is not None
    assert report["declaration_reconciliation"]["perioder"][0]["periode"] == "2024-03"


def test_engine_report_has_none_without_declarations():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0)))
    report = run_all_tests(data)
    assert report["declaration_reconciliation"] is None
