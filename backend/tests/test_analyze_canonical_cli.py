"""
CLI-entrypoint for kanonisk offline-analyse (byggetrin 8, Del 3).

Dækker: happy path (lineage-stempel + rapport skrevet), afstemningsgate
tilkoblet, 'alle'-moduler som CLI-default (mod webappens momskerne-default),
og at en parse-fejl rapporteres pænt (exit-kode != 0, ingen exception).
"""

import csv
import json

from tools import analyze_canonical


_HEADERS = [
    "posting_dates", "tax_point", "vat_period", "credit_note_flag",
    "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
    "debit_amount", "credit_amount", "supply_direction", "currency_fx",
]


def _write_csv(tmp_path, rows):
    path = tmp_path / "gl_entries.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_HEADERS)
        writer.writerows(rows)
    return str(path)


def _write_summary(tmp_path):
    (tmp_path / "transform_summary.json").write_text(json.dumps({
        "mapping_version": "1.0.0",
        "schema_fingerprint": "sha256:test",
        "source_erp": "Test ERP",
    }), encoding="utf-8")


def test_build_report_happy_path_stamps_lineage(tmp_path):
    csv_path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    _write_summary(tmp_path)
    report = analyze_canonical.build_report(csv_path, None, None, 0.01, "alle")
    assert "fejl" not in report
    assert report["lineage"]["mapping_version"] == "1.0.0"
    assert report["lineage"]["schema_fingerprint"] == "sha256:test"
    assert report["lineage"]["catalog_version"]
    assert report["lineage"]["data_contract_version"]
    assert report["afstemning"]["gate_status"] == "afstemning_ikke_udfoert"
    assert report["analytics"]["summary"]["total_transactions"] == 1
    # Byggetrin ~10 (Bal-godkendt 2026-09-18): letvægts tax_table-udtræk til
    # rapportlagets 'Momsmotoren'-sektion.
    assert report["tax_table_oversigt"] == [
        {"tax_code": "U25", "description": "", "tax_percentage": 0.0,
         "vat_calculation_type": "", "sales_vat_account": "",
         "purchase_vat_account": "", "reverse_charge_vat_account": "",
         "setup_matched": False}
    ]


def test_build_report_with_reconciliation_file(tmp_path):
    csv_path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    recon = {
        "reconciliation_version": "1.0.0", "source": "test", "generated": "2026-09-17",
        "accounts": [{"account_id": "1000", "amount": -1250.0}],
    }
    recon_path = tmp_path / "reconciliation.json"
    recon_path.write_text(json.dumps(recon), encoding="utf-8")

    report = analyze_canonical.build_report(csv_path, None, str(recon_path), 0.01, "alle")
    assert report["afstemning"]["gate_status"] == "afstemt"


def test_cli_all_modules_runs_more_controls_than_default(tmp_path):
    csv_path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    report_alle = analyze_canonical.build_report(csv_path, None, None, 0.01, "alle")
    report_default = analyze_canonical.build_report(csv_path, None, None, 0.01, "default")
    n_alle = sum(1 for m in report_alle["analytics"]["moduler"] if m["aktiv"])
    n_default = sum(1 for m in report_default["analytics"]["moduler"] if m["aktiv"])
    assert n_alle >= n_default
    assert n_alle == len(report_alle["analytics"]["moduler"])


def test_cli_main_writes_report_and_returns_zero(tmp_path):
    csv_path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    out_path = tmp_path / "rapport.json"
    rc = analyze_canonical.main([csv_path, "--out", str(out_path)])
    assert rc == 0
    assert out_path.exists()
    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)
    assert data["kanonisk_vej"] is True


def test_cli_main_reports_parse_error_without_crashing(tmp_path):
    bad_path = tmp_path / "not_canonical.csv"
    bad_path.write_text("a,b\n1,2\n", encoding="utf-8")
    out_path = tmp_path / "rapport.json"
    rc = analyze_canonical.main([str(bad_path), "--out", str(out_path)])
    assert rc == 1
    with open(out_path, encoding="utf-8") as f:
        data = json.load(f)
    assert data["fejl"]
