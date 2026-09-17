"""
Kanonisk CSV-parser (byggetrin 8, BALAI-dataflow-arkitektur.md §7): tredje
input-vej ved siden af Excel/CSV og SAF-T XML.

Dækker: genkendelse (upload_router.is_canonical), grundparsning (accounts/
tax_table/transactions/lines afledt korrekt), separator-agnostisk vat_codes
(ALDRIG splittet -- vat-extract skifter D1-separatoren fra "/" til "|" i
skrivende stund), lineage fra transform_summary.json (valgfri sidecar), at
motoren rent faktisk kan køre på outputtet, og best-effort-fejlhåndtering.

Alle fixtures er selvstændige, opdigtede data -- ingen kundedata.
"""

import csv
import json

from analytics.engine import run_all_tests
from parsers import canonical_parser
from parsers import upload_router


_HEADERS = [
    "posting_dates", "tax_point", "vat_period", "credit_note_flag",
    "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
    "debit_amount", "credit_amount", "supply_direction", "currency_fx",
]


def _write_csv(tmp_path, rows, headers=None, filename="gl_entries.csv"):
    path = tmp_path / filename
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers or _HEADERS)
        writer.writerows(rows)
    return str(path)


def _write_summary(tmp_path, **overrides):
    summary = {
        "mapping_version": "1.0.0",
        "schema_fingerprint": "sha256:abc123",
        "source_erp": "Microsoft Dynamics 365 Business Central / NAV",
        "profile_version": "1.0.0",
        "rows_in": 2,
        "rows_out": 2,
        "source_file": "test.xlsx",
    }
    summary.update(overrides)
    (tmp_path / "transform_summary.json").write_text(json.dumps(summary), encoding="utf-8")


# --- Genkendelse (routing) ---------------------------------------------------

def test_looks_like_canonical_true_for_canonical_columns(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    assert canonical_parser.looks_like_canonical(path) is True


def test_looks_like_canonical_false_for_flat_excel_style_csv(tmp_path):
    """Et almindeligt fladt CSV-udtræk (excel_parser-aliaser) må IKKE genkendes
    som kanonisk -- ingen kollision mellem de to veje."""
    path = _write_csv(
        tmp_path,
        [["T1", "2024-01-10", "1000", "Salg", 0, 1250.0, 250.0, "U25", 25.0, "DKK"]],
        headers=["transaction_id", "date", "account_id", "description",
                 "debit", "credit", "vat_amount", "vat_code", "vat_rate", "currency"],
        filename="flat.csv",
    )
    assert canonical_parser.looks_like_canonical(path) is False


def test_upload_router_routes_canonical_csv_to_canonical_parser(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    assert upload_router.is_canonical(path) is True
    assert upload_router.is_saft(path) is False
    data = upload_router.parse_upload(path)
    assert data["parse_info"]["kilde"] == "canonical"
    assert data["header"]["source"] == "canonical"


# --- Grundparsning ------------------------------------------------------------

def test_parse_canonical_derives_accounts_and_tax_table(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-11", "2024-01-11", "2024-01", "false", "F2", "2100", "", "0", "800.0", "0", "purchase", ""],
        ["2024-01-12", "2024-01-12", "2024-01", "false", "F3", "1000", "U25", "125.0", "0", "625.0", "sale", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info

    account_ids = {a["account_id"] for a in canonical["accounts"]}
    assert account_ids == {"1000", "2100"}

    tax_codes = {t["tax_code"] for t in canonical["tax_table"]}
    assert tax_codes == {"U25"}  # tom vat_codes-linje giver INGEN tax_table-post

    assert len(canonical["transactions"]) == 3
    assert canonical["suppliers"] == []
    assert canonical["customers"] == []


def test_parse_canonical_amounts_and_summary(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-11", "2024-01-11", "2024-01", "false", "F2", "2100", "", "0", "800.0", "0", "purchase", ""],
    ])
    canonical, _ = canonical_parser.parse_canonical(path)
    summary = canonical["summary"]
    assert summary["total_debit"] == 800.0
    assert summary["total_credit"] == 1250.0
    assert summary["total_vat"] == 250.0
    assert summary["total_transactions"] == 2

    line0 = canonical["transactions"][0]["lines"][0]
    assert line0["debit_amount"] == 0.0
    assert line0["credit_amount"] == 1250.0
    assert line0["tax_amount"] == 250.0
    assert line0["tax_percentage"] == 0.0  # GAP-10: ingen sats på denne vej
    # Ingen tax_base_amount-kolonne -> fallback debit+credit (samme mønster
    # som data_adapter.adapt_excel_to_saft).
    assert line0["tax_base"] == 1250.0


def test_parse_canonical_vat_codes_are_opaque_never_split(tmp_path):
    """Separator-agnostisk: uanset om D1-koden bruger "/" eller "|" (vat-extract
    skifter separator i skrivende stund), skal koden bevares uændret som ÉN
    streng -- aldrig splittet af denne parser."""
    for combined in ("SALES/SERVICE_VAT_NOT_EU", "SALES|SERVICE_VAT_NOT_EU"):
        path = _write_csv(tmp_path, [
            ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000",
             combined, "250.0", "0", "1250.0", "sale", ""],
        ], filename=f"vc_{hash(combined) & 0xffff}.csv")
        canonical, _ = canonical_parser.parse_canonical(path)
        assert canonical["transactions"][0]["lines"][0]["tax_code"] == combined
        assert canonical["tax_table"][0]["tax_code"] == combined


# --- Lineage -------------------------------------------------------------

def test_parse_canonical_reads_lineage_from_sidecar_summary(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    _write_summary(tmp_path, mapping_version="2.3.1", schema_fingerprint="sha256:deadbeef")
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical["header"]["mapping_version"] == "2.3.1"
    assert canonical["header"]["schema_fingerprint"] == "sha256:deadbeef"
    assert info["lineage"]["mapping_version"] == "2.3.1"
    assert not info["warnings"]  # summary fundet -> ingen advarsel


def test_parse_canonical_without_sidecar_summary_warns_but_still_parses(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None
    assert canonical["header"]["mapping_version"] == ""
    assert canonical["header"]["schema_fingerprint"] == ""
    assert any("transform_summary.json" in w for w in info["warnings"])


def test_parse_canonical_explicit_summary_path(tmp_path):
    """CLI-brug (Del 3): eksplicit --reconciliation/--summary-sti, ikke kun
    sidecar-konventionen."""
    other_dir = tmp_path / "elsewhere"
    other_dir.mkdir()
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    summary_path = other_dir / "transform_summary.json"
    summary_path.write_text(json.dumps({"mapping_version": "9.9.9", "schema_fingerprint": "sha256:x"}),
                             encoding="utf-8")
    canonical, _ = canonical_parser.parse_canonical(path, summary_path=str(summary_path))
    assert canonical["header"]["mapping_version"] == "9.9.9"


# --- Fejlhåndtering (best-effort) -------------------------------------------

def test_parse_canonical_missing_marker_columns_errors_gracefully(tmp_path):
    path = _write_csv(
        tmp_path,
        [["1000", "250.0"]],
        headers=["gl_accounts", "vat_amount"],  # mangler vat_codes/posting_dates
        filename="incomplete.csv",
    )
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is None
    assert info["errors"]


def test_parse_canonical_missing_file(tmp_path):
    canonical, info = canonical_parser.parse_canonical(str(tmp_path / "nope.csv"))
    assert canonical is None
    assert info["errors"]


# --- Motoren kan køre på outputtet -------------------------------------------

def test_engine_runs_on_canonical_output(tmp_path):
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-11", "2024-01-11", "2024-01", "false", "F2", "2100", "", "0", "800.0", "0", "purchase", ""],
    ])
    canonical, _ = canonical_parser.parse_canonical(path)
    report = run_all_tests(canonical)
    assert "total_findings" in report
    assert "categories" in report
