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


# --- Bilagsgruppering (GAP-12, Bal-godkendt 2026-09-17) ----------------------

def test_rows_with_same_invoice_and_date_are_grouped_into_one_transaction(tmp_path):
    """To ensidede GL-linjer (kun debit hhv. kun credit) med samme bilagsnøgle
    (invoice_numbers, posting_dates) skal samles til ÉN transaktion med to
    linjer -- den reelle rettelse for GAP-12/kontrol 10-støjen."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F-1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F-1", "2100", "", "0", "1250.0", "0", "purchase", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info

    assert len(canonical["transactions"]) == 1
    txn = canonical["transactions"][0]
    assert len(txn["lines"]) == 2
    assert txn["total_debit"] == 1250.0
    assert txn["total_credit"] == 1250.0
    # Deterministisk id af nøglen, ikke det gamle rækkebaserede ROW-id.
    assert txn["transaction_id"] == "DOC_F-1_2024-01-10"

    # Linjerne beholder deres række-lineage og et positionelt record_id.
    assert [l["record_id"] for l in txn["lines"]] == ["L1", "L2"]
    assert [l["source_row"] for l in txn["lines"]] == [2, 3]

    # Summary (linje-niveau) er uændret af grupperingen.
    assert canonical["summary"]["total_debit"] == 1250.0
    assert canonical["summary"]["total_credit"] == 1250.0
    assert info["sections"]["lines"] == 2
    assert info["sections"]["transactions"] == 1


def test_grouping_fixes_control_10_false_positive(tmp_path):
    """Den empiriske pointe med GAP-12-rettelsen: to ensidede linjer, der før
    grupperingen hver blev flaget af kontrol 10 (transaktionsbalance), giver
    nu INGEN fund, fordi de er slået sammen til én balanceret transaktion."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F-1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F-1", "2100", "", "0", "1250.0", "0", "purchase", ""],
    ])
    canonical, _ = canonical_parser.parse_canonical(path)
    report = run_all_tests(canonical, active_modules={"moms_kerne"})
    balance_findings = [f for f in report["all_findings"] if f["test_id"] == 10]
    assert balance_findings == []


def test_rows_with_empty_invoice_number_are_never_grouped_across_rows(tmp_path):
    """Tomt/manglende bilagsnummer må ALDRIG gætte en sammenhæng -- selv to
    rækker med samme dato og ellers ens data forbliver hver sin 1-linjes
    transaktion (samme fallback-adfærd som før grupperingen fandtes)."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-10", "2024-01-10", "2024-01", "false", "", "2100", "", "0", "1250.0", "0", "purchase", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info

    assert len(canonical["transactions"]) == 2
    for txn in canonical["transactions"]:
        assert len(txn["lines"]) == 1
    ids = {t["transaction_id"] for t in canonical["transactions"]}
    assert ids == {"ROW-2", "ROW-3"}  # gammelt rækkebaseret id bevaret


def test_same_invoice_number_on_two_dates_is_two_transactions(tmp_path):
    """Samme bilagsnummer, men to forskellige posting_dates, er IKKE samme
    bilag -- nøglen er (invoice_numbers, posting_dates) sammen, ikke hver for
    sig."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F-1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-01-11", "2024-01-11", "2024-01", "false", "F-1", "2100", "", "0", "1250.0", "0", "purchase", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info

    assert len(canonical["transactions"]) == 2
    for txn in canonical["transactions"]:
        assert len(txn["lines"]) == 1
    ids = {t["transaction_id"] for t in canonical["transactions"]}
    assert ids == {"DOC_F-1_2024-01-10", "DOC_F-1_2024-01-11"}


def test_grouped_transaction_multiline_amounts_and_document_date(tmp_path):
    """Tre linjer på samme bilag -- aggregatfelter summeres over alle tre, og
    document_date afledes af gruppens første rækkes tax_point (samme
    proxy-logik som før grupperingen)."""
    path = _write_csv(tmp_path, [
        ["2024-02-01", "2024-01-28", "2024-02", "false", "F-77", "1000", "U25", "100.0", "0", "500.0", "sale", ""],
        ["2024-02-01", "2024-01-29", "2024-02", "false", "F-77", "1000", "U25", "50.0", "0", "250.0", "sale", ""],
        ["2024-02-01", "2024-01-30", "2024-02", "false", "F-77", "2100", "", "0", "750.0", "0", "purchase", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info

    assert len(canonical["transactions"]) == 1
    txn = canonical["transactions"][0]
    assert len(txn["lines"]) == 3
    assert txn["total_debit"] == 750.0
    assert txn["total_credit"] == 750.0
    assert txn["date"] == "2024-02-01"
    # Første rækkes tax_point, ikke sidste.
    assert txn["document_date"] == "2024-01-28"
    assert [l["record_id"] for l in txn["lines"]] == ["L1", "L2", "L3"]
    assert [l["source_row"] for l in txn["lines"]] == [2, 3, 4]


# --- Description (GAP-13, Bal-godkendt 2026-09-17) ---------------------------

_HEADERS_WITH_DESCRIPTION = _HEADERS + ["description"]


def test_parse_canonical_without_description_column_is_empty_not_crash(tmp_path):
    """v2-filer (uden description-kolonnen) skal fortsat parses uden fejl --
    feltet er bare tomt, på både linje- og transaktionsniveau (GAP-13)."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0", "sale", ""],
    ])
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    txn = canonical["transactions"][0]
    assert txn["description"] == ""
    assert txn["lines"][0]["description"] == ""


def test_parse_canonical_reads_description_column_when_present(tmp_path):
    """v3-filer (med description-kolonnen) skal føre værdien ind på BÅDE
    linje- og transaktionsniveau, præcis som Excel-/SAF-T-vejen."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0",
         "sale", "", "Salg af varer"],
    ], headers=_HEADERS_WITH_DESCRIPTION)
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    txn = canonical["transactions"][0]
    assert txn["description"] == "Salg af varer"
    assert txn["lines"][0]["description"] == "Salg af varer"


def test_parse_canonical_description_empty_value_is_empty_string(tmp_path):
    """Kolonnen er til stede, men værdien er tom på denne række -- en reel
    tom description, IKKE en manglende kolonne. Skal fortsat give ""."""
    path = _write_csv(tmp_path, [
        ["2024-01-10", "2024-01-10", "2024-01", "false", "F1", "1000", "U25", "250.0", "0", "1250.0",
         "sale", "", ""],
    ], headers=_HEADERS_WITH_DESCRIPTION)
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    assert canonical["transactions"][0]["description"] == ""


def test_grouped_transaction_uses_first_nonempty_line_description(tmp_path):
    """Et grupperet flerlinje-bilag (GAP-12) tager den FØRSTE ikke-tomme
    linje-description -- ingen gætning på tværs af linjer, samme
    fallback-mønster som document_date."""
    path = _write_csv(tmp_path, [
        ["2024-02-01", "2024-01-28", "2024-02", "false", "F-77", "1000", "U25", "100.0", "0", "500.0",
         "sale", "", ""],
        ["2024-02-01", "2024-01-29", "2024-02", "false", "F-77", "2100", "", "0", "500.0", "0",
         "purchase", "", "Køb af varer"],
    ], headers=_HEADERS_WITH_DESCRIPTION)
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    txn = canonical["transactions"][0]
    assert len(txn["lines"]) == 2
    assert txn["description"] == "Køb af varer"
    assert txn["lines"][0]["description"] == ""
    assert txn["lines"][1]["description"] == "Køb af varer"
