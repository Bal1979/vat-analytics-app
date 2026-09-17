"""
canonical_masterdata.py (byggetrin 8, Del C, Bal-godkendt 2026-09-17).

Dækker: de tre loaders (vat_setup.csv/chart_of_accounts.csv/customers.csv)
hver for sig (manglende fil, tom/ugyldig fil, gyldig fil), samt
enrich_canonical()'s berigelse af en allerede parset kanonisk struktur
(tax_table/accounts/lines/customers) og at fravær af alle tre filer giver
UÆNDRET adfærd (samme strukturelle tomhed som før byggetrin 8/Del C).
"""

import csv
import os

from parsers import canonical_masterdata as md


def _write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _bare_canonical():
    """En minimal kanonisk struktur, som parse_canonical() ville producere
    FØR berigelse -- accounts/tax_table kun med nøgler, ingen metadata."""
    return {
        "accounts": [{"account_id": "5820", "description": "", "account_type": "",
                      "standard_account_id": "", "opening_balance": 0.0, "closing_balance": 0.0}],
        "tax_table": [{"tax_code": "DOMESTIC|REDUCED_PRIVATE_DKRC", "description": "",
                       "tax_percentage": 0.0, "rate": 0.0, "standard_tax_code": "", "country": ""}],
        "transactions": [{
            "transaction_id": "T1",
            "lines": [{"account_id": "5820", "tax_code": "DOMESTIC|REDUCED_PRIVATE_DKRC",
                       "tax_percentage": 0.0, "account_type": "", "standard_account_id": ""}],
        }],
        "customers": [],
    }


# --- load_vat_setup ----------------------------------------------------------

def test_load_vat_setup_missing_file_returns_empty():
    lookup, warnings = md.load_vat_setup("/does/not/exist.csv")
    assert lookup == {}
    assert warnings == []


def test_load_vat_setup_valid_file(tmp_path):
    path = tmp_path / "vat_setup.csv"
    _write_csv(str(path), ["vat_codes", "tax_percentage", "description"],
               [{"vat_codes": "STANDARD|I25", "tax_percentage": "25.0", "description": "Standard købsmoms"}])
    lookup, warnings = md.load_vat_setup(str(path))
    assert warnings == []
    assert lookup["STANDARD|I25"]["tax_percentage"] == 25.0
    assert lookup["STANDARD|I25"]["description"] == "Standard købsmoms"


def test_load_vat_setup_empty_file_warns(tmp_path):
    path = tmp_path / "vat_setup.csv"
    _write_csv(str(path), ["vat_codes", "tax_percentage"], [])
    lookup, warnings = md.load_vat_setup(str(path))
    assert lookup == {}
    assert warnings


def test_load_vat_setup_missing_join_column_warns(tmp_path):
    path = tmp_path / "vat_setup.csv"
    _write_csv(str(path), ["not_vat_codes", "tax_percentage"], [{"not_vat_codes": "X", "tax_percentage": "25"}])
    lookup, warnings = md.load_vat_setup(str(path))
    assert lookup == {}
    assert warnings


# --- load_chart_of_accounts ---------------------------------------------------

def test_load_chart_of_accounts_valid_file(tmp_path):
    path = tmp_path / "chart_of_accounts.csv"
    _write_csv(str(path), ["gl_accounts", "account_type", "standard_account_id"],
               [{"gl_accounts": "5820", "account_type": "Asset", "standard_account_id": "5800"}])
    lookup, warnings = md.load_chart_of_accounts(str(path))
    assert warnings == []
    assert lookup["5820"]["account_type"] == "Asset"
    assert lookup["5820"]["standard_account_id"] == "5800"


def test_load_chart_of_accounts_missing_file_returns_empty():
    lookup, warnings = md.load_chart_of_accounts("/does/not/exist.csv")
    assert lookup == {}
    assert warnings == []


def test_load_chart_of_accounts_opening_closing_balance_none_when_absent(tmp_path):
    path = tmp_path / "chart_of_accounts.csv"
    _write_csv(str(path), ["gl_accounts", "account_type"], [{"gl_accounts": "5820", "account_type": "Asset"}])
    lookup, _ = md.load_chart_of_accounts(str(path))
    assert lookup["5820"]["opening_balance"] is None
    assert lookup["5820"]["closing_balance"] is None


# --- load_customers ------------------------------------------------------------

def test_load_customers_valid_file(tmp_path):
    path = tmp_path / "customers.csv"
    _write_csv(str(path), ["customer_id", "name", "vat_number", "country"],
               [{"customer_id": "K001", "name": "Testkunde ApS", "vat_number": "DK12345678", "country": "DK"}])
    customers, warnings = md.load_customers(str(path))
    assert warnings == []
    assert customers == [{"customer_id": "K001", "name": "Testkunde ApS",
                          "vat_number": "DK12345678", "country": "DK"}]


def test_load_customers_missing_file_returns_empty():
    customers, warnings = md.load_customers("/does/not/exist.csv")
    assert customers == []
    assert warnings == []


def test_load_customers_accepts_vat_extract_ext_prefixed_columns(tmp_path):
    """Bekræftet mod vat-extracts reelle transform-output (byggetrin 8/Del C
    E2E, 2026-09-17): kolonnerne hedder ext_customer_id/ext_customer_name/
    counterparty_country/ext_vat_bus_posting_group -- IKKE customer_id/name/
    country/vat_number. ext_vat_bus_posting_group (en BC-postgruppekode, fx
    "EU"/"DOMESTIC") må ALDRIG mappes til vat_number (misvisende)."""
    path = tmp_path / "customers.csv"
    _write_csv(str(path), ["ext_customer_id", "ext_customer_name", "counterparty_country",
                           "ext_vat_bus_posting_group"],
               [{"ext_customer_id": "CU0001", "ext_customer_name": "50Hertz Transmission GmbH",
                 "counterparty_country": "DE", "ext_vat_bus_posting_group": "EU"}])
    customers, warnings = md.load_customers(str(path))
    assert warnings == []
    assert customers == [{"customer_id": "CU0001", "name": "50Hertz Transmission GmbH",
                          "vat_number": "", "country": "DE"}]


# --- enrich_canonical ---------------------------------------------------------

def test_enrich_canonical_no_sidecar_files_is_noop(tmp_path):
    """Fravær af alle tre filer -- uændret adfærd (samme strukturelle tomhed)."""
    csv_path = str(tmp_path / "gl_entries.csv")
    canonical = _bare_canonical()
    stamdata = md.enrich_canonical(canonical, csv_path)
    assert stamdata == {"vat_setup_koder": 0, "chart_of_accounts_konti": 0,
                        "customers": 0, "advarsler": []}
    assert canonical["tax_table"][0]["tax_percentage"] == 0.0
    assert canonical["transactions"][0]["lines"][0]["tax_percentage"] == 0.0
    assert canonical["accounts"][0]["account_type"] == ""
    assert canonical["customers"] == []


def test_enrich_canonical_with_all_three_sidecars(tmp_path):
    csv_path = str(tmp_path / "gl_entries.csv")
    _write_csv(str(tmp_path / "vat_setup.csv"), ["vat_codes", "tax_percentage"],
               [{"vat_codes": "DOMESTIC|REDUCED_PRIVATE_DKRC", "tax_percentage": "25.0"}])
    _write_csv(str(tmp_path / "chart_of_accounts.csv"), ["gl_accounts", "account_type", "standard_account_id"],
               [{"gl_accounts": "5820", "account_type": "Asset", "standard_account_id": "5800"}])
    _write_csv(str(tmp_path / "customers.csv"), ["customer_id", "name", "vat_number", "country"],
               [{"customer_id": "K001", "name": "Testkunde ApS", "vat_number": "DK12345678", "country": "DK"}])

    canonical = _bare_canonical()
    stamdata = md.enrich_canonical(canonical, csv_path)

    assert stamdata["vat_setup_koder"] == 1
    assert stamdata["chart_of_accounts_konti"] == 1
    assert stamdata["customers"] == 1
    assert stamdata["advarsler"] == []

    # tax_table + line-niveau tax_percentage joinet via vat_codes-strengen
    assert canonical["tax_table"][0]["tax_percentage"] == 25.0
    assert canonical["tax_table"][0]["rate"] == 25.0
    assert canonical["transactions"][0]["lines"][0]["tax_percentage"] == 25.0

    # accounts + line-niveau account_type/standard_account_id joinet via kontonummer
    assert canonical["accounts"][0]["account_type"] == "Asset"
    assert canonical["accounts"][0]["standard_account_id"] == "5800"
    assert canonical["transactions"][0]["lines"][0]["account_type"] == "Asset"
    assert canonical["transactions"][0]["lines"][0]["standard_account_id"] == "5800"

    # customers[] er fyldt strukturelt (KENDT BEGRÆNSNING: intet
    # customer_id-felt på linjerne at joine igennem -- se modulets docstring).
    assert canonical["customers"] == [{"customer_id": "K001", "name": "Testkunde ApS",
                                       "vat_number": "DK12345678", "country": "DK"}]
    assert canonical["transactions"][0]["lines"][0].get("customer_id", "") == ""


def test_enrich_canonical_unmatched_codes_leave_defaults(tmp_path):
    """En vat_setup.csv der ikke indeholder linjens faktiske momskode
    ændrer intet for den linje (ingen gætning på tværs af koder)."""
    csv_path = str(tmp_path / "gl_entries.csv")
    _write_csv(str(tmp_path / "vat_setup.csv"), ["vat_codes", "tax_percentage"],
               [{"vat_codes": "SOME|OTHER|CODE", "tax_percentage": "25.0"}])
    canonical = _bare_canonical()
    md.enrich_canonical(canonical, csv_path)
    assert canonical["tax_table"][0]["tax_percentage"] == 0.0
    assert canonical["transactions"][0]["lines"][0]["tax_percentage"] == 0.0


def test_enrich_canonical_explicit_paths_override_sidecar_convention(tmp_path):
    """Eksplicit angivne stier bruges i stedet for auto-discovery ved siden
    af csv_path (fx til at genbruge én stamdata-fil på tværs af flere
    kørsler/CSV'er)."""
    other_dir = tmp_path / "elsewhere"
    other_dir.mkdir()
    explicit_path = other_dir / "my_vat_setup.csv"
    _write_csv(str(explicit_path), ["vat_codes", "tax_percentage"],
               [{"vat_codes": "DOMESTIC|REDUCED_PRIVATE_DKRC", "tax_percentage": "25.0"}])
    csv_path = str(tmp_path / "gl_entries.csv")
    canonical = _bare_canonical()
    stamdata = md.enrich_canonical(canonical, csv_path, vat_setup_path=str(explicit_path))
    assert stamdata["vat_setup_koder"] == 1
    assert canonical["tax_table"][0]["tax_percentage"] == 25.0
