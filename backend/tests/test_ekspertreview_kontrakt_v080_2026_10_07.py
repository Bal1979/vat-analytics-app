"""
Ekspertreview af standardstrukturen (2026-10-07, Bal-godkendt; datakontrakt
v0.7.0 -> v0.8.0, additiv).

Dækker:
  1. Kontrakten: version 0.8.0, de nye felter (linjer/konti/leverandører/kunder)
     med kilde-flag, noter om forrang/fallback, ændringslog, og ingen felter
     fjernet (additiv).
  2. Forrangskæde for dokumentnr.: supplier_document_id > customer_document_id >
     source_document_id > invoice_numbers (pr. række) + diagnostik for linjer med
     BEGGE sider.
  3. Fallbackkæde for modpartsland: supplier_country > customer_country >
     counterparty_country; hierarkiet momsnr.-præfiks > landefelt (kontrol 70-75)
     er uændret.
  4. chart_of_accounts: name/description-adskillelse, account_type_name,
     account_tax_type (rå værdi), movement_balance + konsistensdiagnostik.
  5. standard_tax_code på leverandører (linjekolonne) og kunder (sidecar).
  6. Bagudkompatibilitet: uden de nye kolonner er output (på de eksisterende
     felter) identisk med den hidtidige adfærd.

Alle fixtures er opdigtede -- ingen kundedata, ingen kundenavne.
"""

import csv

from analytics.categories import cat09_reverse_charge as cat09
from analytics.categories import cat10_vat_reconciliation as cat10
from parsers import canonical_masterdata as md
from parsers import canonical_parser
from tools import build_data_contract as gen
from tools import data_contract_data as dcd
from validation.builders import mk_data, mk_line, mk_txn


_BASE = [
    "posting_dates", "tax_point", "vat_period", "credit_note_flag",
    "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
    "debit_amount", "credit_amount", "supply_direction", "currency_fx",
]


def _row(inv, date="2025-03-10", acct="6000", code="DOMESTIC|U25", vat="250.0",
         debit="1000.0", credit="0", extra=None):
    r = [date, date, date[:7], "false", inv, acct, code, vat, debit, credit, "purchase", ""]
    return r + list(extra or [])


def _parse(tmp_path, rows, extra_headers=()):
    path = tmp_path / "gl_entries.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(_BASE + list(extra_headers))
        w.writerows(rows)
    canonical, info = canonical_parser.parse_canonical(str(path))
    assert canonical is not None, info
    return canonical, info


def _lines(canonical):
    return [ln for t in canonical["transactions"] for ln in t["lines"]]


def _write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def _field(contract, obj, name, sub=False):
    o = contract["objekter"][obj]
    felter = o["sub_objekt"]["felter"] if sub else o["felter"]
    return next(f for f in felter if f["navn"] == name)


_DOC_COLS = ["supplier_document_id", "customer_document_id", "source_document_id"]
_COUNTRY_COLS = ["supplier_country", "customer_country", "counterparty_country"]


# === 1. Kontrakt ================================================================

def test_contract_version_is_080():
    assert dcd.CONTRACT_VERSION == "0.8.0"
    contract, _ = gen.build_contract()
    assert contract["contract_version"] == "0.8.0"


def test_contract_new_line_fields_exist_and_are_canonical_partial():
    contract, _ = gen.build_contract()
    for navn in ("supplier_document_id", "customer_document_id",
                 "supplier_country", "customer_country"):
        f = _field(contract, "transactions", navn, sub=True)
        assert f["kilder"] == {"excel": False, "saft": False, "canonical": "partial"}, navn
        assert f["obligatorisk"] is False
        assert f["ekstension"] is True


def test_contract_new_account_fields():
    contract, _ = gen.build_contract()
    for navn in ("name", "account_type_name", "account_tax_type", "movement_balance"):
        f = _field(contract, "accounts", navn)
        assert f["kilder"]["canonical"] == "partial", navn
        assert f["obligatorisk"] is False
    assert _field(contract, "accounts", "movement_balance")["type"].startswith("number")
    # Konsistensreglen er dokumenteret på både movement_balance og closing_balance.
    assert "closing_balance - opening_balance" in _field(contract, "accounts", "movement_balance")["noter"]
    assert "movement_balance = closing_balance - opening_balance" in \
        _field(contract, "accounts", "closing_balance")["noter"]
    # Normaliseringen af account_tax_type er dokumenteret som åben (rå værdi).
    assert "RÅ" in _field(contract, "accounts", "account_tax_type")["noter"]
    assert "ÅBEN TRÅD" in _field(contract, "accounts", "account_tax_type")["noter"]


def test_contract_standard_tax_code_on_suppliers_and_customers():
    contract, _ = gen.build_contract()
    for obj in ("suppliers", "customers"):
        f = _field(contract, obj, "standard_tax_code")
        assert f["kilder"]["canonical"] == "partial"
        assert f["obligatorisk"] is False


def test_contract_notes_document_chains_and_rationale():
    contract, _ = gen.build_contract()
    doc = _field(contract, "transactions", "source_document_id", sub=True)["noter"]
    assert "supplier_document_id > customer_document_id > source_document_id > invoice_numbers" in doc
    # Den hidtidige noteindledning (v0.7.0) er bevaret.
    assert "Eksternt dokumentnr.; fallback: bilagsnøglen på ældre kanoniske filer" in doc
    country = _field(contract, "transactions", "country", sub=True)["noter"]
    assert "supplier_country > customer_country > counterparty_country" in country
    assert "kontrol 70-75" in country
    sup = _field(contract, "transactions", "supplier_document_id", sub=True)["noter"]
    assert "tvetydig" in sup and "BEGGE sider" in sup


def test_contract_is_additive_no_fields_removed():
    """Alle v0.7.0-feltnavne findes stadig (nøgleudsnit); +10 nye felter = 89."""
    contract, _ = gen.build_contract()
    assert contract["antal_felter"] == 89
    must_exist = {
        ("accounts", "description", False), ("accounts", "opening_balance", False),
        ("accounts", "closing_balance", False), ("transactions", "source_document_id", True),
        ("transactions", "country", True), ("transactions", "supplier_id", True),
        ("suppliers", "country", False), ("customers", "country", False),
    }
    for obj, navn, sub in must_exist:
        assert _field(contract, obj, navn, sub=sub)


def test_contract_changelog_documents_every_new_field():
    contract, _ = gen.build_contract()
    entry = next(e for e in contract["aendringslog"] if e["version"] == "0.8.0")
    felter = {c["felt"] for c in entry["aendringer"]}
    for sti in (
        "transactions[].lines[].supplier_document_id", "transactions[].lines[].customer_document_id",
        "transactions[].lines[].supplier_country", "transactions[].lines[].customer_country",
        "accounts[].name", "accounts[].account_type_name", "accounts[].account_tax_type",
        "accounts[].movement_balance", "suppliers[].standard_tax_code",
        "customers[].standard_tax_code",
    ):
        assert sti in felter, sti
    note_blob = " ".join(c["note"] for c in entry["aendringer"])
    assert "supplier_document_id > customer_document_id > source_document_id" in note_blob
    assert "supplier_country > customer_country > counterparty_country" in note_blob


# === 2. Dokumentnr.: forrangskæde ===============================================

def test_doc_chain_each_level_wins_in_order(tmp_path):
    rows = [
        # supplier + customer + source + invoice -> supplier
        _row("B1", date="2025-03-01", extra=["S-1", "C-1", "EXT-1"]),
        # customer + source + invoice -> customer
        _row("B2", date="2025-03-02", extra=["", "C-2", "EXT-2"]),
        # source + invoice -> source
        _row("B3", date="2025-03-03", extra=["", "", "EXT-3"]),
        # kun invoice -> invoice
        _row("B4", date="2025-03-04", extra=["", "", ""]),
    ]
    canonical, info = _parse(tmp_path, rows, _DOC_COLS)
    docs = [ln["source_document_id"] for ln in _lines(canonical)]
    assert docs == ["S-1", "C-2", "EXT-3", "B4"]
    efl = info["eksterne_linjefelter"]
    assert efl["linjer_supplier_dokumentnr"] == 1
    assert efl["linjer_customer_dokumentnr"] == 2
    assert efl["linjer_eksternt_dokumentnr"] == 1          # kun rækker hvor source_document_id vandt
    assert efl["linjer_dokumentnr_fallback_bilagsnr"] == 1


def test_doc_new_fields_are_carried_on_the_line_and_trimmed(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1", extra=["  S-1 ", " C-1 ", ""])], _DOC_COLS)
    (line,) = _lines(canonical)
    assert line["supplier_document_id"] == "S-1"
    assert line["customer_document_id"] == "C-1"


def test_doc_only_customer_side_column(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1", extra=["K-77"])], ["customer_document_id"])
    (line,) = _lines(canonical)
    assert line["source_document_id"] == "K-77"
    assert line["supplier_document_id"] == ""


def test_lines_with_both_sides_are_counted(tmp_path):
    rows = [
        _row("B1", date="2025-03-01", extra=["S-1", "C-1", "", "DE", ""]),   # begge sider
        _row("B2", date="2025-03-02", extra=["S-2", "", "", "", ""]),         # kun leverandør
        _row("B3", date="2025-03-03", extra=["", "C-3", "", "", "FR"]),       # kun kunde
        _row("B4", date="2025-03-04", extra=["", "", "", "", ""]),
    ]
    canonical, info = _parse(
        tmp_path, rows,
        ["supplier_document_id", "customer_document_id", "supplier_id", "supplier_country",
         "customer_country"],
    )
    # kolonnerækkefølgen i extra følger headerne ovenfor: S-doc, C-doc, supplier_id, s-land, c-land
    assert info["eksterne_linjefelter"]["linjer_med_begge_sider"] == 1
    both = [ln for ln in _lines(canonical)
            if ln["supplier_document_id"] and ln["customer_document_id"]]
    assert len(both) == 1 and both[0]["source_document_id"] == "S-1"   # leverandørsiden vinder


def test_doc_without_new_columns_is_identical_to_v070_behaviour(tmp_path):
    # Eksplicit source_document_id + fallback, som i v0.7.0-testene.
    canonical, info = _parse(tmp_path, [
        _row("B1", extra=["EXT-1"]),
        _row("B2", date="2025-03-11", extra=[""]),
    ], ["source_document_id"])
    assert [ln["source_document_id"] for ln in _lines(canonical)] == ["EXT-1", "B2"]
    efl = info["eksterne_linjefelter"]
    assert (efl["linjer_eksternt_dokumentnr"], efl["linjer_dokumentnr_fallback_bilagsnr"]) == (1, 1)
    assert efl["linjer_supplier_dokumentnr"] == efl["linjer_customer_dokumentnr"] == 0
    assert not (efl["supplier_document_id_kolonne"] or efl["customer_document_id_kolonne"])
    assert all(ln["supplier_document_id"] == "" and ln["customer_document_id"] == ""
               for ln in _lines(canonical))


def test_doc_grouping_is_not_affected_by_new_columns(tmp_path):
    """Bilagsgrupperingen bruger kun bilagsnummeret -- ikke de nye dokumentnumre."""
    canonical, _ = _parse(tmp_path, [
        _row("B1", acct="6000", extra=["S-1", ""]),
        _row("B1", acct="2400", debit="0", credit="1000.0", extra=["S-2", ""]),
    ], ["supplier_document_id", "customer_document_id"])
    assert len(canonical["transactions"]) == 1
    assert [ln["source_document_id"] for ln in _lines(canonical)] == ["S-1", "S-2"]


# === 3. Modpartsland: fallbackkæde ==============================================

def test_country_chain_each_level_wins_in_order(tmp_path):
    rows = [
        _row("B1", date="2025-03-01", extra=["SE", "FI", "DK"]),   # supplier
        _row("B2", date="2025-03-02", extra=["", "FI", "DK"]),     # customer
        _row("B3", date="2025-03-03", extra=["", "", "DK"]),       # counterparty
        _row("B4", date="2025-03-04", extra=["", "", ""]),         # intet
    ]
    canonical, info = _parse(tmp_path, rows, _COUNTRY_COLS)
    assert [ln["country"] for ln in _lines(canonical)] == ["SE", "FI", "DK", ""]
    lines = _lines(canonical)
    assert (lines[0]["supplier_country"], lines[0]["customer_country"]) == ("SE", "FI")
    assert info["eksterne_linjefelter"]["linjer_med_sidespecifikt_land"] == 2


def test_country_without_new_columns_is_the_counterparty_column_as_before(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1", extra=["DENMARK"])], ["counterparty_country"])
    (line,) = _lines(canonical)
    assert line["country"] == "DENMARK"      # rå værdi, uændret
    assert line["supplier_country"] == "" and line["customer_country"] == ""
    canonical, _ = _parse(tmp_path, [_row("B1")])
    assert _lines(canonical)[0]["country"] == ""


def test_country_hierarchy_vat_prefix_over_country_field_is_unchanged(tmp_path):
    """Kontrol 70-75: momsnr.-præfiks > landefelt. Et DE-momsnummer på en linje
    hvis (nye) leverandørland er DK giver stadig DE; uden præfiks bruges landefeltet."""
    canonical, _ = _parse(tmp_path, [
        _row("B1", date="2025-03-01", extra=["DK", "DE123456789"]),
        _row("B2", date="2025-03-02", extra=["SE", ""]),
    ], ["supplier_country", "vat_registration_numbers"])
    l1, l2 = _lines(canonical)
    assert cat09._country(l1, {}) == "DE"       # præfikset vinder over landefeltet "DK"
    assert cat09._country(l2, {}) == "SE"       # intet præfiks -> landefeltet
    assert cat09._country_mismatch(l1, {}) is True


def test_suppliers_country_prefers_supplier_country(tmp_path):
    canonical, _ = _parse(tmp_path, [
        _row("B1", extra=["L-1", "Leverandør Et", "SE", "DK"]),
    ], ["supplier_id", "supplier_name", "supplier_country", "counterparty_country"])
    (sup,) = canonical["suppliers"]
    assert sup["country"] == "SE"
    # Uden supplier_country: som før (linjens samlede land).
    canonical, _ = _parse(tmp_path, [
        _row("B1", extra=["L-1", "Leverandør Et", "DK"]),
    ], ["supplier_id", "supplier_name", "counterparty_country"])
    assert canonical["suppliers"][0]["country"] == "DK"


# === 4. chart_of_accounts =======================================================

def _coa_parse(tmp_path, coa_rows, fieldnames):
    path = tmp_path / "gl_entries.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(_BASE)
        w.writerow(_row("B1", acct="6000"))
        w.writerow(_row("B2", date="2025-03-11", acct="6100"))
    _write_csv(str(tmp_path / "chart_of_accounts.csv"), fieldnames, coa_rows)
    canonical, info = canonical_parser.parse_canonical(str(path))
    assert canonical is not None, info
    return canonical, info


def test_accounts_always_carry_the_new_keys_with_no_signal_defaults(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1")])
    (acc,) = canonical["accounts"]
    assert acc["name"] == "" and acc["account_type_name"] == "" and acc["account_tax_type"] == ""
    assert acc["movement_balance"] is None         # None, ikke 0.0
    assert acc["opening_balance"] == 0.0 and acc["closing_balance"] == 0.0   # uændret default


def test_coa_new_columns_land_on_accounts(tmp_path):
    canonical, info = _coa_parse(tmp_path, [
        {"gl_accounts": "6000", "account_type": "Expense", "ext_name": "Kontonavn A",
         "ext_description": "Beskrivelse af A", "ext_account_type_name": "Resultatkonto",
         "ext_account_tax_type": " Input VAT ", "ext_movement_balance": "1200.50",
         "opening_balance": "100", "closing_balance": "1300.50"},
        {"gl_accounts": "6100", "account_type": "Asset", "ext_name": "Kontonavn B"},
    ], ["gl_accounts", "account_type", "ext_name", "ext_description", "ext_account_type_name",
        "ext_account_tax_type", "ext_movement_balance", "opening_balance", "closing_balance"])
    a, b = canonical["accounts"]
    assert a["name"] == "Kontonavn A" and a["description"] == "Beskrivelse af A"
    assert a["account_type_name"] == "Resultatkonto"
    assert a["account_tax_type"] == "Input VAT"                # rå, kun trimmet
    assert a["movement_balance"] == 1200.5
    # 1300.50 - 100 == 1200.50 -> konsistent
    sd = info["stamdata"]
    assert sd["konti_movement_tjekket"] == 1 and sd["konti_movement_inkonsistent"] == 0
    # Konto B: uden ext_description er description den hidtidige alias-kæde (= navnet).
    assert b["name"] == "Kontonavn B" and b["description"] == "Kontonavn B"
    assert b["movement_balance"] is None


def test_coa_movement_inconsistency_is_counted_not_flagged(tmp_path):
    canonical, info = _coa_parse(tmp_path, [
        {"gl_accounts": "6000", "ext_movement_balance": "999", "opening_balance": "0",
         "closing_balance": "1000"},
    ], ["gl_accounts", "ext_movement_balance", "opening_balance", "closing_balance"])
    assert info["stamdata"]["konti_movement_inkonsistent"] == 1
    assert canonical["accounts"][0]["movement_balance"] == 999.0   # værdien bæres uændret
    # movement uden opening/closing kan ikke tjekkes
    _, info = _coa_parse(tmp_path, [
        {"gl_accounts": "6000", "ext_movement_balance": "5"},
    ], ["gl_accounts", "ext_movement_balance"])
    assert info["stamdata"]["konti_movement_tjekket"] == 0


def test_coa_without_new_columns_is_unchanged(tmp_path):
    """Legacy-alias-kæden: kun ext_name -> description OG name får værdien; ingen
    movement-diagnostik i stamdata (uændret dict)."""
    canonical, info = _coa_parse(tmp_path, [
        {"gl_accounts": "6000", "account_type": "Expense", "ext_name": "Kontonavn A"},
    ], ["gl_accounts", "account_type", "ext_name"])
    (a, _b) = canonical["accounts"]
    assert a["description"] == "Kontonavn A" and a["name"] == "Kontonavn A"
    assert "konti_movement_tjekket" not in info["stamdata"]
    assert set(info["stamdata"]) == {"vat_setup_koder", "chart_of_accounts_konti",
                                     "customers", "advarsler"}


def _mk(tmp_path, name, fieldnames, rows):
    path = tmp_path / name
    _write_csv(str(path), fieldnames, rows)
    return path


def test_coa_legacy_description_column_still_feeds_description(tmp_path):
    p = _mk(tmp_path, "coa1.csv", ["gl_accounts", "description"],
            [{"gl_accounts": "6000", "description": "Gammelt navn"}])
    lookup, _ = md.load_chart_of_accounts(str(p))
    assert lookup["6000"]["description"] == "Gammelt navn"
    assert lookup["6000"]["name"] == "Gammelt navn"


def _acc(**kw):
    base = {"account_id": "961100", "account_type": "", "opening_balance": 0.0,
            "closing_balance": 9999.0}
    base.update(kw)
    return base


def test_control77_matches_on_name_when_description_is_a_separate_text():
    """Kontonavnet (name) bærer 'moms'; beskrivelsen gør ikke -- kontrol 77 må
    ikke miste kontoen, fordi beskrivelsen nu er adskilt fra navnet."""
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[_acc(name="Salgsmoms", description="Skyldig afgift til SKAT")],
    )
    findings = cat10.test_77_vat_account_reconciliation(data)
    assert len(findings) == 1 and findings[0]["test_id"] == 77


def test_control77_unchanged_without_name_field_and_no_false_match():
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[_acc(description="Momsafregning")],       # Excel/SAF-T-formen: kun description
    )
    assert len(cat10.test_77_vat_account_reconciliation(data)) == 1
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[_acc(name="Kassebeholdning", description="Kontant")],
    )
    assert cat10.test_77_vat_account_reconciliation(data) == []


# === 5. standard_tax_code =======================================================

def test_suppliers_standard_tax_code_from_line_column(tmp_path):
    canonical, _ = _parse(tmp_path, [
        _row("B1", date="2025-03-01", extra=["L-1", "Et", ""]),
        _row("B2", date="2025-03-02", extra=["L-1", "Et", "I25"]),
        _row("B3", date="2025-03-03", extra=["L-2", "To", "I0"]),
    ], ["supplier_id", "supplier_name", "supplier_standard_tax_code"])
    by_id = {s["supplier_id"]: s for s in canonical["suppliers"]}
    assert by_id["L-1"]["standard_tax_code"] == "I25"    # første ikke-tomme
    assert by_id["L-2"]["standard_tax_code"] == "I0"


def test_suppliers_standard_tax_code_empty_without_column(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1", extra=["L-1", "Et"])], ["supplier_id", "supplier_name"])
    assert canonical["suppliers"][0]["standard_tax_code"] == ""


def test_customers_standard_tax_code(tmp_path):
    p = _mk(tmp_path, "customers.csv", ["customer_id", "name", "ext_standard_tax_code"],
            [{"customer_id": "K1", "name": "Kunde Et", "ext_standard_tax_code": " U25 "}])
    customers, _ = md.load_customers(str(p))
    assert customers[0]["standard_tax_code"] == "U25"
    p = _mk(tmp_path, "customers2.csv", ["customer_id", "name", "standard_tax_code"],
            [{"customer_id": "K1", "name": "Kunde Et", "standard_tax_code": "U0"}])
    customers, _ = md.load_customers(str(p))
    assert customers[0]["standard_tax_code"] == "U0"
    p = _mk(tmp_path, "customers3.csv", ["customer_id", "name"],
            [{"customer_id": "K1", "name": "Kunde Et"}])
    customers, _ = md.load_customers(str(p))
    assert customers[0]["standard_tax_code"] == ""
