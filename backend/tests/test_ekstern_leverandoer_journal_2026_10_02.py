"""
Opfølgningsrunden 2026-10-02 (Bal-godkendt; eskaleret fra vat-extracts
grundarbejds-runde, commit 2681d08; datakontrakt v0.6.0 -> v0.7.0, additiv).

vat-extract kan nu levere ``supplier_id``, ``supplier_name``, ``journal_id`` og
``source_document_id`` i den kanoniske CSV. Dækker:
  1. Kontrakten: kilder.canonical er true for de tre felter, suppliers[] er
     "partial", feltnoten for source_document_id dokumenterer semantikken,
     ingen felter forsvundet (additiv).
  2. Parseren læser kolonnerne: supplier_id/-name på linjen, journal_id på
     TRANSAKTIONEN (kontraktens placering), nøglesæt-symmetri uden kolonnerne.
  3. source_document_id: den eksterne værdi har FORRANG; ellers (ingen kolonne /
     tom række) fallback til bilagsnummeret -- UÆNDRET for ældre kanoniske filer.
     Bilagsgrupperingen berøres ikke.
  4. suppliers[] afledes af linjerne (Excel-mønsteret), og modparts-kontroller
     ser linje-leveret supplier_id (kontrol 11, 47, 84).

Alle fixtures er opdigtede -- ingen kundedata.
"""

import csv

from analytics.categories import cat02_duplicate_detection as cat02
from analytics.categories import cat06_party_validation as cat06
from analytics.categories import cat11_fraud_mtic as cat11
from parsers import canonical_parser
from tools import build_data_contract as gen
from tools import data_contract_data as dcd


_BASE = [
    "posting_dates", "tax_point", "vat_period", "credit_note_flag",
    "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
    "debit_amount", "credit_amount", "supply_direction", "currency_fx",
]


def _row(inv, date="2025-03-10", acct="6000", code="DOMESTIC|U25", vat="250.0",
         debit="1000.0", credit="0", extra=None):
    r = [date, date, date[:7], "false", inv, acct, code, vat, debit, credit, "purchase", ""]
    return r + list(extra or [])


def _write(tmp_path, rows, extra_headers=(), name="gl_entries.csv"):
    path = tmp_path / name
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(_BASE + list(extra_headers))
        w.writerows(rows)
    return str(path)


def _parse(tmp_path, rows, extra_headers=()):
    canonical, info = canonical_parser.parse_canonical(_write(tmp_path, rows, extra_headers))
    assert canonical is not None, info
    return canonical, info


def _lines(canonical):
    return [ln for t in canonical["transactions"] for ln in t["lines"]]


# --- 1. Kontrakt -----------------------------------------------------------------

def _field(contract, obj, name, sub=False):
    o = contract["objekter"][obj]
    felter = o["sub_objekt"]["felter"] if sub else o["felter"]
    return next(f for f in felter if f["navn"] == name)


def test_contract_v070_canonical_sources_true_for_the_fields():
    assert dcd.CONTRACT_VERSION == "0.7.0"
    contract, _ = gen.build_contract()
    for navn in ("supplier_id", "supplier_name", "source_document_id"):
        f = _field(contract, "transactions", navn, sub=True)
        assert f["kilder"]["canonical"] is True, navn
    # journal_id er et TRANSAKTIONSfelt i kontrakten (ikke et linjefelt).
    assert _field(contract, "transactions", "journal_id")["kilder"]["canonical"] is True
    line_names = {f["navn"] for f in contract["objekter"]["transactions"]["sub_objekt"]["felter"]}
    assert "journal_id" not in line_names


def test_contract_v070_suppliers_list_is_partial_and_additive():
    contract, _ = gen.build_contract()
    for navn in ("supplier_id", "name", "vat_number", "country"):
        assert _field(contract, "suppliers", navn)["kilder"]["canonical"] == "partial", navn
    # Additiv: antallet af kontraktfelter er uændret (79), kun kilder/noter ændret.
    total = sum(
        len(o["felter"]) + (len(o["sub_objekt"]["felter"]) if "sub_objekt" in o else 0)
        for o in contract["objekter"].values()
    )
    assert total == 79


def test_contract_source_document_id_note_documents_semantics():
    contract, _ = gen.build_contract()
    note = _field(contract, "transactions", "source_document_id", sub=True)["noter"]
    assert "Eksternt dokumentnr.; fallback: bilagsnøglen på ældre kanoniske filer" in note
    assert "FORRANG" in note


# --- 2. Parseren læser kolonnerne -----------------------------------------------

def test_supplier_columns_land_on_the_line(tmp_path):
    canonical, info = _parse(tmp_path, [
        _row("B1", extra=["L-100", "Testleverandør A/S", "FINANS", "EXT-1"]),
    ], ["supplier_id", "supplier_name", "journal_id", "source_document_id"])
    (line,) = _lines(canonical)
    assert line["supplier_id"] == "L-100"
    assert line["supplier_name"] == "Testleverandør A/S"
    assert "journal_id" not in line          # kontrakten: transaktionsfelt
    efl = info["eksterne_linjefelter"]
    assert efl["supplier_id_kolonne"] and efl["journal_id_kolonne"]
    assert efl["linjer_med_leverandoer"] == 1 and efl["leverandoerer"] == 1


def test_values_are_trimmed(tmp_path):
    canonical, _ = _parse(tmp_path, [
        _row("B1", extra=["  L-100 ", "  Navn  ", " J1 ", " EXT-1 "]),
    ], ["supplier_id", "supplier_name", "journal_id", "source_document_id"])
    (line,) = _lines(canonical)
    assert (line["supplier_id"], line["supplier_name"]) == ("L-100", "Navn")
    assert canonical["transactions"][0]["journal_id"] == "J1"
    assert line["source_document_id"] == "EXT-1"


def test_journal_id_is_on_the_transaction(tmp_path):
    canonical, _ = _parse(tmp_path, [
        _row("B1", extra=["", "", "FINANS", ""]),
        _row("B2", date="2025-03-11", extra=["", "", "KASSE", ""]),
    ], ["supplier_id", "supplier_name", "journal_id", "source_document_id"])
    assert [t["journal_id"] for t in canonical["transactions"]] == ["FINANS", "KASSE"]


def test_journal_id_defaults_to_import_without_column_or_value(tmp_path):
    canonical, _ = _parse(tmp_path, [_row("B1")])
    assert canonical["transactions"][0]["journal_id"] == "IMPORT"
    canonical, _ = _parse(tmp_path, [_row("B1", extra=["", "", "", ""])],
                          ["supplier_id", "supplier_name", "journal_id", "source_document_id"])
    assert canonical["transactions"][0]["journal_id"] == "IMPORT"


def test_grouped_transaction_takes_first_nonempty_journal_and_counts_conflicts(tmp_path):
    canonical, info = _parse(tmp_path, [
        _row("B1", acct="6000", extra=["", "", "", ""]),
        _row("B1", acct="2400", debit="0", credit="1000.0", extra=["", "", "FINANS", ""]),
        _row("B1", acct="2401", debit="0", credit="0.0", extra=["", "", "KASSE", ""]),
    ], ["supplier_id", "supplier_name", "journal_id", "source_document_id"])
    assert len(canonical["transactions"]) == 1
    assert canonical["transactions"][0]["journal_id"] == "FINANS"
    assert info["eksterne_linjefelter"]["bilag_med_flere_journal_id"] == 1


def test_without_the_columns_behaviour_is_unchanged(tmp_path):
    canonical, info = _parse(tmp_path, [_row("B1"), _row("B2", date="2025-03-11")])
    for ln in _lines(canonical):
        assert ln["supplier_id"] == "" and ln["supplier_name"] == ""
        assert ln["source_document_id"] in ("B1", "B2")
    assert canonical["suppliers"] == []
    efl = info["eksterne_linjefelter"]
    assert not any(efl[k] for k in ("supplier_id_kolonne", "supplier_name_kolonne",
                                    "journal_id_kolonne", "source_document_id_kolonne"))
    assert efl["linjer_eksternt_dokumentnr"] == 0
    assert efl["linjer_dokumentnr_fallback_bilagsnr"] == 2


# --- 3. source_document_id: forrang / fallback ---------------------------------

_ALL4 = ["supplier_id", "supplier_name", "journal_id", "source_document_id"]


def test_external_document_number_takes_precedence(tmp_path):
    canonical, info = _parse(tmp_path, [
        _row("B1", extra=["L-1", "A", "", "LEV-FAKTURA-77"]),
    ], _ALL4)
    (line,) = _lines(canonical)
    assert line["source_document_id"] == "LEV-FAKTURA-77"
    assert info["eksterne_linjefelter"]["linjer_eksternt_dokumentnr"] == 1
    assert info["eksterne_linjefelter"]["linjer_dokumentnr_fallback_bilagsnr"] == 0


def test_empty_external_value_falls_back_to_voucher_number_per_row(tmp_path):
    canonical, info = _parse(tmp_path, [
        _row("B1", extra=["L-1", "A", "", "LEV-1"]),
        _row("B2", date="2025-03-11", extra=["L-1", "A", "", ""]),
    ], _ALL4)
    docs = [ln["source_document_id"] for ln in _lines(canonical)]
    assert docs == ["LEV-1", "B2"]
    efl = info["eksterne_linjefelter"]
    assert (efl["linjer_eksternt_dokumentnr"], efl["linjer_dokumentnr_fallback_bilagsnr"]) == (1, 1)


def test_column_absent_is_byte_for_byte_the_old_behaviour(tmp_path):
    """Uden kolonnen er source_document_id præcis bilagsnummeret (som før)."""
    canonical, _ = _parse(tmp_path, [_row("7000123"), _row("")])
    docs = [ln["source_document_id"] for ln in _lines(canonical)]
    assert docs == ["7000123", ""]


def test_external_number_never_changes_the_voucher_grouping(tmp_path):
    """To rækker med samme bilagsnr./dato er ÉT bilag, uanset hvad det eksterne
    nr. er; to bilag med samme eksterne nr. forbliver to bilag."""
    canonical, _ = _parse(tmp_path, [
        _row("B1", acct="6000", extra=["L-1", "A", "", "EXT-A"]),
        _row("B1", acct="2400", debit="0", credit="1250.0", extra=["L-1", "A", "", "EXT-B"]),
        _row("B2", date="2025-03-12", extra=["L-1", "A", "", "EXT-A"]),
    ], _ALL4)
    assert [len(t["lines"]) for t in canonical["transactions"]] == [2, 1]
    assert canonical["transactions"][0]["transaction_id"] == "DOC_B1_2025-03-10"


# --- 4. suppliers[] + modparts-kontroller --------------------------------------

def test_suppliers_list_derived_from_lines_first_nonempty_wins(tmp_path):
    canonical, info = _parse(
        tmp_path,
        [
            _row("B1", extra=["L-1", "", "", "", "", ""]),
            _row("B2", date="2025-03-11", extra=["L-1", "Leverandør Et", "", "", "DE123456789", "DE"]),
            _row("B3", date="2025-03-12", extra=["L-2", "Leverandør To", "", "", "", ""]),
            _row("B4", date="2025-03-13", extra=["", "", "", "", "", ""]),
        ],
        ["supplier_id", "supplier_name", "journal_id", "source_document_id",
         "vat_registration_numbers", "counterparty_country"],
    )
    assert canonical["suppliers"] == [
        {"supplier_id": "L-1", "name": "Leverandør Et", "vat_number": "DE123456789", "country": "DE"},
        {"supplier_id": "L-2", "name": "Leverandør To", "vat_number": "", "country": ""},
    ]
    assert info["eksterne_linjefelter"]["leverandoerer"] == 2


def test_control_11_exact_duplicate_sees_line_supplier_and_external_doc(tmp_path):
    """Samme leverandørfaktura bogført to gange: FORSKELLIGE interne bilagsnumre
    men samme EKSTERNE nr. -> kun fanget, fordi det eksterne nr. har forrang."""
    rows = [
        _row("B100", extra=["L-1", "Leverandør Et", "", "LEV-FAKTURA-9"]),
        _row("B101", extra=["L-1", "Leverandør Et", "", "LEV-FAKTURA-9"]),
    ]
    with_ext, _ = _parse(tmp_path, rows, _ALL4)
    assert len(cat02.test_11_exact_duplicate(with_ext)) == 1

    # Samme data uden source_document_id-kolonnen: de interne numre er forskellige
    # -> ingen dublet (uændret ældre adfærd).
    without_ext, _ = _parse(tmp_path, rows_no_ext(rows), ["supplier_id", "supplier_name", "journal_id"])
    assert cat02.test_11_exact_duplicate(without_ext) == []


def rows_no_ext(rows):
    return [r[:-1] for r in rows]


def test_control_47_sees_supplier_id_from_the_line(tmp_path):
    canonical, _ = _parse(tmp_path, [
        _row("B1", extra=["L-1", "", "", ""]),          # id uden navn -> fund
        _row("B2", date="2025-03-11", extra=["L-2", "Navngiven", "", ""]),
    ], _ALL4)
    findings = cat06.test_47_missing_party_name(canonical)
    assert len(findings) == 1
    assert "L-1" in findings[0]["description"]


def test_control_84_family_gets_country_and_vat_via_supplier_join(tmp_path):
    """Kontrol 84 henter land/momsnr. på linjen FØR leverandørlisten. Linje B har
    hverken land eller momsnr., men leverandøren har begge på linje A -> via
    suppliers[] (afledt af linje-leveret supplier_id) ses "udenlandsk
    leverandør" + "ugyldigt momsnr" + "højt beløb" = 3 faktorer. Uden
    supplier_id-kolonnen er det samme datasæt kun 2 faktorer (ingen join)."""
    headers = ["supplier_id", "supplier_name", "journal_id", "source_document_id",
               "vat_registration_numbers", "counterparty_country"]
    rows = [
        _row("B1", debit="100.0", vat="25.0", extra=["L-DE", "Udenlandsk ApS", "", "X1", "DE12", "DE"]),
        _row("B2", date="2025-03-11", debit="80000.0", vat="20000.0",
             extra=["L-DE", "Udenlandsk ApS", "", "X2", "", ""]),
    ]
    with_sup, _ = _parse(tmp_path, rows, headers)
    lookup = {s["supplier_id"]: s for s in with_sup["suppliers"]}
    f84 = cat11.test_84_missing_trader(with_sup, lookup)
    assert len(f84) == 1
    flags = f84[0]["transactions"][0]["risk_flags"]
    assert {"udenlandsk leverandør", "ugyldigt momsnr", "højt beløb"} <= set(flags)

    # Samme data, men uden de fire nye kolonner (supplier_id/-name tomme): ingen join.
    headers_old = ["vat_registration_numbers", "counterparty_country"]
    rows_old = [r[:12] + r[16:] for r in rows]
    without_sup, _ = _parse(tmp_path, rows_old, headers_old)
    assert without_sup["suppliers"] == []
    assert cat11.test_84_missing_trader(without_sup, {}) == []
