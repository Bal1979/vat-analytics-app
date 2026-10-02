"""
Indenlandsk Bus.-gruppe som eksplicit materiality-konstant (2026-10-02,
bifund fra Modtagermoms-runden; IMPLEMENTERET MEN AFVENTER Bals endelige
godkendelse).

Baggrund (INGEN kundenavne/-tekster i denne fil): "DOMESTIC" var hårdkodet som
den indenlandske Bus.-gruppe (vat_codes-strengens FØRSTE led i BC's
"gruppe|kode"-konvention) tre steder: cat10._purchase_rubric, cat13.test_104
og fallbacket i vat_rules.is_reverse_charge_sale_code. Dansk BC bruger
"INDLAND" i stedet. Empirisk målt 2026-10-02 på fire danske BC-momsopsætnings-
filer (49 rækker): Momsvirksomhedsbogf.gruppe har PRÆCIS værdirummet
{"INDLAND" (20), "EU" (8), "" blank (21)}.

Nu: ``materiality.DOMESTIC_BUS_GROUP_VALUES`` (default ["domestic","indland"],
env ``MATERIALITY_DOMESTIC_BUS_GROUP_VALUES``) + ÉN fælles funktion
``vat_rules.is_domestic_bus_group`` som alle tre steder bruger.
"""

import pathlib

import pytest

from analytics import materiality
from analytics import vat_rules as vr
from analytics.categories import cat10_vat_reconciliation as cat10
from analytics.categories import cat13_cross_dimension as cat13
from validation.builders import mk_data, mk_line, mk_txn


run_104 = cat13.test_104_foreign_currency_domestic_vat

# Det EMPIRISK målte værdirum for dansk BC (se modul-docstring) -> indenlandsk?
DANSK_BC_GRUPPEVAERDIRUM = {"INDLAND": True, "EU": False, "": False}


# === Konstanten ============================================================

def test_default_er_domestic_og_indland():
    assert materiality.DOMESTIC_BUS_GROUP_VALUES == ["domestic", "indland"]


def test_dansk_bc_gruppevaerdirum_klassificeres_korrekt():
    for value, expected in DANSK_BC_GRUPPEVAERDIRUM.items():
        assert vr.is_domestic_bus_group(value) is expected, repr(value)


@pytest.mark.parametrize("value", ["DOMESTIC", "domestic", "Domestic", "  INDLAND ", "indland", "Indland"])
def test_indenlandsk_case_og_whitespace_normaliseret(value):
    assert vr.is_domestic_bus_group(value) is True


@pytest.mark.parametrize("value", ["EU", "OUTSIDE DK/EU", "UDLAND", "INDLANDS", "DOMESTICS", "", "   ", None])
def test_ikke_indenlandsk_og_ingen_fuzzy_match(value):
    assert vr.is_domestic_bus_group(value) is False


def test_env_override_erstatter_hele_listen(monkeypatch):
    monkeypatch.setattr(materiality, "DOMESTIC_BUS_GROUP_VALUES", ["domestic"])
    assert vr.is_domestic_bus_group("DOMESTIC") is True
    assert vr.is_domestic_bus_group("INDLAND") is False  # tilbage til hidtidig adfærd
    monkeypatch.setattr(materiality, "DOMESTIC_BUS_GROUP_VALUES", ["domestic", "indland", "udland-dk"])
    assert vr.is_domestic_bus_group("Udland-DK") is True


def test_env_parsing_via_strlist(monkeypatch):
    monkeypatch.setenv("MATERIALITY_DOMESTIC_BUS_GROUP_VALUES", "Domestic, INDLAND ,hjemme")
    values = [v.strip().lower() for v in materiality._strlist(
        "MATERIALITY_DOMESTIC_BUS_GROUP_VALUES", ["domestic", "indland"])]
    assert values == ["domestic", "indland", "hjemme"]


# === Kaldssted 1: cat10._purchase_rubric (indenlandsk RC -> 'dkrc') =========

def test_cat10_dansk_indland_modtagermoms_er_dkrc():
    assert cat10.classify_purchase_rubric("INDLAND|REVERSE", "Modtagermoms") == "dkrc"


def test_cat10_engelsk_domestic_reverse_charge_uaendret_dkrc():
    assert cat10.classify_purchase_rubric("DOMESTIC|DKRC", "Reverse Charge VAT") == "dkrc"
    assert cat10.classify_purchase_rubric("Domestic|REVERSE", "Reverse Charge VAT") == "dkrc"


def test_cat10_dansk_eu_modtagermoms_er_ikke_dkrc():
    # EU er udenlandsk RC: uden SERVICE_VAT-mønster -> 'input' (som for engelsk EU).
    assert cat10.classify_purchase_rubric("EU|MOMS", "Modtagermoms") == "input"


def test_cat10_normal_moms_paa_indland_er_input():
    assert cat10.classify_purchase_rubric("INDLAND|MOMS", "Normal moms") == "input"


def test_cat10_blank_bus_gruppe_er_bevidst_ikke_indenlandsk():
    """ÅBENT PUNKT (målt: 4 blanke 'REVERSE'/Modtagermoms-rækker): blank gruppe
    antages IKKE indenlandsk -- ingen gæt. Dokumenterer den nuværende adfærd."""
    assert cat10.classify_purchase_rubric("|REVERSE", "Modtagermoms") == "input"


def test_cat10_env_override_uden_indland_giver_gammel_adfaerd(monkeypatch):
    monkeypatch.setattr(materiality, "DOMESTIC_BUS_GROUP_VALUES", ["domestic"])
    assert cat10.classify_purchase_rubric("INDLAND|REVERSE", "Modtagermoms") == "input"
    assert cat10.classify_purchase_rubric("DOMESTIC|REVERSE", "Modtagermoms") == "dkrc"


# === Kaldssted 2: cat13.test_104 (indenlandsk standardmoms i fremmed valuta) =

def test_104_dansk_indland_standard_vat_i_eur_fyrer():
    line = mk_line(debit_amount=1000.0, tax_code="INDLAND|STANDARD_VAT", currency="EUR")
    findings = run_104(mk_data(mk_txn(line)))
    assert len(findings) == 1 and findings[0]["severity"] == "medium"


def test_104_engelsk_domestic_uaendret():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT", currency="EUR")
    assert len(run_104(mk_data(mk_txn(line)))) == 1


def test_104_eu_gruppe_fyrer_ikke():
    line = mk_line(debit_amount=1000.0, tax_code="EU|STANDARD_VAT", currency="EUR")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_dansk_indland_i_dkk_er_ren():
    line = mk_line(debit_amount=1000.0, tax_code="INDLAND|STANDARD_VAT", currency="DKK")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_env_override_uden_indland(monkeypatch):
    monkeypatch.setattr(materiality, "DOMESTIC_BUS_GROUP_VALUES", ["domestic"])
    line = mk_line(debit_amount=1000.0, tax_code="INDLAND|STANDARD_VAT", currency="EUR")
    assert run_104(mk_data(mk_txn(line))) == []


# === Kaldssted 3: vat_rules.is_reverse_charge_sale_code (Bus.-gruppe-fallback)

def test_rc_sale_code_fallback_indland_er_ikke_rc():
    # Uden beregningstype: Bus.-gruppe-fallbacket. Indenlandsk -> IKKE RC-salgskode.
    assert vr.is_reverse_charge_sale_code("INDLAND|MOMS") is False
    assert vr.is_reverse_charge_sale_code("DOMESTIC|MOMS") is False
    assert vr.is_reverse_charge_sale_code("domestic|MOMS") is False


def test_rc_sale_code_fallback_udenlandsk_er_rc():
    assert vr.is_reverse_charge_sale_code("EU|MOMS") is True
    assert vr.is_reverse_charge_sale_code("OUTSIDE DK/EU|SERVICE_VAT_NOT_EU") is True
    assert vr.is_reverse_charge_sale_code("UDLAND|MOMS") is True


def test_rc_sale_code_beregningstype_er_stadig_forrang():
    # Beregningstypen er ENDELIG, uanset Bus.-gruppe.
    assert vr.is_reverse_charge_sale_code("INDLAND|REVERSE", "Modtagermoms") is True
    assert vr.is_reverse_charge_sale_code("EU|MOMS", "Normal moms") is False


def test_rc_sale_code_opake_koder_uden_pipe_uaendret():
    assert vr.is_reverse_charge_sale_code("RC") is True
    assert vr.is_reverse_charge_sale_code("U25") is False


def test_rc_sale_code_env_override_uden_indland(monkeypatch):
    monkeypatch.setattr(materiality, "DOMESTIC_BUS_GROUP_VALUES", ["domestic"])
    assert vr.is_reverse_charge_sale_code("INDLAND|MOMS") is True  # gammel adfærd


# === Strukturvagt: ingen hårdkodet "DOMESTIC"-sammenligning tilbage =========

def test_ingen_hardkodet_domestic_sammenligning_i_de_tre_moduler():
    root = pathlib.Path(__file__).resolve().parent.parent / "analytics"
    for rel in ("vat_rules.py", "categories/cat10_vat_reconciliation.py",
                "categories/cat13_cross_dimension.py"):
        src = (root / rel).read_text(encoding="utf-8")
        for needle in ('== "DOMESTIC"', '!= "DOMESTIC"', "== 'DOMESTIC'", "!= 'DOMESTIC'"):
            assert needle not in src, f"{rel}: hårdkodet {needle}"
