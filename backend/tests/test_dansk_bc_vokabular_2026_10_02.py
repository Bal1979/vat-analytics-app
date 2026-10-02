"""
Dansk BC-vokabular i RC-genkendelsen (2026-10-02, Bal-godkendt lille runde).

Baggrund (INGEN kundenavne/-tekster i denne fil): en BC-kunde med DANSK UI
leverer momsopsætningens "Momsberegningstype" (engelsk: "VAT Calculation
Type") med danske værdier. Empirisk målt 2026-10-02 på fire danske
BC-opsætningsfiler (49 opsætningsrækker i alt): værdirummet er PRÆCIS
{"Normal moms" (37), "Modtagermoms" (12)}. "Modtagermoms" = dansk BC for
"Reverse Charge VAT"; "Normal moms" = "Normal VAT". "Fuld moms" (= "Full
VAT") forekommer IKKE i de målte filer.

Før denne runde kendte ``vat_rules.is_rc_calc_type`` kun BC's engelske
"reverse charge"-substring og IFS' "Calculated Tax" -- "Modtagermoms"
indeholder ikke "reverse charge" og ville derfor fejlklassificeres som normal
moms (samme fejlklasse som kontrol 82-sagen 2026-09-22).
"""

from analytics import vat_rules as vr
from analytics import materiality
from analytics.categories import cat03_vat_rate_validation as cat03
from analytics.categories import cat09_reverse_charge as cat09
from analytics.categories import cat10_vat_reconciliation as cat10
from tools import generate_report
from validation.builders import mk_data, mk_txn, mk_line


# Det EMPIRISK målte værdirum for dansk BC (se modul-docstring) -> er værdien RC?
DANSK_BC_VAERDIRUM = {"Normal moms": False, "Modtagermoms": True}


# === vat_rules.is_rc_calc_type ==============================================

def test_dansk_bc_vaerdirum_klassificeres_korrekt():
    for value, expected in DANSK_BC_VAERDIRUM.items():
        assert vr.is_rc_calc_type(value) is expected, value


def test_modtagermoms_case_og_whitespace_normaliseret():
    assert vr.is_rc_calc_type("modtagermoms")
    assert vr.is_rc_calc_type("MODTAGERMOMS")
    assert vr.is_rc_calc_type("  Modtagermoms  ")


def test_ingen_fuzzy_match_paa_danske_varianter():
    """Eksplicit værdiliste: kun den målte stavemåde genkendes."""
    assert not vr.is_rc_calc_type("Modtager moms")
    assert not vr.is_rc_calc_type("Modtagermomsx")
    assert not vr.is_rc_calc_type("Moms")
    assert not vr.is_rc_calc_type("Omvendt betalingspligt")
    assert not vr.is_rc_calc_type("Fuld moms")
    assert not vr.is_rc_calc_type("Normal moms")


def test_eksisterende_vokabularer_uaendrede():
    assert vr.is_rc_calc_type("Reverse Charge VAT")
    assert vr.is_rc_calc_type("Calculated Tax")
    assert not vr.is_rc_calc_type("Normal VAT")
    assert not vr.is_rc_calc_type("Full VAT")


def test_rc_calc_type_values_indeholder_dansk_vaerdi():
    assert "modtagermoms" in materiality.RC_CALC_TYPE_VALUES
    assert "calculated tax" in materiality.RC_CALC_TYPE_VALUES


# === Energiafgift-fallback ("Full VAT") -- ikke-RC-vokabularet ==============

def test_full_vat_vokabular_er_eksplicit_og_kun_engelsk_empirisk():
    """'Fuld moms' er IKKE observeret i dansk BC-data (2026-10-02) og er
    derfor ikke sidestillet -- det er en eksplicit, engagement-overstyrbar
    liste, ikke en antagelse."""
    assert materiality.FULL_VAT_CALC_TYPE_VALUES == ["full vat"]


def test_energy_tax_fallback_engelsk_uaendret():
    assert vr.is_energy_tax_code("X", "Full VAT", 0)
    assert not vr.is_energy_tax_code("X", "Full VAT", 25)
    assert not vr.is_energy_tax_code("X", "Normal VAT", 0)
    assert not vr.is_energy_tax_code("X", "Full VAT", None)


def test_energy_tax_fallback_dansk_normal_moms_er_ikke_afgift():
    """Dansk 'Normal moms' med sats 0 = legitim nulsats-kode, aldrig afgift."""
    assert not vr.is_energy_tax_code("X", "Normal moms", 0)
    assert not vr.is_energy_tax_code("X", "Modtagermoms", 0)


def test_energy_tax_fallback_fuld_moms_kun_via_eksplicit_konfiguration(monkeypatch):
    assert not vr.is_energy_tax_code("X", "Fuld moms", 0)
    monkeypatch.setattr(materiality, "FULL_VAT_CALC_TYPE_VALUES", ["full vat", "fuld moms"])
    assert vr.is_energy_tax_code("X", "Fuld moms", 0)
    assert not vr.is_energy_tax_code("X", "Fuld moms", 25)


# === vat_rules.is_reverse_charge_sale_code ==================================

def test_reverse_charge_sale_code_modtagermoms():
    assert vr.is_reverse_charge_sale_code("REVERSE", "Modtagermoms")
    assert vr.is_reverse_charge_sale_code("EU25", "Modtagermoms")


def test_reverse_charge_sale_code_normal_moms_er_endelig():
    """Beregningstypen til stede -> endelig, falder ikke til kode-/gruppe-
    fallback (selv ikke for en kode der ellers ville ligne RC)."""
    assert not vr.is_reverse_charge_sale_code("REVERSE", "Normal moms")
    assert not vr.is_reverse_charge_sale_code("EU|SERVICE_VAT_EU", "Normal moms")


# === cat10._purchase_rubric (kontrol 82) ====================================

def test_purchase_rubric_modtagermoms_domestic_er_dkrc():
    assert cat10._purchase_rubric("DOMESTIC|DKRC_GOODS", "Modtagermoms") == "dkrc"


def test_purchase_rubric_modtagermoms_udenlandsk_ydelse_er_rc_services():
    assert cat10._purchase_rubric("EU|SERVICE_VAT_EU", "Modtagermoms") == "rc_services"


def test_purchase_rubric_normal_moms_er_input_uanset_kode():
    assert cat10._purchase_rubric("DOMESTIC|DKRC_GOODS", "Normal moms") == "input"


# === cat09._is_reverse_charge_marked (kontrol 70-75) ========================

def test_marked_via_modtagermoms_uden_kode_eller_tekst_hint():
    line = mk_line(tax_code="MOMS99", vat_calculation_type="Modtagermoms")
    assert cat09._is_reverse_charge_marked(line, mk_txn(line, description="Almindelig tekst"))


def test_normal_moms_overtrumfer_rc_kodehint():
    line = mk_line(tax_code="RC-SPECIAL", vat_calculation_type="Normal moms")
    assert not cat09._is_reverse_charge_marked(line, mk_txn(line, description=""))


# === Integration: kontrol 70 + kontrol 22 ===================================

def test_70_eu_koeb_med_modtagermoms_flagges_ikke():
    line = mk_line(debit_amount=50000.0, country="DE", tax_code="EU25",
                    tax_amount=0.0, vat_calculation_type="Modtagermoms")
    data = mk_data(mk_txn(line, description="EU-køb"))
    assert cat09.test_70_eu_service_no_rc(data, {}) == []


def test_70_eu_koeb_med_normal_moms_flagges_stadig():
    """Regression: samme scenarie med 'Normal moms' (ikke RC) fanges som før."""
    line = mk_line(debit_amount=50000.0, country="DE", tax_code="EU25",
                    tax_amount=0.0, vat_calculation_type="Normal moms")
    data = mk_data(mk_txn(line, description="EU-køb"))
    assert len(cat09.test_70_eu_service_no_rc(data, {})) == 1


def test_22_salg_med_modtagermoms_og_0_moms_er_korrekt_eksport():
    line = mk_line(credit_amount=50000.0, tax_code="REVERSE", tax_percentage=25.0,
                    tax_amount=0.0, vat_calculation_type="Modtagermoms")
    data = mk_data(mk_txn(line))
    assert cat03.test_22_missing_output_vat(data) == []


# === Rapport-label ==========================================================

def test_rapport_label_for_danske_beregningstyper():
    assert generate_report._calc_type_label("Normal moms") == "Almindelig moms"
    assert generate_report._calc_type_label("Modtagermoms") == "Omvendt betalingspligt"
    assert generate_report._calc_type_label("Normal VAT") == "Almindelig moms"
