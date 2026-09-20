"""
Kalibrering "chip 2's tråd A" (2026-09-20, Bal-godkendt): RC-detektion via
beregningstype (``vat_calculation_type``) udvidet med et eksplicit, ikke-
fuzzy vokabular for ERP'er hvis beregningstype-tekst IKKE indeholder
"reverse charge" som substring.

Baggrund (INGEN kundenavne/-tekster i denne fil): vat-extract udvidede IFS'
vat_setup-mapping med ``ext_vat_calculation_type`` (IFS' rå "Tax Type"-
kolonne). Empirisk værdirum på kunde 2s regenererede vat_setup.csv (21
koder): "Tax" (14), "No Tax" (1), "Calculated Tax" (PRÆCIS de fire hidtil
uafklarede E-koder + de to kendte RC-koder). "Calculated Tax" betyder
ENTYDIGT omvendt betalingspligt i IFS' egen taksonomi -- tilføjet til
``materiality.RC_CALC_TYPE_VALUES`` og konsumeret via den nye, centrale
``vat_rules.is_rc_calc_type``, som nu bruges ÉT sted af ALLE RC-detekterende
kaldssteder (``is_reverse_charge_sale_code``, kontrol 70-75's
``_is_reverse_charge_marked``, kontrol 82's ``_purchase_rubric``) i stedet
for tre uafhængige "reverse charge"-substring-kopier.

Empirisk resultat på kunde 2 (se docs/CHANGELOG.md for det fulde billede):
kontrol 70's residual 9.438 -> 41 (dommen kontrol 70 har ventet på).
"""

from analytics import vat_rules as vr
from analytics import materiality
from analytics.categories import cat09_reverse_charge as cat09
from analytics.categories import cat10_vat_reconciliation as cat10
from validation.builders import mk_data, mk_txn, mk_line


# === vat_rules.is_rc_calc_type ==============================================

def test_is_rc_calc_type_recognizes_bc_reverse_charge_substring():
    assert vr.is_rc_calc_type("Reverse Charge VAT")
    assert vr.is_rc_calc_type("reverse charge")


def test_is_rc_calc_type_recognizes_configured_exact_values():
    """Den nye, eksplicitte værdiliste (IFS' "Calculated Tax") -- eksakt
    match, case-insensitivt, INGEN fuzzy-match."""
    assert vr.is_rc_calc_type("Calculated Tax")
    assert vr.is_rc_calc_type("calculated tax")
    assert vr.is_rc_calc_type("  Calculated Tax  ")


def test_is_rc_calc_type_rejects_unrelated_or_similar_values():
    """Ingen fuzzy-match: en værdi der LIGNER, men ikke er i vokabularet,
    genkendes IKKE."""
    assert not vr.is_rc_calc_type("Normal VAT")
    assert not vr.is_rc_calc_type("Full VAT")
    assert not vr.is_rc_calc_type("Calculated")  # delvist ord, ikke eksakt
    assert not vr.is_rc_calc_type("Tax")
    assert not vr.is_rc_calc_type("No Tax")


def test_is_rc_calc_type_empty_is_false():
    assert not vr.is_rc_calc_type("")
    assert not vr.is_rc_calc_type(None)


def test_rc_calc_type_values_configured_in_materiality():
    """Værdilisten er én samlet, engagement-overstyrbar konstant i
    materiality.py -- ikke hardkodet flere steder."""
    assert "calculated tax" in materiality.RC_CALC_TYPE_VALUES


# === vat_rules.is_reverse_charge_sale_code ==================================

def test_is_reverse_charge_sale_code_recognizes_calculated_tax():
    """En syntetisk IFS-lignende E-kode med beregningstype 'Calculated Tax'
    genkendes som RC-salgskode -- 0 kr. udgående moms er korrekt eksport."""
    assert vr.is_reverse_charge_sale_code("E1G", "Calculated Tax")
    assert vr.is_reverse_charge_sale_code("E0S", "Calculated Tax")


def test_is_reverse_charge_sale_code_calc_type_present_is_final():
    """Er beregningstypen til stede (uanset værdi), er resultatet ENDELIGT
    -- falder IKKE videre til Bus.-gruppe/præfiks-fallback."""
    assert not vr.is_reverse_charge_sale_code("EU|SERVICE_VAT_EU", "Normal VAT")


def test_is_reverse_charge_sale_code_fallback_unchanged_without_calc_type():
    """Fravær af beregningstype -- uændret Bus.-gruppe-/præfiks-fallback."""
    assert vr.is_reverse_charge_sale_code("EU|SERVICE_VAT_EU", "")
    assert not vr.is_reverse_charge_sale_code("DOMESTIC|STANDARD_VAT", "")
    assert vr.is_reverse_charge_sale_code("RC", "")  # RC_CODE_PREFIXES-fallback


# === cat10._purchase_rubric (kontrol 82) ====================================

def test_purchase_rubric_calculated_tax_domestic_is_dkrc():
    assert cat10._purchase_rubric("DOMESTIC|DKRC_GOODS", "Calculated Tax") == "dkrc"


def test_purchase_rubric_calculated_tax_foreign_service_is_rc_services():
    assert cat10._purchase_rubric("EU|SERVICE_VAT_EU", "Calculated Tax") == "rc_services"


def test_purchase_rubric_normal_vat_is_input_regardless_of_code():
    assert cat10._purchase_rubric("E1G", "Normal VAT") == "input"


# === cat09._is_reverse_charge_marked (kontrol 70-75) ========================

def test_marked_via_calc_type_even_without_code_or_text_hint():
    """Kernen i denne kalibrering: en linje med en opak kode (ingen RC/OMV/
    REV/OB-substring) og ingen fritekst-hint genkendes NU som RC via
    beregningstypen alene."""
    line = mk_line(tax_code="E1G", vat_calculation_type="Calculated Tax")
    txn = mk_txn(line, description="Almindelig posteringstekst uden hints")
    assert cat09._is_reverse_charge_marked(line, txn)


def test_calc_type_present_but_not_rc_overrides_code_hint():
    """Beregningstypen er DETERMINISTISK og ENDELIG, når den er til stede --
    overtrumfer selv et kode-hint, der ellers ville udløse fallback-stien."""
    line = mk_line(tax_code="RC-SPECIAL", vat_calculation_type="Normal VAT")
    txn = mk_txn(line, description="")
    assert not cat09._is_reverse_charge_marked(line, txn)


def test_fallback_to_code_and_text_hints_unchanged_without_calc_type():
    """Ingen beregningstype -- uændret adfærd: kode-hint og fritekst-hint
    fungerer som hidtil."""
    line_code_hint = mk_line(tax_code="RC25", vat_calculation_type="")
    assert cat09._is_reverse_charge_marked(line_code_hint, mk_txn(line_code_hint))

    line_text_hint = mk_line(tax_code="X1", vat_calculation_type="")
    txn_text_hint = mk_txn(line_text_hint, description="Omvendt betalingspligt")
    assert cat09._is_reverse_charge_marked(line_text_hint, txn_text_hint)

    line_none = mk_line(tax_code="X1", vat_calculation_type="")
    assert not cat09._is_reverse_charge_marked(line_none, mk_txn(line_none, description="Almindeligt køb"))


# === Integrationstest: kontrol 70 (EU-køb uden RC-markering) ================

def test_70_ifs_style_e_code_with_calculated_tax_is_not_flagged():
    """Kernescenariet: en syntetisk IFS-lignende E-kode (EU-køb, 0 kr. moms)
    med beregningstype 'Calculated Tax' er nu korrekt genkendt som RC og
    udløser IKKE længere kontrol 70's 'manglende RC-markering'-fund."""
    line = mk_line(debit_amount=50000.0, country="DE", tax_code="E1G",
                    tax_amount=0.0, vat_calculation_type="Calculated Tax")
    data = mk_data(mk_txn(line, description="EU-køb"))
    assert cat09.test_70_eu_service_no_rc(data, {}) == []


def test_70_eu_code_without_calc_type_still_fires_unchanged():
    """Regression: samme scenarie UDEN beregningstype (Excel-/SAF-T-
    oprindelse, eller kanonisk vej uden vat_setup.csv) er uændret -- fanges
    fortsat af kontrollen, som hidtil."""
    line = mk_line(debit_amount=50000.0, country="DE", tax_code="E1G",
                    tax_amount=0.0, vat_calculation_type="")
    data = mk_data(mk_txn(line, description="EU-køb"))
    findings = cat09.test_70_eu_service_no_rc(data, {})
    assert len(findings) == 1


# === Integrationstest: kontrol 22 (manglende salgsmoms) =====================

def test_22_ifs_style_e_code_sale_with_calculated_tax_is_not_flagged():
    """En salgslinje med en syntetisk E-kode og 'Calculated Tax' er korrekt
    eksport (0 kr. udgående moms er rigtigt) -- intet kontrol 22-fund."""
    from analytics.categories import cat03_vat_rate_validation as cat03
    line = mk_line(credit_amount=50000.0, tax_code="E1G", tax_percentage=25.0,
                    tax_amount=0.0, vat_calculation_type="Calculated Tax")
    data = mk_data(mk_txn(line))
    assert cat03.test_22_missing_output_vat(data) == []
