"""
Kontrol 22 (2026-09-18, Bal-godkendt gap-analyse-fix A+B).

Fix A — retningsbevidst setup-sats: i BC's VAT Posting Setup er satsen på en
reverse charge-kode BEREGNINGSSATSEN FOR KØBSSIDEN. På SALGSSIDEN er samme
kode nulsats-eksport — 0 kr. udgående moms er korrekt, ikke et fund. Uden
dette skel flagede kontrollen lovligt eksportsalg (fx en linje med koden
"OUTSIDE DK/EU|SERVICE_VAT_NOT_EU" (25,0% — købssidens RC-sats) uden bogført
momsbeløb) som "manglende salgsmoms". Se vat_rules.is_reverse_charge_sale_code
for den fulde BC/NAV-semantik (generel platform-egenskab, ikke kunde-specifik).

Fix B — materialitets-gulv: en kreditlinje under
materiality.CONTROL_22_MIN_BASE (default 1,00 kr.) undertrykkes altid, så
rene afrundingslinjer (0,01 kr.) ikke tæller som "manglende salgsmoms".
"""

from analytics import materiality
from analytics.categories import cat03_vat_rate_validation as cat03
from validation.builders import mk_data, mk_txn, mk_line

run_22 = cat03.test_22_missing_output_vat


# --- Genuine fund (regression -- må IKKE forsvinde) --------------------------

def test_genuine_missing_output_vat_still_fires():
    line = mk_line(credit_amount=10000.0, tax_code="U25", tax_percentage=25.0, tax_amount=0.0)
    findings = run_22(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Manglende salgsmoms"


def test_domestic_bus_group_without_calc_type_still_fires():
    """Bus.-gruppe 'DOMESTIC' er ikke reverse charge på salgssiden."""
    line = mk_line(credit_amount=10000.0, tax_code="DOMESTIC|STANDARD_VAT",
                    tax_percentage=25.0, tax_amount=0.0)
    findings = run_22(mk_data(mk_txn(line)))
    assert len(findings) == 1


def test_normal_vat_calculation_type_still_fires():
    line = mk_line(credit_amount=10000.0, tax_code="DOMESTIC|STANDARD_VAT",
                    tax_percentage=25.0, tax_amount=0.0,
                    vat_calculation_type="Normal VAT")
    findings = run_22(mk_data(mk_txn(line)))
    assert len(findings) == 1


def test_code_without_bus_group_signal_still_fires():
    """Koden følger ikke gruppe|kode-konventionen -- intet Bus.-gruppe-signal
    at læne sig på, så fallback'en gætter IKKE på RC (konservativ, ingen ny
    falsk negativ)."""
    line = mk_line(credit_amount=10000.0, tax_code="U25", tax_percentage=25.0, tax_amount=0.0)
    findings = run_22(mk_data(mk_txn(line)))
    assert len(findings) == 1


# --- Fix A: reverse charge-kode på salgssiden er IKKE et fund ---------------

def test_reverse_charge_export_sale_via_vat_calculation_type_is_clean():
    line = mk_line(credit_amount=10000.0, tax_code="OUTSIDE DK/EU|SERVICE_VAT_NOT_EU",
                    tax_percentage=25.0, tax_amount=0.0,
                    vat_calculation_type="Reverse Charge VAT")
    assert run_22(mk_data(mk_txn(line))) == []


def test_reverse_charge_export_sale_via_bus_group_fallback_is_clean():
    """Uden vat_calculation_type (ingen vat_setup indlæst): fallback på
    Bus.-gruppens første led i vat_codes-strengen -- 'EU'/'OUTSIDE DK/EU' er
    ikke 'DOMESTIC'."""
    line = mk_line(credit_amount=10000.0, tax_code="EU|SERVICE_VAT_EU",
                    tax_percentage=25.0, tax_amount=0.0)
    assert run_22(mk_data(mk_txn(line))) == []


def test_outside_dk_eu_bus_group_fallback_is_clean():
    line = mk_line(credit_amount=10000.0, tax_code="OUTSIDE DK/EU|SERVICE_VAT_NOT_EU",
                    tax_percentage=25.0, tax_amount=0.0)
    assert run_22(mk_data(mk_txn(line))) == []


# --- Fix B: materialitets-gulv -----------------------------------------------

def test_rounding_line_below_floor_is_suppressed():
    line = mk_line(credit_amount=0.01, tax_code="U25", tax_percentage=25.0, tax_amount=0.0)
    assert run_22(mk_data(mk_txn(line))) == []


def test_amount_at_floor_still_fires():
    line = mk_line(credit_amount=1.0, tax_code="U25", tax_percentage=25.0, tax_amount=0.0)
    findings = run_22(mk_data(mk_txn(line)))
    assert len(findings) == 1


def test_materiality_floor_is_configurable(monkeypatch):
    """MATERIALITY_CONTROL_22_MIN_BASE er env-overstyrbar (mønster fra
    materiality.py) -- her sat direkte på modulet, samme teknik som
    test_cat10_period_declaration.py."""
    monkeypatch.setattr(materiality, "CONTROL_22_MIN_BASE", 50.0)
    line = mk_line(credit_amount=20.0, tax_code="U25", tax_percentage=25.0, tax_amount=0.0)
    assert run_22(mk_data(mk_txn(line))) == []
