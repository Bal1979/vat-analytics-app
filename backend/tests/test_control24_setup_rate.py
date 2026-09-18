"""
Kontrol 24 (2026-09-18, Bal-godkendt gap-analyse-fix D): implicit sats
(moms/grundlag) valideres mod SETUP-satsen for linjens EGEN momskode, når
kundens vat_setup er indlæst -- i stedet for den hardkodede 0/25-liste.

Diagnose (empirisk på v4-datasættet): 507 af 525 fund (454 + 55) var
delvis-fradragsret-koder (DOMESTIC|REDUCED_PRIVATE_VAT 13,63636%,
DOMESTIC|REDUCED_REP_VAT 5,26316%), hvis implicitte sats pr. konstruktion
ALDRIG matcher 0/25 -- samme fejlfamilie som kontrol 19-sagen (2026-09-17).
Uden vat_setup: UÆNDRET adfærd (kun 0%/25% er gyldige).
"""

from analytics.categories import cat03_vat_rate_validation as cat03
from validation.builders import mk_data, mk_txn, mk_line

run_24 = cat03.test_24_implied_rate

_SETUP_HEADER = {"vat_setup_loaded": True}


def _setup_table(*entries):
    return [
        {"tax_code": code, "tax_percentage": pct, "setup_matched": True}
        for code, pct in entries
    ]


# --- Uændret adfærd uden vat_setup (regression) ------------------------------

def test_without_vat_setup_invalid_implied_rate_still_fires():
    line = mk_line(debit_amount=1000.0, tax_base=1000.0, tax_amount=120.0)
    findings = run_24(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Implicit sats ugyldig"


def test_without_vat_setup_standard_rate_is_clean():
    line = mk_line(debit_amount=1000.0, tax_base=1000.0, tax_amount=250.0)
    assert run_24(mk_data(mk_txn(line))) == []


def test_header_vat_setup_loaded_false_also_uses_standard_rule():
    """Eksplicit False (ikke kun fravær) -- en delvis-fradragsret-sats uden
    en indlæst opsætning at holde den op imod er fortsat et fund."""
    line = mk_line(debit_amount=1239.8, tax_code="DOMESTIC|REDUCED_PRIVATE_VAT",
                    tax_base=239.8, tax_amount=32.7)
    findings = run_24(mk_data(mk_txn(line), header={"vat_setup_loaded": False}))
    assert len(findings) == 1


# --- MED vat_setup indlæst ----------------------------------------------------

def test_partial_deduction_rate_matching_own_setup_is_clean():
    """13,64% implicit (32.70/239.80) matcher opsætningens 13,63636% for
    DENNE kode -- ikke et fund. Den ÆGTE 2026-09-18-rettelse."""
    line = mk_line(debit_amount=239.8, tax_code="DOMESTIC|REDUCED_PRIVATE_VAT",
                    tax_base=239.8, tax_amount=32.7)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("DOMESTIC|REDUCED_PRIVATE_VAT", 13.63636)))
    assert run_24(data) == []


def test_reduced_rep_vat_matching_own_setup_is_clean():
    """5,26316%-koden (repræsentation) -- samme princip, anden sats."""
    line = mk_line(debit_amount=27018.0, tax_code="DOMESTIC|REDUCED_REP_VAT",
                    tax_base=27018.0, tax_amount=1422.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("DOMESTIC|REDUCED_REP_VAT", 5.26316)))
    assert run_24(data) == []


def test_rate_deviating_from_own_setup_still_fires():
    """Bogført moms/grundlag afviger fra opsætningens sats for SAMME kode --
    den ÆGTE kontrol (F24 i ekspertens katalog), skal fortsat flages."""
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT",
                    tax_base=1000.0, tax_amount=120.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("DOMESTIC|STANDARD_VAT", 25.0)))
    findings = run_24(data)
    assert len(findings) == 1
    assert "opsætningens sats" in findings[0]["description"]


def test_unknown_code_falls_back_to_standard_dk_rates():
    """Koden findes ikke i opsætningen (umatchet) -- vi kan ikke verificere
    mod en ukendt opsætning, så vi falder tilbage til 0/25-listen (kontrol
    19 flager selve "ukendt kode"-situationen separat)."""
    line = mk_line(debit_amount=1000.0, tax_code="HELT|UKENDT",
                    tax_base=1000.0, tax_amount=250.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=[{"tax_code": "HELT|UKENDT", "tax_percentage": 0.0,
                               "setup_matched": False}])
    assert run_24(data) == []  # 25% er stadig gyldig via 0/25-fallback


def test_unknown_code_missing_entirely_still_fires_on_invalid_implicit_rate():
    line = mk_line(debit_amount=1000.0, tax_code="HELT|UKENDT",
                    tax_base=1000.0, tax_amount=120.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER, tax_table=[])
    findings = run_24(data)
    assert len(findings) == 1
    assert "en gyldig dansk sats" in findings[0]["description"]


def test_within_rate_tolerance_of_own_setup_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT",
                    tax_base=1000.0, tax_amount=252.0)  # 25,2% implicit
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("DOMESTIC|STANDARD_VAT", 25.0)))
    assert run_24(data) == []  # inden for vr.RATE_TOLERANCE (0.5pp)


def test_genuine_data_anomaly_with_setup_loaded_still_fires():
    """Base/moms hænger tydeligvis ikke sammen (fx forkert/manglende
    momsgrundlag) -- skal flages, uanset vat_setup."""
    line = mk_line(debit_amount=15382.75, tax_code="EU|SERVICE_VAT_EU",
                    tax_base=0.01, tax_amount=15382.74)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("EU|SERVICE_VAT_EU", 25.0)))
    findings = run_24(data)
    assert len(findings) == 1
