"""
Kontrol 19 (byggetrin 8, Del A, Bal-godkendt 2026-09-17): validering mod
kundens EGEN vat_setup-stamdata, når den er indlæst (header.vat_setup_loaded),
i stedet for den hardkodede danske 0/25-liste.

Diagnose (verificeret manuelt): kundens VAT Posting Setup indeholder bevidste
delvis-fradragsret-konstruktioner (fx 13,63636%/5,26316%) — disse er GYLDIGE,
når de matcher opsætningen for koden. Uden vat_setup: UÆNDRET adfærd.
"""

from analytics.categories import cat03_vat_rate_validation as cat03
from validation.builders import mk_data, mk_txn, mk_line

run_19 = cat03.test_19_invalid_rate

_SETUP_HEADER = {"vat_setup_loaded": True}


def _setup_table(*entries):
    """entries: (tax_code, tax_percentage) -- alle markeret setup_matched=True."""
    return [
        {"tax_code": code, "tax_percentage": pct, "setup_matched": True}
        for code, pct in entries
    ]


# --- Uændret adfærd uden vat_setup (default/fravær af flag) -----------------

def test_without_vat_setup_flag_uses_standard_25_0_rule():
    line = mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=12.0)
    findings = run_19(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Ugyldig momssats"


def test_without_vat_setup_flag_25_percent_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=25.0)
    assert run_19(mk_data(mk_txn(line))) == []


def test_header_vat_setup_loaded_false_also_uses_standard_rule():
    """Eksplicit False (ikke kun fravær) skal give samme uændrede adfærd."""
    line = mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=13.63636)
    findings = run_19(mk_data(mk_txn(line), header={"vat_setup_loaded": False}))
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Ugyldig momssats"


# --- MED vat_setup indlæst ----------------------------------------------------

def test_partial_deduction_rate_matching_setup_is_clean():
    """13,63636% er IKKE en gyldig 0/25-sats, men matcher opsætningen for
    koden -- intet fund. Dette er præcis den falske-HØJ-rettelse Del A løser."""
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|REDUCED_PRIVATE_VAT",
                    tax_percentage=13.63636)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("DOMESTIC|REDUCED_PRIVATE_VAT", 13.63636)))
    assert run_19(data) == []


def test_standard_25_percent_matching_setup_is_still_clean():
    line = mk_line(debit_amount=1000.0, tax_code="STANDARD|I25", tax_percentage=25.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("STANDARD|I25", 25.0)))
    assert run_19(data) == []


def test_rate_deviating_from_setup_fires():
    """Bogført sats afviger fra opsætningens sats for SAMME kode -- den ÆGTE
    kontrol ("bogført sats != opsætningens sats")."""
    line = mk_line(debit_amount=1000.0, tax_code="STANDARD|I25", tax_percentage=20.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("STANDARD|I25", 25.0)))
    findings = run_19(data)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 19
    assert findings[0]["test_name"] == "Sats afviger fra vat_setup"
    assert findings[0]["severity"] == "high"


def test_unknown_code_not_in_setup_fires_as_unknown():
    """Koden findes slet ikke i opsætningen -- fund i sig selv, uanset satsen."""
    line = mk_line(debit_amount=1000.0, tax_code="HELT|UKENDT|KODE", tax_percentage=25.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=[{"tax_code": "HELT|UKENDT|KODE", "tax_percentage": 0.0,
                               "setup_matched": False}])
    findings = run_19(data)
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Ukendt momskode i opsætning"


def test_unknown_code_missing_from_tax_table_entirely_fires_as_unknown():
    """Koden optræder slet ikke i tax_table (fx aldrig set af parseren) --
    behandles samme som "ukendt kode", ikke en KeyError/krasch."""
    line = mk_line(debit_amount=1000.0, tax_code="ANDEN|KODE", tax_percentage=25.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER, tax_table=[])
    findings = run_19(data)
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Ukendt momskode i opsætning"


def test_within_rate_tolerance_of_setup_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="STANDARD|I25", tax_percentage=25.2)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER,
                    tax_table=_setup_table(("STANDARD|I25", 25.0)))
    assert run_19(data) == []  # inden for vr.RATE_TOLERANCE (0.5pp)


def test_lines_without_tax_code_are_skipped_with_setup_loaded():
    line = mk_line(debit_amount=1000.0, tax_code="", tax_percentage=0.0)
    data = mk_data(mk_txn(line), header=_SETUP_HEADER, tax_table=[])
    assert run_19(data) == []
