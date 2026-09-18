"""
Kontrol 104-108 (gap-analysen, Bal-godkendt 2026-09-18,
analytics/categories/cat13_cross_dimension.py). Se modulets docstring for
den fulde mekanik. Disse tests supplerer valideringssuitens clean/defect-par
(validation/scenarios.py) med grænseflade-/kalibreringstests.
"""

from analytics import materiality
from analytics import readiness
from analytics.categories import cat13_cross_dimension as cat13
from validation.builders import mk_data, mk_txn, mk_line

run_104 = cat13.test_104_foreign_currency_domestic_vat
run_105 = cat13.test_105_cross_border_purchase_no_rc
run_106 = cat13.test_106_import_indicator
run_107 = cat13.test_107_atypical_vat_by_source_code
run_108 = cat13.test_108_source_code_spread


# === Kontrol 104: udenlandsk valuta med indenlandsk standardmoms ===========

def test_104_foreign_currency_standard_vat_fires():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT", currency="EUR")
    findings = run_104(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["severity"] == "medium"


def test_104_domestic_currency_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT", currency="DKK")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_empty_currency_defaults_to_dkk_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT", currency="")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_non_standard_domestic_code_is_clean():
    """Reduceret/delvis-fradragsret-koder er IKKE 'standardmoms' -- kontrollen
    er bevidst snæver om produktkoden (jf. gap-analysens F29-fund, som var
    STANDARD_VAT specifikt)."""
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|REDUCED_PRIVATE_VAT", currency="EUR")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_eu_bus_group_is_clean():
    """Bus.-gruppen skal være DOMESTIC -- en EU-kode hører til kontrol 105."""
    line = mk_line(debit_amount=1000.0, tax_code="EU|STANDARD_VAT", currency="EUR")
    assert run_104(mk_data(mk_txn(line))) == []


def test_104_reverse_charge_calc_type_is_clean():
    """En linje der (usædvanligt) allerede er markeret RC via
    vat_calculation_type, hører ikke til denne kontrols population."""
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|STANDARD_VAT", currency="EUR",
                    vat_calculation_type="Reverse Charge VAT")
    assert run_104(mk_data(mk_txn(line))) == []


# === Kontrol 105: EU-/udlandskøb uden reverse charge-beregning =============

def test_105_rc_code_zero_vat_fires_high():
    line = mk_line(debit_amount=1000.0, tax_code="EU|SERVICE_VAT_EU", tax_base=1000.0, tax_amount=0.0)
    findings = run_105(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["severity"] == "high"
    assert findings[0]["estimated_amount"] == 250.0


def test_105_no_vat_family_matched_setup_fires_medium():
    """EU|NO_VAT med vat_calculation_type='Normal VAT' (matchet i vat_setup,
    eksplicit ikke-RC) -- den ægte, empirisk observerede population (57
    bilag på v4-datasættet)."""
    line = mk_line(debit_amount=1000.0, tax_code="EU|NO_VAT", tax_amount=0.0,
                    vat_calculation_type="Normal VAT")
    findings = run_105(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["severity"] == "medium"
    assert findings[0]["test_name"] == "Nulkode på udenlandsk køb"


def test_105_unmatched_no_vat_family_fires_medium():
    """Umatchet kode uden calc type, men produktkoden matcher NO_VAT-mønsteret
    -- samme lavere-sikkerhed-gren som den matchede variant."""
    line = mk_line(debit_amount=1000.0, tax_code="OUTSIDE DK/EU|NO_VAT_NOT_EU", tax_amount=0.0)
    findings = run_105(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["severity"] == "medium"


def test_105_with_vat_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="EU|SERVICE_VAT_EU", tax_amount=250.0)
    assert run_105(mk_data(mk_txn(line))) == []


def test_105_sale_line_is_clean():
    """Kun købslinjer (debet) -- salgssiden er kontrol 22's/vat_rules.
    is_reverse_charge_sale_code's territorium."""
    line = mk_line(credit_amount=1000.0, tax_code="EU|SERVICE_VAT_EU", tax_amount=0.0)
    assert run_105(mk_data(mk_txn(line))) == []


def test_105_domestic_bus_group_is_clean():
    line = mk_line(debit_amount=1000.0, tax_code="DOMESTIC|NO_VAT", tax_amount=0.0)
    assert run_105(mk_data(mk_txn(line))) == []


# === Kontrol 106: import fra 3.-land ========================================

def test_106_goods_not_eu_fires():
    line = mk_line(debit_amount=1000.0, tax_code="OUTSIDE DK/EU|GOODS_VAT_NOT_EU", tax_amount=250.0)
    findings = run_106(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["severity"] == "low"
    assert findings[0]["estimated_amount"] == 250.0


def test_106_fires_even_when_vat_is_zero():
    """Informativt fund -- uafhængigt af om momsen ser korrekt beregnet ud."""
    line = mk_line(debit_amount=1000.0, tax_code="OUTSIDE DK/EU|GOODS_VAT_NOT_EU", tax_amount=0.0)
    assert len(run_106(mk_data(mk_txn(line)))) == 1


def test_106_service_not_eu_is_clean():
    """Ydelser (SERVICE_VAT_NOT_EU), ikke varer -- uden for denne kontrols
    'import af varer'-population."""
    line = mk_line(debit_amount=1000.0, tax_code="OUTSIDE DK/EU|SERVICE_VAT_NOT_EU", tax_amount=250.0)
    assert run_106(mk_data(mk_txn(line))) == []


def test_106_eu_goods_is_clean():
    """EU er ikke '3.-land' -- Bus.-gruppen skal være OUTSIDE DK/EU."""
    line = mk_line(debit_amount=1000.0, tax_code="EU|GOODS_VAT_EU", tax_amount=250.0)
    assert run_106(mk_data(mk_txn(line))) == []


# === Kontrol 107: atypisk moms på bilagstype ================================

def _lines_for_code(code, n, with_vat_indices=()):
    txns = []
    for i in range(n):
        vat = 100.0 if i in with_vat_indices else 0.0
        base = 400.0 if i in with_vat_indices else 0.0
        txns.append(mk_txn(mk_line(debit_amount=500.0, source_code=code, tax_base=base, tax_amount=vat),
                            transaction_id=f"{code}-{i}"))
    return txns


def test_107_minority_vat_lines_flagged():
    """25 linjer, 1 med moms (4% < CONTROL_107_MAX_TYPICAL_VAT_SHARE) --
    netop DEN linje er fundet."""
    data = mk_data(_lines_for_code("PAYMENT", 25, with_vat_indices=(24,)))
    findings = run_107(data)
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["transaction_id"] == "PAYMENT-24"


def test_107_below_min_population_is_clean():
    """Under materiality.CONTROL_107_MIN_LINES_PER_SOURCE_CODE -- for lille
    population til at udlede et 'typisk' mønster."""
    n = materiality.CONTROL_107_MIN_LINES_PER_SOURCE_CODE - 1
    data = mk_data(_lines_for_code("PAYMENT", n, with_vat_indices=(n - 1,)))
    assert run_107(data) == []


def test_107_no_vat_lines_at_all_is_clean():
    data = mk_data(_lines_for_code("PAYMENT", 25))
    assert run_107(data) == []


def test_107_majority_vat_is_clean():
    """Bilagstypen er IKKE 'typisk momsfri' -- moms er normalt her, så ingen
    af linjerne er atypiske."""
    data = mk_data(_lines_for_code("SALES", 25, with_vat_indices=range(20)))
    assert run_107(data) == []


def test_107_missing_source_code_is_clean():
    """Linjer uden source_code (feltet fraværende/tomt) indgår slet ikke i
    nogen bilagstype-population."""
    lines = [mk_txn(mk_line(debit_amount=500.0, tax_amount=100.0 if i == 24 else 0.0),
                    transaction_id=f"L-{i}") for i in range(25)]
    assert run_107(mk_data(lines)) == []


# === Kontrol 108: salg/køb spredt over mange bilagstyper ====================

def _direction_lines(codes_by_count, debit=True):
    txns = []
    i = 0
    for code, n in codes_by_count.items():
        for _ in range(n):
            i += 1
            kw = {"debit_amount": 100.0} if debit else {"credit_amount": 100.0}
            txns.append(mk_txn(mk_line(source_code=code, **kw), transaction_id=f"T-{i}"))
    return txns


def test_108_many_source_codes_fires_one_aggregate_finding():
    data = mk_data(_direction_lines({"A": 10, "B": 10, "C": 5, "D": 3, "E": 2}))
    findings = run_108(data)
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Salg/køb spredt over mange bilagstyper"
    assert findings[0]["description"].startswith("Køb er bogført over")


def test_108_single_source_code_is_clean():
    data = mk_data(_direction_lines({"PURCH": 30}))
    assert run_108(data) == []


def test_108_below_min_lines_is_clean():
    n = materiality.CONTROL_108_MIN_LINES - 1
    data = mk_data(_direction_lines({"A": n // 3 + 1, "B": n // 3, "C": n - 2 * (n // 3) - 1}))
    assert run_108(data) == []


def test_108_sales_and_purchases_tracked_separately():
    """Salg (ÉN bilagstype, X) er ikke 'spredt' og giver intet fund; køb
    (tre bilagstyper) giver ét aggregeret fund -- retningerne vurderes
    uafhængigt af hinanden."""
    purchases = _direction_lines({"A": 10, "B": 10, "C": 10}, debit=True)
    sales = _direction_lines({"X": 30}, debit=False)
    data = mk_data(purchases + sales)
    findings = run_108(data)
    assert len(findings) == 1
    assert findings[0]["description"].startswith("Køb er bogført over")


# === Readiness-gating (kontrol 107/108 kræver source_code) =================

def test_readiness_gates_107_and_108_when_source_code_absent():
    """Et datasæt stort nok til at håndhæve gating (>= readiness.
    MIN_TX_FOR_GATING linjer), men UDEN source_code overhovedet, skal 'ikke
    målbar'-gate 107/108 -- samme mekanik som kontrol 25 uden landekolonne."""
    n = readiness.MIN_TX_FOR_GATING + 1
    lines = [mk_txn(mk_line(debit_amount=100.0), transaction_id=f"T-{i}") for i in range(n)]
    data = mk_data(lines)
    from analytics.engine import CATEGORIES
    from analytics import modules
    assessment = readiness.assess(data, modules.all_module_keys(), CATEGORIES)
    statuses = {c["test_id"]: c["status"] for c in assessment["kontroller"]}
    assert statuses[107] == readiness.STATUS_IKKE_MAALBART
    assert statuses[108] == readiness.STATUS_IKKE_MAALBART


def test_readiness_does_not_gate_104_105_106_on_missing_source_code():
    n = readiness.MIN_TX_FOR_GATING + 1
    lines = [mk_txn(mk_line(debit_amount=100.0), transaction_id=f"T-{i}") for i in range(n)]
    data = mk_data(lines)
    from analytics.engine import CATEGORIES
    from analytics import modules
    assessment = readiness.assess(data, modules.all_module_keys(), CATEGORIES)
    statuses = {c["test_id"]: c["status"] for c in assessment["kontroller"]}
    assert statuses[104] == readiness.STATUS_KOERT
    assert statuses[105] == readiness.STATUS_KOERT
    assert statuses[106] == readiness.STATUS_KOERT
