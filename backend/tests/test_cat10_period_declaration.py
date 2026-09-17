"""
Kontrol 82 — periode-/rubrikafstemning mod momsangivelsen (byggetrin 8, Del B,
Bal-godkendt 2026-09-17). Se analytics/categories/cat10_vat_reconciliation.py
for rubrik-logikkens baggrund.

Dækker: intet angivelses-input (springer over, uændret adfærd), rubrik-
klassifikation (DKRC -> udgående, SERVICE_VAT -> egen rubrik, øvrigt ->
indgående), pr.-periode-match (intet fund), reel difference (severity high)
vs. timing-difference der nulstilles over årstotalen (severity low), samt at
mønster-genkendelsen er konfigurerbar via materiality.py (ikke hårdkodet).
"""

import os

from analytics import materiality
from analytics.categories import cat10_vat_reconciliation as cat10
from validation.builders import mk_data, mk_line, mk_txn


def _decl(*periods):
    return {
        "declarations_version": "1.0.0", "source": "test", "generated": "2024-01-01",
        "periods": [
            {"period": p, "output_vat": o, "input_vat": i, "rc_services": rc,
             "rc_goods": 0.0, "energy_taxes": 0.0, "total": o + i + rc}
            for (p, o, i, rc) in periods
        ],
    }


def test_no_declarations_returns_no_findings():
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0)))
    assert cat10.test_82_period_declaration(data, None) == []
    assert cat10.test_82_period_declaration(data, {}) == []
    assert cat10.test_82_period_declaration(data, {"periods": []}) == []


def test_matching_period_produces_no_finding():
    # Sale-linje: tax_amount NEGATIV (signeret konvention, jf. rubrik-formlen
    # "-sum(vat, sale)") -> beregnet output_vat = +250.0.
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0),
                          period="03", period_year="2024"))
    declarations = _decl(("2024-03", 250.0, 0.0, 0.0))
    findings = cat10.test_82_period_declaration(data, declarations)
    assert findings == []


def test_dkrc_purchase_line_counted_in_output_AND_input_rubric():
    """En DKRC-købslinje (indenlandsk omvendt betalingspligt) tælles med i
    UDGÅENDE moms OG — som fradragsberettiget RC — i indgående moms (den
    netter til nul i angivelsens total). Rettet 2026-09-17 efter manuel
    verifikation mod kundens 3-vejs-afstemning: den tidligere version udelod
    RC-fradragssiden af input_vat og gav en kunstig årsdifference på præcis
    årets RC-sum."""
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                                  tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC"),
                          period="03", period_year="2024"))
    declarations = _decl(("2024-03", 250.0, 250.0, 0.0))
    assert cat10.test_82_period_declaration(data, declarations) == []

    # Udelades RC-fradragssiden af angivelsen (input_vat=0), skal input-
    # rubrikken afvige — og kun den.
    wrong_declarations = _decl(("2024-03", 250.0, 0.0, 0.0))
    findings = cat10.test_82_period_declaration(data, wrong_declarations)
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["rubrik"] == "input_vat"


def test_service_vat_purchase_line_has_own_rubric_and_input_side():
    """RC-ydelser udland: egen rubrik OG fradragssiden i indgående moms."""
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                                  tax_code="SERVICE_VAT_EU"),
                          period="03", period_year="2024"))
    declarations = _decl(("2024-03", 0.0, 250.0, 250.0))
    assert cat10.test_82_period_declaration(data, declarations) == []


def test_ordinary_purchase_line_counted_as_input_vat():
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0, tax_code="STANDARD|I25"),
                          period="03", period_year="2024"))
    declarations = _decl(("2024-03", 0.0, 250.0, 0.0))
    assert cat10.test_82_period_declaration(data, declarations) == []


def test_real_difference_not_resolved_by_annual_total_is_high_severity():
    """Kun én periode i alt -- en difference her kan pr. definition ikke
    nulstilles over årstotalen, så den er et REELT fund (severity high)."""
    data = mk_data(mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0),
                          period="03", period_year="2024"))
    declarations = _decl(("2024-03", 999999.0, 0.0, 0.0))
    findings = cat10.test_82_period_declaration(data, declarations)
    assert len(findings) == 1
    assert findings[0]["severity"] == "high"
    assert findings[0]["test_id"] == 82


def test_timing_difference_resolved_by_annual_total_is_low_severity():
    """To perioder: købsmoms forskudt én periode (settlement-basis) -- pr.
    periode afviger begge, men årstotalen stemmer -> timing, severity low."""
    data = mk_data([
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=100.0, tax_code="STANDARD|I25"),
               transaction_id="T1", period="01", period_year="2024"),
        mk_txn(mk_line(debit_amount=2000.0, tax_amount=200.0, tax_code="STANDARD|I25"),
               transaction_id="T2", period="02", period_year="2024"),
    ])
    # Angivet: periode 1 for højt, periode 2 for lavt med samme beløb --
    # årstotal (300) stemmer præcis med det beregnede (100+200=300).
    declarations = _decl(("2024-01", 0.0, 150.0, 0.0), ("2024-02", 0.0, 150.0, 0.0))
    findings = cat10.test_82_period_declaration(data, declarations)
    assert len(findings) == 2
    assert all(f["severity"] == "low" for f in findings)
    assert all(f["transactions"][0]["timing"] is True for f in findings)


def test_period_without_declared_value_is_not_compared():
    """En periode uden en tilhørende post i angivelsen giver intet fund --
    der er intet at afstemme mod."""
    data = mk_data([
        mk_txn(mk_line(credit_amount=1000.0, tax_amount=-250.0), period="03", period_year="2024"),
        mk_txn(mk_line(credit_amount=2000.0, tax_amount=-500.0), transaction_id="T2",
               period="04", period_year="2024"),
    ])
    declarations = _decl(("2024-03", 250.0, 0.0, 0.0))  # ingen post for 2024-04
    assert cat10.test_82_period_declaration(data, declarations) == []


def test_pattern_matching_is_configurable_via_env(monkeypatch):
    """DKRC/SERVICE_VAT-genkendelsen er IKKE hårdkodet -- en engagement-
    specifik override via env skal kunne klassificere en anden kode-taksonomi."""
    monkeypatch.setattr(materiality, "VAT_DECLARATION_DKRC_PATTERNS", ["custom_dkrc_marker"])
    assert cat10._purchase_rubric("CUSTOM_DKRC_MARKER|WHATEVER") == "dkrc"
    assert cat10._purchase_rubric("DOMESTIC|REDUCED_PRIVATE_DKRC") == "input"  # default mønster slået fra


def test_default_patterns_match_documented_examples():
    assert cat10._purchase_rubric("DOMESTIC|REDUCED_PRIVATE_DKRC") == "dkrc"
    assert cat10._purchase_rubric("SERVICE_VAT_EU") == "rc_services"
    assert cat10._purchase_rubric("SERVICE_VAT_NOT_EU") == "rc_services"
    assert cat10._purchase_rubric("STANDARD|I25") == "input"


# --- supply_direction (byggetrin 8, Del D — empirisk rettelse) --------------
# Bekræftet mod den rigtige BC/NAV-fil: sale/køb afgøres af det EGNE
# supply_direction-felt, ikke debet/kredit. Debet/kredit er kun fallback, når
# supply_direction slet ikke er udfyldt (Excel-/SAF-T-oprindelse).

def test_supply_direction_overrides_debit_credit_heuristic():
    """En DEBET-linje eksplicit tagget supply_direction='sale' tælles som
    salg, ikke køb -- supply_direction har forrang over debet/kredit."""
    assert cat10._line_direction({"debit_amount": 1000.0, "credit_amount": 0.0,
                                  "supply_direction": "sale"}) == "sale"
    assert cat10._line_direction({"debit_amount": 0.0, "credit_amount": 1000.0,
                                  "supply_direction": "purchase"}) == "purchase"


def test_settlement_direction_is_excluded_even_with_debit_credit_set():
    assert cat10._line_direction({"debit_amount": 1000.0, "supply_direction": "settlement"}) is None


def test_missing_supply_direction_falls_back_to_debit_credit():
    assert cat10._line_direction({"debit_amount": 1000.0, "credit_amount": 0.0}) == "purchase"
    assert cat10._line_direction({"debit_amount": 0.0, "credit_amount": 1000.0}) == "sale"
    assert cat10._line_direction({"debit_amount": 0.0, "credit_amount": 0.0}) is None


def test_rubric_sums_signed_before_abs_nets_credit_notes():
    """En kreditnota (negativt vat_amount) i samme rubrik skal NETTE mod de
    øvrige linjer, før abs() anvendes -- ikke blive summeret oveni som en
    ekstra positiv værdi. To DKRC-købslinjer i samme periode: +250 og -100 ->
    output_vat-bidraget fra DKRC skal være |250-100|=150, IKKE
    |250|+|-100|=350."""
    data = mk_data([
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                       tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC", supply_direction="purchase"),
               transaction_id="T1", period="03", period_year="2024"),
        mk_txn(mk_line(debit_amount=-400.0, tax_amount=-100.0,
                       tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC", supply_direction="purchase"),
               transaction_id="T2", period="03", period_year="2024"),
    ])
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["output_vat"] == 150.0
