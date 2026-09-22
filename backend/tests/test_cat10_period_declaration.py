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
from analytics import vat_rules as vr
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


# --- vat_calculation_type (balai_extensions, byggetrin 9/Del A hærdning) ---
# Kontrakt v0.4.1: kontrol 82 bruger feltet FØR navnemønstrene, når det er
# til stede (kun på den kanoniske vej, når vat_setup.csv er indlæst). Se
# cat10._purchase_rubric's docstring for hele beslutningstræet.

def test_calc_type_normal_vat_is_deterministically_input():
    """Beregningstypen siger eksplicit IKKE reverse charge -> 'input',
    UANSET kodenavnet (her et navn der ellers ville matche DKRC-mønstret)."""
    assert cat10._purchase_rubric("DOMESTIC|REDUCED_PRIVATE_DKRC", "Normal VAT") == "input"


def test_calc_type_full_vat_energy_tax_code_is_energy_tax_not_input():
    """Fix-runde 2026-09-22 (Bal-godkendt, FEJL 1 — empirisk påvist mod
    Nordic RCC's TastSelv-angivelse): en afgiftskode som "DOMESTIC|
    ELECTRICITY_TAX" (calc_type "Full VAT") blev tidligere fejlagtigt
    klassificeret 'input' (almindelig købsmoms) og talte dermed med i den
    beregnede input_vat-rubrik. Den hører til angivelsens EGEN
    "energy_taxes"-rubrik (afgift, ikke moms), som v1 bevidst ikke
    afstemmer -- se _compute_period_rubrics/is_energy_tax_code."""
    assert cat10._purchase_rubric("DOMESTIC|ELECTRICITY_TAX", "Full VAT") == "energy_tax"
    assert cat10._purchase_rubric("DOMESTIC|ELECTRICITY_TAX", "Full VAT", 0.0) == "energy_tax"


def test_calc_type_reverse_charge_domestic_bus_group_is_dkrc_without_name_pattern():
    """Bus.-gruppen (første led før '|') afgør indenlandsk-skellet -- koden
    behøver IKKE indeholde 'dkrc' i navnet, når calc_type + Bus.-gruppe
    allerede er deterministisk (hærdning mod navngivnings-afhængighed)."""
    assert cat10._purchase_rubric("DOMESTIC|SOME_OTHER_NAME", "Reverse Charge VAT") == "dkrc"


def test_calc_type_reverse_charge_eu_service_pattern_is_rc_services():
    assert cat10._purchase_rubric("EU|SERVICE_VAT_EU", "Reverse Charge VAT") == "rc_services"


def test_calc_type_reverse_charge_outside_service_pattern_is_rc_services():
    assert cat10._purchase_rubric("OUTSIDE DK/EU|SERVICE_VAT_NOT_EU", "Reverse Charge VAT") == "rc_services"


def test_calc_type_reverse_charge_foreign_goods_without_service_pattern_is_input():
    """RC-varekøb fra udlandet (EU/OUTSIDE, IKKE 'service' i navnet): hverken
    calc_type eller Bus.-gruppen alene skelner ydelse fra vare -- falder til
    'input' (almindelig købsmoms), PRÆCIS som før hærdningen (regressions-
    kriteriet fra opgavens Del A: v4-datasættets 'OUTSIDE DK/EU|GOODS_VAT_NOT_EU'
    og 'EU|GOODS_VAT_EU' skal forblive 'input')."""
    assert cat10._purchase_rubric("EU|GOODS_VAT_EU", "Reverse Charge VAT") == "input"
    assert cat10._purchase_rubric("OUTSIDE DK/EU|GOODS_VAT_NOT_EU", "Reverse Charge VAT") == "input"


def test_calc_type_absent_falls_back_to_pure_name_patterns():
    """Tomt/manglende calc_type (Excel-/SAF-T-oprindelse, eller kanonisk vej
    uden vat_setup.csv) -- uændret ren navnemønster-klassifikation."""
    assert cat10._purchase_rubric("DOMESTIC|REDUCED_PRIVATE_DKRC", "") == "dkrc"
    assert cat10._purchase_rubric("DOMESTIC|REDUCED_PRIVATE_DKRC") == "dkrc"
    assert cat10._purchase_rubric("STANDARD|I25", "") == "input"


def test_compute_period_rubrics_reads_vat_calculation_type_from_line():
    """_compute_period_rubrics sender linjens vat_calculation_type videre til
    _purchase_rubric -- en linje uden 'dkrc' i kodenavnet, men med calc_type
    'Reverse Charge VAT' og Bus.-gruppe DOMESTIC, tælles korrekt med i
    output_vat (ikke input_vat)."""
    data = mk_data(mk_txn(
        mk_line(debit_amount=1000.0, tax_amount=250.0,
                tax_code="DOMESTIC|SOME_OTHER_NAME",
                vat_calculation_type="Reverse Charge VAT"),
        period="03", period_year="2024"))
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["output_vat"] == 250.0
    assert computed["2024-03"]["input_vat"] == 250.0  # RC-fradragssiden, jf. modulets dokumentation


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


# --- FEJL 1 (fix-runde 2026-09-22, Bal-godkendt): elafgift/energiafgift ----
# Empirisk påvist mod Nordic RCC's TastSelv-angivelse: afgiftskoder (fx
# elafgift) blev talt med i input_vat-rubrikken, men hører til angivelsens
# EGEN "energy_taxes"-rubrik, som v1 bevidst ikke afstemmer. SYNTETISKE
# koder/tal (INGEN kundedata).

def test_is_energy_tax_code_matches_generic_name_pattern():
    """Kode-navnemønstret (materiality.VAT_DECLARATION_ENERGY_TAX_PATTERNS,
    default "_TAX") er GENERISK -- BC/NAVs suffikskonvention for afgifter,
    ikke en kundespecifik værdi. Matcher enhver "..._TAX"-kode, ikke kun
    elafgift."""
    assert vr.is_energy_tax_code("DOMESTIC|ELECTRICITY_TAX") is True
    assert vr.is_energy_tax_code("DOMESTIC|CO2_TAX") is True
    assert vr.is_energy_tax_code("DOMESTIC|SOME_OTHER_TAX", "Normal VAT", 25.0) is True


def test_is_energy_tax_code_structural_fallback_requires_both_signals():
    """Sekundært signal: tax_percentage PRÆCIS 0 OG calc_type "Full VAT" --
    begge skal være til stede. Et kodenavn UDEN "_TAX"-mønstret, men med
    denne kombination, genkendes stadig (strukturelt signal, ikke kun
    navnemønster). tax_percentage=0 ALENE (fx en almindelig nulsats-/
    fritagelseskode) er IKKE nok -- undgår falske positiver."""
    assert vr.is_energy_tax_code("DOMESTIC|SOME_DUTY", "Full VAT", 0.0) is True
    assert vr.is_energy_tax_code("DOMESTIC|NO_VAT", "Normal VAT", 0.0) is False
    assert vr.is_energy_tax_code("DOMESTIC|SOME_DUTY", "Full VAT", 25.0) is False
    assert vr.is_energy_tax_code("DOMESTIC|SOME_DUTY", "Full VAT", None) is False


def test_is_energy_tax_code_no_signal_is_false():
    assert vr.is_energy_tax_code("") is False
    assert vr.is_energy_tax_code("DOMESTIC|STANDARD_VAT", "Normal VAT", 25.0) is False


def test_energy_tax_line_excluded_entirely_from_input_vat_rubric():
    """En elafgifts-/energiafgiftslinje bidrager IKKE til input_vat -- hverken
    som almindelig købsmoms, DKRC eller RC-ydelser -- den er strukturelt
    udenfor de tre afstemte rubrikker (v1 afstemmer ikke "energy_taxes")."""
    data = mk_data([
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                       tax_code="DOMESTIC|STANDARD_VAT", supply_direction="purchase"),
               transaction_id="T1", period="03", period_year="2024"),
        mk_txn(mk_line(debit_amount=500.0, tax_amount=100.0,
                       tax_code="DOMESTIC|ELECTRICITY_TAX",
                       vat_calculation_type="Full VAT", supply_direction="purchase"),
               transaction_id="T2", period="03", period_year="2024"),
    ])
    computed = cat10._compute_period_rubrics(data)
    # Kun den almindelige købsmomslinje (250.00) -- elafgiften (100.00) er
    # UDELADT, ikke medregnet.
    assert computed["2024-03"]["input_vat"] == 250.0


def test_default_energy_tax_pattern_documented_example():
    """materiality.VAT_DECLARATION_ENERGY_TAX_PATTERNS's default ("_TAX")
    rammer PRÆCIS det dokumenterede eksempel (RCC-empiri: "DOMESTIC|
    ELECTRICITY_TAX")."""
    assert vr.text_matches_any("DOMESTIC|ELECTRICITY_TAX",
                                materiality.VAT_DECLARATION_ENERGY_TAX_PATTERNS)


def test_energy_tax_pattern_is_configurable_via_env(monkeypatch):
    monkeypatch.setenv("MATERIALITY_VAT_DECLARATION_ENERGY_TAX_PATTERNS", "MINERAL_OIL_TAX")
    import importlib
    from analytics import materiality as mat
    importlib.reload(mat)
    try:
        assert "mineral_oil_tax" in mat.VAT_DECLARATION_ENERGY_TAX_PATTERNS
        assert "_tax" not in mat.VAT_DECLARATION_ENERGY_TAX_PATTERNS
    finally:
        monkeypatch.delenv("MATERIALITY_VAT_DECLARATION_ENERGY_TAX_PATTERNS", raising=False)
        importlib.reload(mat)


# --- FEJL 2 (fix-runde 2026-09-22, Bal-godkendt): fradragsprocent på -------
# fradragssiden. Samme kildefelt/formel som kontrol 109
# (non_deductible_vat_pct), genbrugt via vat_form.code_rate_lookup -- ikke en
# parallel opsætningslæsning. SYNTETISKE koder/tal (INGEN kundedata).

def _setup(tax_code, non_deductible_pct, tax_percentage=25.0):
    return {"header": {"vat_setup_loaded": True},
            "tax_table": [{"tax_code": tax_code, "tax_percentage": tax_percentage,
                           "non_deductible_vat_pct": non_deductible_pct, "setup_matched": True}]}


def test_partial_deduction_reduces_input_vat_for_ordinary_purchase_code():
    """En almindelig købskode med 40% ikke-fradragsberettiget (RCC-mønster,
    syntetisk kode/tal): bogført moms 1.000,00, men kun 60% (600,00) er
    fradragsberettiget -- input_vat-rubrikken skal afspejle DET
    fradragsberettigede beløb, ikke det fulde bogførte."""
    extra = _setup("DOMESTIC|REDUCED_PRIVATE_DKRC_LIKE", 40.0)
    data = mk_data(mk_txn(mk_line(debit_amount=4000.0, tax_amount=1000.0,
                                  tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC_LIKE",
                                  supply_direction="purchase"),
                          period="03", period_year="2024"), **extra)
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["input_vat"] == 600.0


def test_partial_deduction_applies_to_dkrc_fradragsside_but_not_output_vat():
    """DKRC med delvis fradragsret (RCC-facit-mønster: 40% ikke-fradrags-
    berettiget): output_vat (liability-siden) forbliver DET FULDE beløb --
    kun DKRCs bidrag til input_vat (fradragssiden) reduceres. To-sidet
    omvendt betalingspligt-mekanik: fuld udgående forpligtelse, begrænset
    indgående fradrag."""
    extra = _setup("DOMESTIC|REDUCED_PRIVATE_DKRC", 40.0)
    data = mk_data(mk_txn(mk_line(debit_amount=4000.0, tax_amount=1000.0,
                                  tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC",
                                  supply_direction="purchase"),
                          period="03", period_year="2024"), **extra)
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["output_vat"] == 1000.0  # FULDT beløb, uændret
    assert computed["2024-03"]["input_vat"] == 600.0      # KUN 60% fradrag


def test_partial_deduction_applies_to_rc_services_fradragsside_but_not_own_rubric():
    """RC-ydelser fra udlandet med delvis fradragsret: egen rc_services-
    rubrik forbliver DET FULDE beløb, kun bidraget til input_vat
    reduceres."""
    extra = _setup("EU|SERVICE_VAT_EU", 25.0)
    data = mk_data(mk_txn(mk_line(debit_amount=2000.0, tax_amount=800.0,
                                  tax_code="EU|SERVICE_VAT_EU",
                                  supply_direction="purchase"),
                          period="03", period_year="2024"), **extra)
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["rc_services"] == 800.0  # FULDT beløb, uændret
    assert computed["2024-03"]["input_vat"] == 600.0      # 75% af 800 = 600


def test_full_deduction_code_is_unaffected_by_partial_deduction_fix():
    """non_deductible_vat_pct None/0 (fuld fradragsret, uændret opsætning) ->
    UÆNDRET adfærd, 100% af den bogførte moms tælles med."""
    extra = _setup("DOMESTIC|STANDARD_VAT", 0.0)
    data = mk_data(mk_txn(mk_line(debit_amount=4000.0, tax_amount=1000.0,
                                  tax_code="DOMESTIC|STANDARD_VAT",
                                  supply_direction="purchase"),
                          period="03", period_year="2024"), **extra)
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["input_vat"] == 1000.0


def test_partial_deduction_without_vat_setup_loaded_is_unaffected():
    """Uden vat_setup indlæst (intet tax_table-opslag) -- uændret adfærd,
    100% fradrag, PRÆCIS som før denne fix-runde."""
    data = mk_data(mk_txn(mk_line(debit_amount=4000.0, tax_amount=1000.0,
                                  tax_code="DOMESTIC|REDUCED_PRIVATE_DKRC",
                                  supply_direction="purchase"),
                          period="03", period_year="2024"))
    computed = cat10._compute_period_rubrics(data)
    assert computed["2024-03"]["input_vat"] == 1000.0


def test_deductible_fraction_helper_matches_control_109_formula():
    """_deductible_fraction genbruger PRÆCIS kontrol 109's formel
    (max(0, 100 - nd_pct) / 100) -- samme kildefelt, ingen parallel logik."""
    setup_by_code = {"X": {"tax_percentage": 25.0, "non_deductible_vat_pct": 40.0,
                            "setup_matched": True}}
    assert cat10._deductible_fraction("X", setup_by_code) == 0.6
    assert cat10._deductible_fraction("UKENDT_KODE", setup_by_code) == 1.0
    setup_by_code_unmatched = {"X": {"setup_matched": False}}
    assert cat10._deductible_fraction("X", setup_by_code_unmatched) == 1.0
    setup_by_code_full = {"X": {"non_deductible_vat_pct": 0.0, "setup_matched": True}}
    assert cat10._deductible_fraction("X", setup_by_code_full) == 1.0
    setup_by_code_none = {"X": {"non_deductible_vat_pct": None, "setup_matched": True}}
    assert cat10._deductible_fraction("X", setup_by_code_none) == 1.0
