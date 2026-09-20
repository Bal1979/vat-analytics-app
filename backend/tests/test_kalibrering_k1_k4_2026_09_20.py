"""
Kalibreringsrunden K1-K4 (gap-analyse-runde 2/kunde 2, Bal-godkendt
2026-09-20) — de fire kontroller, der "vågnede" med F1-F3's D-parter-join,
men støjede voldsomt på kunde 2s IFS-datasæt. Se docs/CHANGELOG.md for den
fulde empiriske fordeling og før/efter-tabel.

K1 (kontrol 28): landeform-bevidst momsnummer-formatvalidering.
K2 (kontrol 32): udelad momskode-løse linjer + aggregér pr. part-nøgle.
K3 (kontrol 25): kontobaseret gren (bilagsaggregering) + konservativ
                 landebestemmelse.
K4 (kontrol 84): konservativ, generisk koncernintern-undtagelse.
"""

from analytics import vat_rules as vr
from analytics import vat_form as vf
from analytics.categories import cat03_vat_rate_validation as cat03
from analytics.categories import cat04_cross_border_eu as cat04
from analytics.categories import cat11_fraud_mtic as cat11
from validation.builders import mk_data, mk_txn, mk_line


# === K1: kontrol 28 -- landeform-bevidst momsnummer-format ===================

def test_k1_non_eu_prefix_is_not_validated_against_eu_format():
    """Et globalt Tax ID med et gyldigt, ikke-EU landepræfiks (schweizisk
    'CHE'-nummer) skal IKKE flages, selvom det aldrig ville matche noget
    EU-lands regex."""
    valid, reason = vr.validate_eu_vat_format("CHE123456789", "CH")
    assert valid is True
    assert "ikke-EU" in reason


def test_k1_unknown_country_and_no_prefix_is_not_validated():
    """Hverken nummeret selv eller det oplyste land peger på et EU-land --
    'ikke valideret', ikke 'ugyldigt'."""
    valid, reason = vr.validate_eu_vat_format("123456789", "")
    assert valid is True
    assert "ikke valideret" in reason


def test_k1_genuine_eu_format_error_still_fires_high():
    """Et EU-præfikseret nummer, der reelt fejler sit eget lands format,
    er FORTSAT et fund (uændret sværhedsgrad) -- den type må ikke forsvinde."""
    line = mk_line(debit_amount=1000.0, country="DE", vat_number="DE12")
    findings = cat04.test_28_invalid_eu_vat_format(
        mk_data(mk_txn(line)), {"suppliers": {}, "customers": {}}
    )
    assert len(findings) == 1
    assert findings[0]["severity"] == "high"


def test_k1_valid_eu_number_is_clean():
    line = mk_line(debit_amount=1000.0, country="DE", vat_number="DE123456789")
    findings = cat04.test_28_invalid_eu_vat_format(
        mk_data(mk_txn(line)), {"suppliers": {}, "customers": {}}
    )
    assert findings == []


# === K2: kontrol 32 -- momskode-scope + pr.-part-aggregering =================

def _ctx():
    return {"suppliers": {}, "customers": {}}


def test_k2_line_without_tax_code_is_not_flagged():
    """Strukturelt uden for momsscope (fx en bankpostering) -- ingen
    momskode betyder intet modpartskrav, selvom valutaen ser udenlandsk ud."""
    line = mk_line(debit_amount=1000.0, country="", currency="EUR", tax_code="")
    findings = cat04.test_32_missing_country_on_foreign(mk_data(mk_txn(line)), _ctx())
    assert findings == []


def test_k2_vat_relevant_line_missing_country_is_flagged():
    line = mk_line(debit_amount=1000.0, country="", currency="EUR", tax_code="I25")
    findings = cat04.test_32_missing_country_on_foreign(mk_data(mk_txn(line)), _ctx())
    assert len(findings) == 1
    assert findings[0]["severity"] == "low"


def test_k2_multiple_lines_same_party_aggregate_to_one_finding():
    """Samme part (identificeret ved momsnummer) på flere posteringer giver
    ÉT fund, ikke ét pr. linje (kontrol 80-mønstret)."""
    lines = [
        mk_line(debit_amount=100.0 * i, country="", vat_number="DE123456789",
                tax_code="I25", record_id=f"L{i}")
        for i in range(1, 6)
    ]
    data = mk_data(mk_txn(lines))
    findings = cat04.test_32_missing_country_on_foreign(data, _ctx())
    assert len(findings) == 1
    assert "5 postering" in findings[0]["description"]


def test_k2_different_parties_give_separate_findings():
    lines = [
        mk_line(debit_amount=100.0, country="", vat_number="DE123456789", tax_code="I25"),
        mk_line(debit_amount=100.0, country="", vat_number="FR12345678901", tax_code="I25"),
    ]
    data = mk_data(mk_txn(lines))
    findings = cat04.test_32_missing_country_on_foreign(data, _ctx())
    assert len(findings) == 2


# === K3: kontrol 25 -- kontobaseret gren (bilagsaggregering) =================

def _force_account_based(data):
    data["_vat_form_cache"] = {"form": vf.FORM_ACCOUNT_BASED}
    return data


def _setup_table(*entries):
    return [{"tax_code": code, "tax_percentage": pct} for code, pct in entries]


def test_k3_dispatcher_uses_line_based_branch_by_default():
    """Et lille/almindeligt (linjebaseret) datasæt rammer den UÆNDREDE
    linjebaserede gren -- ren regression."""
    line = mk_line(debit_amount=1000.0, tax_code="N0", tax_percentage=0.0, tax_base=1000.0)
    findings = cat03.test_25_zero_rate_domestic(mk_data(mk_txn(line)))
    assert len(findings) == 1


def test_k3_account_based_no_finding_when_voucher_has_vat_elsewhere():
    """Grundlagslinje (0% på egen linje) + en momskonto-linje i SAMME bilag
    med det faktiske momsbeløb -- ikke et fund (F3-formhypotesen)."""
    base_line = mk_line(account_id="4000", debit_amount=1000.0, tax_code="E1G",
                         record_id="L1")
    vat_line = mk_line(account_id="27200", debit_amount=250.0, tax_code="E1G",
                        tax_amount=250.0, record_id="L2")
    data = mk_data(mk_txn([base_line, vat_line]),
                    tax_table=_setup_table(("E1G", 25.0)))
    _force_account_based(data)
    findings = cat03._test_25_account_based(data)
    assert findings == []


def test_k3_account_based_finding_when_voucher_has_no_vat_anywhere():
    """Samme form, men INGEN linje i bilaget bogfører moms for en 0%-kode,
    og modparten er ubestridt indenlandsk (tom landetekst) -- ét fund."""
    base_line = mk_line(account_id="4000", debit_amount=1000.0, tax_code="E0G",
                         country="")
    data = mk_data(mk_txn([base_line]), tax_table=_setup_table(("E0G", 0.0)))
    _force_account_based(data)
    findings = cat03._test_25_account_based(data)
    assert len(findings) == 1
    assert findings[0]["severity"] == "medium"


def test_k3_account_based_unresolvable_foreign_name_is_not_flagged():
    """K3's konservative landebestemmelse: en udfyldt, men ikke-normaliserbar
    landetekst (fuldt engelsk landenavn, som normalize_country ikke kender)
    må IKKE antages indenlandsk -- 86,9% af FØR-fundene var reelt
    udenlandske bilag fejlklassificeret på præcis denne måde."""
    base_line = mk_line(account_id="4000", debit_amount=1000.0, tax_code="E0S",
                         country="ROMANIA")
    data = mk_data(mk_txn([base_line]), tax_table=_setup_table(("E0S", 0.0)))
    _force_account_based(data)
    findings = cat03._test_25_account_based(data)
    assert findings == []


def test_k3_account_based_known_foreign_country_is_not_flagged():
    base_line = mk_line(account_id="4000", debit_amount=1000.0, tax_code="E0S",
                         country="DE")
    data = mk_data(mk_txn([base_line]), tax_table=_setup_table(("E0S", 0.0)))
    _force_account_based(data)
    assert cat03._test_25_account_based(data) == []


# === K4: kontrol 84 -- konservativ koncernintern-undtagelse ==================

def _fraud_line(**kw):
    # tax_code sat efter K5-b's momskode-værn (kontrol 84-efterforskningen,
    # 2026-09-20), så disse tests fortsat rammer koncern-undtagelseslogikken
    # og ikke standser i værnet.
    defaults = dict(debit_amount=60000.0, country="DE", vat_number="",
                     tax_code="I25", description="Køb af mobiltelefoner")
    defaults.update(kw)
    return mk_line(**defaults)


def test_k4_intercompany_flag_exempts_line():
    line = _fraud_line(intercompany=True)
    findings = cat11.test_84_missing_trader(mk_data(mk_txn(line)), {})
    assert findings == []


def test_k4_internal_party_code_pattern_exempts_line_without_flag():
    """Selv uden kildesystemets intercompany-flag: et momsnummer formet som
    en intern koncern-partskode (landepræfiks + 1-3 cifre, fx 'US10') er et
    generisk, ikke kunde-navngivet koncern-signal."""
    line = _fraud_line(vat_number="US10", intercompany=False)
    findings = cat11.test_84_missing_trader(mk_data(mk_txn(line)), {})
    assert findings == []


def test_k4_genuine_missing_trader_pattern_still_fires():
    """Regression: uden koncern-signal fyrer kontrollen fortsat på den
    klassiske missing-trader-kombination (samme scenarie som
    validation/scenarios.py's planted defect for kontrol 84)."""
    line = _fraud_line(intercompany=False)
    findings = cat11.test_84_missing_trader(mk_data(mk_txn(line)), {})
    assert len(findings) == 1
    assert findings[0]["severity"] == "critical"


def test_k4_control_86_and_92_unaffected_by_intercompany_flag():
    """#86/#92 er UÆNDREDE af K4 -- ingen logikændring, kun verificeret at de
    hører til forensic_statistik-modulet (allerede default FRA)."""
    from analytics import modules
    assert modules.CONTROL_MODULE.get(84) == "forensic_statistik"
    assert modules.CONTROL_MODULE.get(86) == "forensic_statistik"
    assert modules.CONTROL_MODULE.get(92) == "forensic_statistik"
    assert modules.MODULES["forensic_statistik"]["default_active"] is False
