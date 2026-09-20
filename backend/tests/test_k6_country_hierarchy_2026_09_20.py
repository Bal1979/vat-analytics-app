"""
K6-kalibrering (2026-09-20, Bal-godkendt spor "andre spor vi stadig mangler at
dække"): kontrol 71 (RC på indenlandsk handel) voksede 2.478 -> 10.836 efter
RC-detektion-via-beregningstype-rundens dom. Hovedsessionens empiriske dyk
(join af kunde 2s IFS-fund mod kundens kanoniske CSV) viste at landefeltet i
IFS-udtrækket er leverings-/bogføringsland, IKKE modpartens hjemland -- mens
momsnummerets EGET landepræfiks er det pålidelige signal for modpartens
faktiske registreringsland.

Fix: nyt, generisk hierarki i ``vat_rules`` (``vat_number_country``,
``counterparty_country``, ``country_field_mismatch``) -- gyldigt momsnummer-
landepræfiks FØRST, landefeltet som fallback. Bruges af
``cat09_reverse_charge._country`` (kontrol 70-75). Et "XX"-/ukendt præfiks
eller et tomt momsnummer giver INTET præfiks-signal (ingen særkode for "XX"
specifikt -- det er blot ikke et kendt land) og falder derfor tilbage til
landefeltet, som hidtil. En uoverensstemmelse mellem landefelt og momsnr-
præfiks tælles som metadata (``country_source_mismatch``) på fundet i stedet
for at blive et selvstændigt fund -- "mindst indgribende"-valget.

Empirisk på kunde 2 (se docs/CHANGELOG.md for det fulde billede): kontrol 71
10.836 -> 4.139 (RC×DK-kernepopulationen 2.478 + et XX-præfiks-residual 1.175
+ 486 vat-nummerløse E-kode-linjer, alle korrekt uændrede af hierarkiet fordi
de mangler et præfiks-signal). Kontrol 70 41 -> 348 (+307, nye ægte EU-køb
uden RC, tidligere skjult bag et "DENMARK"/"ESTONIA"-landefelt men med et
PL/EE/ES/BE/NL-momsnummerpræfiks). Vagtposter 22/25/27/28/30/72/73/74/84/87/
88/109 uændrede. BC-v5 byte-for-byte identisk (23.083 med sidecars/23.549
uden).
"""

from analytics import vat_rules as vr
from analytics.categories import cat09_reverse_charge as cat09
from validation.builders import mk_data, mk_txn, mk_line


# === vat_rules.vat_number_country ===========================================

def test_vat_number_country_eu_prefix():
    assert vr.vat_number_country("DE123456789") == "DE"
    assert vr.vat_number_country("PL1234567890") == "PL"


def test_vat_number_country_greece_prefix_maps_to_gr():
    assert vr.vat_number_country("EL123456789") == "GR"


def test_vat_number_country_recognizes_non_eu_iso_prefix():
    """GB/CH/US/CN m.fl. er ikke EU, men ægte, kendte ISO-landekoder --
    genkendes stadig som et landesignal (samme generiske landetabel som
    ``normalize_country``, ingen ny parallel logik)."""
    assert vr.vat_number_country("GB123456789") == "GB"
    assert vr.vat_number_country("CH123456789") == "CH"


def test_vat_number_country_xx_placeholder_is_no_signal():
    """"XX" er IKKE en rigtig ISO-landekode -- intet gæt, tomt resultat.
    Ingen særkode for "XX" i implementeringen: den er simpelthen fraværende
    fra landetabellen, ligesom ethvert andet ukendt to-bogstavs-præfiks."""
    assert vr.vat_number_country("XX1234567") == ""


def test_vat_number_country_empty_or_no_prefix():
    assert vr.vat_number_country("") == ""
    assert vr.vat_number_country(None) == ""
    assert vr.vat_number_country("12345678") == ""  # intet alfabetisk præfiks


# === vat_rules.counterparty_country (hierarkiet) ============================

def test_counterparty_country_vat_prefix_wins_over_country_field():
    """Kernen i K6: et gyldigt momsnummer-præfiks trumfer et modstridende
    landefelt."""
    assert vr.counterparty_country("DENMARK", "DE123456789") == "DE"


def test_counterparty_country_falls_back_when_vat_empty():
    assert vr.counterparty_country("DENMARK", "") == "DK"


def test_counterparty_country_falls_back_when_prefix_unknown():
    """XX-præfiks/tomt momsnummer: landefeltet gælder (fallback), ingen gæt."""
    assert vr.counterparty_country("DENMARK", "XX1234567") == "DK"


def test_counterparty_country_agreeing_signals():
    assert vr.counterparty_country("GERMANY", "DE123456789") == "DE"


def test_counterparty_country_both_unknown_is_empty():
    assert vr.counterparty_country("", "") == ""
    assert vr.counterparty_country("", "XX1234567") == ""


# === vat_rules.country_field_mismatch (metadata-signal) =====================

def test_country_field_mismatch_true_when_both_known_and_disagree():
    assert vr.country_field_mismatch("DENMARK", "DE123456789") is True


def test_country_field_mismatch_false_when_agree():
    assert vr.country_field_mismatch("GERMANY", "DE123456789") is False


def test_country_field_mismatch_none_when_no_vat_prefix_signal():
    """Intet grundlag for sammenligning -- IKKE det samme som "enige"."""
    assert vr.country_field_mismatch("DENMARK", "") is None
    assert vr.country_field_mismatch("DENMARK", "XX1234567") is None


def test_country_field_mismatch_none_when_country_field_empty():
    assert vr.country_field_mismatch("", "DE123456789") is None


# === cat09._country / _country_mismatch (kontrol 70-75's fælles hierarki) ===

def test_cat09_country_uses_vat_prefix_over_country_field():
    line = mk_line(country="DENMARK", vat_number="DE123456789")
    assert cat09._country(line, {}) == "DE"


def test_cat09_country_falls_back_on_xx_prefix():
    line = mk_line(country="DENMARK", vat_number="XX1234567")
    assert cat09._country(line, {}) == "DK"


def test_cat09_country_mismatch_flag_true():
    line = mk_line(country="DENMARK", vat_number="DE123456789")
    assert cat09._country_mismatch(line, {}) is True


def test_cat09_country_mismatch_flag_none_without_signal():
    line = mk_line(country="DENMARK", vat_number="")
    assert cat09._country_mismatch(line, {}) is None


# === Integrationstest: kontrol 71 (RC på indenlandsk handel) ================

def test_71_genuine_foreign_vat_prefix_is_no_longer_flagged():
    """Kernescenariet der drev K6: et RC-markeret bilag med landefeltet
    "DENMARK" (leverings-/bogføringsland) men et ægte udenlandsk EU-
    momsnummer (DE) er et korrekt EU-varekøb -- IKKE indenlandsk RC-misbrug."""
    line = mk_line(debit_amount=1000.0, country="DENMARK", vat_number="DE123456789",
                    tax_code="E1G", vat_calculation_type="Calculated Tax")
    data = mk_data(mk_txn(line))
    assert cat09.test_71_rc_on_domestic(data, {}) == []


def test_71_xx_prefix_residual_still_flagged_unchanged():
    """XX-præfikset momsnummer giver intet landesignal -- hierarkiet falder
    tilbage til landefeltet (DENMARK -> DK), samme resultat som før K6."""
    line = mk_line(debit_amount=1000.0, country="DENMARK", vat_number="XX1234567",
                    tax_code="E1G", vat_calculation_type="Calculated Tax")
    data = mk_data(mk_txn(line))
    findings = cat09.test_71_rc_on_domestic(data, {})
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["country_source_mismatch"] is None


def test_71_rc_code_on_dk_vat_number_still_flagged_unchanged():
    """RC×DK-kernepopulationen (den oprindelige, reelle observation) er
    UÆNDRET af K6 -- momsnummerets eget præfiks bekræfter blot det samme
    land landefeltet allerede sagde."""
    line = mk_line(debit_amount=1000.0, country="DENMARK", vat_number="DK12345678",
                    tax_code="RC")
    txn = mk_txn(line, description="Almindelig postering")
    data = mk_data(txn)
    findings = cat09.test_71_rc_on_domestic(data, {})
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["country_source_mismatch"] is False


def test_71_no_vat_number_falls_back_unchanged():
    """Ingen momsnummer overhovedet -- uændret adfærd (samme sti som før K6:
    landefeltet alene afgør sagen)."""
    line = mk_line(debit_amount=1000.0, country="DENMARK", vat_number="",
                    tax_code="E1G", vat_calculation_type="Calculated Tax")
    data = mk_data(mk_txn(line))
    findings = cat09.test_71_rc_on_domestic(data, {})
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["country_source_mismatch"] is None


# === Integrationstest: kontrol 70 (EU-køb uden RC) — ny detektion ===========

def test_70_finds_eu_purchase_hidden_behind_wrong_country_field():
    """Kontrol 70's vækst-scenarie: landefeltet siger "DENMARK", men
    momsnummeret er polsk (PL) -- et ægte EU-køb uden moms og uden RC-
    markering, som landefeltet alene ville have skjult som indenlandsk."""
    line = mk_line(debit_amount=50000.0, country="DENMARK", vat_number="PL1234567890",
                    tax_code="0", tax_amount=0.0, vat_calculation_type="")
    data = mk_data(mk_txn(line, description="Køb"))
    findings = cat09.test_70_eu_service_no_rc(data, {})
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["country"] == "PL"
    assert findings[0]["transactions"][0]["country_source_mismatch"] is True


def test_70_agreeing_country_and_vat_prefix_unaffected():
    """Regression: når landefelt og momsnummer er enige, er kontrol 70
    uændret."""
    line = mk_line(debit_amount=50000.0, country="DE", vat_number="DE123456789",
                    tax_code="E1G", tax_amount=0.0, vat_calculation_type="")
    data = mk_data(mk_txn(line, description="EU-køb"))
    findings = cat09.test_70_eu_service_no_rc(data, {})
    assert len(findings) == 1
    assert findings[0]["transactions"][0]["country_source_mismatch"] is False
