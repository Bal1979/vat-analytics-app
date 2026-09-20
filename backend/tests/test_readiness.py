"""
Datagrundlag / kørbarhed: hvilke kontroller kan køre på et datasæt, og hvad mangler.

Adskiller de tre tilstande, der i dag alle ligner "grøn / 0 fund":
  kørt · sprunget over (manglende felt) · modul fra · kræver eksterne data.
"""

from analytics import readiness as rd
from analytics import modules
from analytics.engine import CATEGORIES, run_all_tests
from validation.builders import mk_line, mk_txn, mk_data

DEFAULT = set(modules.default_active_keys())   # kun momskernen
ALLE = modules.all_module_keys()


def test_profile_counts_populated_fields():
    data = mk_data(mk_txn([mk_line(country="DK"), mk_line(country="")]))
    prof = rd.profile_dataset(data)
    assert prof["linjer"] == 2
    assert prof["felter"]["country"]["udfyldte_linjer"] == 1
    assert prof["felter"]["country"]["til_stede"] is True


def test_external_controls_always_flagged():
    res = rd.assess(mk_data(mk_txn(mk_line())), ALLE, CATEGORIES)
    for tid in (82, 83, 85, 90, 99):
        c = next(x for x in res["kontroller"] if x["test_id"] == tid)
        assert c["status"] == rd.STATUS_EKSTERNE_DATA, tid


def test_control_in_off_module_is_modul_fra():
    # Forensic-kontrol (84) er default-fra.
    res = rd.assess(mk_data(mk_txn(mk_line())), DEFAULT, CATEGORIES)
    c84 = next(x for x in res["kontroller"] if x["test_id"] == 84)
    assert c84["status"] == rd.STATUS_MODUL_FRA


def test_blocked_when_required_field_absent():
    # Ingen landeinfo -> grænseoverskridende kontrol (27) kan ikke køre.
    res = rd.assess(mk_data(mk_txn(mk_line(country="", tax_code=""))), DEFAULT, CATEGORIES)
    c27 = next(x for x in res["kontroller"] if x["test_id"] == 27)
    assert c27["status"] == rd.STATUS_SPRUNGET_DATA
    assert "country" in c27["manglende_felter"]


def test_runs_when_required_field_present():
    res = rd.assess(mk_data(mk_txn(mk_line(country="DE", tax_code="K25"))), DEFAULT, CATEGORIES)
    c27 = next(x for x in res["kontroller"] if x["test_id"] == 27)
    assert c27["status"] == rd.STATUS_KOERT


def test_missing_data_summary_lists_blocking_field():
    res = rd.assess(mk_data(mk_txn(mk_line(country="", tax_code=""))), DEFAULT, CATEGORIES)
    felter = {m["felt"] for m in res["manglende_data"]}
    assert "country" in felter
    land = next(m for m in res["manglende_data"] if m["felt"] == "country")
    assert land["antal"] >= 1 and 27 in land["blokerer_kontroller"]


def test_summary_totals_add_up():
    res = rd.assess(mk_data(mk_txn(mk_line(country="DK", tax_code="S25"))), DEFAULT, CATEGORIES)
    s = res["opsummering"]
    assert s["i_alt"] == 109
    assert s["koert"] + s["sprunget_over_data"] + s["modul_fra"] + s["kraever_eksterne_data"] == 109


def test_report_carries_datagrundlag():
    data = mk_data(mk_txn(mk_line(country="DK", tax_code="S25")))
    rep = run_all_tests(data)
    assert "datagrundlag" in rep
    assert rep["datagrundlag"]["opsummering"]["i_alt"] == 109


# --- Del B: "ikke målbar"-gating (medium-fund-analysen, Bal-godkendt 2026-09-17) ---

def _big_dataset(n, **line_kw):
    """n transaktioner, ingen af dem har country/vat_number/description sat
    (mk_line/mk_txn's neutrale defaults) -- simulerer et strukturelt fravær
    over en population, der er stor nok til at håndhæve (>= MIN_TX_FOR_GATING)."""
    return mk_data([mk_txn(mk_line(debit_amount=1000.0, tax_code="N0", tax_percentage=0.0,
                                    tax_base=1000.0, **line_kw), transaction_id=f"T-{i}")
                    for i in range(n)])


def test_small_dataset_never_gated_even_at_0pct():
    """Under MIN_TX_FOR_GATING skal status forblive STATUS_SPRUNGET_DATA
    (uændret, informativ) -- IKKE den håndhævede STATUS_IKKE_MAALBART.
    Dette er præcis situationen for valideringssuitens et-transaktions-
    scenarier, som planter en ægte defekt ved at tømme ét felt."""
    data = mk_data(mk_txn(mk_line(country="", tax_code="N0", tax_percentage=0.0, tax_base=1000.0)))
    res = rd.assess(data, ALLE, CATEGORIES)
    c25 = next(x for x in res["kontroller"] if x["test_id"] == 25)
    assert c25["status"] == rd.STATUS_SPRUNGET_DATA


def test_large_dataset_0pct_country_is_ikke_maalbar():
    """Over MIN_TX_FOR_GATING og 0% country -> STATUS_IKKE_MAALBART, med
    dansk klienttekst i aarsag."""
    data = _big_dataset(rd.MIN_TX_FOR_GATING)
    res = rd.assess(data, ALLE, CATEGORIES)
    c25 = next(x for x in res["kontroller"] if x["test_id"] == 25)
    assert c25["status"] == rd.STATUS_IKKE_MAALBART
    assert c25["aarsag"] == "Kan ikke måles: Modpartens land findes ikke i datagrundlaget"
    assert res["opsummering"]["ikke_maalbar"] >= 1


def test_large_dataset_with_some_country_coverage_not_gated():
    """Selv én udfyldt linje ud af mange holder feltet 'til stede' -- v1's
    0%-tærskel, ingen fuzzy mellemtrin. Delvist udfyldte felter gates ALDRIG."""
    data = _big_dataset(rd.MIN_TX_FOR_GATING)
    data["transactions"][0]["lines"][0]["country"] = "DE"
    res = rd.assess(data, ALLE, CATEGORIES)
    c25 = next(x for x in res["kontroller"] if x["test_id"] == 25)
    assert c25["status"] == rd.STATUS_KOERT


def test_engine_suppresses_findings_for_ikke_maalbar_control():
    """analytics.engine.run_all_tests skal RENT FAKTISK fjerne kontrol 25's
    fund, når country er strukturelt fraværende på en stor population --
    ikke kun rapportere det (det er hele pointen med Del B)."""
    data = _big_dataset(rd.MIN_TX_FOR_GATING)
    rep = run_all_tests(data, active_modules=modules.all_module_keys())
    assert not any(f["test_id"] == 25 for f in rep["all_findings"])
    assert rep["ikke_maalbare_fund_fjernet"] >= 1


def test_control_04_description_subcheck_gated_on_large_absent_dataset():
    """Kontrol 4's Description-delcheck gates, når description er 0% udfyldt
    på en stor population -- men TransactionID/AccountID-delcheckene skal
    blive ved med at køre uændret (multi-felt-kontrol, delkontrol_gates)."""
    data = _big_dataset(rd.MIN_TX_FOR_GATING)
    # mk_txn's default description ("Postering") skal tømmes eksplicit her --
    # denne test undersøger netop 0%-fraværet.
    for txn in data["transactions"]:
        txn["description"] = ""
    # Ødelæg AccountID på én transaktion -- det delcheck skal STADIG fyre.
    data["transactions"][0]["lines"][0]["account_id"] = ""
    res = rd.assess(data, ALLE, CATEGORIES)
    gates = res["delkontrol_gates"]
    assert any(g["test_id"] == 4 and g["felt"] == "description" for g in gates)
    match = next(g for g in gates if g["test_id"] == 4)
    assert match["besked"] == "Kan ikke måles: Bilagstekst/beskrivelse findes ikke i datagrundlaget"

    rep = run_all_tests(data, active_modules=modules.all_module_keys())
    c4_findings = [f for f in rep["all_findings"] if f["test_id"] == 4]
    assert len(c4_findings) == 1  # kun AccountID-defekten, ingen Description-støj
    assert c4_findings[0]["transactions"][0]["missing_fields"] == ["AccountID (linje L1)"]


def test_control_04_not_gated_when_description_present():
    """KRITISK afgrænsning: er description til stede (selv delvist), skal
    kontrol 4 opføre sig helt uændret -- fanger en reel tom description.
    mk_txn's default description ("Postering") gør at hele populationen
    reelt er dækket her, bortset fra ÉN plantet defekt."""
    data = _big_dataset(rd.MIN_TX_FOR_GATING)
    data["transactions"][0]["description"] = ""  # én reel defekt
    res = rd.assess(data, ALLE, CATEGORIES)
    assert res["delkontrol_gates"] == []

    rep = run_all_tests(data, active_modules=modules.all_module_keys())
    c4_findings = [f for f in rep["all_findings"] if f["test_id"] == 4]
    assert len(c4_findings) == 1
    assert c4_findings[0]["transactions"][0]["missing_fields"] == ["Description"]
