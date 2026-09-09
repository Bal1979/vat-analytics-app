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
    assert s["i_alt"] == 103
    assert s["koert"] + s["sprunget_over_data"] + s["modul_fra"] + s["kraever_eksterne_data"] == 103


def test_report_carries_datagrundlag():
    data = mk_data(mk_txn(mk_line(country="DK", tax_code="S25")))
    rep = run_all_tests(data)
    assert "datagrundlag" in rep
    assert rep["datagrundlag"]["opsummering"]["i_alt"] == 103
