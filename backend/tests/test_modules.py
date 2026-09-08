"""
Modul-slankning: momskernen er default TIL; forensic/statistik, datakvalitet,
dublet-recovery og e-handel/særordninger er default FRA. Intet slettes — en
deaktiveret kontrol emitterer bare ingen fund, og alt kan tændes igen.

Testene dækker: kontrol->modul-mapping (alle 103), default-tilstand, konfig-
opløsning (override > env > default) og at motoren rent faktisk filtrerer fund
fra default-fra moduler, mens kernen består.
"""

import json
import os

from analytics import modules
from analytics.engine import run_all_tests
from validation.scenarios import SCENARIOS

_CATALOG = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "catalog", "rules.json")


# --- Mapping ---------------------------------------------------------------

def test_all_103_controls_mapped_to_known_module():
    assert set(modules.CONTROL_MODULE) == set(range(1, 104))
    assert all(v in modules.MODULES for v in modules.CONTROL_MODULE.values())


def test_only_core_is_default_active():
    assert modules.default_active_keys() == ["moms_kerne"]
    assert modules.MODULES["moms_kerne"]["default_active"] is True
    assert all(not meta["default_active"]
               for key, meta in modules.MODULES.items() if key != "moms_kerne")


def test_core_keeps_the_agreed_vat_controls():
    # Stikprøve fra berigelsesnotatet: disse SKAL blive i kernen.
    for tid in (11, 16, 17, 36, 46, 49, 57, 59, 72, 74, 81, 82, 83):
        assert modules.module_of(tid) == "moms_kerne", tid


def test_offmodule_controls_are_out_of_core():
    # Stikprøve: eksplicit fra-skårne kontroller må IKKE ligge i kernen.
    for tid in (41, 42, 55, 62, 63, 69, 84, 93):
        assert modules.module_of(tid) == "forensic_statistik", tid
    for tid in (47, 48, 51, 54):
        assert modules.module_of(tid) == "datakvalitet", tid
    for tid in (13, 15, 18):
        assert modules.module_of(tid) == "dublet_recovery", tid
    for tid in range(94, 104):
        assert modules.module_of(tid) == "ehandel_saerordninger", tid


# --- Konfig-opløsning ------------------------------------------------------

def test_resolve_default_is_core_only(monkeypatch):
    monkeypatch.delenv("ANALYTICS_MODULES", raising=False)
    assert modules.resolve_active_modules() == {"moms_kerne"}


def test_resolve_override_wins_and_ignores_unknown(monkeypatch):
    monkeypatch.setenv("ANALYTICS_MODULES", "alle")  # skal ignoreres pga. override
    got = modules.resolve_active_modules(["moms_kerne", "forensic_statistik", "vroevl"])
    assert got == {"moms_kerne", "forensic_statistik"}


def test_resolve_env_list_and_alle(monkeypatch):
    monkeypatch.setenv("ANALYTICS_MODULES", "forensic_statistik, datakvalitet")
    assert modules.resolve_active_modules() == {"forensic_statistik", "datakvalitet"}
    monkeypatch.setenv("ANALYTICS_MODULES", "alle")
    assert modules.resolve_active_modules() == modules.all_module_keys()


# --- Motor-filtrering (integration via valideringsscenarier) ---------------

def _defect_scenario_for(module_key):
    """Første valideringsscenarie hvis kontrol hører til det givne modul."""
    for s in SCENARIOS:
        if modules.module_of(s["test_id"]) == module_key:
            return s
    return None


def test_default_suppresses_offmodule_finding(monkeypatch):
    monkeypatch.delenv("ANALYTICS_MODULES", raising=False)
    s = _defect_scenario_for("forensic_statistik")
    assert s, "forventede mindst ét forensic-scenarie"
    tid = s["test_id"]
    default_ids = {f["test_id"] for f in run_all_tests(s["defect"])["all_findings"]}
    all_ids = {f["test_id"] for f in run_all_tests(
        s["defect"], active_modules=modules.all_module_keys())["all_findings"]}
    assert tid not in default_ids   # slået fra som standard
    assert tid in all_ids           # men fanges når modulet tændes


def test_core_finding_survives_default(monkeypatch):
    monkeypatch.delenv("ANALYTICS_MODULES", raising=False)
    s = _defect_scenario_for("moms_kerne")
    assert s
    default_ids = {f["test_id"] for f in run_all_tests(s["defect"])["all_findings"]}
    assert s["test_id"] in default_ids


def test_report_exposes_module_overview(monkeypatch):
    monkeypatch.delenv("ANALYTICS_MODULES", raising=False)
    s = _defect_scenario_for("forensic_statistik")
    rep = run_all_tests(s["defect"])
    assert "moduler" in rep and "filtrerede_fund" in rep
    core = next(m for m in rep["moduler"] if m["noegle"] == "moms_kerne")
    off = next(m for m in rep["moduler"] if m["noegle"] == "forensic_statistik")
    assert core["aktiv"] is True and off["aktiv"] is False
    assert rep["filtrerede_fund"] >= 1  # forensic-defekten blev filtreret fra


# --- Katalog bærer modul-felterne -----------------------------------------

def test_catalog_carries_module_fields():
    with open(_CATALOG, encoding="utf-8") as f:
        cat = json.load(f)
    assert "analyse_moduler" in cat
    assert {m["noegle"] for m in cat["analyse_moduler"]} == set(modules.MODULES)
    for r in cat["regler"]:
        assert r["analyse_modul"] in modules.MODULES
        assert isinstance(r["default_aktiv"], bool)
