"""
tools/report_curation.py — kunderapportens kuraterings-nøglemekanisme
(byggetrin ~10, Bal-godkendt 2026-09-18): seed (ny kuration), merge
(genkørsel — rådgiverens indhold vinder) og fil-I/O.
"""

import json

from analytics import materiality
from tools import report_curation as rc


def _finding(test_id, severity="medium", account_id="4000", amount=0.0):
    return {
        "test_id": test_id, "test_name": "x", "severity": severity,
        "estimated_amount": amount, "description": "beskrivelse",
        "transactions": [{"account_id": account_id, "transaction_id": "T1",
                           "date": "2025-01-10", "amount": amount, "description": "linje"}],
    }


# --- build_auto_block / should_promote --------------------------------------

def test_build_auto_block_counts_and_sums():
    findings = [_finding(80, amount=100.0), _finding(80, amount=250.0)]
    auto = rc.build_auto_block("momsbehandling_pr_konto", findings, now="2026-01-01T00:00:00+00:00")
    assert auto["kilde_kontroller"] == [80]
    assert auto["fund_antal"] == 2
    assert auto["beloeb_i_alt"] == 350.0
    assert len(auto["evidens"]) == 2
    assert auto["evidens"][0]["konto"] == "4000"


def test_build_auto_block_caps_evidence_rows():
    findings = [_finding(80, amount=1.0) for _ in range(15)]
    auto = rc.build_auto_block("momsbehandling_pr_konto", findings)
    assert auto["fund_antal"] == 15
    assert len(auto["evidens"]) == 10


def test_should_promote_high_severity_always_true():
    auto = {"severity_counts": {"high": 1}, "beloeb_i_alt": 0.0}
    assert rc.should_promote("dataanomalier", auto) is True


def test_should_promote_timing_always_true_even_with_no_findings():
    auto = {"severity_counts": {}, "beloeb_i_alt": 0.0}
    assert rc.should_promote("timing", auto) is True


def test_should_promote_medium_below_threshold_is_false():
    auto = {"severity_counts": {"medium": 1}, "beloeb_i_alt": 1.0}
    assert rc.should_promote("dataanomalier", auto) is False


def test_should_promote_medium_above_threshold_is_true():
    auto = {"severity_counts": {"medium": 1},
            "beloeb_i_alt": materiality.REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD}
    assert rc.should_promote("dataanomalier", auto) is True


# --- seed_curation -----------------------------------------------------------

def test_seed_curation_creates_one_group_per_theme_with_findings():
    findings = [_finding(19, severity="high"), _finding(82, severity="low")]
    curation = rc.seed_curation(findings, now="2026-01-01T00:00:00+00:00")
    assert set(curation["grupper"]) == {"kodeopsaetning", "timing"}
    assert curation["kurationsfil_version"] == rc.CURATION_VERSION
    assert curation["sidst_seedet"] == "2026-01-01T00:00:00+00:00"


def test_seed_curation_ignores_unmapped_controls():
    findings = [_finding(63, severity="high")]  # forensic — ikke tema-mappet
    curation = rc.seed_curation(findings)
    assert curation["grupper"] == {}


def test_seed_group_status_is_ny_ikke_kurateret():
    findings = [_finding(19, severity="high")]
    curation = rc.seed_curation(findings)
    assert curation["grupper"]["kodeopsaetning"]["status"] == "ny_ikke_kurateret"


def test_seed_group_has_advisor_editable_fields_prefilled():
    findings = [_finding(19, severity="high")]
    grp = rc.seed_curation(findings)["grupper"]["kodeopsaetning"]
    assert grp["spoergsmaal"]
    assert grp["hvorfor"]
    assert grp["anbefaling"]
    assert grp["advisor_notes"] == ""
    assert grp["horisont"] == "0-3"


# --- merge_curation: rådgiverens indhold vinder ------------------------------

def test_merge_preserves_advisor_edited_text():
    findings = [_finding(19, severity="high")]
    existing = rc.seed_curation(findings, now="2026-01-01T00:00:00+00:00")
    existing["grupper"]["kodeopsaetning"]["spoergsmaal"] = "Rådgiverens eget spørgsmål?"
    existing["grupper"]["kodeopsaetning"]["medtag"] = False
    existing["grupper"]["kodeopsaetning"]["status"] = "kurateret"
    existing["grupper"]["kodeopsaetning"]["advisor_notes"] = "Aftalt med kunden på mødet."

    findings2 = findings + [_finding(19, severity="high")]  # genkørsel, ét fund mere
    merged = rc.merge_curation(existing, findings2, now="2026-02-01T00:00:00+00:00")

    grp = merged["grupper"]["kodeopsaetning"]
    assert grp["spoergsmaal"] == "Rådgiverens eget spørgsmål?"
    assert grp["medtag"] is False
    assert grp["status"] == "kurateret"
    assert grp["advisor_notes"] == "Aftalt med kunden på mødet."
    # men auto-blokken ER opdateret:
    assert grp["auto"]["fund_antal"] == 2
    assert grp["auto"]["seneste_koersel"] == "2026-02-01T00:00:00+00:00"
    assert merged["sidst_seedet"] == "2026-01-01T00:00:00+00:00"  # uændret
    assert merged["sidst_opdateret"] == "2026-02-01T00:00:00+00:00"


def test_merge_adds_new_theme_as_ny_ikke_kurateret_without_touching_existing():
    existing = rc.seed_curation([_finding(19, severity="high")], now="2026-01-01T00:00:00+00:00")
    existing["grupper"]["kodeopsaetning"]["spoergsmaal"] = "Uændret tekst"

    findings2 = [_finding(19, severity="high"), _finding(82, severity="high")]
    merged = rc.merge_curation(existing, findings2, now="2026-02-01T00:00:00+00:00")

    assert merged["grupper"]["kodeopsaetning"]["spoergsmaal"] == "Uændret tekst"
    assert "timing" in merged["grupper"]
    assert merged["grupper"]["timing"]["status"] == "ny_ikke_kurateret"


def test_merge_marks_stale_group_without_deleting_advisor_content():
    existing = rc.seed_curation([_finding(19, severity="high")], now="2026-01-01T00:00:00+00:00")
    existing["grupper"]["kodeopsaetning"]["spoergsmaal"] = "Bevar denne tekst"

    merged = rc.merge_curation(existing, [], now="2026-02-01T00:00:00+00:00")  # ingen fund denne gang

    grp = merged["grupper"]["kodeopsaetning"]
    assert grp["spoergsmaal"] == "Bevar denne tekst"  # IKKE slettet
    assert grp["status"] == "ingen_fund_i_seneste_koersel"
    assert grp["auto"]["fund_antal"] == 0
    assert grp["auto"]["evidens"] == []


# --- Fil-I/O + resolve_curation ----------------------------------------------

def test_load_curation_missing_file_returns_none(tmp_path):
    assert rc.load_curation(str(tmp_path / "does_not_exist.json")) is None


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "kuration.json"
    data = rc.seed_curation([_finding(19, severity="high")])
    rc.save_curation(str(path), data)
    loaded = rc.load_curation(str(path))
    assert loaded == data


def test_resolve_curation_without_path_does_not_persist():
    curation, written = rc.resolve_curation(None, [_finding(19, severity="high")])
    assert written is False
    assert "kodeopsaetning" in curation["grupper"]


def test_resolve_curation_seeds_when_file_missing(tmp_path):
    path = tmp_path / "kuration.json"
    curation, written = rc.resolve_curation(str(path), [_finding(19, severity="high")])
    assert written is True
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == curation


def test_resolve_curation_merges_when_file_exists(tmp_path):
    path = tmp_path / "kuration.json"
    rc.resolve_curation(str(path), [_finding(19, severity="high")])
    data = json.loads(path.read_text(encoding="utf-8"))
    data["grupper"]["kodeopsaetning"]["advisor_notes"] = "min note"
    path.write_text(json.dumps(data), encoding="utf-8")

    curation, written = rc.resolve_curation(str(path), [_finding(19, severity="high")])
    assert written is True
    assert curation["grupper"]["kodeopsaetning"]["advisor_notes"] == "min note"


# --- promoted_groups ----------------------------------------------------------

def test_promoted_groups_excludes_unpromoted_and_stale():
    findings = [_finding(19, severity="high"), _finding(46, severity="medium", amount=1.0)]
    curation = rc.seed_curation(findings)
    promoted = rc.promoted_groups(curation)
    themes = {g["tema"] for g in promoted}
    assert "kodeopsaetning" in themes       # høj -> forfremmet
    assert "proces" not in themes           # medium, under tærskel -> ikke forfremmet


def test_promoted_groups_excludes_groups_with_zero_findings_even_if_medtag_true():
    curation = rc.seed_curation([_finding(19, severity="high")])
    curation["grupper"]["kodeopsaetning"]["auto"]["fund_antal"] = 0
    assert rc.promoted_groups(curation) == []


def test_promoted_groups_follow_theme_order():
    findings = [_finding(5, severity="high"), _finding(19, severity="high")]
    curation = rc.seed_curation(findings)
    promoted = rc.promoted_groups(curation)
    themes = [g["tema"] for g in promoted]
    assert themes.index("kodeopsaetning") < themes.index("timing")
