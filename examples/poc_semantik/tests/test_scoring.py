"""Tests for score_poc.score() — udelukkende syntetiske facit/predictions."""
from score_poc import score


def facit_line(line_id, fund_id):
    return {"line_id": line_id, "fund_id": fund_id, "account": "1", "supplier": "S", "text": "t"}


def test_correct_fund_found_counts_as_recall():
    facit_lines = [facit_line("L1", "F02")]
    predictions = {"L1": {"fund_id": "F02", "begrundelse_kort": "..."}}
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids={"F02"})
    assert r["recall_pr_fund_id"]["F02"] == {"facit_antal": 1, "fundet": 1, "recall": 1.0}
    assert r["andel_korrekte_total"] == 1.0


def test_missed_fund_counts_as_miss_not_recall():
    facit_lines = [facit_line("L1", "F02")]
    predictions = {"L1": {"fund_id": "OK"}}
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids={"F02"})
    assert r["recall_pr_fund_id"]["F02"]["fundet"] == 0
    assert len(r["misset_fund_som_ok"]) == 1
    assert r["misset_fund_som_ok"][0]["line_id"] == "L1"


def test_wrong_fund_id_is_tracked_separately_from_miss():
    facit_lines = [facit_line("L1", "F02")]
    predictions = {"L1": {"fund_id": "F05"}}
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids={"F02", "F05"})
    assert len(r["forkert_fund_id"]) == 1
    assert r["forkert_fund_id"][0] == {"line_id": "L1", "facit_fund": "F02", "predicted": "F05"}
    assert len(r["misset_fund_som_ok"]) == 0


def test_ok_predicted_as_fund_is_false_positive_not_auto_wrong_judgement():
    facit_lines = [facit_line("L1", "OK")]
    predictions = {"L1": {"fund_id": "F02", "begrundelse_kort": "ser mistaenkelig ud"}}
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids={"F02"})
    assert r["ok_noejagtighed"] == {"total": 1, "korrekt": 0, "andel": 0.0}
    assert len(r["falske_positiver_ok_til_fund"]) == 1
    assert r["falske_positiver_ok_til_fund"][0]["predicted"] == "F02"


def test_ok_correctly_predicted():
    facit_lines = [facit_line("L1", "OK")]
    predictions = {"L1": {"fund_id": "OK"}}
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids=set())
    assert r["ok_noejagtighed"] == {"total": 1, "korrekt": 1, "andel": 1.0}


def test_hallucinated_fund_id_flagged():
    facit_lines = [facit_line("L1", "OK")]
    predictions = {"L1": {"fund_id": "F99"}}  # findes ikke i kataloget
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids={"F02"})
    assert len(r["hallucinerede_fund_id"]) == 1
    assert r["hallucinerede_fund_id"][0] == {"line_id": "L1", "predicted": "F99"}


def test_cross_line_fund_excluded_from_recall_and_reported_separately():
    facit_lines = [facit_line("L1", "F01"), facit_line("L2", "F02")]
    predictions = {"L1": {"fund_id": "OK"}, "L2": {"fund_id": "F02"}}
    r = score(facit_lines, predictions, excluded_funds={"F01"}, valid_fund_ids={"F01", "F02"})
    assert "F01" not in r["recall_pr_fund_id"]
    assert r["uden_for_pr_linje_scope"]["antal_linjer"] == 1
    assert r["uden_for_pr_linje_scope"]["fund_ids"] == ["F01"]
    # F01 tæller hverken som korrekt eller forkert i totalen
    assert r["andel_korrekte_total"] == 1.0


def test_only_lines_with_predictions_are_scored():
    facit_lines = [facit_line("L1", "OK"), facit_line("L2", "OK")]
    predictions = {"L1": {"fund_id": "OK"}}  # L2 er ikke behandlet endnu (fx roegtest)
    r = score(facit_lines, predictions, excluded_funds=set(), valid_fund_ids=set())
    assert r["antal_scorede_linjer"] == 1
    assert r["ok_noejagtighed"]["total"] == 1
