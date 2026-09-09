"""
Score-kalibrering: mange lav-/medium-fund må ikke alene tvinge en kategori i rød.
RØD (score < 50) skal drives af kritiske/høje fund. Loftet pr. severity-tier
(materiality.SEVERITY_PENALTY_CAPS) sikrer det.
"""

from analytics.engine import build_report


def _data():
    return {"header": {"currency": "DKK"}, "summary": {}, "transactions": []}


def _finding(test_id, severity):
    return {
        "test_id": test_id, "test_name": "t", "impact_type": "compliance",
        "direction": "neutral", "severity": severity, "description": "d",
        "estimated_amount": 0, "transactions": [],
    }


def _cat_score(rep, cat_id):
    return next(c for c in rep["categories"] if c["id"] == cat_id)["score"]


def test_many_low_findings_do_not_force_red():
    # 26 lav + 2 medium i kat 5 (timing). Uden loft: 26*3 + 2*8 = 94 -> score 6 (rød).
    # Med loft: min(78,15) + min(16,30) = 15 + 16 = 31 -> score 69 (gul, ikke rød).
    findings = [_finding(40, "low") for _ in range(26)] + [_finding(40, "medium") for _ in range(2)]
    score = _cat_score(build_report(_data(), findings), 5)
    assert score == 69
    assert score >= 50  # ikke rød


def test_low_only_stays_green():
    # Mange lav-fund alene -> loft 15 -> score 85 (grøn).
    findings = [_finding(40, "low") for _ in range(50)]
    assert _cat_score(build_report(_data(), findings), 5) == 85


def test_medium_only_at_most_yellow():
    # Mange medium alene -> loft 30 -> score 70 (gul), aldrig rød.
    findings = [_finding(40, "medium") for _ in range(20)]
    assert _cat_score(build_report(_data(), findings), 5) == 70


def test_critical_can_still_make_red():
    # Kritiske fund skal stadig kunne give rød.
    findings = [_finding(40, "critical") for _ in range(3)]  # 3*25 = 75 -> score 25
    assert _cat_score(build_report(_data(), findings), 5) < 50


def test_clean_category_is_100():
    assert _cat_score(build_report(_data(), []), 5) == 100
