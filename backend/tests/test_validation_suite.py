"""Gate: hvert valideringsscenarie skal fange sin plantede defekt OG være tavst
på den rene baseline. Kører i CI sammen med resten af suiten."""

import pytest

from validation.scenarios import SCENARIOS
from validation.run_validation import _fired_ids


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[f"ctrl_{s['test_id']}" for s in SCENARIOS])
def test_planted_defect_is_caught_and_clean_is_silent(scenario):
    tid = scenario["test_id"]
    assert tid not in _fired_ids(scenario["clean"]), \
        f"Kontrol {tid} fyrede på ren baseline (falsk alarm)"
    assert tid in _fired_ids(scenario["defect"]), \
        f"Kontrol {tid} fangede ikke den plantede defekt"
