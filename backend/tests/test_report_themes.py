"""
tools/report_themes.py — den faste tema<->kontrol-mapping og de
deterministiske tekst-udkast til kunderapportens sektion 4 (byggetrin ~10).
"""

from tools import report_themes as rt


def test_theme_of_maps_known_controls():
    assert rt.theme_of(19) == "kodeopsaetning"
    assert rt.theme_of(80) == "momsbehandling_pr_konto"
    assert rt.theme_of(1) == "dataanomalier"
    assert rt.theme_of(7) == "dataanomalier"
    assert rt.theme_of(24) == "dataanomalier"
    assert rt.theme_of(60) == "dataanomalier"
    assert rt.theme_of(46) == "proces"
    assert rt.theme_of(14) == "proces"
    assert rt.theme_of(82) == "timing"
    assert rt.theme_of(5) == "timing"


def test_theme_of_unmapped_control_returns_none():
    assert rt.theme_of(63) is None  # forensic/statistik — ikke tema-mappet
    assert rt.theme_of(999) is None


def test_timing_is_always_promoted():
    assert "timing" in rt.ALWAYS_PROMOTED_THEMES


def test_theme_order_covers_all_six_themes():
    assert set(rt.THEME_ORDER) == set(rt.THEMES)
    assert len(rt.THEME_ORDER) == 6


def _finding(test_id, account_id=None, tax_code=None, amount=0.0, description=""):
    ref = {}
    if account_id:
        ref["account_id"] = account_id
    if tax_code:
        ref["tax_code"] = tax_code
    return {"test_id": test_id, "estimated_amount": amount, "description": description,
            "transactions": [ref] if ref else []}


def test_draft_content_kodeopsaetning_mentions_code_when_present():
    """Kontrol 19's transaktionsreferencer bærer ikke momskoden strukturelt —
    kun den danske beskrivelsestekst gør (se _txn_ref i
    cat03_vat_rate_validation.py). Udkastet skal udtrække koden DERFRA."""
    findings = [_finding(
        19, description="Momssats 12% på linje L1 i transaktion T1 afviger fra "
                         "opsætningens sats for momskode 'DOMESTIC|WEIRD_CODE' (25%).",
    )]
    draft = rt.draft_content("kodeopsaetning", findings)
    assert "DOMESTIC|WEIRD_CODE" in draft["spoergsmaal"]
    assert draft["hvorfor"]
    assert draft["anbefaling"]


def test_draft_content_kodeopsaetning_generic_without_code():
    draft = rt.draft_content("kodeopsaetning", [_finding(19)])
    assert "?" in draft["spoergsmaal"]


def test_draft_content_momsbehandling_pr_konto_mentions_account():
    findings = [_finding(80, account_id="4000", amount=1000.0)]
    draft = rt.draft_content("momsbehandling_pr_konto", findings)
    assert "4000" in draft["spoergsmaal"]


def test_draft_content_timing_is_static_and_never_crashes_on_empty():
    draft = rt.draft_content("timing", [])
    assert draft["spoergsmaal"]
    assert draft["hvorfor"]
    assert draft["anbefaling"]


def test_draft_content_unknown_theme_returns_generic_fallback():
    draft = rt.draft_content("ukendt_tema", [])
    assert draft["spoergsmaal"]


def test_no_control_ids_leak_into_drafted_text():
    """Formkravet: kunden ser aldrig kontrolnumre i sektion 4 — heller ikke i
    de auto-genererede tekst-udkast."""
    findings = [_finding(80, account_id="4000", amount=1000.0)]
    for theme_key in rt.THEME_ORDER:
        draft = rt.draft_content(theme_key, findings)
        for text in draft.values():
            assert "kontrol" not in text.lower()
