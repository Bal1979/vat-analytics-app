"""Tests for dedup.py — udelukkende syntetiske linjer, ingen kundedata."""
from dedup import group_key, group_lines, normalize_text, reduction_stats


def make_line(line_id, account="404200", supplier="Acme A/S", text="Kaffe til kontoret",
              vat_business_group="DOMESTIC", vat_product_group="STANDARD_VAT",
              currency=None, vat_amount=100.0, amount_excl_vat=400.0):
    return {
        "line_id": line_id, "account": account, "supplier": supplier, "text": text,
        "vat_business_group": vat_business_group, "vat_product_group": vat_product_group,
        "currency": currency, "vat_amount": vat_amount, "amount_excl_vat": amount_excl_vat,
    }


def test_normalize_text_strips_digits_and_punctuation():
    assert normalize_text("Faktura 12345, konto: ABC!") == "faktura konto abc"


def test_normalize_text_handles_none():
    assert normalize_text(None) == ""


def test_normalize_text_truncates():
    long_text = "a" * 100
    assert len(normalize_text(long_text, max_len=50)) == 50


def test_identical_lines_group_together():
    lines = [make_line("L1"), make_line("L2")]
    groups = group_lines(lines)
    assert len(groups) == 1
    assert set(groups[0]["member_line_ids"]) == {"L1", "L2"}
    assert groups[0]["member_count"] == 2


def test_different_invoice_numbers_still_group(monkeypatch=None):
    # Fakturanumre i teksten maa ikke forhindre gruppering.
    lines = [
        make_line("L1", text="Faktura 2449599 - husleje"),
        make_line("L2", text="Faktura 2449612 - husleje"),
    ]
    groups = group_lines(lines)
    assert len(groups) == 1


def test_different_vat_code_splits_group():
    # Samme konto/leverandoer/tekst men forskellig momskode maa IKKE smeltes
    # sammen (fx F04-moenstret: identisk faktura kodet forskelligt).
    lines = [
        make_line("L1", vat_product_group="STANDARD_VAT", vat_amount=100.0),
        make_line("L2", vat_product_group="NO_VAT", vat_amount=0.0),
    ]
    groups = group_lines(lines)
    assert len(groups) == 2


def test_different_currency_splits_group():
    # Afgoerende for F29 (udenlandsk valuta med dansk moms).
    lines = [
        make_line("L1", currency=None),
        make_line("L2", currency="EUR"),
    ]
    groups = group_lines(lines)
    assert len(groups) == 2


def test_different_supplier_splits_group():
    lines = [make_line("L1", supplier="Acme A/S"), make_line("L2", supplier="Other ApS")]
    groups = group_lines(lines)
    assert len(groups) == 2


def test_representative_is_first_member_deterministic():
    lines = [make_line("L1", amount_excl_vat=111.0), make_line("L2", amount_excl_vat=222.0)]
    groups = group_lines(lines)
    assert groups[0]["representative"]["line_id"] == "L1"
    assert groups[0]["amount_min"] == 111.0
    assert groups[0]["amount_max"] == 222.0


def test_group_id_stable_order():
    lines = [make_line("L1", account="100"), make_line("L2", account="200"), make_line("L3", account="100")]
    groups = group_lines(lines)
    ids = [g["group_id"] for g in groups]
    assert ids == ["G0001", "G0002"]  # konto 100-gruppen set foerst


def test_reduction_stats():
    lines = [make_line(f"L{i}") for i in range(10)]
    groups = group_lines(lines)
    stats = reduction_stats(lines, groups)
    assert stats["antal_linjer"] == 10
    assert stats["antal_unikke_grupper"] == 1
    assert stats["reduktionsfaktor"] == 10.0


def test_group_key_is_hashable_tuple():
    key = group_key(make_line("L1"))
    assert isinstance(key, tuple)
    hash(key)  # skal ikke fejle
