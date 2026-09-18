"""
Kontrol 60 (2026-09-18, Bal-godkendt gap-analyse-fix D): en negativ momslinje
der beviseligt nettes af en matchende positiv modpost -- SAMME konto +
momskode, sum ~ 0 -- er et internt allokerings-/tilbageførselsmønster, ikke
en fejlpostering. To netto-veje:

  1. Samme bilag (transaktion): summen af tax_amount for linjer med samme
     konto+kode i SAMME transaktion er ~ 0.
  2. Reversal-par: en ANDEN transaktion har en linje med samme konto+kode og
     nøjagtig modsat momsbeløb, bogført inden for
     materiality.CONTROL_60_REVERSAL_WINDOW_DAYS dage.

STRUKTUREL regel -- ingen kunde-specifikke bilagspræfikser (fx "PA") indgår.
Diagnose (empirisk på v4-datasættet): 1.181 af 1.303 fund (~91%) var
beviseligt nettet (40 i samme bilag, 1.141 som reversal-par); 122 uden
modpost forbliver fund -- det er dem, kontrol 60 er til for.
"""

from analytics import materiality
from analytics.categories import cat07_amount_threshold as cat07
from validation.builders import mk_data, mk_txn, mk_line

run_60 = cat07.test_60_negative_vat


# --- Genuine fund (regression -- må IKKE forsvinde) --------------------------

def test_negative_vat_without_counterpart_still_fires():
    line = mk_line(debit_amount=1000.0, account_id="4000", tax_code="I25", tax_amount=-100.0)
    findings = run_60(mk_data(mk_txn(line)))
    assert len(findings) == 1
    assert findings[0]["test_name"] == "Negativt momsbeløb"


def test_credit_note_marked_transaction_is_still_exempt():
    line = mk_line(debit_amount=1000.0, account_id="4000", tax_code="I25", tax_amount=-100.0)
    data = mk_data(mk_txn(line, description="Kreditnota 1234"))
    assert run_60(data) == []


def test_positive_counterpart_on_different_account_does_not_suppress():
    """Modposten skal være på SAMME konto+kode -- en positiv linje på en
    anden konto er ikke bevis for netting."""
    neg = mk_line(debit_amount=1000.0, account_id="4000", tax_code="I25", tax_amount=-100.0)
    pos = mk_line(debit_amount=1000.0, account_id="9999", tax_code="I25", tax_amount=100.0)
    data = mk_data(mk_txn([neg, pos]))
    findings = run_60(data)
    assert len(findings) == 1


def test_positive_counterpart_outside_window_does_not_suppress():
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-100.0)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=100.0)
    data = mk_data([
        mk_txn(neg, transaction_id="T-NEG", date="2024-06-01"),
        mk_txn(pos, transaction_id="T-POS", date="2024-01-01"),  # > 31 dage før
    ])
    findings = run_60(data)
    assert len(findings) == 1


# --- Samme-bilag netting -----------------------------------------------------

def test_same_document_offsetting_lines_are_suppressed():
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-100.0)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=100.0)
    data = mk_data(mk_txn([neg, pos]))
    assert run_60(data) == []


def test_same_document_partial_offset_still_fires():
    """Summen skal være ~ 0 -- et delvist modsvar er ikke bevis for netting."""
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-100.0)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=40.0)
    data = mk_data(mk_txn([neg, pos]))
    findings = run_60(data)
    assert len(findings) == 1


# --- Reversal-par -------------------------------------------------------------

def test_reversal_pair_same_day_across_documents_is_suppressed():
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-716.31)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=716.31)
    data = mk_data([
        mk_txn(neg, transaction_id="T-ALLOC", date="2025-09-10"),
        mk_txn(pos, transaction_id="T-ORIG", date="2025-09-10"),
    ])
    assert run_60(data) == []


def test_reversal_pair_within_window_is_suppressed():
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-500.0)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=500.0)
    data = mk_data([
        mk_txn(neg, transaction_id="T-NEG", date="2025-09-30"),
        mk_txn(pos, transaction_id="T-POS", date="2025-09-05"),  # 25 dage før, inden for 31
    ])
    assert run_60(data) == []


def test_reversal_pair_wrong_tax_code_does_not_suppress():
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-500.0)
    pos = mk_line(account_id="4000", tax_code="ANDEN", tax_amount=500.0)
    data = mk_data([
        mk_txn(neg, transaction_id="T-NEG", date="2025-09-10"),
        mk_txn(pos, transaction_id="T-POS", date="2025-09-10"),
    ])
    findings = run_60(data)
    assert len(findings) == 1


def test_reversal_window_is_configurable(monkeypatch):
    """MATERIALITY_CONTROL_60_REVERSAL_WINDOW_DAYS er env-overstyrbar (samme
    mønster som materiality.py's øvrige knapper)."""
    monkeypatch.setattr(materiality, "CONTROL_60_REVERSAL_WINDOW_DAYS", 3)
    neg = mk_line(account_id="4000", tax_code="I25", tax_amount=-500.0)
    pos = mk_line(account_id="4000", tax_code="I25", tax_amount=500.0)
    data = mk_data([
        mk_txn(neg, transaction_id="T-NEG", date="2025-09-10"),
        mk_txn(pos, transaction_id="T-POS", date="2025-09-05"),  # 5 dage -- uden for det snævrede vindue
    ])
    findings = run_60(data)
    assert len(findings) == 1
