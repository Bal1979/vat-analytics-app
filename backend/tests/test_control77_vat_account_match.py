"""
Kontrol 77 — momskontoafstemning: kontonavns-heuristikken (byggetrin 9, Del B,
Bal-godkendt 2026-09-17).

Baggrund: cat10.test_77_vat_account_reconciliation identificerede tidligere
KUN en momskonto ved at søge efter den danske streng 'moms' i kontonavnet —
den kunne derfor ALDRIG matche en engelsksproget kontoplan (fx en BC/NAV-
standardopsætning, hvor momskonti typisk hedder "VAT payable"/"Output VAT").
Udvidet til også at matche 'vat' (case-insensitivt), med UÆNDRET adfærd på
danske kontoplaner.
"""

from analytics.categories import cat10_vat_reconciliation as cat10
from validation.builders import mk_data, mk_line, mk_txn


def test_matches_danish_moms_account_name_unchanged():
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[{"account_id": "7010", "description": "Momsafregning",
                   "account_type": "", "opening_balance": 0.0, "closing_balance": 9999.0}],
    )
    findings = cat10.test_77_vat_account_reconciliation(data)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 77


def test_matches_english_vat_account_name():
    """Den engelsksprogede kontoplan (BC/NAV m.fl.) kunne tidligere ALDRIG
    trigge kontrollen, fordi 'vat' ikke matchede 'moms'-mønstret."""
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[{"account_id": "961100", "description": "VAT payable",
                   "account_type": "", "opening_balance": 0.0, "closing_balance": 9999.0}],
    )
    findings = cat10.test_77_vat_account_reconciliation(data)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 77


def test_matches_vat_case_insensitively():
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[{"account_id": "961100", "description": "Output Vat",
                   "account_type": "", "opening_balance": 0.0, "closing_balance": 9999.0}],
    )
    findings = cat10.test_77_vat_account_reconciliation(data)
    assert len(findings) == 1


def test_no_matching_account_name_gives_no_findings():
    data = mk_data(
        mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
        accounts=[{"account_id": "1000", "description": "Kassebeholdning",
                   "account_type": "", "opening_balance": 0.0, "closing_balance": 9999.0}],
    )
    assert cat10.test_77_vat_account_reconciliation(data) == []


def test_no_accounts_at_all_gives_no_findings():
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)), accounts=[])
    assert cat10.test_77_vat_account_reconciliation(data) == []
