"""
Momsrelevans-scope: en momskontrol må kun undertrykke et fund, når linjen positivt
er en balancekonto (SAF-T AccountType). Ukendt kontotype (fx Excel) => uændret adfærd.
"""

from analytics.categories import cat10_vat_reconciliation as cat10
from analytics import vat_rules as vr
from validation.builders import mk_data, mk_txn, mk_line

run_80 = cat10.test_80_revenue_without_output_vat


def test_is_non_vat_account():
    assert vr.is_non_vat_account({"account_type": "Liability"})
    assert vr.is_non_vat_account({"account_type": "asset"})
    assert vr.is_non_vat_account({"account_type": "Equity"})
    assert not vr.is_non_vat_account({"account_type": "Sale"})
    assert not vr.is_non_vat_account({"account_type": "Expense"})
    assert not vr.is_non_vat_account({"account_type": ""})   # ukendt -> ikke undertrykt
    assert not vr.is_non_vat_account({})


def test_80_suppressed_on_balance_account():
    line = mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0,
                   account_type="Liability")
    assert run_80(mk_data(mk_txn(line))) == []


def test_80_fires_on_revenue_account():
    line = mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0,
                   account_type="Sale")
    findings = run_80(mk_data(mk_txn(line)))
    assert len(findings) == 1 and findings[0]["test_id"] == 80


def test_80_unchanged_when_account_type_unknown():
    # Flad import uden kontoplan: adfærd som før (fyrer).
    line = mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0)
    findings = run_80(mk_data(mk_txn(line)))
    assert len(findings) == 1 and findings[0]["test_id"] == 80


# --- Robust scope via StandardAccountID (increment B) ---------------------
# Rigtige klientfiler mislabeler AccountType="Other"; StandardAccountID er det
# pålidelige signal (≥ 5000 = balancekonto i standardkontoplanen).

def test_is_non_vat_via_standard_account_id_even_when_type_other():
    # Balancekonto (5800) men AccountType fejlmærket "Other" -> stadig undertrykt.
    assert vr.is_non_vat_account({"account_type": "Other", "standard_account_id": "5800"})


def test_resultat_standard_account_is_vat_relevant():
    # Resultatkonto (1001 = Nettoomsætning) -> IKKE undertrykt, selv med "Other".
    assert not vr.is_non_vat_account({"account_type": "Other", "standard_account_id": "1001"})


def test_80_suppressed_on_balance_standard_account_with_type_other():
    line = mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0,
                   account_type="Other", standard_account_id="6900")
    assert run_80(mk_data(mk_txn(line))) == []


def test_80_fires_on_result_standard_account_with_type_other():
    line = mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0,
                   account_type="Other", standard_account_id="1010")
    findings = run_80(mk_data(mk_txn(line)))
    assert len(findings) == 1 and findings[0]["test_id"] == 80
