"""
Standardkontoplan-nature: balance vs. resultat ud fra StandardAccountID.

Grænserne følger ERST-standardkontoplanens sektions-headere: RESULTATOPGØRELSE
starter ved 1000, BALANCE ved 5000. Så ≥ 5000 = balance, 1000–4999 = resultat,
alt andet = ukendt ('').
"""

from analytics import standard_accounts as sa


def test_result_range():
    assert sa.account_nature("1001") == "resultat"   # Nettoomsætning
    assert sa.account_nature("4999") == "resultat"
    assert sa.account_nature(1010) == "resultat"      # accepterer også int


def test_balance_range():
    assert sa.account_nature("5000") == "balance"
    assert sa.account_nature("5800") == "balance"     # Datterselskaber (aktiv)
    assert sa.account_nature("7680") == "balance"     # Salgsmoms (passiv)


def test_unknown_returns_empty():
    assert sa.account_nature("") == ""
    assert sa.account_nature("abc") == ""
    assert sa.account_nature(None) == ""
    assert sa.account_nature("999") == ""             # under RESULTATOPGØRELSE


def test_is_balance_account():
    assert sa.is_balance_account("6900")
    assert not sa.is_balance_account("1001")
    assert not sa.is_balance_account("")
