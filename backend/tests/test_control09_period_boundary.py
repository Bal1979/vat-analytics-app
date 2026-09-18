"""
Kontrol 9 (2026-09-18, Bal-godkendt gap-analyse-fix C): off-by-one i
periodeafgrænsningen for en periode, der slutter i december.

Rodårsag: ``period_end`` blev sat til ``datetime(end_year, 12, 31)`` -- en
INKLUSIV øvre grænse -- mens sammenligningen (``txn_date >= period_end``)
forudsætter en EKSKLUSIV øvre grænse (samme konvention som den generelle,
ikke-december gren). Konsekvens: enhver transaktion bogført PRÆCIS på
periodens sidste dag (fx 31/12 for en kalenderårsperiode) blev fejlagtigt
rapporteret som liggende UDEN FOR perioden -- en selvmodsigende fundtekst
("... har dato 2025-12-31 der ligger uden for ... (1/2025 - 12/2025)"),
fordi 2025-12-31 reelt ER periodens sidste dag.
"""

from analytics.categories import cat01_transaction_integrity as cat01
from validation.builders import mk_data, mk_txn, mk_line

run_09 = cat01.test_09_tax_point


def test_last_day_of_december_within_full_year_period_is_clean():
    line = mk_line(debit_amount=1000.0)
    data = mk_data(mk_txn(line, date="2024-12-31"))
    assert run_09(data) == []


def test_first_day_of_january_within_full_year_period_is_clean():
    line = mk_line(debit_amount=1000.0)
    data = mk_data(mk_txn(line, date="2024-01-01"))
    assert run_09(data) == []


def test_date_in_next_year_still_fires():
    """Regressionstjek: en ægte periode-overskridelse skal stadig fanges."""
    line = mk_line(debit_amount=1000.0)
    data = mk_data(mk_txn(line, date="2025-01-01"))
    findings = run_09(data)
    assert len(findings) == 1
    assert findings[0]["test_id"] == 9


def test_date_before_period_still_fires():
    line = mk_line(debit_amount=1000.0)
    data = mk_data(mk_txn(line, date="2023-12-31"))
    findings = run_09(data)
    assert len(findings) == 1


def test_non_december_end_month_keeps_exclusive_upper_bound():
    """Regressionstjek: en periode der IKKE slutter i december (fx et
    kvartal, apr-jun) brugte allerede en korrekt eksklusiv øvre grænse --
    bugfixen må ikke ændre den gren."""
    line = mk_line(debit_amount=1000.0)
    period = {"start": "4", "start_year": "2024", "end": "6", "end_year": "2024"}
    data = mk_data(mk_txn(line, date="2024-06-30"), header={
        "company_name": "T", "period": period,
    })
    assert run_09(data) == []


def test_day_after_non_december_period_end_still_fires():
    line = mk_line(debit_amount=1000.0)
    period = {"start": "4", "start_year": "2024", "end": "6", "end_year": "2024"}
    data = mk_data(mk_txn(line, date="2024-07-01"), header={
        "company_name": "T", "period": period,
    })
    findings = run_09(data)
    assert len(findings) == 1
