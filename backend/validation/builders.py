"""Datakonstruktører til valideringssuiten — bygger den SAF-T-lignende struktur,
analysemotoren forventer (samme form som parsers/data_adapter.py producerer)."""


def mk_line(**kw):
    """En linje med neutrale defaults. Overskriv kun det, scenariet kræver."""
    d = {
        "record_id": "L1",
        "account_id": "4000",
        "description": "",
        "debit_amount": 0.0,
        "credit_amount": 0.0,
        "tax_code": "",
        "tax_percentage": 0.0,
        "tax_base": 0.0,
        "tax_amount": 0.0,
        "currency": "DKK",
        "supplier_id": "",
        "supplier_name": "",
        "customer_id": "",
        "customer_name": "",
        "source_document_id": "",
        "country": "",
        "ship_from_country": "",
        "ship_to_country": "",
        "vat_number": "",
    }
    d.update(kw)
    return d


def mk_txn(lines, **kw):
    if isinstance(lines, dict):
        lines = [lines]
    debit = sum(l.get("debit_amount", 0) or 0 for l in lines)
    credit = sum(l.get("credit_amount", 0) or 0 for l in lines)
    t = {
        "transaction_id": "T1",
        "date": "2024-03-15",
        "document_date": "",
        "description": "Postering",
        "journal_id": "IMPORT",
        "period": "03",
        "period_year": "2024",
        "total_debit": debit,
        "total_credit": credit,
        "lines": lines,
    }
    t.update(kw)
    return t


def mk_data(txns, **kw):
    if isinstance(txns, dict):
        txns = [txns]
    header = {
        "company_name": "Testvirksomhed ApS",
        "currency": "DKK",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "period": {"start": "1", "start_year": "2024", "end": "12", "end_year": "2024"},
    }
    d = {
        "header": header,
        "accounts": [],
        "tax_table": [],
        "transactions": txns,
        "suppliers": [],
        "customers": [],
        "summary": {"total_transactions": len(txns)},
    }
    d.update(kw)
    return d
