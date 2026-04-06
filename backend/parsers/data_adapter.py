"""
Data Adapter: Converts flat Excel parser output to SAF-T-style structures
expected by the analytics engine.

The Excel parser returns flat transaction dicts with fields like:
    transaction_id, date, account_id, debit_amount, credit_amount, vat_amount, ...

The analytics engine (cat01, cat02, etc.) expects SAF-T-style transactions with:
    - txn["lines"]: list of line-level dicts with tax_code, tax_percentage,
      tax_base, tax_amount, debit_amount, credit_amount, record_id, etc.
    - txn["total_debit"], txn["total_credit"]
    - txn["period"], txn["period_year"]
    - data["summary"] at the top level
    - data["tax_table"] entries with "tax_percentage" (not "rate")
"""

def adapt_excel_to_saft(parsed_data: dict) -> dict:
    """
    Transform flat Excel parser output into the SAF-T-style structure
    the analytics engine expects.

    Args:
        parsed_data: Output from parse_excel() with keys:
            header, accounts, tax_table, transactions, suppliers, customers, parse_info

    Returns:
        A new dict with the same top-level keys but with transactions restructured
        to have lines[], total_debit, total_credit, and with a summary added.
    """
    transactions = parsed_data.get("transactions", [])
    header = parsed_data.get("header", {})
    accounts = parsed_data.get("accounts", [])
    tax_table = parsed_data.get("tax_table", [])
    suppliers = parsed_data.get("suppliers", [])
    customers = parsed_data.get("customers", [])
    parse_info = parsed_data.get("parse_info", {})

    # --- Fix tax_table: ensure each entry has "tax_percentage" ---
    adapted_tax_table = []
    for entry in tax_table:
        adapted_entry = dict(entry)
        if "tax_percentage" not in adapted_entry:
            adapted_entry["tax_percentage"] = adapted_entry.get("rate", 0.0)
        adapted_tax_table.append(adapted_entry)

    # Build a lookup from tax_code -> tax_percentage
    tax_rate_lookup = {
        t["tax_code"]: t["tax_percentage"] for t in adapted_tax_table
    }

    # --- Adapt transactions: wrap each flat txn into SAF-T structure ---
    adapted_transactions = []
    for idx, txn in enumerate(transactions):
        debit = txn.get("debit_amount", 0.0) or 0.0
        credit = txn.get("credit_amount", 0.0) or 0.0
        vat_amount = txn.get("vat_amount") or 0.0
        vat_code = txn.get("vat_code", "") or ""
        vat_rate = txn.get("vat_rate") or 0.0

        # Determine tax_percentage: prefer the rate from the tax table,
        # fall back to the row-level vat_rate
        tax_percentage = tax_rate_lookup.get(vat_code, vat_rate)

        # Compute tax_base: if vat_amount and tax_percentage are known, derive it;
        # otherwise use (debit + credit) as the base
        if vat_amount and tax_percentage:
            tax_base = round(vat_amount / (tax_percentage / 100), 2)
        else:
            tax_base = round(debit + credit, 2)

        # Build the line object that cat01/cat02 tests expect
        line = {
            "record_id": f"L{idx + 1}",
            "account_id": txn.get("account_id", ""),
            "description": txn.get("description", ""),
            "debit_amount": debit,
            "credit_amount": credit,
            "tax_code": vat_code,
            "tax_percentage": tax_percentage,
            "tax_base": tax_base,
            "tax_amount": vat_amount,
            "currency": txn.get("currency", "DKK") or "DKK",
            "supplier_id": txn.get("supplier_id", "") or "",
            "supplier_name": txn.get("supplier_name", "") or "",
            "customer_id": txn.get("customer_id", "") or "",
            "customer_name": txn.get("customer_name", "") or "",
            "source_document_id": txn.get("invoice_number", "") or "",
        }

        # Derive period and period_year from the date if not already present
        date_str = txn.get("date") or ""
        period = txn.get("period", "") or ""
        year = txn.get("year", "") or ""

        if date_str and not period:
            try:
                period = str(int(date_str[5:7]))  # "2024-03-15" -> "3" -> "03"
                period = period.zfill(2)
            except (ValueError, IndexError):
                period = ""

        if date_str and not year:
            try:
                year = date_str[:4]
            except (ValueError, IndexError):
                year = ""

        adapted_txn = {
            "transaction_id": txn.get("transaction_id", f"ROW-{idx + 2}"),
            "date": date_str,
            "description": txn.get("description", ""),
            "journal_id": txn.get("journal_id", "IMPORT") or "IMPORT",
            "period": period,
            "period_year": year,
            "total_debit": debit,
            "total_credit": credit,
            "lines": [line],
        }
        adapted_transactions.append(adapted_txn)

    # --- Enrich header with period sub-dict for test_09 (tax point) ---
    period_start_str = header.get("period_start", "")
    period_end_str = header.get("period_end", "")
    if period_start_str and period_end_str and "period" not in header:
        try:
            header["period"] = {
                "start": str(int(period_start_str[5:7])),
                "start_year": period_start_str[:4],
                "end": str(int(period_end_str[5:7])),
                "end_year": period_end_str[:4],
            }
        except (ValueError, IndexError):
            pass

    # --- Build summary from the data ---
    total_debit = sum(t["total_debit"] for t in adapted_transactions)
    total_credit = sum(t["total_credit"] for t in adapted_transactions)
    total_vat = sum(
        line["tax_amount"]
        for t in adapted_transactions
        for line in t["lines"]
    )
    summary = {
        "total_transactions": len(adapted_transactions),
        "total_debit": round(total_debit, 2),
        "total_credit": round(total_credit, 2),
        "total_vat": round(total_vat, 2),
        "period_start": header.get("period_start", ""),
        "period_end": header.get("period_end", ""),
        "currency": header.get("currency", "DKK"),
    }

    return {
        "header": header,
        "accounts": accounts,
        "tax_table": adapted_tax_table,
        "transactions": adapted_transactions,
        "suppliers": suppliers,
        "customers": customers,
        "parse_info": parse_info,
        "summary": summary,
    }
