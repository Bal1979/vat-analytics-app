"""
SAF-T -> VAT Analytics — produktions-parser.

Mapper en dansk SAF-T Financial-fil (v1.0/2.0/2.1) til den SAMME kanoniske
struktur, ``data_adapter.adapt_excel_to_saft`` producerer, så de eksisterende
103 kontroller kører uændret — uanset om inputtet er et fladt Excel-udtræk eller
en SAF-T-fil.

Designprincipper:
  * **Best-effort:** parseren kører også på en ufuldstændig eller ikke-XSD-gyldig
    SAF-T (fx en fil der deklarerer v1.0 men har fuldt datasæt). Manglende
    sektioner giver tomme lister, ikke exceptions. Den strukturelle gyldighed er
    SAF-T Validators bord — her handler det om at få analysen kørt.
  * **Namespace-agnostisk:** matcher på element-localname, så DK-namespacet
    (og evt. versionsvarianter) ikke gør parseren skør.
  * **XML-hærdet:** DTD/ENTITY afvises (blokerer billion-laughs/XXE), da inputtet
    er en upload fra en bruger.
  * **Beriger linjen:** bærer ``account_type`` (SAF-T AccountType) og
    ``standard_account_id`` (StandardAccountID -> standardkontoplan-rolle) med, så
    momsrelevans-scopet kan udelukke balanceposter robust.

Motoren importeres IKKE her — parseren producerer kun data.
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET


# --- XML-hærdning ----------------------------------------------------------

class _SafeParseError(Exception):
    """Rejst når inputtet indeholder en DTD/ENTITY (afvist af sikkerhedshensyn)."""


def _looks_like_dtd(head: bytes) -> bool:
    upper = head.upper()
    return b"<!DOCTYPE" in upper or b"<!ENTITY" in upper


def looks_like_saft(path: str) -> bool:
    """Hurtig sniff: er filen sandsynligvis en SAF-T (uden at parse hele filen)?
    Bruges til routing sammen med filendelsen."""
    try:
        with open(path, "rb") as f:
            head = f.read(4096)
    except OSError:
        return False
    upper = head.upper()
    return b"AUDITFILE" in upper or b"STANDARDAUDITFILE" in upper


def _safe_parse(path: str):
    """Parse XML med DTD/ENTITY afvist. Returnerer root eller rejser."""
    with open(path, "rb") as f:
        head = f.read(4096)
    if _looks_like_dtd(head):
        raise _SafeParseError("XML med DOCTYPE/ENTITY afvises af sikkerhedshensyn.")
    # Stdlib expat henter ikke eksterne entiteter; DTD-afvisningen ovenfor
    # blokerer interne entitets-bomber. Vi parser derefter normalt.
    tree = ET.parse(path)
    return tree.getroot()


# --- Hjælpere (localname-baseret) -----------------------------------------

def _local(tag) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else tag


def _child(el, name):
    if el is None:
        return None
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _children(el, name):
    if el is None:
        return []
    return [c for c in el if _local(c.tag) == name]


def _txt(el, name, default="") -> str:
    c = _child(el, name)
    return c.text.strip() if (c is not None and c.text) else default


def _num(el, name) -> float:
    """Beløb: enten <name>value</name> eller <name><Amount>value</Amount></name>."""
    c = _child(el, name)
    if c is None:
        return 0.0
    a = _child(c, "Amount")
    raw = (a.text if (a is not None and a.text) else c.text) or "0"
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def _find_first(root, name):
    for el in root.iter():
        if _local(el.tag) == name:
            return el
    return None


# --- Parsning --------------------------------------------------------------

def parse_saft(path: str):
    """Parse en SAF-T-fil til (canonical_dict, info).

    Returnerer ``(None, info)`` hvis XML slet ikke kan parses (info["errors"]
    forklarer hvorfor). Ellers ``(canonical, info)`` — best-effort, også på en
    ufuldstændig/fejlmærket fil.
    """
    info = {"errors": [], "warnings": [], "sections": {}, "kilde": "SAF-T"}
    try:
        root = _safe_parse(path)
    except _SafeParseError as e:
        return None, {**info, "errors": [str(e)]}
    except ET.ParseError as e:
        return None, {**info, "errors": [f"XML kunne ikke parses: {e}"]}
    except OSError as e:
        return None, {**info, "errors": [f"Filen kunne ikke læses: {e}"]}

    # --- Header ---
    header = _find_first(root, "Header")
    version = _txt(header, "AuditFileVersion") if header is not None else ""
    default_ccy = _txt(header, "DefaultCurrencyCode", "DKK") if header is not None else "DKK"
    company_name = ""
    p_start = p_end = ""
    if header is not None:
        comp = _child(header, "Company")
        company_name = _txt(comp, "Name") if comp is not None else ""
        sel = _child(header, "SelectionCriteria")
        if sel is not None:
            p_start = _txt(sel, "SelectionStartDate")
            p_end = _txt(sel, "SelectionEndDate")

    # --- MasterFiles ---
    accounts = []
    gla = _find_first(root, "GeneralLedgerAccounts")
    for acc in _children(gla, "Account"):
        accounts.append({
            "account_id": _txt(acc, "AccountID"),
            "description": _txt(acc, "AccountDescription"),
            "account_type": _txt(acc, "AccountType"),
            "standard_account_id": _txt(acc, "StandardAccountID"),
            "opening_balance": _num(acc, "OpeningDebitBalance") - _num(acc, "OpeningCreditBalance"),
            "closing_balance": _num(acc, "ClosingDebitBalance") - _num(acc, "ClosingCreditBalance"),
        })

    tax_table = []
    tt = _find_first(root, "TaxTable")
    for entry in _children(tt, "TaxTableEntry"):
        for det in _children(entry, "TaxCodeDetails"):
            tax_table.append({
                "tax_code": _txt(det, "TaxCode"),
                "description": _txt(det, "Description"),
                "tax_percentage": _num(det, "TaxPercentage"),
                "standard_tax_code": _txt(det, "StandardTaxCode"),
                "country": _txt(det, "Country"),
            })

    def _party_country_vat(p):
        addr = _child(p, "Address")
        country = _txt(addr, "Country") if addr is not None else ""
        taxreg = _child(p, "TaxRegistration")
        vat = _txt(taxreg, "TaxRegistrationNumber") if taxreg is not None else ""
        if not country and taxreg is not None:
            country = _txt(taxreg, "Country")
        # v1.0-eksporter lægger CVR direkte som RegistrationNumber/CVR på parten.
        if not vat:
            vat = _txt(p, "RegistrationNumber") or _txt(p, "CVR")
        return country, vat

    customers, cust_lookup = [], {}
    for c in _children(_find_first(root, "Customers"), "Customer"):
        cid = _txt(c, "CustomerID")
        country, vat = _party_country_vat(c)
        rec = {"customer_id": cid, "name": _txt(c, "Name"), "vat_number": vat, "country": country}
        customers.append(rec)
        cust_lookup[cid] = rec

    suppliers, supp_lookup = [], {}
    for s in _children(_find_first(root, "Suppliers"), "Supplier"):
        sid = _txt(s, "SupplierID")
        country, vat = _party_country_vat(s)
        rec = {"supplier_id": sid, "name": _txt(s, "Name"), "vat_number": vat, "country": country}
        suppliers.append(rec)
        supp_lookup[sid] = rec

    tax_rate_lookup = {t["tax_code"]: t["tax_percentage"] for t in tax_table if t["tax_code"]}
    acct_type_lookup = {a["account_id"]: a["account_type"] for a in accounts}
    std_acct_lookup = {a["account_id"]: a["standard_account_id"] for a in accounts}

    # --- GeneralLedgerEntries ---
    transactions = []
    gle = _find_first(root, "GeneralLedgerEntries")
    for jrnl in _children(gle, "Journal"):
        jid = _txt(jrnl, "JournalID") or "GL"
        for t in _children(jrnl, "Transaction"):
            tdate = _txt(t, "TransactionDate")
            posting = _txt(t, "GLPostingDate") or tdate
            t_cust = _txt(t, "CustomerID")
            t_supp = _txt(t, "SupplierID")
            lines = []
            total_debit = total_credit = 0.0
            for ln in _children(t, "Line"):
                debit = _num(ln, "DebitAmount")
                credit = _num(ln, "CreditAmount")
                total_debit += debit
                total_credit += credit
                ti = _child(ln, "TaxInformation")
                tax_code = _txt(ti, "TaxCode") if ti is not None else ""
                tax_pct = _num(ti, "TaxPercentage") if ti is not None else 0.0
                if not tax_pct and tax_code:
                    tax_pct = tax_rate_lookup.get(tax_code, 0.0)
                tax_base = _num(ti, "TaxBase") if ti is not None else 0.0
                tax_amount = _num(ti, "TaxAmount") if ti is not None else 0.0
                ti_country = _txt(ti, "Country") if ti is not None else ""
                ded = _child(ti, "Deductibles") if ti is not None else None
                non_deductible = _num(ded, "NonDeductibleAmount") if ded is not None else None

                acct_id = _txt(ln, "AccountID")
                sup_id = _txt(ln, "SupplierID") or t_supp
                cust_id = _txt(ln, "CustomerID") or t_cust
                country = ti_country
                vat_number = ""
                sup_name = cust_name = ""
                if sup_id and sup_id in supp_lookup:
                    country = country or supp_lookup[sup_id]["country"]
                    vat_number = supp_lookup[sup_id]["vat_number"]
                    sup_name = supp_lookup[sup_id]["name"]
                if cust_id and cust_id in cust_lookup:
                    country = country or cust_lookup[cust_id]["country"]
                    vat_number = vat_number or cust_lookup[cust_id]["vat_number"]
                    cust_name = cust_lookup[cust_id]["name"]

                lines.append({
                    "record_id": _txt(ln, "RecordID") or f"L{len(lines) + 1}",
                    "account_id": acct_id,
                    "account_type": acct_type_lookup.get(acct_id, ""),
                    "standard_account_id": std_acct_lookup.get(acct_id, ""),
                    "description": _txt(ln, "Description"),
                    "debit_amount": debit,
                    "credit_amount": credit,
                    "tax_code": tax_code,
                    "tax_percentage": tax_pct,
                    "tax_base": tax_base if tax_base else round(debit + credit, 2),
                    "tax_amount": tax_amount,
                    "currency": default_ccy,
                    "supplier_id": sup_id, "supplier_name": sup_name,
                    "customer_id": cust_id, "customer_name": cust_name,
                    "source_document_id": _txt(t, "Description"),
                    "country": country,
                    "ship_from_country": "", "ship_to_country": "",  # ikke i SAF-T Financial
                    "vat_number": vat_number,
                    "non_deductible_amount": non_deductible,
                })
            # SAF-T Period er en regnskabsperiode ("1"); kontrollerne forventer
            # kalendermåned. Udled måneden af posteringsdatoen for at undgå
            # padding-/regnskabsperiode-artefakter (bekræftet på rigtige filer).
            month = posting[5:7] if len(posting) >= 7 else _txt(t, "Period").zfill(2)
            transactions.append({
                "transaction_id": _txt(t, "TransactionID"),
                "date": posting,
                "document_date": tdate,
                "description": _txt(t, "Description"),
                "journal_id": jid,
                "period": month,
                "period_year": _txt(t, "PeriodYear") or (posting[:4] if len(posting) >= 4 else ""),
                "total_debit": total_debit, "total_credit": total_credit,
                "lines": lines,
            })

    info["sections"] = {
        "version": version,
        "accounts": len(accounts),
        "tax_table": len(tax_table),
        "customers": len(customers),
        "suppliers": len(suppliers),
        "transactions": len(transactions),
        "lines": sum(len(t["lines"]) for t in transactions),
        "non_deductible_present": any(
            l.get("non_deductible_amount") is not None
            for t in transactions for l in t["lines"]),
    }
    # Best-effort-advarsler (fejler ikke — analysen kører alligevel).
    if not transactions:
        info["warnings"].append("Ingen transaktioner fundet (GeneralLedgerEntries mangler eller er tom).")
    if not accounts:
        info["warnings"].append("Ingen kontoplan (GeneralLedgerAccounts) fundet — kontotype-scope er inaktivt.")

    header_dict = {
        "company_name": company_name,
        "currency": default_ccy,
        "period_start": p_start,
        "period_end": p_end,
        "saft_version": version,
    }
    if p_start and p_end:
        try:
            header_dict["period"] = {
                "start": str(int(p_start[5:7])), "start_year": p_start[:4],
                "end": str(int(p_end[5:7])), "end_year": p_end[:4],
            }
        except (ValueError, IndexError):
            pass

    canonical = {
        "header": header_dict,
        "accounts": accounts,
        "tax_table": tax_table,
        "transactions": transactions,
        "suppliers": suppliers,
        "customers": customers,
        "summary": {
            "total_transactions": len(transactions),
            "currency": default_ccy,
            "period_start": p_start,
            "period_end": p_end,
        },
        "parse_info": info,
    }
    return canonical, info


def preview_saft(path: str) -> dict:
    """Let preview til /preview: SAF-T-version + sektions-optælling (ingen kolonner)."""
    canonical, info = parse_saft(path)
    if canonical is None:
        return {"type": "saft", "error": info["errors"][0] if info["errors"] else "Ugyldig SAF-T"}
    return {
        "type": "saft",
        "sections": info["sections"],
        "warnings": info["warnings"],
        "company": canonical["header"].get("company_name", ""),
    }
