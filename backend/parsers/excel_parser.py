"""
Excel/CSV Parser for VAT Analytics.
Konverterer Excel/CSV data til det samme standardformat som SAF-T parseren,
så analytics engine kan genbruge alle 103 tests.

Understøtter fleksibel kolonne-mapping: brugeren behøver ikke have præcise
kolonnenavne — parseren forsøger at auto-detektere baseret på almindelige navne.
"""

import pandas as pd
import re
from datetime import datetime
from typing import Optional


# Kolonnenavn-aliaser for auto-detektion
COLUMN_ALIASES = {
    "transaction_id": [
        "transaction_id", "transaktions_id", "trans_id", "voucher", "bilag",
        "bilagsnr", "bilagsnummer", "voucher_no", "doc_no", "document_number",
        "journal_entry", "posteringsnr", "entry_no", "id",
    ],
    "date": [
        "date", "dato", "transaction_date", "transaktionsdato", "bogføringsdato",
        "posting_date", "bogforingsdato", "document_date", "fakturadato",
        "invoice_date", "valuedato", "value_date",
    ],
    "account_id": [
        "account_id", "konto", "kontonr", "kontonummer", "account", "account_no",
        "account_number", "gl_account", "finans_konto", "hovedkonto",
    ],
    "account_description": [
        "account_description", "kontobeskrivelse", "kontonavn", "account_name",
        "account_desc", "beskrivelse_konto",
    ],
    "description": [
        "description", "beskrivelse", "text", "tekst", "narrative", "posting_text",
        "posteringstekst", "bilagstekst", "memo", "comment",
    ],
    "debit": [
        "debit", "debet", "debit_amount", "debitbeløb", "debitbelob",
    ],
    "credit": [
        "credit", "kredit", "credit_amount", "kreditbeløb", "kreditbelob",
    ],
    "amount": [
        "amount", "beløb", "belob", "total", "net_amount", "nettobeløb",
    ],
    "vat_amount": [
        "vat_amount", "moms", "momsbeløb", "momsbelob", "vat", "tax_amount",
        "momsbeloeb", "skat", "afgift",
    ],
    "vat_code": [
        "vat_code", "momskode", "tax_code", "moms_kode", "afgiftskode",
        "vat_type", "momstype", "tax_type",
    ],
    "vat_rate": [
        "vat_rate", "momssats", "momsprocent", "tax_rate", "moms_pct",
        "vat_pct", "vat_percentage",
    ],
    "supplier_id": [
        "supplier_id", "leverandør_id", "leverandornr", "leverandørnr",
        "vendor_id", "vendor_no", "supplier_no", "creditor_id", "kreditornr",
    ],
    "supplier_name": [
        "supplier_name", "leverandør", "leverandornavn", "leverandørnavn",
        "vendor_name", "vendor", "creditor_name",
    ],
    "customer_id": [
        "customer_id", "kunde_id", "kundenr", "customer_no", "debitor_id",
        "debitornr",
    ],
    "customer_name": [
        "customer_name", "kunde", "kundenavn", "customer", "debitor_name",
        "debitornavn",
    ],
    "invoice_number": [
        "invoice_number", "fakturanr", "fakturanummer", "invoice_no", "invoice",
        "ekstern_bilag", "external_doc",
    ],
    "currency": [
        "currency", "valuta", "valutakode", "currency_code",
    ],
    "journal_id": [
        "journal_id", "journal", "journalnr", "journal_no", "journal_type",
        "journaltype", "kladde",
    ],
    "period": [
        "period", "periode", "month", "måned", "maaned",
    ],
    "year": [
        "year", "år", "aar", "fiscal_year", "regnskabsår",
    ],
    "country": [
        "country", "land", "landekode", "country_code",
    ],
    "vat_number": [
        "vat_number", "momsnr", "momsnummer", "cvr", "cvr_nr", "cvrnummer",
        "tax_id", "vat_id", "vat_registration",
    ],
}


def _normalize_column_name(name):
    """Normalisér kolonnenavn til lowercase, strip whitespace og specialtegn."""
    return re.sub(r"[^a-z0-9_æøåü]", "", str(name).lower().strip().replace(" ", "_"))


def _detect_columns(df):
    """
    Auto-detektér hvilke kolonner der mapper til hvilke felter.
    Returnerer dict: standard_field_name -> actual_column_name
    """
    mapping = {}
    normalized = {_normalize_column_name(col): col for col in df.columns}

    for field, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            norm_alias = _normalize_column_name(alias)
            if norm_alias in normalized:
                mapping[field] = normalized[norm_alias]
                break

    return mapping


def _safe_float(value, default=0.0):
    """Konvertér en værdi til float sikkert."""
    if pd.isna(value) or value is None:
        return default
    try:
        # Håndtér danske talformater (1.234,56 -> 1234.56)
        s = str(value).strip()
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except (ValueError, TypeError):
        return default


def _safe_str(value, default=""):
    """Konvertér en værdi til string sikkert."""
    if pd.isna(value) or value is None:
        return default
    return str(value).strip()


def _safe_date(value):
    """Konvertér en værdi til ISO dato-streng."""
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    # Prøv at parse streng
    for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%Y%m%d"]:
        try:
            return datetime.strptime(str(value).strip()[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return str(value).strip()[:10]


def parse_excel(file_path: str, sheet_name: Optional[str] = None):
    """
    Parser en Excel/CSV fil og returnerer data i standardformatet.

    Returnerer samme struktur som SAF-T parseren:
    {
        "header": {...},
        "accounts": [...],
        "tax_table": [...],
        "transactions": [...],
        "suppliers": [...],
        "customers": [...],
        "parse_info": {...}  # Info om parsing-processen
    }
    """
    # Læs fil
    if file_path.endswith(".csv"):
        # Prøv forskellige separatorer og encodings
        for sep in [";", ",", "\t"]:
            for encoding in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(file_path, sep=sep, encoding=encoding)
                    if len(df.columns) > 1:
                        break
                except Exception:
                    continue
            else:
                continue
            break
    else:
        df = pd.read_excel(file_path, sheet_name=sheet_name or 0)

    if df.empty:
        return {
            "header": {},
            "accounts": [],
            "tax_table": [],
            "transactions": [],
            "suppliers": [],
            "customers": [],
            "parse_info": {"error": "Filen er tom", "rows": 0, "columns": 0},
        }

    # Auto-detektér kolonner
    col_map = _detect_columns(df)

    parse_info = {
        "rows": len(df),
        "columns": len(df.columns),
        "detected_columns": col_map,
        "unmapped_columns": [
            col for col in df.columns
            if col not in col_map.values()
        ],
        "source_type": "csv" if file_path.endswith(".csv") else "excel",
    }

    # Byg transaktioner
    transactions = []
    accounts_seen = {}
    suppliers_seen = {}
    customers_seen = {}
    vat_codes_seen = {}

    for idx, row in df.iterrows():
        # Bestem beløb (debit/credit eller samlet amount)
        debit = _safe_float(row.get(col_map.get("debit"))) if "debit" in col_map else 0.0
        credit = _safe_float(row.get(col_map.get("credit"))) if "credit" in col_map else 0.0

        if "amount" in col_map and debit == 0 and credit == 0:
            amount = _safe_float(row.get(col_map.get("amount")))
            if amount >= 0:
                debit = amount
            else:
                credit = abs(amount)

        # Transaktions-ID
        txn_id = _safe_str(row.get(col_map.get("transaction_id"))) or f"ROW-{idx + 2}"

        # Dato
        date_val = row.get(col_map.get("date")) if "date" in col_map else None
        date_str = _safe_date(date_val)

        # Konto
        account_id = _safe_str(row.get(col_map.get("account_id")))
        account_desc = _safe_str(row.get(col_map.get("account_description")))

        # Moms
        vat_amount = _safe_float(row.get(col_map.get("vat_amount"))) if "vat_amount" in col_map else None
        vat_code = _safe_str(row.get(col_map.get("vat_code"))) if "vat_code" in col_map else ""
        vat_rate = _safe_float(row.get(col_map.get("vat_rate"))) if "vat_rate" in col_map else None

        # Leverandør / Kunde
        supplier_id = _safe_str(row.get(col_map.get("supplier_id"))) if "supplier_id" in col_map else ""
        supplier_name = _safe_str(row.get(col_map.get("supplier_name"))) if "supplier_name" in col_map else ""
        customer_id = _safe_str(row.get(col_map.get("customer_id"))) if "customer_id" in col_map else ""
        customer_name = _safe_str(row.get(col_map.get("customer_name"))) if "customer_name" in col_map else ""

        txn = {
            "transaction_id": txn_id,
            "date": date_str,
            "account_id": account_id,
            "account_description": account_desc,
            "description": _safe_str(row.get(col_map.get("description"))),
            "debit_amount": debit,
            "credit_amount": credit,
            "vat_amount": vat_amount,
            "vat_code": vat_code,
            "vat_rate": vat_rate,
            "journal_id": _safe_str(row.get(col_map.get("journal_id"))) or "IMPORT",
            "invoice_number": _safe_str(row.get(col_map.get("invoice_number"))),
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "currency": _safe_str(row.get(col_map.get("currency"))) or "DKK",
            "country": _safe_str(row.get(col_map.get("country"))),
            "vat_number": _safe_str(row.get(col_map.get("vat_number"))),
            "period": _safe_str(row.get(col_map.get("period"))),
            "year": _safe_str(row.get(col_map.get("year"))),
        }
        transactions.append(txn)

        # Saml unikke konti
        if account_id and account_id not in accounts_seen:
            accounts_seen[account_id] = {
                "account_id": account_id,
                "description": account_desc,
                "account_type": "",
                "opening_balance": 0.0,
                "closing_balance": 0.0,
            }

        # Saml unikke leverandører
        if supplier_id and supplier_id not in suppliers_seen:
            suppliers_seen[supplier_id] = {
                "supplier_id": supplier_id,
                "name": supplier_name,
                "vat_number": _safe_str(row.get(col_map.get("vat_number"))) if supplier_id else "",
                "country": _safe_str(row.get(col_map.get("country"))),
            }

        # Saml unikke kunder
        if customer_id and customer_id not in customers_seen:
            customers_seen[customer_id] = {
                "customer_id": customer_id,
                "name": customer_name,
                "vat_number": "",
                "country": "",
            }

        # Saml unikke momskoder
        if vat_code and vat_code not in vat_codes_seen:
            vat_codes_seen[vat_code] = {
                "tax_code": vat_code,
                "description": f"Momskode {vat_code}",
                "rate": vat_rate if vat_rate is not None else 0.0,
            }

    # Byg header fra tilgængelig data
    header = {
        "company_name": "",
        "registration_number": "",
        "currency": "DKK",
        "period_start": "",
        "period_end": "",
        "source": "Excel/CSV import",
    }

    # Forsøg at udlede periode fra data
    dates = [t["date"] for t in transactions if t["date"]]
    if dates:
        sorted_dates = sorted(dates)
        header["period_start"] = sorted_dates[0]
        header["period_end"] = sorted_dates[-1]

    return {
        "header": header,
        "accounts": list(accounts_seen.values()),
        "tax_table": list(vat_codes_seen.values()),
        "transactions": transactions,
        "suppliers": list(suppliers_seen.values()),
        "customers": list(customers_seen.values()),
        "parse_info": parse_info,
    }


def get_column_mapping_preview(file_path: str, sheet_name: Optional[str] = None):
    """
    Returnerer en preview af de første 5 rækker + auto-detekteret kolonne-mapping.
    Bruges til at lade brugeren bekræfte/rette mappingen før analyse.
    """
    if file_path.endswith(".csv"):
        for sep in [";", ",", "\t"]:
            for encoding in ["utf-8", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(file_path, sep=sep, encoding=encoding, nrows=5)
                    if len(df.columns) > 1:
                        break
                except Exception:
                    continue
            else:
                continue
            break
    else:
        df = pd.read_excel(file_path, sheet_name=sheet_name or 0, nrows=5)

    col_map = _detect_columns(df)

    return {
        "columns": list(df.columns),
        "preview_rows": df.head(5).fillna("").to_dict(orient="records"),
        "auto_mapping": col_map,
        "unmapped_columns": [col for col in df.columns if col not in col_map.values()],
        "required_fields": ["transaction_id", "date", "account_id", "amount/debit/credit"],
        "optional_fields": list(COLUMN_ALIASES.keys()),
    }
