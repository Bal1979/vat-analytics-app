"""
Upload-routing: vælg den rigtige parser (fladt Excel/CSV vs. SAF-T XML) og
returnér ALTID den samme kanoniske, motor-klare struktur.

Kontrakt (begge stier):
    {parse_info: {...evt. "error"...}, header, summary, accounts, tax_table,
     transactions, suppliers, customers}

- Excel/CSV: parse_excel -> adapt_excel_to_saft (uændret adfærd).
- SAF-T XML: saft_parser.parse_saft (allerede motor-formet).

Routing sker på filendelse (.xml) ELLER indholds-sniff (AuditFile-rod), så en
SAF-T-fil med en atypisk endelse også fanges.
"""

from __future__ import annotations

import os

from parsers.excel_parser import parse_excel, get_column_mapping_preview
from parsers.data_adapter import adapt_excel_to_saft
from parsers import saft_parser

_EMPTY = {
    "header": {}, "summary": {}, "accounts": [], "tax_table": [],
    "transactions": [], "suppliers": [], "customers": [],
}


def is_saft(file_path: str) -> bool:
    """SAF-T hvis .xml-endelse eller indholds-sniff genkender en AuditFile-rod."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".xml":
        return True
    return saft_parser.looks_like_saft(file_path)


def parse_upload(file_path: str, progress_callback=None) -> dict:
    """Parse en upload til den kanoniske struktur. Fejl rapporteres i
    ``parse_info["error"]`` (kaster ikke)."""
    if is_saft(file_path):
        canonical, info = saft_parser.parse_saft(file_path)
        if canonical is None:
            err = info["errors"][0] if info.get("errors") else "Ugyldig SAF-T-fil."
            return {**_EMPTY, "parse_info": {"error": err, "kilde": "SAF-T"}}
        return canonical

    parsed = parse_excel(file_path, progress_callback=progress_callback)
    if parsed.get("parse_info", {}).get("error"):
        return {**_EMPTY, "header": parsed.get("header", {}),
                "parse_info": parsed["parse_info"]}
    return adapt_excel_to_saft(parsed)


def preview_upload(file_path: str) -> dict:
    """Preview til /preview: kolonne-mapping for Excel, sektions-optælling for SAF-T."""
    if is_saft(file_path):
        return saft_parser.preview_saft(file_path)
    return get_column_mapping_preview(file_path)
