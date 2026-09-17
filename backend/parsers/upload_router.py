"""
Upload-routing: vælg den rigtige parser (fladt Excel/CSV vs. SAF-T XML vs.
kanonisk gl_entries-CSV) og returnér ALTID den samme kanoniske, motor-klare
struktur.

Kontrakt (alle tre stier):
    {parse_info: {...evt. "error"...}, header, summary, accounts, tax_table,
     transactions, suppliers, customers}

- Excel/CSV (fladt udtræk): parse_excel -> adapt_excel_to_saft (uændret adfærd).
- SAF-T XML: saft_parser.parse_saft (allerede motor-formet).
- Kanonisk CSV (vat-extract's dataextract.transform-output, byggetrin 8):
  canonical_parser.parse_canonical.

Routing-rækkefølge (første match vinder):
  1. SAF-T: .xml-endelse ELLER indholds-sniff (AuditFile-rod) — uændret.
  2. Kanonisk CSV: .csv/.tsv-endelse OG header-sniff genkender de kanoniske
     objekt-model-kolonnenavne (gl_accounts/vat_codes/posting_dates) — et
     fladt Excel/CSV-udtræk bruger ALDRIG disse præcise kolonnenavne (jf.
     excel_parser.COLUMN_ALIASES), så der er ingen kollisionsrisiko.
  3. Ellers: fladt Excel/CSV via excel_parser (uændret fallback).
"""

from __future__ import annotations

import os

from parsers.excel_parser import parse_excel, get_column_mapping_preview
from parsers.data_adapter import adapt_excel_to_saft
from parsers import saft_parser
from parsers import canonical_parser

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


def is_canonical(file_path: str) -> bool:
    """Kanonisk gl_entries-CSV hvis endelsen er .csv/.tsv OG header-rækken
    indeholder de kanoniske objekt-model-markørkolonner (se
    canonical_parser.REQUIRED_MARKER_COLUMNS)."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in (".csv", ".tsv"):
        return False
    return canonical_parser.looks_like_canonical(file_path)


def parse_upload(file_path: str, progress_callback=None) -> dict:
    """Parse en upload til den kanoniske struktur. Fejl rapporteres i
    ``parse_info["error"]`` (kaster ikke)."""
    if is_saft(file_path):
        canonical, info = saft_parser.parse_saft(file_path)
        if canonical is None:
            err = info["errors"][0] if info.get("errors") else "Ugyldig SAF-T-fil."
            return {**_EMPTY, "parse_info": {"error": err, "kilde": "SAF-T"}}
        return canonical

    if is_canonical(file_path):
        canonical, info = canonical_parser.parse_canonical(file_path)
        if canonical is None:
            err = info["errors"][0] if info.get("errors") else "Ugyldig kanonisk fil."
            return {**_EMPTY, "parse_info": {"error": err, "kilde": "canonical"}}
        return canonical

    parsed = parse_excel(file_path, progress_callback=progress_callback)
    if parsed.get("parse_info", {}).get("error"):
        return {**_EMPTY, "header": parsed.get("header", {}),
                "parse_info": parsed["parse_info"]}
    return adapt_excel_to_saft(parsed)


def preview_upload(file_path: str) -> dict:
    """Preview til /preview: kolonne-mapping for Excel, sektions-optælling for
    SAF-T/kanonisk CSV."""
    if is_saft(file_path):
        return saft_parser.preview_saft(file_path)
    if is_canonical(file_path):
        return canonical_parser.preview_canonical(file_path)
    return get_column_mapping_preview(file_path)
