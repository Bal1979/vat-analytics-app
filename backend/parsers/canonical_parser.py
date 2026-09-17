"""
Kanonisk CSV -> VAT Analytics — TREDJE input-vej (BALAI-dataflow-arkitektur.md
§7, byggetrin 8, Bal-godkendt 2026-09-17).

Læser den kanoniske ``gl_entries``-CSV, som ``dataextract.transform``
(vat-extract-repoet) producerer deterministisk fra en Bal-godkendt mapping, og
bygger PRÆCIS samme kontrakt-struktur som de to andre input-veje:
    * Excel/CSV (fladt udtræk) -> ``parsers.excel_parser`` + ``data_adapter.adapt_excel_to_saft``
    * SAF-T XML                -> ``parsers.saft_parser.parse_saft``
    * Kanonisk CSV (denne fil) -> ``parse_canonical``

Alle tre producerer:
    {parse_info, header, summary, accounts, tax_table, transactions, suppliers,
     customers}

Kilde for feltkorrespondencen: krydsmapningen i vat-extract
(``dataextract/catalog/analytics_mapping.json``), som mapper objekt-modellens
kanoniske feltnavne (fx ``gl_accounts``, ``vat_codes``, ``debit_amount``) til
motorens kontraktfelter (fx ``accounts[].account_id``, ``tax_table[].tax_code``,
``transactions[].lines[].debit_amount``). Se pr.-kolonne-kommentarer nedenfor.

Designprincipper (samme disciplin som ``saft_parser.py``):
  * **Best-effort:** manglende VALGFRIE kolonner giver tomme/0-felter, ikke
    exceptions — kun de kolonner filen faktisk har, bruges. Kun et lille sæt
    "marker"-kolonner er obligatoriske for overhovedet at genkende filen som
    kanonisk (se ``REQUIRED_MARKER_COLUMNS``).
  * **Separator-agnostisk for ``vat_codes``:** vat-extract er ved at skifte
    D1-kombinationens interne separator (fra "/" til "|"). Denne parser
    behandler ALTID ``vat_codes`` som en opaque streng — den splitter, parser
    eller fortolker aldrig separatoren. Et evt. fremtidigt behov for at splitte
    koden i sine to dele hører hjemme i vat-extract (kilden), ikke her.
  * **Ingen ændring af rådata:** filen åbnes read-only og røres aldrig
    (jf. brugerens faste regel: BALAI analyserer, retter aldrig i uploadede/
    transformerede filer).
  * **Én række = én transaktion = én linje.** Den kanoniske gl_entries-fil er
    allerede linje-niveau (én GL-postering pr. række) — motoren forventer
    transaktioner MED ``lines[]``, så hver række pakkes ind i en
    1-linje-transaktion. Samme mønster som
    ``data_adapter.adapt_excel_to_saft`` bruger for det flade Excel-udtræk.
  * **Kendt gap, dokumenteret (ikke skjult):** den aktuelle BC/NAV-mapping
    (seedet 2026-09-16) producerer INGEN selvstændig momssats-kolonne
    (``tax_percentage``), INGEN kontoplan-metadata (``account_type`` /
    ``standard_account_id`` / saldi), og INGEN leverandør-/kundestamdata. Disse
    kontraktfelter er derfor strukturelt tomme på den kanoniske vej i dag —
    ikke en fejl i denne parser, men et reelt datahul i objekt-modellens
    nuværende ``analytics_mapping.json``-dækning. Se
    ``catalog/data_contract.json``'s ``known_gaps`` (GAP-10/GAP-11) og
    ``tools/data_contract_data.py``.
  * **Kendt, bekræftet støjkilde (GAP-12, udviklings-E2E 2026-09-17):** uden en
    dokument-/bilagsgrupperingsnøgle er hver 1-linjes "transaktion" typisk
    ENSIDET (kun debit ELLER credit, aldrig begge) — kontrol 10
    (transaktionsbalance, kategori 1) vil derfor flage NÆSTEN HVER transaktion
    som "ubalanceret". Bekræftet på den rigtige BC/NAV-fil: 125.885 kritiske
    fund af 125.986 transaktioner i udviklings-E2E'en. Strukturelt
    falsk-positivt-mønster, IKKE 125.885 reelle bogføringsfejl — se
    known_gaps GAP-12 for begrundelse og Bal-anbefaling (overvej at nedvægte
    kontrol 10 for ``parse_info.kilde == "canonical"``-kørsler, indtil en
    grupperingsnøgle findes). Rettes IKKE her.

Motoren importeres IKKE her — parseren producerer kun data.
"""

from __future__ import annotations

import csv
import json
import os


# --- Kolonner ----------------------------------------------------------------

# De kolonner, den BC/NAV-seedede mapping (2026-09-16) faktisk producerer i dag
# (transform_summary.json["fields_mapped"]). Bruges KUN til logging/diagnostik
# — parseren læser enhver kolonne der findes, ignorerer resten, og fejler ikke
# hvis en fremtidig mapping tilføjer/fjerner kolonner (best-effort).
KNOWN_CANONICAL_COLUMNS = {
    "posting_dates", "tax_point", "vat_period", "credit_note_flag",
    "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
    "debit_amount", "credit_amount", "supply_direction", "currency_fx",
    # Fremtidige mapping-udvidelser denne parser allerede kan bruge, hvis/når
    # vat-extract begynder at levere dem (jf. analytics_mapping.json's
    # "reelle funktionelle huller" og harmonization_recommendations):
    "tax_percentage", "tax_base_amount", "ship_from", "ship_to",
    "counterparty_country", "vat_registration_numbers",
    "account_type", "standard_account_id",
}

# Minimumssæt for overhovedet at genkende filen som "kanonisk gl_entries" i
# routing (upload_router.is_canonical). Bevidst lille: robust mod en mapping,
# der (endnu) ikke leverer alle KNOWN_CANONICAL_COLUMNS.
REQUIRED_MARKER_COLUMNS = {"gl_accounts", "vat_codes", "posting_dates"}


def _read_header(path: str) -> list:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return []


def looks_like_canonical(path: str) -> bool:
    """Sniff: er filen sandsynligvis en kanonisk gl_entries-CSV fra
    ``dataextract.transform``? Bruges af upload_router til routing, sammen med
    filendelsen (.csv/.tsv)."""
    try:
        header = {h.strip() for h in _read_header(path)}
    except (OSError, UnicodeDecodeError, csv.Error):
        return False
    return REQUIRED_MARKER_COLUMNS.issubset(header)


# --- Hjælpere ------------------------------------------------------------

def _num(value) -> float:
    """Beløb: tom/uparsebar -> 0.0. Transformationsmotoren i vat-extract
    garanterer ingen stille udeladelser (§8.3-udeladelsespolitikken skriver
    '' ved fejl) — denne fallback er et ekstra forsvarslag, ikke den primære
    datakvalitetsgaranti."""
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _bool(value) -> bool:
    return str(value or "").strip().lower() in ("true", "1", "ja", "yes")


def _currency_code(value) -> str:
    """``currency_fx``-primitivens format: '' (DKK) eller 'KODE:beløb'."""
    s = str(value or "").strip()
    if not s:
        return "DKK"
    code = s.split(":", 1)[0].strip()
    return code or "DKK"


def _split_vat_period(value) -> tuple:
    """'YYYY-MM' -> (period='MM', period_year='YYYY'). Aldrig fejlende."""
    s = str(value or "").strip()
    if len(s) >= 7 and s[4] == "-":
        return s[5:7], s[:4]
    return "", ""


# --- Summary/lineage-sidecar -----------------------------------------------

def _read_transform_summary(summary_path: str) -> dict:
    """Læser transform_summary.json (lineage: mapping_version,
    schema_fingerprint, ...). Valgfri — tom dict hvis filen mangler/er ugyldig
    (kanonisk analyse må ikke kræve sidecar-filen for at kunne køre)."""
    if not summary_path or not os.path.exists(summary_path):
        return {}
    try:
        with open(summary_path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}


def _default_summary_path(csv_path: str) -> str:
    """Konvention: transform_summary.json liggende ved siden af CSV'en (samme
    mønster som dataextract.transform's CLI skriver den)."""
    return os.path.join(os.path.dirname(os.path.abspath(csv_path)), "transform_summary.json")


# --- Parsning ----------------------------------------------------------------

def parse_canonical(csv_path: str, summary_path: str | None = None) -> tuple:
    """Parse en kanonisk gl_entries-CSV til (canonical_dict, info).

    ``summary_path``: valgfri sti til transform_summary.json (lineage). Hvis
    ikke angivet, forsøges filen ved siden af ``csv_path`` automatisk; findes
    den ikke, køres analysen alligevel (blot uden lineage-stempel) — jf.
    opgavens krav om at afstemnings-/lineage-lag er valgfrie, ikke blokerende.
    """
    info = {"errors": [], "warnings": [], "sections": {}, "kilde": "canonical"}

    if not os.path.exists(csv_path):
        return None, {**info, "errors": [f"Filen findes ikke: {csv_path}"]}

    lineage = _read_transform_summary(summary_path or _default_summary_path(csv_path))
    if not lineage:
        info["warnings"].append(
            "Ingen transform_summary.json fundet ved siden af filen — "
            "lineage (mapping_version/schema_fingerprint) stemples ikke."
        )

    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = set(reader.fieldnames or [])
            if not REQUIRED_MARKER_COLUMNS.issubset(fieldnames):
                missing = sorted(REQUIRED_MARKER_COLUMNS - fieldnames)
                return None, {
                    **info,
                    "errors": [
                        "Filen mangler obligatoriske kanoniske kolonner: "
                        f"{missing}. Forventede mindst {sorted(REQUIRED_MARKER_COLUMNS)}."
                    ],
                }
            rows = list(reader)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return None, {**info, "errors": [f"CSV kunne ikke læses: {e}"]}

    accounts_seen = {}
    tax_codes_seen = {}
    transactions = []
    total_debit = total_credit = total_vat = 0.0
    min_date = max_date = ""

    for idx, row in enumerate(rows):
        gl_account = (row.get("gl_accounts") or "").strip()
        vat_code = (row.get("vat_codes") or "").strip()
        posting_date = (row.get("posting_dates") or "").strip()
        tax_point = (row.get("tax_point") or "").strip()
        vat_period = row.get("vat_period") or ""
        invoice_number = (row.get("invoice_numbers") or "").strip()

        debit = _num(row.get("debit_amount"))
        credit = _num(row.get("credit_amount"))
        vat_amount = _num(row.get("vat_amount"))
        # Fremtidssikret: bruges hvis/når mappingen leverer den direkte
        # (jf. analytics_mapping.json's "reelle funktionelle huller").
        # KENDT GAB i dag: kolonnen findes ikke i den seedede BC-mapping, så
        # tax_percentage er altid 0.0 på denne vej (se known_gaps GAP-10).
        tax_percentage = _num(row.get("tax_percentage")) if "tax_percentage" in row else 0.0
        imported_base = _num(row.get("tax_base_amount")) if "tax_base_amount" in row else 0.0
        if imported_base:
            tax_base = round(imported_base, 2)
        elif vat_amount and tax_percentage:
            tax_base = round(vat_amount / (tax_percentage / 100), 2)
        else:
            tax_base = round(debit + credit, 2)

        currency = _currency_code(row.get("currency_fx"))
        period, period_year = _split_vat_period(vat_period)

        if gl_account:
            accounts_seen.setdefault(gl_account, True)
        if vat_code:
            tax_codes_seen.setdefault(vat_code, True)
        if posting_date:
            if not min_date or posting_date < min_date:
                min_date = posting_date
            if not max_date or posting_date > max_date:
                max_date = posting_date

        line = {
            "record_id": "L1",
            "account_id": gl_account,
            # KENDT GAB (GAP-11): ingen kontoplan-fil på denne vej -> altid "".
            "account_type": "",
            "standard_account_id": "",
            "description": "",
            "debit_amount": debit,
            "credit_amount": credit,
            "tax_code": vat_code,
            "tax_percentage": tax_percentage,
            "tax_base": tax_base,
            "tax_amount": vat_amount,
            "currency": currency,
            "supplier_id": "", "supplier_name": "",
            "customer_id": "", "customer_name": "",
            "source_document_id": invoice_number,
            "country": (row.get("counterparty_country") or "").strip() if "counterparty_country" in row else "",
            "ship_from_country": (row.get("ship_from") or "").strip() if "ship_from" in row else "",
            "ship_to_country": (row.get("ship_to") or "").strip() if "ship_to" in row else "",
            "vat_number": (row.get("vat_registration_numbers") or "").strip() if "vat_registration_numbers" in row else "",
            "non_deductible_amount": None,
            # Ekstra, endnu-ikke-kontraktbårne canonical-only felter — bevares
            # for sporbarhed/fremtidig kontraktudvidelse, ingen kontrol læser
            # dem i dag (jf. analytics_mapping.json's object_model_only-liste):
            "credit_note_flag": _bool(row.get("credit_note_flag")),
            "supply_direction": (row.get("supply_direction") or "").strip(),
            "tax_point": tax_point,
        }

        total_debit += debit
        total_credit += credit
        total_vat += vat_amount

        transactions.append({
            "transaction_id": f"ROW-{idx + 2}",  # +2: header = række 1
            "date": posting_date,
            # tax_point er den nærmeste kanoniske proxy for "dokument-/
            # transaktionsdato adskilt fra bogføringsdato" (§2a's document_date-
            # ekstension) — begrundelse: TaxPointDate er SAF-T's periodiserings-
            # felt, konceptuelt tættest på transaktionens afgiftsudløsende dato,
            # ikke bogføringsdatoen. Falder tilbage til posting_date, hvis tom.
            "document_date": tax_point or posting_date,
            "description": "",
            "journal_id": "IMPORT",
            "period": period,
            "period_year": period_year,
            "total_debit": debit,
            "total_credit": credit,
            "lines": [line],
        })

    accounts = [
        {
            "account_id": acc_id,
            "description": "",
            "account_type": "",
            "standard_account_id": "",
            "opening_balance": 0.0,
            "closing_balance": 0.0,
        }
        for acc_id in sorted(accounts_seen)
    ]
    tax_table = [
        {
            "tax_code": code,
            "description": "",
            "tax_percentage": 0.0,
            "rate": 0.0,
            "standard_tax_code": "",
            "country": "",
        }
        for code in sorted(tax_codes_seen)
    ]

    mapping_version = lineage.get("mapping_version", "")
    schema_fingerprint = lineage.get("schema_fingerprint", "")

    header = {
        "company_name": "",
        "registration_number": "",
        "currency": "DKK",
        "period_start": min_date,
        "period_end": max_date,
        "source": "canonical",
        "saft_version": "",
        # Lineage (byggetrin 8 — reproducerbarhed, jf. BALAI-dataflow-
        # arkitektur.md §7): hvilken godkendt mapping og hvilket kilde-skema
        # producerede denne kanoniske fil. Se tools/data_contract_data.py.
        "mapping_version": mapping_version,
        "schema_fingerprint": schema_fingerprint,
    }
    if min_date and max_date:
        try:
            header["period"] = {
                "start": str(int(min_date[5:7])), "start_year": min_date[:4],
                "end": str(int(max_date[5:7])), "end_year": max_date[:4],
            }
        except (ValueError, IndexError):
            pass

    info["sections"] = {
        "rows": len(rows),
        "accounts": len(accounts),
        "tax_table": len(tax_table),
        "transactions": len(transactions),
        "lines": len(transactions),  # 1:1 i denne vej
    }
    info["lineage"] = {
        "mapping_version": mapping_version,
        "schema_fingerprint": schema_fingerprint,
        "source_erp": lineage.get("source_erp", ""),
        "profile_version": lineage.get("profile_version", ""),
        "rows_in": lineage.get("rows_in"),
        "rows_out": lineage.get("rows_out"),
        "source_file": lineage.get("source_file", ""),
    }
    if not transactions:
        info["warnings"].append("Ingen rækker fundet i den kanoniske CSV.")
    if not accounts:
        info["warnings"].append("Ingen kontoplan-signal (gl_accounts alle tomme) — kontotype-scope er inaktivt.")
    if not tax_table:
        info["warnings"].append("Ingen momskoder fundet (vat_codes alle tomme).")

    canonical = {
        "header": header,
        "accounts": accounts,
        "tax_table": tax_table,
        "transactions": transactions,
        "suppliers": [],  # KENDT GAB (GAP-11): ingen leverandørstamdata på denne vej i dag.
        "customers": [],  # KENDT GAB (GAP-11): ingen kundestamdata på denne vej i dag.
        "summary": {
            "total_transactions": len(transactions),
            "total_debit": round(total_debit, 2),
            "total_credit": round(total_credit, 2),
            "total_vat": round(total_vat, 2),
            "currency": "DKK",
            "period_start": min_date,
            "period_end": max_date,
        },
        "parse_info": info,
    }
    return canonical, info


def preview_canonical(csv_path: str) -> dict:
    """Let preview til /preview: rækkeantal + sektions-optælling (ingen
    kundedata gengives)."""
    canonical, info = parse_canonical(csv_path)
    if canonical is None:
        return {"type": "canonical", "error": info["errors"][0] if info["errors"] else "Ugyldig kanonisk fil"}
    return {
        "type": "canonical",
        "sections": info["sections"],
        "warnings": info["warnings"],
        "lineage": info.get("lineage", {}),
    }
