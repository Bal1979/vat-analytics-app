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
  * **Bilagsgruppering (GAP-12, rettet 2026-09-17, Bal-godkendt).** Den
    kanoniske gl_entries-fil er linje-niveau (én GL-postering pr. række), men
    BC/NAV's GL-poster balancerer PR. BILAG/journal-transaktion, ikke pr.
    række — og den seedede mapping leverer ikke BC's Entry No./Transaction No.
    Den pragmatiske bilagsnøgle er derfor ``(invoice_numbers, posting_dates)``:
    rækker med samme, IKKE-TOMME ``invoice_numbers`` og samme ``posting_dates``
    samles til ÉN transaktion med flere ``lines[]``. Rækker med tomt/manglende
    ``invoice_numbers`` grupperes ALDRIG sammen (heller ikke med hinanden på
    samme dato) — der er ingen evidens for en sammenhæng, så de forbliver hver
    sin egen 1-linjes transaktion (samme fallback som før grupperingen fandtes,
    og samme mønster som ``data_adapter.adapt_excel_to_saft`` bruger for det
    flade Excel-udtræk uden bilagskolonne). Transaktions-id for en grupperet
    transaktion er deterministisk af nøglen (``DOC_<invoice>_<dato>``); en
    ugrupperet 1-linjes transaktion beholder det gamle ``ROW-<rækkenr>``-id.
    Hver linje bærer sin oprindelige CSV-rækkenummer videre i det
    canonical-only feltet ``source_row`` (samme sporbarhedsprincip som
    ``credit_note_flag``/``supply_direction``/``tax_point`` nedenfor — ikke et
    kontraktfelt, men bevaret for lineage). Se ``_group_key`` og
    ``_build_transaction`` nedenfor samt known_gaps GAP-12 i
    ``tools/data_contract_data.py`` for empirisk baggrund (125.885 kritiske
    falsk-positive fund af 125.986 rækker på kontrol 10/transaktionsbalance,
    FØR denne gruppering, på den rigtige BC/NAV-fil).
  * **Description (GAP-13, lukket 2026-09-17, Bal-godkendt).** Motorsidens
    del af medium-fund-analysen (kontrol 4 flagede 50.479 bilag for
    manglende Description på v2-filen, fordi feltet slet ikke blev læst her
    -- ikke fordi det manglede i kilden). Parseren læser nu en valgfri
    ``description``-kolonne og fører den ind på BÅDE linje- og
    transaktionsniveau, præcis som Excel-/SAF-T-vejen (``data_adapter.py``/
    ``saft_parser.py``). Kolonnen kan mangle på ældre kanoniske filer (v2) --
    da er feltet fortsat "" (uændret adfærd, ingen crash). Se
    ``analytics/readiness.py`` (Del B) for hvordan motoren undgår at flage
    50.479 falske "mangler Description"-fund, når kolonnen slet ikke findes
    i datagrundlaget.
  * **Kendt gap, dokumenteret (ikke skjult) — DELVIST LUKKET (byggetrin 8,
    Del C, Bal-godkendt 2026-09-17):** den seedede BC/NAV-mapping (2026-09-16)
    producerer selv INGEN selvstændig momssats-kolonne (``tax_percentage``),
    INGEN kontoplan-metadata (``account_type``/``standard_account_id``/
    saldi) og INGEN leverandør-/kundestamdata på ``gl_entries``-niveau. Tre
    VALGFRIE stamdata-sidecar-filer (``vat_setup.csv``, ``chart_of_accounts.csv``,
    ``customers.csv``, se ``parsers/canonical_masterdata.py``) kan nu levere
    dette som en separat berigelse EFTER selve parsningen, når filerne
    findes ved siden af CSV'en. Fravær af én eller alle tre filer giver
    UÆNDRET adfærd (samme strukturelle 0.0/""-tomhed som før). ``customers.csv``
    fylder kun den selvstændige ``customers[]``-liste — den kan IKKE joines
    til linjerne (intet ``customer_id``-felt i ``gl_entries``), så kontrol
    94-97 er fortsat blinde på den kanoniske vej (se ``canonical_masterdata.py``'s
    docstring). Se ``catalog/data_contract.json``'s ``known_gaps``
    (GAP-10/GAP-11) og ``tools/data_contract_data.py`` for status.

Motoren importeres IKKE her — parseren producerer kun data.
"""

from __future__ import annotations

import csv
import json
import os

from parsers import canonical_masterdata


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
    # GAP-13 (lukket 2026-09-17, Bal-godkendt): vat-extract tilføjer en
    # description-kolonne parallelt med denne opgave. Valgfri -- se
    # kolonnehåndteringen i parse_canonical nedenfor.
    "description",
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


# --- Bilagsgruppering (GAP-12) -----------------------------------------------

def _group_key(invoice_number: str, posting_date: str, source_row: int):
    """Den pragmatiske bilagsnøgle: ``(invoice_numbers, posting_dates)``, når
    ``invoice_numbers`` faktisk er udfyldt. Et tomt/manglende bilagsnummer
    giver ALDRIG en gruppering på tværs af rækker (heller ikke ved samme
    dato) — vi gætter aldrig en sammenhæng, der ikke er evidens for. I stedet
    får hver sådan række en unik nøgle (sit eget rækkenummer), så den forbliver
    sin egen 1-linjes transaktion, præcis som før grupperingen fandtes."""
    if invoice_number:
        return ("DOC", invoice_number, posting_date)
    return ("ROW", source_row)


def _build_transaction(group_key: tuple, members: list) -> dict:
    """Byg én transaktion (med et eller flere ``lines[]``) af de rækker, der
    deler en bilagsnøgle. Aggregat-felter (total_debit/total_credit) summeres
    over linjerne — samme konvention som ``data_adapter.adapt_excel_to_saft``
    og ``saft_parser.parse_saft`` bruger (record_id positionelt ``L<n>`` inden
    for transaktionen; ``total_debit``/``total_credit`` = sum af linjernes
    debit-/kreditbeløb)."""
    lines = []
    for pos, member in enumerate(members):
        line = dict(member["line"])
        line["record_id"] = f"L{pos + 1}"
        # Række-lineage: canonical-only felt (ikke i data_contract.json, samme
        # status som credit_note_flag/supply_direction/tax_point) -- så en
        # grupperet transaktions linjer altid kan spores tilbage til deres
        # oprindelige CSV-række, uanset hvor mange rækker der er grupperet.
        line["source_row"] = member["source_row"]
        lines.append(line)

    first = members[0]
    period, period_year = _split_vat_period(first["vat_period"])
    total_debit = round(sum(l["debit_amount"] for l in lines), 2)
    total_credit = round(sum(l["credit_amount"] for l in lines), 2)

    if group_key[0] == "DOC":
        _, invoice_number, key_date = group_key
        transaction_id = f"DOC_{invoice_number}_{key_date}"
    else:
        transaction_id = f"ROW-{group_key[1]}"

    # GAP-13 (lukket 2026-09-17, Bal-godkendt): transaktionens description er
    # samme kilde-værdi som linjernes (kolonnen er række-/linjeniveau i den
    # kanoniske CSV, ligesom Excel-vejens flade description -- se
    # data_adapter.adapt_excel_to_saft, der spejler linje- og
    # transaktionsniveau fra samme kildefelt). For en grupperet
    # flerlinje-transaktion (samme bilagsnøgle) tages den FØRSTE
    # ikke-tomme description blandt de grupperede linjer -- ingen gætning på
    # tværs af linjer, bare det første reelle signal, samme mønster som
    # document_date-fallbacket ovenfor.
    description = next((l["description"] for l in lines if l["description"]), "")

    return {
        "transaction_id": transaction_id,
        "date": first["posting_date"],
        # Samme document_date-logik som før grupperingen (§2a's document_date-
        # ekstension): tax_point er den nærmeste kanoniske proxy for
        # transaktionsdato adskilt fra bogføringsdato, fallback til
        # posting_date. Grupperede rækker deler pr. definition posting_date;
        # tax_point tages fra gruppens første række.
        "document_date": first["tax_point"] or first["posting_date"],
        "description": description,
        "journal_id": "IMPORT",
        "period": period,
        "period_year": period_year,
        "total_debit": total_debit,
        "total_credit": total_credit,
        "lines": lines,
    }


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

def parse_canonical(csv_path: str, summary_path: str | None = None,
                     vat_setup_path: str | None = None,
                     chart_of_accounts_path: str | None = None,
                     customers_path: str | None = None) -> tuple:
    """Parse en kanonisk gl_entries-CSV til (canonical_dict, info).

    ``summary_path``: valgfri sti til transform_summary.json (lineage). Hvis
    ikke angivet, forsøges filen ved siden af ``csv_path`` automatisk; findes
    den ikke, køres analysen alligevel (blot uden lineage-stempel) — jf.
    opgavens krav om at afstemnings-/lineage-lag er valgfrie, ikke blokerende.

    ``vat_setup_path``/``chart_of_accounts_path``/``customers_path``: valgfrie
    stier til de tre kanoniske stamdata-sidecar-filer (byggetrin 8, Del C).
    Ikke angivet -> forsøges ved siden af ``csv_path`` under de aftalte navne
    (``vat_setup.csv``/``chart_of_accounts.csv``/``customers.csv``). Se
    parsers/canonical_masterdata.py for berigelseslogikken. Alle tre er
    uafhængigt valgfrie — fravær af én ændrer intet for de øvrige eller for
    resten af parsningen.
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
    total_debit = total_credit = total_vat = 0.0
    min_date = max_date = ""

    # Bilagsgruppering (GAP-12): rækker samles pr. bilagsnøgle
    # ``_group_key(invoice_number, posting_date, source_row)`` i FØRSTE
    # forekomst-rækkefølge, så transaktionsrækkefølgen i outputtet forbliver
    # stabil/reproducerbar. ``groups`` samler linje-kandidaterne pr. nøgle;
    # transaktionerne bygges bagefter af ``_build_transaction``.
    group_order = []
    groups: dict = {}

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

        if gl_account:
            accounts_seen.setdefault(gl_account, True)
        if vat_code:
            tax_codes_seen.setdefault(vat_code, True)
        if posting_date:
            if not min_date or posting_date < min_date:
                min_date = posting_date
            if not max_date or posting_date > max_date:
                max_date = posting_date

        # GAP-13 (lukket 2026-09-17, Bal-godkendt): description er en valgfri
        # kolonne -- fraværende kolonne giver "" (uændret adfærd), en
        # tilstedeværende kolonne læses og føres med, PRÆCIS som Excel-/
        # SAF-T-vejen bærer description på linjeniveau (data_adapter.py /
        # saft_parser.py). Ingen gætning/udledning ved fravær.
        description = (row.get("description") or "").strip() if "description" in row else ""

        line = {
            "account_id": gl_account,
            # KENDT GAB (GAP-11): ingen kontoplan-fil på denne vej -> altid "".
            "account_type": "",
            "standard_account_id": "",
            "description": description,
            "debit_amount": debit,
            "credit_amount": credit,
            "tax_code": vat_code,
            "tax_percentage": tax_percentage,
            # balai_extensions (kontrakt v0.4.0): nøglesæt-symmetri -- ALTID
            # til stede, default "" (intet signal). Fyldes af
            # canonical_masterdata.enrich_canonical NÅR vat_setup.csv er
            # indlæst OG linjens kode findes deri (kontrol 82, Bal-godkendt
            # 2026-09-17).
            "vat_calculation_type": "",
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

        source_row = idx + 2  # +2: header = række 1
        key = _group_key(invoice_number, posting_date, source_row)
        if key not in groups:
            groups[key] = []
            group_order.append(key)
        groups[key].append({
            "line": line,
            "source_row": source_row,
            "posting_date": posting_date,
            "tax_point": tax_point,
            "vat_period": vat_period,
        })

    transactions = [_build_transaction(key, groups[key]) for key in group_order]

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
            # Byggetrin 8, Del A (Bal-godkendt 2026-09-17): nøglesæt-symmetri
            # (samme princip som resten af filen) -- ALTID til stede, default
            # False. Sættes til True af canonical_masterdata.enrich_canonical
            # NÅR vat_setup.csv er indlæst OG denne kode findes deri (se
            # kontrol 19, cat03_vat_rate_validation.py).
            "setup_matched": False,
            # balai_extensions (Bal-godkendt 2026-09-17, kontrakt v0.4.0):
            # pr.-kode-konfiguration fra vat_setup.csv. Samme nøglesæt-
            # symmetri: ALTID til stede, defaults = "intet signal".
            # non_deductible_vat_pct er None (ikke 0.0) uden signal, så
            # "fuld fradragsret (0%)" kan skelnes fra "ukendt".
            "non_deductible_vat_pct": None,
            "allow_non_deductible_vat": "",
            "vat_calculation_type": "",
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
        # Byggetrin 8, Del A (Bal-godkendt 2026-09-17): nøglesæt-symmetri --
        # ALTID til stede, default False. Sættes til True af
        # canonical_masterdata.enrich_canonical NÅR vat_setup.csv findes og
        # indeholder mindst én gyldig række -- se kontrol 19,
        # cat03_vat_rate_validation.py, for hvordan flaget bruges.
        "vat_setup_loaded": False,
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
        # Efter bilagsgruppering (GAP-12): "transactions" = antal BILAG (kan
        # være < rows, når flere linjer deler en bilagsnøgle); "lines" =
        # samlet linjeantal, som altid er 1:1 med "rows" (grupperingen
        # ændrer aldrig antallet af linjer, kun hvordan de er pakket).
        "transactions": len(transactions),
        "lines": len(rows),
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
        "suppliers": [],  # KENDT GAB (GAP-11, uændret): ingen leverandørstamdata-fil i dag.
        "customers": [],  # GAP-11 (delvist lukket nedenfor, hvis customers.csv findes ved
                          # siden af CSV'en — se canonical_masterdata.enrich_canonical).
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

    # Del C (byggetrin 8, Bal-godkendt 2026-09-17): tre valgfrie kanoniske
    # stamdata-sidecar-filer (vat_setup.csv/chart_of_accounts.csv/
    # customers.csv), auto-opdaget ved siden af CSV'en (eller eksplicit
    # angivet) — samme mønster som transform_summary.json. Ingen af filerne
    # er obligatoriske; fravær giver uændret adfærd (GAP-10/GAP-11 forbliver
    # åbne som hidtil for det datasæt). Se parsers/canonical_masterdata.py.
    stamdata = canonical_masterdata.enrich_canonical(
        canonical, csv_path,
        vat_setup_path=vat_setup_path, chart_of_accounts_path=chart_of_accounts_path,
        customers_path=customers_path,
    )
    info["stamdata"] = stamdata
    info["warnings"].extend(stamdata["advarsler"])

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
