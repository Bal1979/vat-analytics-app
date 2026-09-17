"""
canonical_masterdata.py — TRE valgfrie kanoniske stamdata-sidecar-filer
(byggetrin 8, Del C, BALAI-dataflow-arkitektur.md §7, Bal-godkendt 2026-09-17).

Samme mønster som transform_summary.json (canonical_parser._read_transform_summary):
findes filen ved siden af gl_entries-CSV'en (eller en eksplicit sti er givet),
læses den og beriger den kanoniske struktur EFTER parse_canonical() har bygget
den. Fravær af en fil = uændret adfærd — samme "best-effort, ingen exceptions"-
disciplin som resten af canonical_parser.py. Ingen ændring af rådata
(read-only), samme regel som canonical_parser.py.

    * vat_setup.csv          — momskode-opslag (join-nøgle: ``vat_codes``-
                                strengen, SAMME opake streng som gl_entries'
                                ``vat_codes``-kolonne — splittes/fortolkes
                                aldrig, samme designprincip som
                                canonical_parser.py). Fylder
                                tax_table[].tax_percentage/rate (+
                                description/standard_tax_code/country hvis
                                til stede) OG joiner tax_percentage direkte
                                ind på transactions[].lines[] (samme mønster
                                som data_adapter.adapt_excel_to_saft bruger
                                for Excel-vejen) — så kategori 3 (momssats-
                                validering, kontrol 19-26) får et REELT
                                grundlag på den kanoniske vej, i stedet for
                                det strukturelle 0.0 GAP-10 dokumenterer.
                                Sætter desuden ``header.vat_setup_loaded =
                                True`` og ``tax_table[].setup_matched``
                                (byggetrin 8, Del A, Bal-godkendt
                                2026-09-17): kontrol 19 validerer herefter
                                MOD OPSÆTNINGEN i stedet for den hardkodede
                                0/25-liste, så bevidste delvis-fradragsret-
                                konstruktioner (fx 13,63636 %/5,26316 %) ikke
                                giver falske HØJ-fund — se
                                cat03_vat_rate_validation.py for
                                valideringslogikken og dens begrundelse.
                                Bærer desuden tre balai_extensions-felter pr.
                                kode ind på tax_table[] (§2a, Bal-godkendt
                                2026-09-17, kontrakt v0.4.0):
                                non_deductible_vat_pct/
                                allow_non_deductible_vat (feature 83/BAL-055/
                                §42-fradragsbegrænsning) og
                                vat_calculation_type (deterministisk RC-
                                mekanisme-flag, hærder kontrol 82's DKRC/
                                SERVICE_VAT-mønstergenkendelse) — se
                                load_vat_setup's docstring for kolonnenavne.

    * chart_of_accounts.csv  — kontoplan (join-nøgle: kontonummeret, SAMME
                                streng som gl_entries' ``gl_accounts``-
                                kolonne). Fylder accounts[].account_type/
                                standard_account_id/opening_balance/
                                closing_balance OG joiner account_type/
                                standard_account_id ind på
                                transactions[].lines[] — så
                                vat_rules.is_non_vat_account() (kontrol 80's
                                momsrelevans-scope) kan aktiveres, i stedet
                                for det strukturelle "" GAP-11 dokumenterer.

    * customers.csv          — kundestamdata. Fylder den SELVSTÆNDIGE
                                customers[]-liste (customer_id/name/
                                vat_number/country). KENDT, ÅBEN BEGRÆNSNING
                                (dokumenteret, ikke skjult — se
                                tools/data_contract_data.py GAP-11): den
                                seedede BC/NAV-mapping leverer i dag INGEN
                                kunde-reference-kolonne på gl_entries-
                                rækkerne (intet 'customer_id'/kundenr.-felt
                                at joine linjerne igennem).
                                cat12_ecommerce_special._cust_country()
                                joiner via ``line.get("customer_id")`` eller
                                ``txn.get("customer_id")``, som derfor ALTID
                                er "" på den kanoniske vej i dag —
                                UAFHÆNGIGT af om customers.csv er leveret.
                                customers.csv fylder dermed customers[]-
                                listen STRUKTURELT (klar til brug, samme
                                "nøglesæt-symmetri før værdi-symmetri"-
                                mønster som GAP-08/GAP-09), men aktiverer
                                IKKE kontrol 94-97 alene — det kræver at
                                gl_entries-mappingen (vat-extract, uden for
                                denne opgaves scope) også leverer en kunde-
                                reference pr. linje. Se CHANGELOG.md for en
                                udtrykkelig før/efter-vurdering.

Hver loader er defensiv: en manglende/ugyldig/tom fil giver ALDRIG en
exception, kun en tom berigelse + en advarsel i den liste, der returneres.
En teknisk fejl (fx forkert encoding) må aldrig fremstå som et fagligt
resultat — samme regel som reconciliation_gate.py og analytics/
vat_declarations.py.
"""

from __future__ import annotations

import csv
import os


def _read_rows(path: str) -> list:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _num_or_none(value):
    """Tal, eller None hvis tom/uparsebar — så 'ingen værdi angivet' kan
    skelnes fra 'værdien er faktisk 0' (samme disciplin som ACCOUNT_FIELDS'
    opening_balance/closing_balance i tools/data_contract_data.py)."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _sidecar_path(csv_path: str, filename: str) -> str:
    """Konvention: sidecar-filen ligger ved siden af gl_entries-CSV'en (samme
    mønster som canonical_parser._default_summary_path bruger for
    transform_summary.json)."""
    return os.path.join(os.path.dirname(os.path.abspath(csv_path)), filename)


# --- vat_setup.csv -----------------------------------------------------------

def load_vat_setup(path: str) -> tuple:
    """Læs vat_setup.csv -> ({vat_codes_streng: {tax_percentage, description,
    standard_tax_code, country, non_deductible_vat_pct,
    allow_non_deductible_vat, vat_calculation_type}}, advarsler). Manglende
    fil -> ({}, []) (helt stille — filen er valgfri, jf. opgavens Del C).

    De tre sidste felter er balai_extensions (§2a, Bal-godkendt 2026-09-17,
    kontrakt v0.4.0) og læses fra vat-extracts "ext_"-præfikserede
    transform-kolonner (``ext_non_deductible_vat_pct``/
    ``ext_allow_non_deductible_vat``/``ext_vat_calculation_type`` — jf.
    vat-extract/tools/seed_master_data_mappings.py), med et upræfikset
    fallback for robusthed mod en anden mapping-variant (samme mønster som
    load_customers). Værdierne bæres RÅT (trimmet, ikke normaliseret) —
    fx "Allow"/"Do Not Allow" hhv. "Normal VAT"/"Reverse Charge VAT"/
    "Full VAT" fra BC/NAV. non_deductible_vat_pct er None (ikke 0.0) når
    kolonnen mangler/er tom, så "fuld fradragsret (0%)" kan skelnes fra
    "intet signal".

    ALIAS-BUGFIX (2026-09-17): ``description`` blev tidligere KUN læst fra
    en upræfikset ``description``-kolonne, men vat-extracts reelle
    transform-output navngiver den ``ext_description`` (samme
    "ext_"-præfiks-konvention som de tre §2a-kolonner ovenfor) — så
    kodebeskrivelsen var strukturelt altid tom på den kanoniske vej.
    Læser nu ``ext_description`` FØRST, med upræfikset ``description`` som
    fallback (samme retningslinje som resten af loaderen: accepter begge
    navne, ingen adfærdsændring for en fil der allerede brugte det
    upræfiksede navn)."""
    if not path or not os.path.exists(path):
        return {}, []
    try:
        rows = _read_rows(path)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return {}, [f"vat_setup.csv kunne ikke læses: {e}"]

    lookup = {}
    for row in rows:
        code = (row.get("vat_codes") or "").strip()
        if not code:
            continue
        pct = _num_or_none(row.get("tax_percentage"))
        nd_pct = _num_or_none(row.get("ext_non_deductible_vat_pct"))
        if nd_pct is None:
            nd_pct = _num_or_none(row.get("non_deductible_vat_pct"))
        lookup[code] = {
            "tax_percentage": pct if pct is not None else 0.0,
            "description": (row.get("ext_description") or row.get("description") or "").strip(),
            "standard_tax_code": (row.get("standard_tax_code") or "").strip(),
            "country": (row.get("country") or "").strip(),
            "non_deductible_vat_pct": nd_pct,
            "allow_non_deductible_vat": (row.get("ext_allow_non_deductible_vat")
                                          or row.get("allow_non_deductible_vat")
                                          or "").strip(),
            "vat_calculation_type": (row.get("ext_vat_calculation_type")
                                      or row.get("vat_calculation_type")
                                      or "").strip(),
        }
    warnings = []
    if not lookup:
        warnings.append("vat_setup.csv fundet, men indeholdt ingen gyldige rækker "
                        "(mangler 'vat_codes'-kolonnen, eller den er tom overalt?).")
    return lookup, warnings


# --- chart_of_accounts.csv ----------------------------------------------------

def load_chart_of_accounts(path: str) -> tuple:
    """Læs chart_of_accounts.csv -> ({gl_accounts_nummer: {account_type,
    standard_account_id, opening_balance, closing_balance, description}},
    advarsler). Manglende fil -> ({}, [])

    ALIAS-BUGFIX (2026-09-17): kontonavnet blev tidligere KUN læst fra en
    upræfikset ``description``-kolonne, men vat-extracts reelle
    transform-output navngiver den ``ext_name`` (samme
    "ext_"-præfiks-konvention som vat_setup.csv's ``ext_description``) — så
    kontonavnet var strukturelt altid tomt på den kanoniske vej. Læser nu
    ``ext_name`` FØRST, med upræfikset ``description``/``name`` som fallback
    (accepter begge navne, ingen adfærdsændring for en fil der allerede
    brugte et af de upræfiksede navne)."""
    if not path or not os.path.exists(path):
        return {}, []
    try:
        rows = _read_rows(path)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return {}, [f"chart_of_accounts.csv kunne ikke læses: {e}"]

    lookup = {}
    for row in rows:
        acc = (row.get("gl_accounts") or "").strip()
        if not acc:
            continue
        lookup[acc] = {
            "account_type": (row.get("account_type") or "").strip(),
            "standard_account_id": (row.get("standard_account_id") or "").strip(),
            "opening_balance": _num_or_none(row.get("opening_balance")),
            "closing_balance": _num_or_none(row.get("closing_balance")),
            "description": (row.get("ext_name") or row.get("description")
                             or row.get("name") or "").strip(),
        }
    warnings = []
    if not lookup:
        warnings.append("chart_of_accounts.csv fundet, men indeholdt ingen gyldige rækker "
                        "(mangler 'gl_accounts'-kolonnen, eller den er tom overalt?).")
    return lookup, warnings


# --- customers.csv -------------------------------------------------------------

def load_customers(path: str) -> tuple:
    """Læs customers.csv -> ([{customer_id, name, vat_number, country}, ...],
    advarsler). Manglende fil -> ([], []).

    Kolonnenavne accepteres i to varianter (samme værdi, forskellig kilde-
    navngivning) — bekræftet mod vat-extracts reelle transform-output
    (byggetrin 8/Del C E2E, 2026-09-17): dedikerede "ext_"-præfikserede
    target_object-felter (``ext_customer_id``/``ext_customer_name``) for
    kunde-id/-navn, der ikke i forvejen er canonical-navngivning i
    analytics_mapping, OG ``counterparty_country`` (allerede en genkendt
    canonical kolonne, samme navn som på gl_entries-rækkerne) for land. En
    simplere ``customer_id``/``name``/``country``-navngivning understøttes
    også, for robusthed mod en anden mapping-variant. BEVIDST IKKE
    understøttet: at bruge ``ext_vat_bus_posting_group`` (en BC-postgruppe-
    kode som "DOMESTIC"/"EU"/"OUTSIDE DK/EU", IKKE et momsnummer) som
    ``vat_number`` — det ville være en misvisende værdi, ikke en reel
    momsnummer-oplysning. ``vat_number`` er derfor kun udfyldt, hvis en
    ægte ``vat_number``-kolonne findes.

    KENDT BEGRÆNSNING (se modulets docstring): listen kan i dag ikke joines
    til transactions[].lines[]/txn på den kanoniske vej — gl_entries bærer
    ingen customer_id-kolonne i den seedede BC/NAV-mapping."""
    if not path or not os.path.exists(path):
        return [], []
    try:
        rows = _read_rows(path)
    except (OSError, UnicodeDecodeError, csv.Error) as e:
        return [], [f"customers.csv kunne ikke læses: {e}"]

    customers = []
    for row in rows:
        cust_id = (row.get("customer_id") or row.get("ext_customer_id") or "").strip()
        if not cust_id:
            continue
        name = (row.get("name") or row.get("ext_customer_name") or "").strip()
        country = (row.get("country") or row.get("counterparty_country") or "").strip()
        customers.append({
            "customer_id": cust_id,
            "name": name,
            "vat_number": (row.get("vat_number") or "").strip(),
            "country": country,
        })
    warnings = []
    if not customers:
        warnings.append("customers.csv fundet, men indeholdt ingen gyldige rækker "
                        "(mangler 'customer_id'/'ext_customer_id'-kolonnen, eller "
                        "den er tom overalt?).")
    return customers, warnings


# --- Berigelse -----------------------------------------------------------------

def enrich_canonical(canonical: dict, csv_path: str,
                      vat_setup_path: str = None,
                      chart_of_accounts_path: str = None,
                      customers_path: str = None) -> dict:
    """Berig en allerede parset kanonisk struktur (parse_canonical()'s output)
    med de tre valgfrie stamdata-sidecar-filer. Modificerer ``canonical`` IN
    PLACE (parse først, berig bagefter — samme livscyklus som lineage-
    stemplingen) og returnerer et diagnostik-dict (antal matchede
    koder/konti/kunder + advarsler — INGEN kundedata-tekstværdier).

    Stier: eksplicit angivet, ELLER (default) samme mappe som ``csv_path``
    under de aftalte filnavne — samme konvention som transform_summary.json.
    Fravær af én fil ændrer intet for de øvrige (uafhængige berigelser)."""
    vat_setup_path = vat_setup_path or _sidecar_path(csv_path, "vat_setup.csv")
    coa_path = chart_of_accounts_path or _sidecar_path(csv_path, "chart_of_accounts.csv")
    customers_path = customers_path or _sidecar_path(csv_path, "customers.csv")

    vat_lookup, w1 = load_vat_setup(vat_setup_path)
    coa_lookup, w2 = load_chart_of_accounts(coa_path)
    customers, w3 = load_customers(customers_path)
    warnings = [*w1, *w2, *w3]

    if vat_lookup:
        # Kontrol 19-signal (byggetrin 8, Del A, Bal-godkendt 2026-09-17):
        # ``header.vat_setup_loaded`` fortæller kontrol 19 (cat03_vat_rate_
        # validation.test_19_invalid_rate), at kundens EGEN vat_setup er til
        # stede, og at satsen derfor skal valideres mod OPSÆTNINGEN (pr.
        # momskode) i stedet for den hardkodede 0/25-liste. Sat uafhængigt af
        # om DENNE linjes kode faktisk matcher et opslag -- det er netop
        # meningen: en kode der ikke findes i opsætningen, er selv et fund
        # ("ukendt kode"), ikke en grund til at falde tilbage til 0/25.
        canonical.setdefault("header", {})["vat_setup_loaded"] = True
        for entry in canonical.get("tax_table", []):
            info = vat_lookup.get(entry.get("tax_code", ""))
            # setup_matched: True hvis DENNE kode findes i vat_setup.csv.
            # Skelner "kendt kode, sats 0%" fra "kode ukendt i opsætning" --
            # begge ville ellers have tax_percentage==0.0 (den strukturelle
            # default) og være umulige at skelne for kontrol 19.
            entry["setup_matched"] = info is not None
            if not info:
                continue
            entry["tax_percentage"] = info["tax_percentage"]
            entry["rate"] = info["tax_percentage"]
            if info["description"]:
                entry["description"] = info["description"]
            if info["standard_tax_code"]:
                entry["standard_tax_code"] = info["standard_tax_code"]
            if info["country"]:
                entry["country"] = info["country"]
            # balai_extensions (§2a, Bal-godkendt 2026-09-17, kontrakt
            # v0.4.0): pr.-kode-konfiguration. Sættes ubetinget for MATCHEDE
            # koder (også når værdien er tom/None -- det ER signalet fra
            # opsætningen); umatchede koder beholder parserens defaults
            # (None/""), jf. setup_matched-skelnen ovenfor.
            entry["non_deductible_vat_pct"] = info["non_deductible_vat_pct"]
            entry["allow_non_deductible_vat"] = info["allow_non_deductible_vat"]
            entry["vat_calculation_type"] = info["vat_calculation_type"]
        for txn in canonical.get("transactions", []):
            for line in txn.get("lines", []):
                info = vat_lookup.get(line.get("tax_code", ""))
                if info:
                    line["tax_percentage"] = info["tax_percentage"]
                    # balai_extensions (kontrakt v0.4.0): vat_calculation_type
                    # joines nu OGSÅ ned på linjeniveau (ikke kun tax_table),
                    # fordi kontrol 82 (cat10._purchase_rubric) klassificerer
                    # rubrik PR. LINJE og har derfor brug for feltet dér —
                    # se opgavens Del A, punkt 2, Bal-godkendt 2026-09-17.
                    line["vat_calculation_type"] = info["vat_calculation_type"]

    if coa_lookup:
        for acc in canonical.get("accounts", []):
            info = coa_lookup.get(acc.get("account_id", ""))
            if not info:
                continue
            if info["account_type"]:
                acc["account_type"] = info["account_type"]
            if info["standard_account_id"]:
                acc["standard_account_id"] = info["standard_account_id"]
            if info["opening_balance"] is not None:
                acc["opening_balance"] = info["opening_balance"]
            if info["closing_balance"] is not None:
                acc["closing_balance"] = info["closing_balance"]
            if info["description"]:
                acc["description"] = info["description"]
        for txn in canonical.get("transactions", []):
            for line in txn.get("lines", []):
                info = coa_lookup.get(line.get("account_id", ""))
                if not info:
                    continue
                if info["account_type"]:
                    line["account_type"] = info["account_type"]
                if info["standard_account_id"]:
                    line["standard_account_id"] = info["standard_account_id"]

    if customers:
        canonical["customers"] = customers

    return {
        "vat_setup_koder": len(vat_lookup),
        "chart_of_accounts_konti": len(coa_lookup),
        "customers": len(customers),
        "advarsler": warnings,
    }
