"""
data_contract_data.py — single source for VAT Analytics' maskinlæsbare
inputkontrakt (den kanoniske struktur analysemotoren kører på).

Baggrund
--------
Motorens inputkontrakt lever i dag i fire usynkroniserede kilder:
    1. parsers/data_adapter.py       (docstring + kode — felterne i txn/lines)
    2. parsers/excel_parser.py       (COLUMN_ALIASES — hvilke kildekolonner
                                       mapper til hvilket kanonisk felt)
    3. parsers/upload_router.py      (docstring-kontrakten på top-niveau)
    4. analytics/readiness.py        (FIELD_INFO/CATEGORY_REQUIREMENTS —
                                       hvilke kontroller kræver hvilke felter)
Ingen af dem håndhæver noget (ingen JSON Schema/pydantic). Dette modul er
ÉN samlet, hånd-vedligeholdt beskrivelse af hele kontrakten — analogt til
``catalog/rule_notes.json`` som sidecar for regelkataloget. Det er bevidst
DESKRIPTIVT, ikke håndhævende: ingen runtime-validering indføres her (jf.
BALAI-dataflow-arkitektur.md §7, byggetrin 2). ``tools/build_data_contract.py``
læser dette modul (samt ``analytics/modules.py`` og ``analytics/materiality.py``
for run_config, så de tal aldrig duplikeres i hånden) og skriver
``catalog/data_contract.json``.

Rediger IKKE catalog/data_contract.json i hånden — kør
``python tools/build_data_contract.py``. Drift er CI-gated
(``tests/test_data_contract_fresh.py``), ligesom regelkataloget.

Konventioner pr. felt
----------------------
    navn            — feltnavn i den kanoniske struktur
    type            — "string" | "number" | "int" | "bool" | "object" | "list[...]"
    obligatorisk     — True/False (til stede i den kanoniske struktur — IKKE det
                       samme som "aldrig tom"; mange felter er obligatoriske i
                       strukturen men lovligt tomme/0 når kilden ikke har data)
    format          — frit tekstformat/gyldige værdier, "" hvis ikke relevant
    status          — "implemented" | "implemented_partial" | "planned"
    kilder          — dict {excel: bool|"partial", saft: bool|"partial"} — er
                       feltet reelt udfyldt fra hver input-vej i dag?
    ekstension      — True hvis feltet er en ``balai_extensions``-udvidelse
                       (BALAI-dataflow-arkitektur.md §2a) — dvs. det er IKKE et
                       nativt SAF-T Financial-felt, motoren kræver det alligevel.
    kraeves_af      — fritekst: hvilke kontrol-id'er/kategorier/features feltet
                       låser op, krydset mod analytics/readiness.py + kode-gennemgang
                       af analytics/categories/cat*.py
    noter           — kendte skævheder/forbehold specifikt for feltet
"""

from __future__ import annotations

CONTRACT_VERSION = "0.1.0"

# ---------------------------------------------------------------------------
# 1. HEADER
# ---------------------------------------------------------------------------

HEADER_FIELDS = [
    {
        "navn": "company_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol låser sig op af feltet i dag; vises i UI/rapport-header.",
        "noter": "Excel-vejen sætter altid \"\" (ingen kolonne-alias findes for "
                 "virksomhedsnavn) — kun SAF-T-vejen (Company/Name) udfylder det.",
    },
    {
        "navn": "registration_number", "type": "string", "obligatorisk": False, "format": "CVR (8 cifre)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False},
        "ekstension": False,
        "kraeves_af": "Ingen — nøglen findes kun i Excel-vejens header-dict og er altid \"\".",
        "noter": "UOVERENSSTEMMELSE: nøglen findes slet ikke i SAF-T-vejens header-dict "
                 "(saft_parser.parse_saft), men er altid til stede (tom) i Excel-vejens. "
                 "Reelt dødt felt i dag på begge veje.",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217, fx \"DKK\"",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True},
        "ekstension": False,
        "kraeves_af": "summary.currency; ingen kontrol læser header.currency direkte.",
        "noter": "Excel-vejen sætter en fast standardværdi \"DKK\" — læses ikke fra en "
                 "kildekolonne (kun linje-niveau currency er kolonne-drevet).",
    },
    {
        "navn": "period_start", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True},
        "ekstension": False,
        "kraeves_af": "summary.period_start; grundlag for header.period (kontrol 9).",
        "noter": "Excel: min(dato) over transaktionerne. SAF-T: "
                 "Header/SelectionCriteria/SelectionStartDate.",
    },
    {
        "navn": "period_end", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True},
        "ekstension": False,
        "kraeves_af": "summary.period_end; grundlag for header.period (kontrol 9).",
        "noter": "Excel: max(dato). SAF-T: SelectionCriteria/SelectionEndDate.",
    },
    {
        "navn": "period", "type": "object {start, start_year, end, end_year}",
        "obligatorisk": False, "format": "start/end: måned uden padding (\"3\"); *_year: \"YYYY\"",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True},
        "ekstension": False,
        "kraeves_af": "Kontrol 9 (tax point / periode-check, kategori 5).",
        "noter": "Afledt/beregnet i begge parsere, kun når period_start OG period_end "
                 "begge er udfyldt. Findes ikke som selvstændig kildekolonne.",
    },
    {
        "navn": "source", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented",
        "kilder": {"excel": True, "saft": False},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol — diagnostik/UI (\"Excel/CSV import\").",
        "noter": "Findes kun i Excel-vejens header — ingen tilsvarende nøgle i SAF-T-vejen.",
    },
    {
        "navn": "saft_version", "type": "string", "obligatorisk": False, "format": "\"1.0\"|\"2.0\"|\"2.1\" (selvangivet)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True},
        "ekstension": True,
        "kraeves_af": "Del af balai_extensions-versionstriplen (se BALAI_EXTENSIONS "
                       "nedenfor) — endnu ikke konsumeret af nogen kontrol.",
        "noter": "Svarer til SAF-T's native, selvangivne AuditFileVersion — det er "
                 "netop dette ENE felt, BAL-004/BAL-005-erfaringen viser kan være "
                 "forkert (Danoil/Føniksbyen erklærede v1.0, indeholdt v2.0-data). "
                 "Findes slet ikke i Excel-vejens header.",
    },
]

# ---------------------------------------------------------------------------
# 2. ACCOUNTS (liste)
# ---------------------------------------------------------------------------

ACCOUNT_FIELDS = [
    {
        "navn": "account_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Nøgle til linje-join (account_type/standard_account_id-opslag) i "
                       "data_adapter/saft_parser; kategori 2 (test_15, Asset-filter).",
        "noter": "",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst i rapport.",
        "noter": "",
    },
    {
        "navn": "account_type", "type": "string", "obligatorisk": False,
        "format": "SAF-T AccountType-enum, fx \"Asset\"|\"Liability\"|\"Equity\"|\"Revenue\"|\"Expense\"|\"Other\"",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 1 -> kontrol 80 "
                       "(revenue_without_output_vat); kategori 2 test_15 "
                       "(duplicate_payment, filtrerer \"Asset\"-konti).",
        "noter": "Excel-vejen sætter ALTID \"\" — der findes ingen COLUMN_ALIASES-nøgle "
                 "for kontotype, så feltet kan ikke udfyldes selv hvis kildefilen har "
                 "en kolonne til det. Dokumenteret, tilsigtet adfærd (CLAUDE.md): "
                 "ukendt kontotype -> uændret/konservativ scope.",
    },
    {
        "navn": "standard_account_id", "type": "string", "obligatorisk": False,
        "format": "ERST-standardkontoplan-kode, fx \"5800\" (resultat: 1000-4999, balance: >=5000)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 2 (robust scope) via "
                       "standard_accounts.account_nature() -> kontrol 80 + hele "
                       "momskernens balancekonto-udelukkelse.",
        "noter": "Excel-vejens accounts[]-dict har slet ikke denne nøgle (ikke engang "
                 "tom streng) — kun SAF-T-vejen bærer den. Native SAF-T-element "
                 "(StandardAccountID), IKKE en balai_extension.",
    },
    {
        "navn": "opening_balance", "type": "number", "obligatorisk": False, "format": "DKK, kan være negativ",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag.",
        "noter": "Excel-vejen sætter altid 0.0 (ingen kolonne-alias for åbningssaldo).",
    },
    {
        "navn": "closing_balance", "type": "number", "obligatorisk": False, "format": "DKK, kan være negativ",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "Kontrol 77 (vat_account_reconciliation, kategori 10) — "
                       "afstemmer beregnet moms mod kontoens closing_balance.",
        "noter": "VIGTIGT: Excel-vejen sætter altid 0.0 (ingen kolonne-alias). "
                 "Kontrol 77 kører derfor på Excel-import mod en KONSTANT nulsaldo "
                 "for alle momskonti — se known_gaps.",
    },
]

# ---------------------------------------------------------------------------
# 3. TAX_TABLE (liste)
# ---------------------------------------------------------------------------

TAX_TABLE_FIELDS = [
    {
        "navn": "tax_code", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering, 19-26); kategori 9 (reverse "
                       "charge, 70-75); kategori 10 (afstemning, 76-83); readiness "
                       "signal-felt for kategori 3/9/10.",
        "noter": "",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "Excel: syntetisk \"Momskode {code}\" hvis ingen beskrivelseskolonne. "
                 "SAF-T: nativ TaxTableEntry/TaxCodeDetails/Description.",
    },
    {
        "navn": "tax_percentage", "type": "number", "obligatorisk": True, "format": "procent, fx 25.0",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering); readiness FIELD_INFO.",
        "noter": "Nøglenavnet motoren forventer. For Excel-oprindelse skrives det af "
                 "data_adapter fra 'rate' hvis 'tax_percentage' mangler (se 'rate' "
                 "nedenfor); for SAF-T-oprindelse er det nativt.",
    },
    {
        "navn": "rate", "type": "number", "obligatorisk": False, "format": "procent, fx 25.0",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte (kun bro-felt til tax_percentage).",
        "noter": "UOVERENSSTEMMELSE: kun til stede på Excel-afledte tax_table-poster "
                 "(excel_parser's rå output-nøgle, bevaret af data_adapter ved siden "
                 "af tax_percentage). SAF-T-poster har aldrig denne nøgle. Bør ikke "
                 "bruges af ny kode — brug tax_percentage.",
    },
    {
        "navn": "standard_tax_code", "type": "string", "obligatorisk": False,
        "format": "Skattestyrelsens StandardTaxCode (jf. tax_codes.py-katalog)",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag (potentiel bro til feature 82, rubrik-afstemning).",
        "noter": "Findes kun for SAF-T-oprindelse (nativ StandardTaxCode).",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag — relevant for udenlandske momskoder.",
        "noter": "Findes kun for SAF-T-oprindelse (nativ TaxTableEntry/Country).",
    },
]

# ---------------------------------------------------------------------------
# 4. TRANSACTIONS (liste) — top-niveau
# ---------------------------------------------------------------------------

TRANSACTION_FIELDS = [
    {
        "navn": "transaction_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 2 (dubletdetektion, 11-18) som transaktions-nøgle; "
                       "gennemgående som finding-reference i alle kategorier.",
        "noter": "Excel-vejen falder tilbage til \"ROW-{n}\" (n=idx+2) hvis kilden "
                 "mangler en id-kolonne; SAF-T-vejen har intet fallback (nativt "
                 "TransactionID kan i teorien være tomt).",
    },
    {
        "navn": "date", "type": "string", "obligatorisk": True, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing & periodetest, 39-46); period/period_year-udledning.",
        "noter": "Bogføringsdato. SAF-T: GLPostingDate (fallback TransactionDate).",
    },
    {
        "navn": "document_date", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": True,
        "kraeves_af": "Kontrol 46 (faktura-/bogføringslag, kategori 5).",
        "noter": "balai_extension pr. §2a — begrundelse: SAF-T-eksporter bærer ikke "
                 "altid dato-lagdelingen konsistent, selvom feltet teknisk kan "
                 "udledes af native TransactionDate. SAF-T-vejen mapper "
                 "document_date=TransactionDate, date=GLPostingDate (falder sammen "
                 "hvis kilden kun har én dato).",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst i finding-tekster.",
        "noter": "",
    },
    {
        "navn": "journal_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst/reference.",
        "noter": "Default \"IMPORT\" (Excel) / \"GL\" (SAF-T) hvis kilden ikke har et journal-id.",
    },
    {
        "navn": "period", "type": "string", "obligatorisk": False, "format": "\"1\"-\"12\", zero-padded i Excel-vejen",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing/periodetest).",
        "noter": "Excel: udledt af date hvis ikke allerede sat, zero-padded (\"03\"). "
                 "SAF-T: udledt af posteringsmåneden (IKKE SAF-T's regnskabsperiode-"
                 "felt <Period>, som er en løbenummer-periode, ikke kalendermåned).",
    },
    {
        "navn": "period_year", "type": "string", "obligatorisk": False, "format": "\"YYYY\"",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing/periodetest).",
        "noter": "",
    },
    {
        "navn": "total_debit", "type": "number", "obligatorisk": True, "format": "DKK, sum af linjernes debit_amount",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "summary.total_debit (kun Excel-vejen, se summary-uoverensstemmelse); "
                       "diverse beløbskontroller i kategori 7/10.",
        "noter": "",
    },
    {
        "navn": "total_credit", "type": "number", "obligatorisk": True, "format": "DKK, sum af linjernes credit_amount",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Samme som total_debit.",
        "noter": "",
    },
    {
        "navn": "lines", "type": "list[line]", "obligatorisk": True, "format": "mindst ét element i praksis",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Alle 103 kontroller itererer transactions[].lines[].",
        "noter": "Excel-vejen pakker altid PRÆCIS én linje pr. flad kildereække "
                 "(fladt udtræk har ingen bilagsstruktur); SAF-T-vejen bærer "
                 "native multi-linje-bilag. Se LINE_FIELDS.",
    },
]

# ---------------------------------------------------------------------------
# 5. LINE (transactions[].lines[])
# ---------------------------------------------------------------------------

LINE_FIELDS = [
    {
        "navn": "record_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — finding-reference.",
        "noter": "Excel: syntetisk \"L{n}\" (altid præcis én linje). SAF-T: nativt "
                 "RecordID eller syntetisk fallback.",
    },
    {
        "navn": "account_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Join til accounts[] (account_type/standard_account_id).",
        "noter": "",
    },
    {
        "navn": "account_type", "type": "string", "obligatorisk": False, "format": "se ACCOUNT_FIELDS",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 1 -> kontrol 80.",
        "noter": "Joinet fra accounts[].account_type via account_id — samme "
                 "Excel-begrænsning som accounts[].account_type.",
    },
    {
        "navn": "standard_account_id", "type": "string", "obligatorisk": False, "format": "se ACCOUNT_FIELDS",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 2 -> kontrol 80 + momskerne-scope.",
        "noter": "Excel-vejens linje-dict har slet ikke denne nøgle (data_adapter "
                 "sætter den aldrig — heller ikke som tom streng). "
                 "line.get(\"standard_account_id\") returnerer None, ikke \"\".",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "",
    },
    {
        "navn": "debit_amount", "type": "number", "obligatorisk": True, "format": "DKK, >= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Stort set alle 103 kontroller (beløbsgrundlag).",
        "noter": "KENDT GAB (uden for scope her): data_adapter bruger "
                 "'txn.get(\"debit_amount\", 0.0) or 0.0' — en reel, men falsk "
                 "0-værdi i kilden bliver umulig at skelne fra 'ingen kolonne'. "
                 "Se known_gaps.",
    },
    {
        "navn": "credit_amount", "type": "number", "obligatorisk": True, "format": "DKK, >= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Samme som debit_amount.",
        "noter": "Samme 'or 0.0'-fallback-gab som debit_amount. Se known_gaps.",
    },
    {
        "navn": "tax_code", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3, 9, 10 (readiness signal-felt).",
        "noter": "",
    },
    {
        "navn": "tax_percentage", "type": "number", "obligatorisk": False, "format": "procent",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering) — readiness FIELD_INFO.",
        "noter": "",
    },
    {
        "navn": "tax_base", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3 (satskontrol mod grundlag); feature 83 "
                       "(fremtidig fradragsbrøk-beregning).",
        "noter": "KENDT GAB (uden for scope her): tre-trins fallback i "
                 "data_adapter (importeret grundlag > udledt af vat/rate > "
                 "debit+credit) kan stille et beregnet tal, der ikke er kildens "
                 "faktiske momsgrundlag, uden at det er synligt i outputtet. "
                 "Se known_gaps.",
    },
    {
        "navn": "tax_amount", "type": "number", "obligatorisk": True, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 3, 9, 10 — momsbeløb er kernen i afstemningen.",
        "noter": "KENDT GAB (uden for scope her): 'txn.get(\"vat_amount\") or 0.0' "
                 "kan ikke skelne '0 kr. moms' fra 'momsbeløb ukendt'. Se known_gaps.",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217, default \"DKK\"",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kontrol 33 (currency/country-konsistens, kategori 4).",
        "noter": "",
    },
    {
        "navn": "supplier_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 4 (EU/grænseoverskridende), 6 (parts-validering), "
                       "9 (reverse charge), 11 (MTIC) — join til suppliers[].",
        "noter": "",
    },
    {
        "navn": "supplier_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kontrol 47 (manglende partsnavn, kategori 6).",
        "noter": "",
    },
    {
        "navn": "customer_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 12 (e-handel) — join til customers[]; kategori 6.",
        "noter": "",
    },
    {
        "navn": "customer_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kontrol 47 (manglende partsnavn), kontrol 52 (kunde uden moms-nr).",
        "noter": "",
    },
    {
        "navn": "source_document_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": "partial"}, "ekstension": False,
        "kraeves_af": "Kontrol 2/kategori 2 (dubletdetektion, 11-18) — readiness "
                       "signal-felt CATEGORY_REQUIREMENTS[2].",
        "noter": "VIGTIGT MODELLERINGS-GAB: for Excel-oprindelse er det den reelle "
                 "fakturanummer-kolonne (invoice_number). For SAF-T-oprindelse "
                 "sætter saft_parser den til transaktionens FRITEKST-beskrivelse "
                 "(Transaction/Description) — IKKE et fakturanummer, fordi SAF-T "
                 "Financial GeneralLedgerEntries ikke bærer et selvstændigt "
                 "dokument-id på transaktionsniveau. Dubletdetektion på "
                 "SAF-T-oprindelse dedupliker derfor reelt på fritekst, et "
                 "svagere signal end på Excel-oprindelse. Se known_gaps.",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2 eller landenavn",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 4 (grænseoverskridende/EU), 9, 11, 12 — readiness "
                       "signal-felt for kategori 4/9/11/12.",
        "noter": "Modpartens land (ikke vareflow — se ship_from/ship_to_country).",
    },
    {
        "navn": "ship_from_country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": True,
        "kraeves_af": "Kontrol 36 (triangulation/place-of-supply, kategori 4) — "
                       "CONTROL_REQUIREMENTS[36] i readiness.py.",
        "noter": "balai_extension pr. §2a. SAF-T Financial's MovementOfGoods er "
                 "valgfri og typisk tom i ERP-eksporten — saft_parser sætter "
                 "feltet til \"\" ubetinget i dag (kommentar i koden: "
                 "\"ikke i SAF-T Financial\"). Skal på sigt injiceres fra "
                 "objekt-modellens lager-/leverance-data (vat-extract).",
    },
    {
        "navn": "ship_to_country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": True,
        "kraeves_af": "Kontrol 36 (triangulation/place-of-supply, kategori 4).",
        "noter": "Samme status som ship_from_country.",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat (VIES-lignende, ikke tjek-ciffer-valideret her)",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Kategori 4 (EU-momsnummerformat), kategori 6 (leverandør/"
                       "kunde-validering, inkl. kontrol 49 CONTROL_REQUIREMENTS) — "
                       "readiness signal-felt.",
        "noter": "",
    },
    {
        "navn": "non_deductible_amount", "type": "number (nullable)", "obligatorisk": False, "format": "DKK",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": True,
        "kraeves_af": "Feature 83 (delvis fradragsret — pt. registreret inaktiv/"
                       "kræver eksterne data i readiness.EXTERNAL_DATA); BAL-055 "
                       "(SAF-T Validator-siden af samme felt).",
        "noter": "balai_extension pr. §2a. Findes KUN via SAF-T-vejen "
                 "(TaxInformation/Deductibles/NonDeductibleAmount, kan være None "
                 "selv der hvis Deductibles-elementet mangler). Excel-vejens "
                 "linje-dict har slet ikke denne nøgle — ingen kolonne-alias, "
                 "ingen adapter-håndtering. Reel funktionsforskel mellem "
                 "input-vejene, ikke kun en dokumentationsdetalje.",
    },
]

# ---------------------------------------------------------------------------
# 6. SUPPLIERS (liste)
# ---------------------------------------------------------------------------

SUPPLIER_FIELDS = [
    {
        "navn": "supplier_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Join-nøgle fra lines[].supplier_id i kategori 4/6/9/11.",
        "noter": "",
    },
    {
        "navn": "name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "cat04._vat_of() fallback (linje-niveau har forrang); "
                       "kategori 6/9/11 supplier_lookup.",
        "noter": "",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "cat04._country_of() fallback (linje-niveau har forrang).",
        "noter": "Excel-vejen udleder leverandørens land fra transaktionens "
                 "generiske 'country'-kolonne (kan være upræcist ved flere parter "
                 "på samme række, men rammer normalt rigtigt i et fladt udtræk).",
    },
]

# ---------------------------------------------------------------------------
# 7. CUSTOMERS (liste)
# ---------------------------------------------------------------------------

CUSTOMER_FIELDS = [
    {
        "navn": "customer_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Join-nøgle fra lines[].customer_id i kategori 12.",
        "noter": "",
    },
    {
        "navn": "name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "customer_lookup i cat04 (via customers[]) og cat12 — men se note.",
        "noter": "VIGTIGT GAB: excel_parser hårdkoder customer_info[\"vat_number\"]=\"\" "
                 "ALTID (parsers/excel_parser.py, _process_row) — modsat "
                 "supplier_info, hvor vat_number udfyldes. For Excel-oprindelse "
                 "er kundens moms-nr. derfor strukturelt utilgængeligt via "
                 "customers[], uanset om kildefilen har kolonnen. (Linje-niveau "
                 "vat_number, brugt af de fleste kontroller, er upåvirket.)",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True}, "ekstension": False,
        "kraeves_af": "cat12._cust_country() — kontrol 94/95/96/97 (e-handel/OSS/"
                       "fjernsalg/digitale ydelser, kategori 12) læser UDELUKKENDE "
                       "customers[].country, uden linje-niveau-fallback.",
        "noter": "VIGTIGT GAB, samme rodårsag som vat_number ovenfor: "
                 "excel_parser hårdkoder customer_info[\"country\"]=\"\" ALTID. "
                 "Konsekvens: kontrol 94/95/96/97 kan STRUKTURELT ALDRIG finde en "
                 "kundes land på Excel/CSV-oprindelse — selv med en udfyldt "
                 "'Land'-kolonne i kilden — fordi _cust_country() ikke falder "
                 "tilbage til lines[].country. Modulet 'ehandel_saerordninger' er "
                 "default FRA, så det er ikke synligt i en standardkørsel, men "
                 "bider hvis modulet tændes på et Excel-udtræk.",
    },
]

# ---------------------------------------------------------------------------
# 8. SUMMARY
# ---------------------------------------------------------------------------

SUMMARY_FIELDS = [
    {
        "navn": "total_transactions", "type": "int", "obligatorisk": True, "format": ">= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Readiness _MIN_TX_FOR_STATISTIK-tærskel (kategori 8).",
        "noter": "",
    },
    {
        "navn": "total_debit", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol læser summary.total_debit direkte i dag "
                       "(informativt/rapport-niveau).",
        "noter": "UOVERENSSTEMMELSE: nøglen mangler HELT i saft_parser's summary-dict.",
    },
    {
        "navn": "total_credit", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": False,
        "kraeves_af": "Samme som total_debit.",
        "noter": "UOVERENSSTEMMELSE: nøglen mangler HELT i saft_parser's summary-dict.",
    },
    {
        "navn": "total_vat", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False}, "ekstension": False,
        "kraeves_af": "Samme som total_debit/total_credit.",
        "noter": "UOVERENSSTEMMELSE: nøglen mangler HELT i saft_parser's summary-dict.",
    },
    {
        "navn": "period_start", "type": "string", "obligatorisk": False, "format": "ISO-dato",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — rapport-header.",
        "noter": "",
    },
    {
        "navn": "period_end", "type": "string", "obligatorisk": False, "format": "ISO-dato",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — rapport-header.",
        "noter": "",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217",
        "status": "implemented", "kilder": {"excel": True, "saft": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — rapport-header.",
        "noter": "",
    },
]

# ---------------------------------------------------------------------------
# 9. PARSE_INFO — kun diagnostik/UI, IKKE konsumeret af analysemotoren
# ---------------------------------------------------------------------------

PARSE_INFO_NOTE = (
    "parse_info er en del af upload_router.py's docstring-kontrakt "
    "({parse_info, header, summary, ...}), men grep over analytics/*.py + "
    "analytics/categories/*.py bekræfter at INGEN kontrol/readiness-logik læser "
    "parse_info — det er ren parsing-diagnostik (rækker/kolonner/fejl/advarsler) "
    "vist i UI'et. Optages derfor ikke som et objekt med feltkrav i denne "
    "kontrakt, kun nævnt her for udtømmenhedens skyld (jf. opgavens fire kilder)."
)

# ---------------------------------------------------------------------------
# OBJEKT-REGISTER
# ---------------------------------------------------------------------------

OBJECTS = {
    "header": {
        "beskrivelse": "Fil-/regnskabs-metadata. Ét objekt pr. upload.",
        "felter": HEADER_FIELDS,
    },
    "accounts": {
        "beskrivelse": "Kontoplan (liste). Kan være tom (fladt Excel-udtræk uden kontoplan).",
        "felter": ACCOUNT_FIELDS,
    },
    "tax_table": {
        "beskrivelse": "Momskode-opslag (liste). Kan være tom.",
        "felter": TAX_TABLE_FIELDS,
    },
    "transactions": {
        "beskrivelse": "Transaktioner (liste). Hver transaktion bærer lines[].",
        "felter": TRANSACTION_FIELDS,
        "sub_objekt": {"navn": "lines", "beskrivelse": "Linjer pr. transaktion (liste, min. 1 i praksis).", "felter": LINE_FIELDS},
    },
    "suppliers": {
        "beskrivelse": "Leverandør-stamdata (liste). Kan være tom.",
        "felter": SUPPLIER_FIELDS,
    },
    "customers": {
        "beskrivelse": "Kunde-stamdata (liste). Kan være tom.",
        "felter": CUSTOMER_FIELDS,
    },
    "summary": {
        "beskrivelse": "Beregnede totaler for hele udtrækket. Ét objekt pr. upload.",
        "felter": SUMMARY_FIELDS,
    },
}

# ---------------------------------------------------------------------------
# BALAI_EXTENSIONS — eksplicit §2a-afsnit (BALAI-dataflow-arkitektur.md)
# ---------------------------------------------------------------------------
# Hvert felt her optræder OGSÅ i sit naturlige objekt ovenfor (ekstension=True)
# undtagen version-triplen, som endnu ikke har et fysisk hjem i strukturen.

BALAI_EXTENSIONS = [
    {
        "felt": "ship_from_country", "sti": "transactions[].lines[].ship_from_country",
        "status": "implemented_partial",
        "begrundelse": "Kontrol 36 (place-of-supply/triangulation) — MovementOfGoods "
                        "er valgfri og typisk tom i ERP-eksporten.",
    },
    {
        "felt": "ship_to_country", "sti": "transactions[].lines[].ship_to_country",
        "status": "implemented_partial",
        "begrundelse": "Kontrol 36 (place-of-supply/triangulation), samme som ship_from_country.",
    },
    {
        "felt": "ekstern_indberettet_momsangivelse", "sti": "(intet fysisk hjem endnu)",
        "status": "planned",
        "begrundelse": "Kontrol 82 (afstemning periode vs. angivelse) — findes pr. "
                        "definition ikke i et enkelt-virksomheds-udtræk; kræver "
                        "eksternt input (angivelses-tal). Kontrol 82 er registreret "
                        "i readiness.EXTERNAL_DATA og i modules som bevidst inaktiv.",
    },
    {
        "felt": "ekstern_fradragsbroek", "sti": "(intet fysisk hjem endnu)",
        "status": "planned",
        "begrundelse": "Feature 83 (delvis fradragsret) — fordelingsnøgle "
                        "(omsætningsfordeling) findes ikke i et enkelt-virksomheds-"
                        "udtræk. Se også non_deductible_amount, som er den "
                        "linje-niveau-halvdel af samme feature og ER implementeret "
                        "(kun SAF-T-vejen).",
    },
    {
        "felt": "document_date", "sti": "transactions[].document_date",
        "status": "implemented",
        "begrundelse": "Kontrol 46 (faktura-/bogføringslag) — adskilt fra "
                        "bogføringsdato (date), fordi SAF-T-eksporter ikke altid "
                        "bærer begge datofelter konsistent.",
    },
    {
        "felt": "non_deductible_amount", "sti": "transactions[].lines[].non_deductible_amount",
        "status": "implemented_partial",
        "begrundelse": "Feature 83 (delvis fradragsret) + BAL-055 (SAF-T Validator). "
                        "Implementeret kun via SAF-T-vejen (Deductibles/"
                        "NonDeductibleAmount); Excel-vejen mangler kolonne-alias og "
                        "adapter-håndtering.",
    },
    {
        "felt": "version_erklaeret", "sti": "header.saft_version",
        "status": "implemented_partial",
        "begrundelse": "Version-triplen (BAL-004/BAL-005-erfaring: Danoil/Føniksbyen "
                        "erklærede v1.0, indeholdt v2.0-data). 'Erklæret' svarer i "
                        "dag til SAF-T's native, selvangivne AuditFileVersion — kun "
                        "for SAF-T-oprindelse.",
    },
    {
        "felt": "version_strukturelt_detekteret", "sti": "(intet fysisk hjem endnu)",
        "status": "planned",
        "begrundelse": "Version-triplen — strukturel detektion af den FAKTISKE "
                        "XSD-form (uafhængigt af hvad filen selv hævder). Hører "
                        "naturligt hjemme i saf-t-validator/oversætteren (§7), ikke "
                        "kun i vat-analytics.",
    },
    {
        "felt": "version_maal", "sti": "(intet fysisk hjem endnu)",
        "status": "planned",
        "begrundelse": "Version-triplen — den version, kørslen/engagementet skal "
                        "validere/analysere IMOD (fx ved en kommende "
                        "ikrafttrædelsesdato for v2.1-krav).",
    },
]

# ---------------------------------------------------------------------------
# RUN_CONFIG — hånd-vedligeholdt metadata for run-time-konfiguration
# ---------------------------------------------------------------------------
# Selve default-værdierne/nøglerne for analytics_modules hentes DYNAMISK fra
# analytics/modules.py af generatoren (samme mønster som build_rules_catalog.py),
# så de aldrig kan drifte fra koden. Materialitets-tærsklerne er hånd-beskrevet
# her (env-navn + betydning), fordi materiality.py ikke gemmer env-navnet efter
# opslag — men build_data_contract.py verificerer at hvert env-navn her rent
# faktisk forekommer i analytics/materiality.py's kildekode (drift-fanger).

MATERIALITY_RUN_CONFIG = [
    {
        "env": "MATERIALITY_WEIGHT_CRITICAL", "kode_navn": "SEVERITY_WEIGHTS[critical]",
        "default": 25, "beskrivelse": "Score-vægt pr. kritisk fund (engine.build_report).",
    },
    {
        "env": "MATERIALITY_WEIGHT_HIGH", "kode_navn": "SEVERITY_WEIGHTS[high]",
        "default": 15, "beskrivelse": "Score-vægt pr. højt fund.",
    },
    {
        "env": "MATERIALITY_WEIGHT_MEDIUM", "kode_navn": "SEVERITY_WEIGHTS[medium]",
        "default": 8, "beskrivelse": "Score-vægt pr. medium fund.",
    },
    {
        "env": "MATERIALITY_WEIGHT_LOW", "kode_navn": "SEVERITY_WEIGHTS[low]",
        "default": 3, "beskrivelse": "Score-vægt pr. lavt fund.",
    },
    {
        "env": "MATERIALITY_CAP_CRITICAL", "kode_navn": "SEVERITY_PENALTY_CAPS[critical]",
        "default": 100, "beskrivelse": "Loft på det samlede score-fradrag fra kritiske fund i én kategori.",
    },
    {
        "env": "MATERIALITY_CAP_HIGH", "kode_navn": "SEVERITY_PENALTY_CAPS[high]",
        "default": 100, "beskrivelse": "Loft for høje fund.",
    },
    {
        "env": "MATERIALITY_CAP_MEDIUM", "kode_navn": "SEVERITY_PENALTY_CAPS[medium]",
        "default": 30, "beskrivelse": "Loft for medium fund (så medium-only højst giver gul).",
    },
    {
        "env": "MATERIALITY_CAP_LOW", "kode_navn": "SEVERITY_PENALTY_CAPS[low]",
        "default": 15, "beskrivelse": "Loft for lave fund (så lav-only forbliver ~grøn).",
    },
    {
        "env": "MATERIALITY_CASH_LIMIT", "kode_navn": "CASH_LIMIT",
        "default": 20000.0, "beskrivelse": "Kontantbetalingsgrænse (DKK) — kategori 7.",
    },
    {
        "env": "MATERIALITY_APPROVAL_THRESHOLDS", "kode_navn": "APPROVAL_THRESHOLDS",
        "default": [10000, 25000, 50000, 100000, 250000, 500000],
        "beskrivelse": "Interne godkendelses-/indberetningsgrænser (komma-liste, DKK) — kategori 7.",
    },
    {
        "env": "MATERIALITY_LARGE_VAT_NO_DOCUMENT", "kode_navn": "LARGE_VAT_NO_DOCUMENT",
        "default": 5000.0, "beskrivelse": "Stort momsbeløb uden bilag (DKK) — kontrol 59.",
    },
    {
        "env": "MATERIALITY_INPUT_OUTPUT_RATIO", "kode_navn": "INPUT_OUTPUT_RATIO",
        "default": 3.0, "beskrivelse": "Forhold købsmoms/salgsmoms der udløser flag — kontrol 76.",
    },
    {
        "env": "MATERIALITY_INVOICE_POSTING_LAG_DAYS", "kode_navn": "INVOICE_POSTING_LAG_DAYS",
        "default": 30, "beskrivelse": "Lag mellem faktura- og bogføringsdato i dage — kontrol 46.",
    },
    {
        "env": "MATERIALITY_DISTANCE_SELLING_DKK", "kode_navn": "DISTANCE_SELLING_THRESHOLD_DKK",
        "default": 74500.0, "beskrivelse": "Fjernsalgstærskel EU B2C (DKK, ~10.000 EUR) — kontrol 95.",
    },
]

# ---------------------------------------------------------------------------
# KNOWN_GAPS — kendte problemer/uoverensstemmelser, IKKE rettet her (uden for scope)
# ---------------------------------------------------------------------------

KNOWN_GAPS = [
    {
        "id": "GAP-01",
        "titel": "'or 0.0'-fallback på debit/credit/vat_amount skjuler manglende data",
        "beskrivelse": "data_adapter.adapt_excel_to_saft bruger 'txn.get(...) or 0.0' "
                       "på debit_amount/credit_amount/vat_amount. En reel 0-værdi i "
                       "kilden bliver umulig at skelne fra en manglende kolonne/tom "
                       "celle. Kendt, bevidst uden for scope for denne kontrakt "
                       "(adfærdsændring, ikke beskrivelse).",
        "beroerte_felter": ["transactions[].lines[].debit_amount",
                             "transactions[].lines[].credit_amount",
                             "transactions[].lines[].tax_amount"],
    },
    {
        "id": "GAP-02",
        "titel": "tax_base kan være et beregnet skøn uden at være markeret som sådan",
        "beskrivelse": "tax_base falder tilbage fra 'importeret grundlag' til "
                       "'udledt af vat_amount/tax_percentage' til 'debit+credit', "
                       "uden at outputtet mærker hvilken sti der blev brugt.",
        "beroerte_felter": ["transactions[].lines[].tax_base"],
    },
    {
        "id": "GAP-03",
        "titel": "accounts[]/lines[] mangler account_type + standard_account_id strukturelt på Excel-vejen",
        "beskrivelse": "Der findes ingen COLUMN_ALIASES-indgang for kontotype eller "
                       "standardkontoplan-id — feltet kan ikke udfyldes selv hvis "
                       "kildefilen har en relevant kolonne. Konsekvens: "
                       "vat_rules.is_non_vat_account() (momsrelevans-scope, bl.a. "
                       "kontrol 80) er strukturelt inaktiv på Excel/CSV-import — "
                       "dokumenteret og tilsigtet konservativ adfærd (CLAUDE.md), "
                       "men værd at have eksplicit i kontrakten.",
        "beroerte_felter": ["accounts[].account_type", "accounts[].standard_account_id",
                             "transactions[].lines[].account_type",
                             "transactions[].lines[].standard_account_id"],
    },
    {
        "id": "GAP-04",
        "titel": "accounts[].opening_balance/closing_balance er altid 0.0 på Excel-vejen",
        "beskrivelse": "Ingen kolonne-alias for saldi. Kontrol 77 "
                       "(vat_account_reconciliation) afstemmer beregnet moms mod "
                       "accounts[].closing_balance — på Excel-import sker denne "
                       "afstemning derfor reelt mod en konstant nulsaldo for alle "
                       "momskonti, hvilket kan give et misvisende resultat "
                       "(falsk positiv ELLER falsk negativ afhængig af fortegn).",
        "beroerte_felter": ["accounts[].opening_balance", "accounts[].closing_balance"],
    },
    {
        "id": "GAP-05",
        "titel": "customers[].vat_number og customers[].country er hårdkodet tomme på Excel-vejen",
        "beskrivelse": "excel_parser._process_row sætter disse to felter til \"\" "
                       "ubetinget for kunder (i modsætning til leverandører, hvor de "
                       "udfyldes fra kildekolonner). cat12's kontrol 94/95/96/97 "
                       "(e-handel/OSS/fjernsalg/digitale ydelser) læser UDELUKKENDE "
                       "customers[].country uden linje-niveau-fallback og kan derfor "
                       "strukturelt aldrig finde en kundes land på Excel/CSV-"
                       "oprindelse. Modulet er default FRA, så gabet er usynligt i "
                       "en standardkørsel, men bider hvis 'ehandel_saerordninger' "
                       "tændes på et Excel-udtræk.",
        "beroerte_felter": ["customers[].vat_number", "customers[].country"],
    },
    {
        "id": "GAP-06",
        "titel": "source_document_id betyder noget forskelligt på de to input-veje",
        "beskrivelse": "Excel-oprindelse: reelt fakturanummer (invoice_number-kolonne). "
                       "SAF-T-oprindelse: transaktionens fritekstbeskrivelse "
                       "(Transaction/Description), fordi SAF-T Financial "
                       "GeneralLedgerEntries ikke bærer et selvstændigt dokument-id "
                       "på transaktionsniveau. Dubletdetektion (kategori 2) er "
                       "dermed et svagere signal på SAF-T-oprindelse end på "
                       "Excel-oprindelse, uden at det er synligt i outputtet.",
        "beroerte_felter": ["transactions[].lines[].source_document_id"],
    },
    {
        "id": "GAP-07",
        "titel": "summary mangler total_debit/total_credit/total_vat på SAF-T-vejen",
        "beskrivelse": "data_adapter.adapt_excel_to_saft bygger summary med "
                       "total_debit/total_credit/total_vat; saft_parser.parse_saft's "
                       "summary-dict har dem slet ikke. Al kode/UI der læser disse "
                       "nøgler uden .get()-fallback vil fejle eller mistolke "
                       "SAF-T-oprindede rapporter.",
        "beroerte_felter": ["summary.total_debit", "summary.total_credit", "summary.total_vat"],
    },
    {
        "id": "GAP-08",
        "titel": "header.registration_number/source/saft_version er ikke symmetriske mellem input-veje",
        "beskrivelse": "registration_number/source findes kun (og er reelt aldrig "
                       "udfyldt for registration_number) i Excel-vejens header; "
                       "saft_version findes kun i SAF-T-vejens. Kode der antager "
                       "samme nøglesæt på tværs af input-veje vil ramme KeyError "
                       "eller stille (og forkert) tavshed.",
        "beroerte_felter": ["header.registration_number", "header.source", "header.saft_version"],
    },
    {
        "id": "GAP-09",
        "titel": "tax_table-poster har forskellige nøgler afhængigt af oprindelse",
        "beskrivelse": "Excel-afledte poster bærer både 'rate' (rå) og "
                       "'tax_percentage' (adapteret); SAF-T-poster bærer kun "
                       "'tax_percentage', plus 'standard_tax_code'/'country' som "
                       "Excel-poster aldrig har.",
        "beroerte_felter": ["tax_table[].rate", "tax_table[].standard_tax_code", "tax_table[].country"],
    },
]
