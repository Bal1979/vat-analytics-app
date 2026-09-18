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
    kilder          — dict {excel: bool|"partial", saft: bool|"partial",
                       canonical: bool|"partial"} — er feltet reelt udfyldt fra
                       hver input-vej i dag? ("canonical" = den kanoniske
                       gl_entries-CSV fra vat-extracts dataextract.transform,
                       byggetrin 8, parsers/canonical_parser.py.)
    ekstension      — True hvis feltet er en ``balai_extensions``-udvidelse
                       (BALAI-dataflow-arkitektur.md §2a) — dvs. det er IKKE et
                       nativt SAF-T Financial-felt, motoren kræver det alligevel.
    kraeves_af      — fritekst: hvilke kontrol-id'er/kategorier/features feltet
                       låser op, krydset mod analytics/readiness.py + kode-gennemgang
                       af analytics/categories/cat*.py
    noter           — kendte skævheder/forbehold specifikt for feltet
"""

from __future__ import annotations

CONTRACT_VERSION = "0.4.4"

# ---------------------------------------------------------------------------
# 1. HEADER
# ---------------------------------------------------------------------------

HEADER_FIELDS = [
    {
        "navn": "company_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True, "canonical": False},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol låser sig op af feltet i dag; vises i UI/rapport-header.",
        "noter": "Excel-vejen sætter altid \"\" (ingen kolonne-alias findes for "
                 "virksomhedsnavn) — kun SAF-T-vejen (Company/Name) udfylder det.",
    },
    {
        "navn": "registration_number", "type": "string", "obligatorisk": False, "format": "CVR (8 cifre)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": False},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag — nøglen er altid \"\" på begge veje.",
        "noter": "Trin 3 (GAP-08, delvist lukket): nøglen er nu til stede (tom streng) "
                 "PÅ BEGGE VEJE — saft_parser.parse_saft sætter den også, så kode der "
                 "antager samme nøglesæt ikke længere rammer KeyError. kilder er "
                 "stadig False/False, fordi ingen af parserne udtrækker en REEL CVR: "
                 "SAF-T Financial DK's Header/Company bærer ikke konsekvent et "
                 "selvstændigt CVR for indberetterens egen virksomhed på tværs af "
                 "versioner/eksportører, og Excel-vejen har ingen kolonne-alias for "
                 "det. Reelt dødt felt i dag på begge veje — symmetrisk dødt, ikke "
                 "længere asymmetrisk.",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217, fx \"DKK\"",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True, "canonical": True},
        "ekstension": False,
        "kraeves_af": "summary.currency; ingen kontrol læser header.currency direkte.",
        "noter": "Excel-vejen sætter en fast standardværdi \"DKK\" — læses ikke fra en "
                 "kildekolonne (kun linje-niveau currency er kolonne-drevet).",
    },
    {
        "navn": "period_start", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True, "canonical": True},
        "ekstension": False,
        "kraeves_af": "summary.period_start; grundlag for header.period (kontrol 9).",
        "noter": "Excel: min(dato) over transaktionerne. SAF-T: "
                 "Header/SelectionCriteria/SelectionStartDate.",
    },
    {
        "navn": "period_end", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True, "canonical": True},
        "ekstension": False,
        "kraeves_af": "summary.period_end; grundlag for header.period (kontrol 9).",
        "noter": "Excel: max(dato). SAF-T: SelectionCriteria/SelectionEndDate.",
    },
    {
        "navn": "period", "type": "object {start, start_year, end, end_year}",
        "obligatorisk": False, "format": "start/end: måned uden padding (\"3\"); *_year: \"YYYY\"",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Kontrol 9 (tax point / periode-check, kategori 5).",
        "noter": "Afledt/beregnet i begge parsere, kun når period_start OG period_end "
                 "begge er udfyldt. Findes ikke som selvstændig kildekolonne.",
    },
    {
        "navn": "source", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented",
        "kilder": {"excel": True, "saft": True, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol — diagnostik/UI (\"Excel/CSV import\"/\"SAF-T XML import\").",
        "noter": "Trin 3 (GAP-08, lukket for dette felt): saft_parser sætter nu også "
                 "\"SAF-T XML import\" — begge veje bærer en reel, meningsfuld "
                 "diagnostikværdi (ikke en tom placeholder).",
    },
    {
        "navn": "saft_version", "type": "string", "obligatorisk": False, "format": "\"1.0\"|\"2.0\"|\"2.1\" (selvangivet)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": True, "canonical": False},
        "ekstension": True,
        "kraeves_af": "Del af balai_extensions-versionstriplen (se BALAI_EXTENSIONS "
                       "nedenfor) — endnu ikke konsumeret af nogen kontrol.",
        "noter": "Svarer til SAF-T's native, selvangivne AuditFileVersion — det er "
                 "netop dette ENE felt, BAL-004/BAL-005-erfaringen viser kan være "
                 "forkert (Danoil/Føniksbyen erklærede v1.0, indeholdt v2.0-data). "
                 "Trin 3 (GAP-08, delvist lukket): nøglen er nu til stede (tom "
                 "streng) i Excel-vejens header for nøglesæt-symmetri — men bærer "
                 "ingen reel værdi dér (semantisk umuligt: der er intet selvangivet "
                 "AuditFileVersion at læse fra et fladt udtræk), så kilder.excel "
                 "forbliver False.",
    },
    {
        "navn": "mapping_version", "type": "string", "obligatorisk": False, "format": "semver, fx \"1.0.0\"",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol — reproducerbarheds-/lineage-stempel i "
                       "analyserapporten (byggetrin 8, den billige "
                       "reproducerbarheds-forbedring anbefalet i trin 3-runden).",
        "noter": "NY (0.2.0, byggetrin 8, Bal-godkendt 2026-09-17). IKKE et "
                 "SAF-T- eller balai_extensions-felt — det er pipeline-lineage "
                 "for den kanoniske vej: hvilken Bal-godkendt mapping "
                 "(dataextract/mappings/, vat-extract) der producerede "
                 "gl_entries-CSV'en. Læses fra transform_summary.json ved "
                 "siden af CSV'en (parsers/canonical_parser.py). Tom streng "
                 "på Excel-/SAF-T-vejen (findes pr. definition ikke der).",
    },
    {
        "navn": "schema_fingerprint", "type": "string", "obligatorisk": False, "format": "\"sha256:<hex>\"",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Ingen kontrol — reproducerbarheds-/lineage-stempel, samme "
                       "begrundelse som mapping_version.",
        "noter": "NY (0.2.0, byggetrin 8). Hash af kildefilens kolonnenavne+typer "
                 "(dataextract.profiling, vat-extract) — identificerer PRÆCIS "
                 "hvilket kilde-skema (ERP-udtræksformat) mappingen blev "
                 "godkendt til. Ingen kundedata i hash'en. Tom streng på "
                 "Excel-/SAF-T-vejen.",
    },
    {
        "navn": "vat_setup_loaded", "type": "boolean", "obligatorisk": False, "format": "",
        "status": "implemented",
        "kilder": {"excel": False, "saft": False, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Kontrol 19 (cat03_vat_rate_validation.test_19_invalid_rate, "
                       "byggetrin 8, Del A, Bal-godkendt 2026-09-17): vælger "
                       "valideringsvej — True = validér mod kundens EGEN vat_setup "
                       "(pr. momskode, se tax_table[].setup_matched); False = "
                       "uændret adfærd (kun 0%/25% er gyldige danske satser).",
        "noter": "NY (0.3.1). Nøglesæt-symmetri (samme princip som resten af "
                 "canonical_parser.py): ALTID til stede på den kanoniske vej, "
                 "default False, sat af canonical_parser.parse_canonical ved "
                 "opbygning af header. Sættes til True af parsers/"
                 "canonical_masterdata.enrich_canonical NÅR ``vat_setup.csv`` "
                 "findes og indeholder mindst én gyldig række (uafhængigt af om "
                 "DENNE fils konkrete momskoder rent faktisk matcher — se GAP-10). "
                 "Findes ALDRIG på Excel-/SAF-T-vejen i dag (ingen vat_setup-"
                 "koncept dér) — nøglen er derfor fraværende, ikke False, på disse "
                 "veje; kontrol 19 bruger \"header.get('vat_setup_loaded')\" så "
                 "fravær og False giver samme (uændrede) adfærd.",
    },
]

# ---------------------------------------------------------------------------
# 2. ACCOUNTS (liste)
# ---------------------------------------------------------------------------

ACCOUNT_FIELDS = [
    {
        "navn": "account_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Nøgle til linje-join (account_type/standard_account_id-opslag) i "
                       "data_adapter/saft_parser; kategori 2 (test_15, Asset-filter).",
        "noter": "",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Kontrol 77 (momskontoafstemning, cat10.test_77_vat_account_"
                       "reconciliation — matcher kontonavnet mod 'moms'/'vat') og "
                       "kontrol 80's kontolabel i finding-teksten (account_names). "
                       "Ikke længere kun 'kontekst i rapport'.",
        "noter": "Byggetrin 9/Del B alias-bugfix (2026-09-17, Bal-godkendt): den "
                 "kanoniske vejs chart_of_accounts.csv-loader læste tidligere KUN "
                 "en upræfikset 'description'-kolonne, men vat-extracts reelle "
                 "transform-output navngiver kontonavnet 'ext_name' — feltet var "
                 "derfor strukturelt altid tomt på den kanoniske vej. "
                 "canonical_masterdata.load_chart_of_accounts accepterer nu "
                 "'ext_name' (først) ELLER 'description'/'name' (fallback). "
                 "'partial' fordi berigelsen kun sker NÅR chart_of_accounts.csv "
                 "er indlæst som sidecar (valgfri fil).",
    },
    {
        "navn": "account_type", "type": "string", "obligatorisk": False,
        "format": "SAF-T AccountType-enum, fx \"Asset\"|\"Liability\"|\"Equity\"|\"Revenue\"|\"Expense\"|\"Other\"",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 1 -> kontrol 80 "
                       "(revenue_without_output_vat); kategori 2 test_15 "
                       "(duplicate_payment, filtrerer \"Asset\"-konti).",
        "noter": "Trin 3 (GAP-03, delvist lukket): COLUMN_ALIASES har nu en "
                 "\"account_type\"-indgang (kontotype/konto_type/accounttype), så "
                 "feltet KAN udfyldes, hvis kildefilen har en relevant kolonne. "
                 "Fraværende kolonne giver stadig \"\" — samme konservative "
                 "fallback-adfærd som før (CLAUDE.md: ukendt kontotype -> uændret "
                 "scope). Et almindeligt fladt GL-udtræk vil typisk stadig mangle "
                 "denne kolonne, så den praktiske effekt afhænger af kildefilen.",
    },
    {
        "navn": "standard_account_id", "type": "string", "obligatorisk": False,
        "format": "ERST-standardkontoplan-kode, fx \"5800\" (resultat: 1000-4999, balance: >=5000)",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 2 (robust scope) via "
                       "standard_accounts.account_nature() -> kontrol 80 + hele "
                       "momskernens balancekonto-udelukkelse.",
        "noter": "Trin 3 (GAP-03, delvist lukket): COLUMN_ALIASES har nu en "
                 "\"standard_account_id\"-indgang. Nøglen er altid til stede i "
                 "Excel-vejens accounts[]-dict (tidligere manglede den helt); tom "
                 "streng når kildefilen ikke har kolonnen. Native SAF-T-element "
                 "(StandardAccountID), IKKE en balai_extension.",
    },
    {
        "navn": "opening_balance", "type": "number (nullable)", "obligatorisk": False,
        "format": "DKK, kan være negativ",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag.",
        "noter": "Trin 3 (GAP-04, delvist lukket): COLUMN_ALIASES har nu en "
                 "\"opening_balance\"-indgang. None (ikke stille 0.0) når kildefilen "
                 "ikke har kolonnen, så \"ingen saldodata\" kan skelnes fra \"saldo "
                 "er faktisk 0\" — ingen eksisterende kontrol læser feltet, så "
                 "None-fallbacket ændrer ikke nogen kontrols adfærd.",
    },
    {
        "navn": "closing_balance", "type": "number (nullable)", "obligatorisk": False,
        "format": "DKK, kan være negativ",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Kontrol 77 (vat_account_reconciliation, kategori 10) — "
                       "afstemmer beregnet moms mod kontoens closing_balance.",
        "noter": "Trin 3 (GAP-04, delvist lukket): COLUMN_ALIASES har nu en "
                 "\"closing_balance\"-indgang, så kontrol 77 kan afstemme mod en "
                 "reel saldo på Excel-import, når kildefilen bærer den. Uden "
                 "kolonnen er værdien None; cat10_vat_reconciliation.test_77 bruger "
                 "allerede 'a.get(\"closing_balance\", 0) or 0', så None giver "
                 "samme \"ingen saldoinformation\"-adfærd som tidligere 0.0 — "
                 "uændret kontrol-adfærd i det tilfælde. Et fladt GL-udtræk uden "
                 "en selvstændig kontoplan-fane vil typisk stadig mangle kolonnen.",
    },
]

# ---------------------------------------------------------------------------
# 3. TAX_TABLE (liste)
# ---------------------------------------------------------------------------

TAX_TABLE_FIELDS = [
    {
        "navn": "tax_code", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering, 19-26); kategori 9 (reverse "
                       "charge, 70-75); kategori 10 (afstemning, 76-83); readiness "
                       "signal-felt for kategori 3/9/10.",
        "noter": "",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial",
        "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst i rapport (momskode-tekst).",
        "noter": "Excel: syntetisk \"Momskode {code}\" hvis ingen beskrivelseskolonne. "
                 "SAF-T: nativ TaxTableEntry/TaxCodeDetails/Description. Byggetrin "
                 "9/Del B alias-bugfix (2026-09-17, Bal-godkendt): den kanoniske "
                 "vejs vat_setup.csv-loader læste tidligere KUN en upræfikset "
                 "'description'-kolonne, men vat-extracts reelle transform-output "
                 "navngiver momskode-beskrivelsen 'ext_description' — feltet var "
                 "derfor strukturelt altid tomt på den kanoniske vej. "
                 "canonical_masterdata.load_vat_setup accepterer nu "
                 "'ext_description' (først) ELLER 'description' (fallback). "
                 "'partial' fordi berigelsen kun sker NÅR vat_setup.csv er "
                 "indlæst som sidecar (valgfri fil).",
    },
    {
        "navn": "tax_percentage", "type": "number", "obligatorisk": True, "format": "procent, fx 25.0",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering); readiness FIELD_INFO.",
        "noter": "Nøglenavnet motoren forventer. For Excel-oprindelse skrives det af "
                 "data_adapter fra 'rate' hvis 'tax_percentage' mangler (se 'rate' "
                 "nedenfor); for SAF-T-oprindelse er det nativt. Byggetrin 8/Del C "
                 "(Bal-godkendt 2026-09-17, GAP-10 delvist lukket): den VALGFRIE "
                 "``vat_setup.csv``-sidecar-fil (join-nøgle: vat_codes-strengen) "
                 "kan levere en reel værdi på den kanoniske vej — se "
                 "parsers/canonical_masterdata.py. Uden filen: fortsat 0.0 (uændret).",
    },
    {
        "navn": "rate", "type": "number", "obligatorisk": False, "format": "procent, fx 25.0",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte (kun bro-felt til tax_percentage).",
        "noter": "Trin 3 (GAP-09, lukket for dette felt): saft_parser afleder nu "
                 "også \"rate\" = tax_percentage (reel værdi, ikke en placeholder) "
                 "på SAF-T-oprindede poster — nøglen er ikke længere kun til stede "
                 "på Excel-afledte poster. Bør stadig ikke bruges af ny kode — "
                 "brug tax_percentage, som er navnet motoren forventer.",
    },
    {
        "navn": "standard_tax_code", "type": "string", "obligatorisk": False,
        "format": "Skattestyrelsens StandardTaxCode (jf. tax_codes.py-katalog)",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag (potentiel bro til feature 82, rubrik-afstemning).",
        "noter": "Trin 3 (GAP-09, delvist lukket): nøglen er nu til stede (tom "
                 "streng) også på Excel-afledte tax_table-poster (data_adapter), så "
                 "kode der antager samme nøglesæt ikke rammer KeyError. Bærer "
                 "stadig ingen reel værdi dér — semantisk umuligt uden en "
                 "kildekolonne for Skattestyrelsens StandardTaxCode.",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol i dag — relevant for udenlandske momskoder.",
        "noter": "Trin 3 (GAP-09, delvist lukket): nøglen er nu til stede (tom "
                 "streng) også på Excel-afledte tax_table-poster (data_adapter). "
                 "Bærer stadig ingen reel værdi dér — Excel-parseren har ingen "
                 "kolonne-alias for momskodens land (adskilt fra transaktionens "
                 "modparts-land, som allerede findes som lines[].country).",
    },
    {
        "navn": "setup_matched", "type": "boolean", "obligatorisk": False, "format": "",
        "status": "implemented",
        "kilder": {"excel": False, "saft": False, "canonical": True},
        "ekstension": False,
        "kraeves_af": "Kontrol 19 (byggetrin 8, Del A): skelner \"kode kendt i "
                       "vat_setup, sats 0%\" fra \"kode findes slet ikke i "
                       "opsætningen\" — begge ville ellers dele tax_percentage==0.0 "
                       "(den strukturelle default) og være umulige at skelne.",
        "noter": "NY (0.3.1). Nøglesæt-symmetri: ALTID til stede på HVER "
                 "tax_table-entry på den kanoniske vej, default False (sat af "
                 "canonical_parser.parse_canonical). Sættes til True af "
                 "enrich_canonical, KUN når header.vat_setup_loaded også er True "
                 "OG denne specifikke kode findes i vat_setup.csv. Findes aldrig "
                 "på Excel-/SAF-T-vejen.",
    },
    {
        "navn": "non_deductible_vat_pct", "type": "number", "obligatorisk": False,
        "format": "procent, fx 40.0 — None = intet signal",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Feature 83 (delvis fradragsret) + BAL-055-familien "
                       "(momsfradragsbegrænsning, §42) — ingen aktiv kontrol "
                       "konsumerer feltet endnu. Pr.-KODE-konfiguration: den "
                       "tredje granularitet ved siden af non_deductible_amount "
                       "(linjeniveau, kun SAF-T-vejen) og ekstern_fradragsbroek "
                       "(virksomhedsbred, planned).",
        "noter": "balai_extension pr. §2a (Bal-godkendt 2026-09-17). Kilde: "
                 "vat_setup.csv-sidecarens ext_non_deductible_vat_pct (BC/NAV "
                 "'Non-Deductible VAT %', jf. vat-extract/tools/"
                 "seed_master_data_mappings.py). Nøglesæt-symmetri: altid til "
                 "stede på den kanoniske vej, default None ('intet signal' — "
                 "adskilt fra 0.0 = 'fuld fradragsret'); reel værdi kun for "
                 "koder matchet i vat_setup.csv (setup_matched). BEVIDST IKKE "
                 "det kanoniske pro_rata (virksomhedsbred fradragsprocent) — "
                 "se vat-extract-mappingens control_rationale.",
    },
    {
        "navn": "allow_non_deductible_vat", "type": "string", "obligatorisk": False,
        "format": "rå ERP-værdi, fx 'Allow'/'Do Not Allow' (BC/NAV)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Feature 83 + BAL-055-familien — ledsage-flag til "
                       "non_deductible_vat_pct: om ERP'et automatisk bogfører "
                       "en ikke-fradragsberettiget andel for koden.",
        "noter": "balai_extension pr. §2a (Bal-godkendt 2026-09-17). Kilde: "
                 "vat_setup.csv-sidecarens ext_allow_non_deductible_vat. "
                 "Bæres RÅT (trimmet, ikke normaliseret). Nøglesæt-symmetri: "
                 "altid til stede på den kanoniske vej, default \"\".",
    },
    {
        "navn": "vat_calculation_type", "type": "string", "obligatorisk": False,
        "format": "rå ERP-værdi, fx 'Normal VAT'/'Reverse Charge VAT'/'Full VAT' (BC/NAV)",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Kontrol 82 AKTIV (byggetrin 9, Del A, Bal-godkendt "
                       "2026-09-17): cat10._purchase_rubric bruger feltet FØR "
                       "DKRC/SERVICE_VAT-navnemønstrene, når det er til stede "
                       "(deterministisk 'reverse charge'-detektion + Bus.-"
                       "gruppen til indenlandsk/udenlandsk-skellet); mønstrene "
                       "bevares som fallback uden feltet, og som sidste skelnen "
                       "mellem RC-ydelser og RC-varekøb fra udlandet (se noter). "
                       "Sekundært kategori 9 (70-75, ikke konsumeret endnu).",
        "noter": "balai_extension pr. §2a (Bal-godkendt 2026-09-17). Kilde: "
                 "vat_setup.csv-sidecarens ext_vat_calculation_type, joinet "
                 "BÅDE på tax_table[] og transactions[].lines[] (kontrol 82 "
                 "klassificerer pr. linje). Dokumenteret forbehold (uændret "
                 "efter aktivering): flaget skelner reverse charge fra normal "
                 "OG (sammen med Bus.-gruppen) indenlandsk fra udenlandsk RC, "
                 "men IKKE alene RC-ydelser fra RC-varekøb fra udlandet — "
                 "begge bruger samme beregningstype og Bus.-gruppe i den "
                 "observerede BC/NAV-taksonomi. Den sidste skelnen falder "
                 "fortsat tilbage til SERVICE_VAT-navnemønstret (materiality."
                 "VAT_DECLARATION_SERVICE_VAT_PATTERNS), bevidst, ikke en "
                 "overset huls. Regressionsverificeret: kontrol 82's E2E-"
                 "resultat på byggetrin 8/9's v4-datasæt er uændret efter "
                 "aktiveringen. SAF-T-nativt alternativ på sigt: en eksplicit "
                 "besluttet StandardTaxCode-mapping (jf. vat-extracts "
                 "ext_tax_category-rationale), som kan overflødiggøre denne "
                 "ekstension — migreringsomfanget er dermed dokumenteret på "
                 "forhånd, som §2a kræver. Nøglesæt-symmetri: altid til stede "
                 "på den kanoniske vej, default \"\".",
    },
    {
        "navn": "sales_vat_account", "type": "string", "obligatorisk": False,
        "format": "kontonummer, rå ERP-værdi (fx BC 'Sales VAT Account')",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Kunderapportens 'Momsmotoren'-sektion "
                       "(tools/generate_report.py, byggetrin ~10, Bal-godkendt "
                       "2026-09-18) — kobler momskoden til den momskonto, "
                       "ERP'et selv bogfører salgsmoms på for koden. Ingen "
                       "aktiv KONTROL konsumerer feltet — det er "
                       "pædagogisk/dokumenterende i kunderapporten, ikke et "
                       "input til en regel.",
        "noter": "balai_extension pr. §2a. Kilde: vat_setup.csv-sidecarens "
                 "ext_sales_vat_account. Bæres RÅT. Nøglesæt-symmetri: altid "
                 "til stede på den kanoniske vej, default \"\".",
    },
    {
        "navn": "purchase_vat_account", "type": "string", "obligatorisk": False,
        "format": "kontonummer, rå ERP-værdi (fx BC 'Purch. VAT Account')",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Kunderapportens 'Momsmotoren'-sektion — samme "
                       "begrundelse som sales_vat_account, købssiden.",
        "noter": "balai_extension pr. §2a. Kilde: vat_setup.csv-sidecarens "
                 "ext_purchase_vat_account. Bæres RÅT. Nøglesæt-symmetri: "
                 "altid til stede på den kanoniske vej, default \"\".",
    },
    {
        "navn": "reverse_charge_vat_account", "type": "string", "obligatorisk": False,
        "format": "kontonummer, rå ERP-værdi (fx BC 'Reverse Chrg. VAT Acc.')",
        "status": "implemented_partial",
        "kilder": {"excel": False, "saft": False, "canonical": "partial"},
        "ekstension": True,
        "kraeves_af": "Kunderapportens 'Momsmotoren'-sektion — viser den "
                       "separate omvendt betalingspligt-konto ved siden af "
                       "sales_vat_account/purchase_vat_account, når ERP'et "
                       "bogfører RC-moms på sin egen konto (typisk BC/NAV).",
        "noter": "balai_extension pr. §2a. Kilde: vat_setup.csv-sidecarens "
                 "ext_reverse_charge_vat_account. Bæres RÅT. "
                 "Nøglesæt-symmetri: altid til stede på den kanoniske vej, "
                 "default \"\".",
    },
]

# ---------------------------------------------------------------------------
# 4. TRANSACTIONS (liste) — top-niveau
# ---------------------------------------------------------------------------

TRANSACTION_FIELDS = [
    {
        "navn": "transaction_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 2 (dubletdetektion, 11-18) som transaktions-nøgle; "
                       "gennemgående som finding-reference i alle kategorier.",
        "noter": "Excel-vejen falder tilbage til \"ROW-{n}\" (n=idx+2) hvis kilden "
                 "mangler en id-kolonne; SAF-T-vejen har intet fallback (nativt "
                 "TransactionID kan i teorien være tomt).",
    },
    {
        "navn": "date", "type": "string", "obligatorisk": True, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing & periodetest, 39-46); period/period_year-udledning.",
        "noter": "Bogføringsdato. SAF-T: GLPostingDate (fallback TransactionDate).",
    },
    {
        "navn": "document_date", "type": "string", "obligatorisk": False, "format": "ISO-dato YYYY-MM-DD",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": True,
        "kraeves_af": "Kontrol 46 (faktura-/bogføringslag, kategori 5).",
        "noter": "balai_extension pr. §2a — begrundelse: SAF-T-eksporter bærer ikke "
                 "altid dato-lagdelingen konsistent, selvom feltet teknisk kan "
                 "udledes af native TransactionDate. SAF-T-vejen mapper "
                 "document_date=TransactionDate, date=GLPostingDate (falder sammen "
                 "hvis kilden kun har én dato).",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kontrol 4 (faktura-feltfuldstændighed, kategori 1) læser "
                       "txn['description'] direkte — tidligere fejlagtigt "
                       "dokumenteret som 'ingen kontrol direkte'.",
        "noter": "GAP-13 (lukket 2026-09-17, Bal-godkendt): canonical_parser læser "
                 "nu en valgfri description-kolonne fra den kanoniske CSV og "
                 "sætter transaktionens description til den FØRSTE ikke-tomme "
                 "linje-description i bilaget (samme fallback-princip som "
                 "document_date). Kolonnen kan mangle på ældre kanoniske filer "
                 "(v2) — da er feltet fortsat \"\" (uændret, ingen crash). Se "
                 "analytics/readiness.py: kontrol 4's Description-delcheck gates "
                 "(rapporterer 'ikke målbar' i stedet for per-bilag-støj), når "
                 "feltet er 0% udfyldt på en population stor nok til at udelukke "
                 "tilfældighed.",
    },
    {
        "navn": "journal_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst/reference.",
        "noter": "Default \"IMPORT\" (Excel) / \"GL\" (SAF-T) hvis kilden ikke har et journal-id.",
    },
    {
        "navn": "period", "type": "string", "obligatorisk": False, "format": "\"1\"-\"12\", zero-padded i Excel-vejen",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing/periodetest).",
        "noter": "Excel: udledt af date hvis ikke allerede sat, zero-padded (\"03\"). "
                 "SAF-T: udledt af posteringsmåneden (IKKE SAF-T's regnskabsperiode-"
                 "felt <Period>, som er en løbenummer-periode, ikke kalendermåned).",
    },
    {
        "navn": "period_year", "type": "string", "obligatorisk": False, "format": "\"YYYY\"",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 5 (timing/periodetest).",
        "noter": "",
    },
    {
        "navn": "total_debit", "type": "number", "obligatorisk": True, "format": "DKK, sum af linjernes debit_amount",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "summary.total_debit (kun Excel-vejen, se summary-uoverensstemmelse); "
                       "diverse beløbskontroller i kategori 7/10.",
        "noter": "",
    },
    {
        "navn": "total_credit", "type": "number", "obligatorisk": True, "format": "DKK, sum af linjernes credit_amount",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Samme som total_debit.",
        "noter": "",
    },
    {
        "navn": "lines", "type": "list[line]", "obligatorisk": True, "format": "mindst ét element i praksis",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
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
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — finding-reference.",
        "noter": "Excel: syntetisk \"L{n}\" (altid præcis én linje). SAF-T: nativt "
                 "RecordID eller syntetisk fallback.",
    },
    {
        "navn": "account_id", "type": "string", "obligatorisk": True, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Join til accounts[] (account_type/standard_account_id).",
        "noter": "",
    },
    {
        "navn": "account_type", "type": "string", "obligatorisk": False, "format": "se ACCOUNT_FIELDS",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 1 -> kontrol 80.",
        "noter": "Joinet fra accounts[].account_type via account_id. Trin 3 "
                 "(GAP-03, delvist lukket): data_adapter joiner nu den samme værdi "
                 "som accounts[].account_type — reel på Excel-vejen, når "
                 "kildefilen har kontotype-kolonnen; ellers \"\" (uændret adfærd). "
                 "Byggetrin 8/Del C (GAP-11 delvist lukket): på den kanoniske vej "
                 "joiner ``chart_of_accounts.csv`` (join-nøgle: kontonummeret) "
                 "feltet direkte ind på linjen, hvis sidecar-filen findes.",
    },
    {
        "navn": "standard_account_id", "type": "string", "obligatorisk": False, "format": "se ACCOUNT_FIELDS",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "vat_rules.is_non_vat_account signal 2 -> kontrol 80 + momskerne-scope.",
        "noter": "Trin 3 (GAP-03, delvist lukket): data_adapter joiner nu "
                 "standard_account_id fra accounts[] via account_id — nøglen er "
                 "altid til stede (tidligere manglede den helt på Excel-vejen; "
                 "line.get(\"standard_account_id\") returnerede None). Tom streng "
                 "når kildefilen ikke har kolonnen. Byggetrin 8/Del C: samme "
                 "``chart_of_accounts.csv``-join som account_type ovenfor.",
    },
    {
        "navn": "description", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — kontekst i finding-tekster.",
        "noter": "GAP-13 (lukket 2026-09-17, Bal-godkendt): canonical_parser "
                 "læser nu en valgfri description-kolonne pr. linje, samme "
                 "mønster som account_type/standard_account_id — fraværende "
                 "kolonne giver fortsat \"\" (uændret adfærd).",
    },
    {
        "navn": "debit_amount", "type": "number", "obligatorisk": True, "format": "DKK, >= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Stort set alle 103 kontroller (beløbsgrundlag).",
        "noter": "KENDT GAB (uden for scope her): data_adapter bruger "
                 "'txn.get(\"debit_amount\", 0.0) or 0.0' — en reel, men falsk "
                 "0-værdi i kilden bliver umulig at skelne fra 'ingen kolonne'. "
                 "Se known_gaps.",
    },
    {
        "navn": "credit_amount", "type": "number", "obligatorisk": True, "format": "DKK, >= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Samme som debit_amount.",
        "noter": "Samme 'or 0.0'-fallback-gab som debit_amount. Se known_gaps.",
    },
    {
        "navn": "tax_code", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 3, 9, 10 (readiness signal-felt).",
        "noter": "",
    },
    {
        "navn": "tax_percentage", "type": "number", "obligatorisk": False, "format": "procent",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Kategori 3 (momssats-validering) — readiness FIELD_INFO.",
        "noter": "Byggetrin 8/Del C: ``vat_setup.csv`` (join-nøgle: vat_codes-"
                 "strengen) joiner en reel værdi direkte ind på linjen på den "
                 "kanoniske vej, hvis sidecar-filen findes. Se tax_table.tax_percentage.",
    },
    {
        "navn": "tax_base", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
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
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kategori 3, 9, 10 — momsbeløb er kernen i afstemningen.",
        "noter": "KENDT GAB (uden for scope her): 'txn.get(\"vat_amount\") or 0.0' "
                 "kan ikke skelne '0 kr. moms' fra 'momsbeløb ukendt'. Se known_gaps.",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217, default \"DKK\"",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Kontrol 33 (currency/country-konsistens, kategori 4).",
        "noter": "",
    },
    {
        "navn": "supplier_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kategori 4 (EU/grænseoverskridende), 6 (parts-validering), "
                       "9 (reverse charge), 11 (MTIC) — join til suppliers[].",
        "noter": "",
    },
    {
        "navn": "supplier_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kontrol 47 (manglende partsnavn, kategori 6).",
        "noter": "",
    },
    {
        "navn": "customer_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kategori 12 (e-handel) — join til customers[]; kategori 6.",
        "noter": "",
    },
    {
        "navn": "customer_name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kontrol 47 (manglende partsnavn), kontrol 52 (kunde uden moms-nr).",
        "noter": "",
    },
    {
        "navn": "source_document_id", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": "partial", "canonical": True}, "ekstension": False,
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
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kategori 4 (grænseoverskridende/EU), 9, 11, 12 — readiness "
                       "signal-felt for kategori 4/9/11/12.",
        "noter": "Modpartens land (ikke vareflow — se ship_from/ship_to_country).",
    },
    {
        "navn": "ship_from_country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False, "canonical": False}, "ekstension": True,
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
        "status": "implemented_partial", "kilder": {"excel": True, "saft": False, "canonical": False}, "ekstension": True,
        "kraeves_af": "Kontrol 36 (triangulation/place-of-supply, kategori 4).",
        "noter": "Samme status som ship_from_country.",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat (VIES-lignende, ikke tjek-ciffer-valideret her)",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Kategori 4 (EU-momsnummerformat), kategori 6 (leverandør/"
                       "kunde-validering, inkl. kontrol 49 CONTROL_REQUIREMENTS) — "
                       "readiness signal-felt.",
        "noter": "",
    },
    {
        "navn": "non_deductible_amount", "type": "number (nullable)", "obligatorisk": False, "format": "DKK",
        "status": "implemented_partial", "kilder": {"excel": False, "saft": True, "canonical": False}, "ekstension": True,
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
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Join-nøgle fra lines[].supplier_id i kategori 4/6/9/11.",
        "noter": "",
    },
    {
        "navn": "name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
        "kraeves_af": "cat04._vat_of() fallback (linje-niveau har forrang); "
                       "kategori 6/9/11 supplier_lookup.",
        "noter": "",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": False}, "ekstension": False,
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
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Join-nøgle fra lines[].customer_id i kategori 12.",
        "noter": "Byggetrin 8/Del C (Bal-godkendt 2026-09-17): den VALGFRIE "
                 "``customers.csv``-sidecar-fil fylder customers[] STRUKTURELT på "
                 "den kanoniske vej (parsers/canonical_masterdata.py). KENDT, ÅBEN "
                 "BEGRÆNSNING (ikke skjult): den seedede gl_entries-mapping bærer "
                 "INGEN customer_id-kolonne på selve transaktionslinjerne — join-"
                 "nøglen ovenfor (lines[].customer_id) er derfor altid \"\" på den "
                 "kanoniske vej, uafhængigt af om customers.csv leveres. "
                 "customers.csv aktiverer dermed IKKE kontrol 94-97 alene (se "
                 "GAP-11 og canonical_masterdata.py's docstring for detaljer).",
    },
    {
        "navn": "name", "type": "string", "obligatorisk": False, "format": "",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte.",
        "noter": "Byggetrin 8/Del C: se customer_id ovenfor for begrænsningen.",
    },
    {
        "navn": "vat_number", "type": "string", "obligatorisk": False, "format": "EU-momsnummerformat",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "Ingen kontrol læser customers[].vat_number direkte i dag — "
                       "cat04's customer_lookup falder kun tilbage til suppliers[] "
                       "(ikke customers[]), og cat12/cat06 (kontrol 52) bruger "
                       "udelukkende linje-niveau vat_number. En reel motorændring "
                       "kræves for at en kontrol skal KONSUMERE feltet — det er "
                       "uden for denne opgaves scope (kun parsere/adapter/kontrakt/"
                       "tests). Feltet er nu leveret strukturelt, klar til brug.",
        "noter": "Trin 3 (GAP-05, lukket): excel_parser har nu en dedikeret "
                 "\"customer_vat_number\"-kolonne-alias, med fallback til den "
                 "generiske \"vat_number\"-kolonne (samme mønster som "
                 "supplier_info). Tom streng, håndteret pænt, når ingen af "
                 "kolonnerne findes. Byggetrin 8/Del C: se customer_id ovenfor for "
                 "den kanoniske vejs join-begrænsning.",
    },
    {
        "navn": "country", "type": "string", "obligatorisk": False, "format": "ISO 3166-1 alpha-2",
        "status": "implemented_partial", "kilder": {"excel": True, "saft": True, "canonical": "partial"}, "ekstension": False,
        "kraeves_af": "cat12._cust_country() — kontrol 94/95/96/97 (e-handel/OSS/"
                       "fjernsalg/digitale ydelser, kategori 12). RETTELSE til "
                       "tidligere version af denne kontrakt: _cust_country() "
                       "tjekker FAKTISK lines[].country FØRST og falder kun "
                       "tilbage til customers[].country, hvis linjens eget "
                       "country-felt er tomt — ikke omvendt.",
        "noter": "Trin 3 (GAP-05, lukket): excel_parser har nu en dedikeret "
                 "\"customer_country\"-kolonne-alias, med fallback til den "
                 "generiske \"country\"-kolonne. Praktisk effekt: kontrol 94-97 "
                 "kunne allerede (før denne rettelse) finde landet via "
                 "lines[].country, når kildefilen har en generisk landekolonne "
                 "(_cust_country()'s linje-niveau-fallback); det ægte gab var "
                 "smallere end oprindeligt dokumenteret — kun de tilfælde, hvor "
                 "linjens eget country-felt er tomt, men en kundespecifik "
                 "land-kolonne findes. Den situation er nu også dækket.",
    },
]

# ---------------------------------------------------------------------------
# 8. SUMMARY
# ---------------------------------------------------------------------------

SUMMARY_FIELDS = [
    {
        "navn": "total_transactions", "type": "int", "obligatorisk": True, "format": ">= 0",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Readiness _MIN_TX_FOR_STATISTIK-tærskel (kategori 8).",
        "noter": "",
    },
    {
        "navn": "total_debit", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol læser summary.total_debit direkte i dag "
                       "(informativt/rapport-niveau).",
        "noter": "Trin 3 (GAP-07, lukket): saft_parser beregner nu total_debit "
                 "deterministisk som summen af transaktionernes total_debit — "
                 "samme beregning som data_adapter.adapt_excel_to_saft bruger.",
    },
    {
        "navn": "total_credit", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Samme som total_debit.",
        "noter": "Trin 3 (GAP-07, lukket): se total_debit.",
    },
    {
        "navn": "total_vat", "type": "number", "obligatorisk": False, "format": "DKK",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Samme som total_debit/total_credit.",
        "noter": "Trin 3 (GAP-07, lukket): beregnet som summen af tax_amount over "
                 "alle linjer, samme beregning som data_adapter.",
    },
    {
        "navn": "period_start", "type": "string", "obligatorisk": False, "format": "ISO-dato",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — rapport-header.",
        "noter": "",
    },
    {
        "navn": "period_end", "type": "string", "obligatorisk": False, "format": "ISO-dato",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
        "kraeves_af": "Ingen kontrol direkte — rapport-header.",
        "noter": "",
    },
    {
        "navn": "currency", "type": "string", "obligatorisk": True, "format": "ISO 4217",
        "status": "implemented", "kilder": {"excel": True, "saft": True, "canonical": True}, "ekstension": False,
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
        "felt": "ekstern_indberettet_momsangivelse", "sti": "(separat sidecar-fil: vat_declarations.json)",
        "status": "implemented_partial",
        "begrundelse": "Kontrol 82 (periode-/rubrikafstemning) — byggetrin 8, Del A/B "
                        "(Bal-godkendt 2026-09-17): AKTIVERET. Findes pr. definition "
                        "ikke i et enkelt-virksomheds-udtræk; kræver stadig eksternt "
                        "input (analytics/vat_declarations.py, "
                        "declarations_version 1.0.0). Uden filen forbliver kontrollen "
                        "'kræver eksterne data' (readiness.EXTERNAL_DATA), uændret "
                        "adfærd for eksisterende input-veje. Rubrik-logikken (ikke "
                        "retningslogik) og DKRC/SERVICE_VAT-mønster-genkendelsen er "
                        "dokumenteret i analytics/categories/cat10_vat_reconciliation.py.",
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
        "felt": "non_deductible_vat_pct", "sti": "tax_table[].non_deductible_vat_pct",
        "status": "implemented_partial",
        "begrundelse": "Feature 83 (delvis fradragsret) + BAL-055-familien "
                        "(momsfradragsbegrænsning, §42) — pr.-KODE ikke-"
                        "fradragsprocent fra kundens egen momsopsætning "
                        "(vat_setup.csv, ext_non_deductible_vat_pct). Den "
                        "kanoniske CSV-vej har ellers INTET fradrags-"
                        "begrænsnings-signal (Deductibles/NonDeductibleAmount "
                        "er transaktionsbårent og findes kun på SAF-T-vejen). "
                        "Hvorfor ikke SAF-T Financial: TaxTable bærer ingen "
                        "fradragsbegrænsnings-konfiguration pr. kode. "
                        "Bal-godkendt 2026-09-17 (kontrakt v0.4.0); kun "
                        "kontrakt+parser i dag — ingen kontrol konsumerer "
                        "feltet endnu.",
    },
    {
        "felt": "allow_non_deductible_vat", "sti": "tax_table[].allow_non_deductible_vat",
        "status": "implemented_partial",
        "begrundelse": "Feature 83 + BAL-055-familien — ledsage-flag til "
                        "non_deductible_vat_pct (om ERP'et automatisk "
                        "bogfører en ikke-fradragsberettiget andel for "
                        "koden; vat_setup.csv, ext_allow_non_deductible_vat). "
                        "Samme SAF-T-begrundelse og status som "
                        "non_deductible_vat_pct.",
    },
    {
        "felt": "vat_calculation_type", "sti": "tax_table[].vat_calculation_type / "
                "transactions[].lines[].vat_calculation_type",
        "status": "implemented_partial",
        "begrundelse": "Kontrol 82 AKTIV (byggetrin 9, Del A, Bal-godkendt "
                        "2026-09-17) — deterministisk mekanisme-flag pr. "
                        "momskode (Normal/Reverse Charge/Full VAT; "
                        "vat_setup.csv, ext_vat_calculation_type), joinet "
                        "både på tax_table[] og lines[] og brugt i "
                        "cat10._purchase_rubric FØR DKRC/SERVICE_VAT-"
                        "navnemønstrene, der ellers er kalibreret til én "
                        "kundes kodenavngivning: 'reverse charge' i feltet + "
                        "Bus.-gruppen (vat_codes' første led) skelner "
                        "indenlandsk RC (-> udgående rubrik) fra RC fra "
                        "udlandet deterministisk; navnemønstret bevares som "
                        "fallback (feltet fraværende) OG som sidste skelnen "
                        "mellem RC-ydelser og RC-varekøb fra udlandet (feltet "
                        "kan ikke alene afgøre den skelnen). Sekundært "
                        "kategori 9 (70-75, ikke konsumeret endnu). Hvorfor "
                        "ikke SAF-T Financial: TaxTable har intet "
                        "mekanisme-flag pr. kode — det native alternativ er "
                        "en fremtidig, eksplicit besluttet StandardTaxCode-"
                        "mapping, som kan overflødiggøre denne ekstension. "
                        "Regressionsverificeret uændret E2E-resultat på "
                        "v4-datasættet efter aktiveringen.",
    },
    {
        "felt": "sales_vat_account / purchase_vat_account / "
                "reverse_charge_vat_account",
        "sti": "tax_table[].sales_vat_account / tax_table[].purchase_vat_account / "
               "tax_table[].reverse_charge_vat_account",
        "status": "implemented_partial",
        "begrundelse": "Kunderapportens 'Momsmotoren'-sektion (byggetrin ~10, "
                        "Bal-godkendt 2026-09-18): pædagogisk tabel der kobler "
                        "momskode -> momskonto -> angivelsens rubrik for "
                        "kunden. Findes ikke i SAF-T Financial TaxTable "
                        "(som ikke bærer kontoreferencer pr. kode) — hentet "
                        "fra vat_setup.csv-sidecarens ext_sales_vat_account/"
                        "ext_purchase_vat_account/ext_reverse_charge_vat_account. "
                        "Ingen aktiv kontrol konsumerer felterne (kun "
                        "rapport-laget).",
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
    {
        "env": "MATERIALITY_VAT_DECLARATION_DKRC_PATTERNS", "kode_navn": "VAT_DECLARATION_DKRC_PATTERNS",
        "default": ["DKRC"], "beskrivelse": "Delstrengs-mønstre der klassificerer en købslinjes "
                       "momskode som indenlandsk omvendt betalingspligt (tælles i UDGÅENDE moms) "
                       "— kontrol 82, byggetrin 8/Del B.",
    },
    {
        "env": "MATERIALITY_VAT_DECLARATION_SERVICE_VAT_PATTERNS", "kode_navn": "VAT_DECLARATION_SERVICE_VAT_PATTERNS",
        "default": ["SERVICE_VAT"], "beskrivelse": "Delstrengs-mønstre der klassificerer en "
                       "købslinjes momskode som RC-ydelser fra udlandet (egen rubrik) — kontrol 82.",
    },
    {
        "env": "MATERIALITY_VAT_DECLARATION_TOLERANCE", "kode_navn": "VAT_DECLARATION_TOLERANCE",
        "default": 1.0, "beskrivelse": "Tolerance (DKK) for periode-/rubrikafstemningen — kontrol 82.",
    },
    {
        "env": "MATERIALITY_CONTROL_22_MIN_BASE", "kode_navn": "CONTROL_22_MIN_BASE",
        "default": 1.0, "beskrivelse": "Materialitets-gulv (DKK) — en kreditlinje under denne "
                       "grænse undertrykkes altid i kontrol 22 (manglende salgsmoms), så rene "
                       "afrundingslinjer (fx 0,01 kr.) ikke flages — 2026-09-18-rettelsen.",
    },
    {
        "env": "MATERIALITY_CONTROL_60_NET_TOLERANCE", "kode_navn": "CONTROL_60_NET_TOLERANCE",
        "default": 0.02, "beskrivelse": "Tolerance (DKK) for at anse to momsbeløb for at "
                       "'summere til nul' i kontrol 60's netting-tjek (samme bilag eller "
                       "reversal-par) — 2026-09-18-rettelsen.",
    },
    {
        "env": "MATERIALITY_CONTROL_60_REVERSAL_WINDOW_DAYS", "kode_navn": "CONTROL_60_REVERSAL_WINDOW_DAYS",
        "default": 31, "beskrivelse": "Antal dage en negativ momslinje og en modsvarende positiv "
                       "postering (samme konto+momskode) må ligge fra hinanden for at tælle som "
                       "et 'reversal-par' i kontrol 60 og dermed undertrykkes — 2026-09-18-rettelsen.",
    },
    {
        "env": "MATERIALITY_REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD",
        "kode_navn": "REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD",
        "default": 100000.0, "beskrivelse": "Kunderapportens kuraterings-seed (tools/"
                       "report_curation.py, byggetrin ~10): en tema-gruppe uden kritiske/"
                       "høje fund forfremmes ('medtag': true) i den auto-seedede kuration, "
                       "når gruppens samlede estimerede beløb når denne tærskel. "
                       "Høj-fund-grupper og timing-temaet forfremmes altid, uanset beløb.",
    },
]

# ---------------------------------------------------------------------------
# KNOWN_GAPS — kendte problemer/uoverensstemmelser, IKKE rettet her (uden for scope)
# ---------------------------------------------------------------------------

KNOWN_GAPS = [
    {
        "id": "GAP-01",
        "status": "aaben",
        "titel": "'or 0.0'-fallback på debit/credit/vat_amount skjuler manglende data",
        "beskrivelse": "data_adapter.adapt_excel_to_saft bruger 'txn.get(...) or 0.0' "
                       "på debit_amount/credit_amount/vat_amount. En reel 0-værdi i "
                       "kilden bliver umulig at skelne fra en manglende kolonne/tom "
                       "celle. Kendt, bevidst uden for scope for denne kontrakt "
                       "(adfærdsændring, ikke beskrivelse) — uændret i trin 3.",
        "beroerte_felter": ["transactions[].lines[].debit_amount",
                             "transactions[].lines[].credit_amount",
                             "transactions[].lines[].tax_amount"],
    },
    {
        "id": "GAP-02",
        "status": "aaben",
        "titel": "tax_base kan være et beregnet skøn uden at være markeret som sådan",
        "beskrivelse": "tax_base falder tilbage fra 'importeret grundlag' til "
                       "'udledt af vat_amount/tax_percentage' til 'debit+credit', "
                       "uden at outputtet mærker hvilken sti der blev brugt. "
                       "Uændret i trin 3.",
        "beroerte_felter": ["transactions[].lines[].tax_base"],
    },
    {
        "id": "GAP-03",
        "status": "delvist_lukket",
        "titel": "accounts[]/lines[] kunne ikke bære account_type + standard_account_id fra Excel-vejen",
        "beskrivelse": "Trin 3 (2026-09-14): COLUMN_ALIASES har nu \"account_type\" "
                       "og \"standard_account_id\"-indgange, så vat_rules."
                       "is_non_vat_account() (momsrelevans-scope, bl.a. kontrol 80) "
                       "KAN aktiveres på Excel/CSV-import, når kildefilen har de "
                       "relevante kolonner. Delvist, ikke helt, lukket: et typisk "
                       "fladt GL-udtræk uden separat kontoplan-fane vil stadig "
                       "mangle kolonnerne, og adfærden forbliver den dokumenterede, "
                       "konservative default (CLAUDE.md: ukendt kontotype -> "
                       "uændret scope) i det tilfælde.",
        "beroerte_felter": ["accounts[].account_type", "accounts[].standard_account_id",
                             "transactions[].lines[].account_type",
                             "transactions[].lines[].standard_account_id"],
    },
    {
        "id": "GAP-04",
        "status": "delvist_lukket",
        "titel": "accounts[].opening_balance/closing_balance var altid 0.0 på Excel-vejen",
        "beskrivelse": "Trin 3 (2026-09-14): COLUMN_ALIASES har nu \"opening_balance\" "
                       "og \"closing_balance\"-indgange. Kontrol 77 "
                       "(vat_account_reconciliation) kan derfor afstemme mod en "
                       "reel saldo på Excel-import, når kildefilen bærer saldi pr. "
                       "konto. Uden kolonnen er værdien None (ikke en stille 0.0) — "
                       "cat10's 'a.get(\"closing_balance\", 0) or 0' giver samme "
                       "\"ingen saldoinformation\"-adfærd som før. Delvist, ikke "
                       "helt, lukket: de fleste flade GL-udtræk vil stadig mangle "
                       "kontosaldi pr. række.",
        "beroerte_felter": ["accounts[].opening_balance", "accounts[].closing_balance"],
    },
    {
        "id": "GAP-05",
        "status": "lukket",
        "titel": "customers[].vat_number og customers[].country var hårdkodet tomme på Excel-vejen",
        "beskrivelse": "Trin 3 (2026-09-14): excel_parser har nu dedikerede "
                       "\"customer_vat_number\"/\"customer_country\"-kolonne-"
                       "aliaser, med fallback til de generiske \"vat_number\"/"
                       "\"country\"-kolonner (samme mønster som supplier_info "
                       "allerede brugte). Fravær af kolonner håndteres pænt (tom "
                       "streng, ikke crash). PRÆCISERING ift. tidligere version af "
                       "denne kontrakt: cat12._cust_country() falder FAKTISK "
                       "tilbage til lines[].country FØR customers[].country (ikke "
                       "omvendt, som tidligere beskrevet her) — det reelle gab var "
                       "derfor smallere end først dokumenteret, men er nu lukket "
                       "for begge stier (linje- og kunde-niveau). Ingen kontrol "
                       "læser customers[].vat_number i dag (se feltets egen note) "
                       "— det er en motor-observation, ikke en resterende "
                       "parser-/adapter-mangel.",
        "beroerte_felter": ["customers[].vat_number", "customers[].country"],
    },
    {
        "id": "GAP-06",
        "status": "aaben",
        "titel": "source_document_id betyder noget forskelligt på de to input-veje — formentlig uløseligt",
        "beskrivelse": "Excel-oprindelse: reelt fakturanummer (invoice_number-"
                       "kolonne). SAF-T-oprindelse: transaktionens "
                       "fritekstbeskrivelse (Transaction/Description). Dette er "
                       "IKKE en parser-mangel, der kan rettes ved bedre mapping — "
                       "det er en strukturel grænse i selve SAF-T Financial-"
                       "skemaet: GeneralLedgerEntries/Transaction har intet "
                       "selvstændigt dokument-/fakturanummer-element på "
                       "transaktions- eller linjeniveau (kun JournalID, "
                       "TransactionID og fritekst-Description); et sådant nummer "
                       "findes typisk kun i et separat SourceDocuments-afsnit i "
                       "SAF-T (fakturaer/betalinger som selvstændige objekter), som "
                       "denne parser ikke joiner mod GL-transaktioner i dag. "
                       "Konsekvens: dubletdetektion (kategori 2, kontrol 11-18) er "
                       "et systematisk svagere signal på SAF-T-oprindelse end på "
                       "Excel-oprindelse (fritekst dedupliker dårligere end et "
                       "fakturanummer), uden at dette er synligt i outputtet. "
                       "Reel lukning kræver enten (a) at parseren udvides til at "
                       "joine SourceDocuments mod GL-linjer via dokument-"
                       "referencer — en ikke-triviel SAF-T-strukturel udvidelse, "
                       "eller (b) et separat, eksplicit svagheds-flag i outputtet "
                       "frem for at overloade source_document_id. Begge er uden "
                       "for denne opgaves scope (kun parsere/adapter/kontrakt/"
                       "tests, ingen motor- eller SAF-T-parser-strukturændringer).",
        "beroerte_felter": ["transactions[].lines[].source_document_id"],
    },
    {
        "id": "GAP-07",
        "status": "lukket",
        "titel": "summary manglede total_debit/total_credit/total_vat på SAF-T-vejen",
        "beskrivelse": "Trin 3 (2026-09-14): saft_parser.parse_saft beregner nu "
                       "total_debit/total_credit/total_vat deterministisk af de "
                       "parsede transaktioner/linjer — samme beregning som "
                       "data_adapter.adapt_excel_to_saft bruger for Excel-vejen. "
                       "summary-objektet er nu symmetrisk mellem de to input-veje.",
        "beroerte_felter": ["summary.total_debit", "summary.total_credit", "summary.total_vat"],
    },
    {
        "id": "GAP-08",
        "status": "delvist_lukket",
        "titel": "header.registration_number/source/saft_version var ikke symmetriske mellem input-veje",
        "beskrivelse": "Trin 3 (2026-09-14): (1) source er nu lukket helt — begge "
                       "veje sætter en reel diagnostikværdi (\"Excel/CSV import\" "
                       "hhv. \"SAF-T XML import\"). (2) registration_number og "
                       "saft_version er nu til stede (tom streng) på BEGGE veje "
                       "— nøglesæt-symmetri, ingen KeyError-risiko — men bærer "
                       "stadig kun en reel værdi på den ene vej (saft_version) "
                       "eller ingen af dem (registration_number, semantisk umuligt "
                       "at udlede pålideligt fra nogen af kilderne i dag). Delvist "
                       "lukket: nøglesæt-symmetri opnået, værdi-symmetri ikke (og "
                       "for registration_number næppe realistisk uden ny "
                       "SAF-T-udtræksfunktionalitet).",
        "beroerte_felter": ["header.registration_number", "header.source", "header.saft_version"],
    },
    {
        "id": "GAP-09",
        "status": "delvist_lukket",
        "titel": "tax_table-poster havde forskellige nøgler afhængigt af oprindelse",
        "beskrivelse": "Trin 3 (2026-09-14): (1) rate er nu lukket helt — "
                       "saft_parser afleder \"rate\" = tax_percentage, så begge "
                       "veje har nøglen med en reel værdi. (2) standard_tax_code/"
                       "country er nu til stede (tom streng) også på Excel-afledte "
                       "poster (data_adapter) — nøglesæt-symmetri — men bærer "
                       "stadig ingen reel værdi dér, da Excel-parseren ikke har "
                       "kildekolonner for momskodens standardkode/land. Delvist "
                       "lukket, samme mønster som GAP-08.",
        "beroerte_felter": ["tax_table[].rate", "tax_table[].standard_tax_code", "tax_table[].country"],
    },
    {
        "id": "GAP-10",
        "status": "delvist_lukket",
        "titel": "Kanonisk vej har ingen momssats — tax_table/lines uden tax_percentage/rate",
        "beskrivelse": "Byggetrin 8 (2026-09-17): den Bal-godkendte BC/NAV-mapping "
                       "(dataextract/mappings/, seedet 2026-09-16) producerer i dag "
                       "KUN den sammensatte ``vat_codes`` (D1-kombinationen) — ingen "
                       "selvstændig kolonne for den numeriske momssats i selve "
                       "gl_entries-CSV'en. canonical_parser.parse_canonical sætter "
                       "derfor stadig tax_percentage=0.0/rate=0.0, HVIS ikke andet "
                       "leveres — bevidst ikke gættet/udledt (jf. §2a designprincip "
                       "5: motoren 'auto-healer' aldrig manglende data). "
                       "DELVIST LUKKET, byggetrin 8/Del C (2026-09-17, Bal-godkendt): "
                       "en VALGFRI sidecar-fil ``vat_setup.csv`` (join-nøgle: "
                       "vat_codes-strengen, samme opake streng som gl_entries' "
                       "vat_codes-kolonne) kan nu joine en reel tax_percentage/rate "
                       "ind i BÅDE tax_table[] og transactions[].lines[] — se "
                       "parsers/canonical_masterdata.py. Kategori 3 (satsvalidering, "
                       "kontrol 19-26) får dermed et reelt grundlag, NÅR filen "
                       "leveres. Delvist, ikke helt, lukket: uden vat_setup.csv "
                       "forbliver adfærden strukturelt 0.0, uændret — filen er "
                       "vat-extracts/klientens leverance, ikke noget motoren kan "
                       "garantere findes. Byggetrin 8/Del A (2026-09-17, "
                       "Bal-godkendt): kontrol 19 (cat03_vat_rate_validation) "
                       "konsumerer nu ``header.vat_setup_loaded``/``tax_table[]."
                       "setup_matched`` direkte og validerer MOD OPSÆTNINGEN pr. "
                       "momskode i stedet for den hardkodede 0/25-liste, når filen "
                       "er indlæst — se felt-dokumentationen for disse to nye "
                       "felter. Løste 642 falske HØJ-fund fra bevidste delvis-"
                       "fradragsret-satser (fx 13,63636%/5,26316%) i kundens VAT "
                       "Posting Setup.",
        "beroerte_felter": ["tax_table[].tax_percentage", "tax_table[].rate",
                             "transactions[].lines[].tax_percentage",
                             "header.vat_setup_loaded", "tax_table[].setup_matched"],
    },
    {
        "id": "GAP-11",
        "status": "delvist_lukket",
        "titel": "Kanonisk vej har ingen stamdata — kontoplan-metadata og leverandør-/kundeoplysninger altid tomme",
        "beskrivelse": "Byggetrin 8 (2026-09-17): den seedede BC/NAV-mapping "
                       "producerer udelukkende ``gl_entries`` (transaktionslinjer) "
                       "— intet target_object for kontoplan (accounts[].account_type/"
                       "standard_account_id/opening_balance/closing_balance), "
                       "leverandører eller kunder (jf. mapping_og_transformation.md's "
                       "'Ét target_object pr. transformations-kørsel'-begrænsning). "
                       "canonical_parser afleder derfor kun accounts[].account_id "
                       "(distinkte gl_accounts) fra selve gl_entries-filen. "
                       "DELVIST LUKKET, byggetrin 8/Del C (2026-09-17, Bal-godkendt): "
                       "to VALGFRIE sidecar-filer kan nu berige dette EFTER "
                       "parsningen (se parsers/canonical_masterdata.py): "
                       "(1) ``chart_of_accounts.csv`` (join-nøgle: kontonummeret) "
                       "joiner account_type/standard_account_id/opening_balance/"
                       "closing_balance ind i BÅDE accounts[] og "
                       "transactions[].lines[] — vat_rules.is_non_vat_account "
                       "(kontrol 80's momsrelevans-scope) KAN dermed aktiveres, når "
                       "filen leveres. (2) ``customers.csv`` fylder den "
                       "SELVSTÆNDIGE customers[]-liste (customer_id/name/"
                       "vat_number/country). KENDT, FORTSAT ÅBEN BEGRÆNSNING for "
                       "kunde-siden (ikke skjult): gl_entries bærer INGEN "
                       "customer_id-kolonne på selve linjerne, så cat12."
                       "_cust_country()'s join (line.get('customer_id')) altid er "
                       "\"\" på den kanoniske vej — customers.csv alene aktiverer "
                       "IKKE kontrol 94-97 (kræver at gl_entries-mappingen også "
                       "leverer en kunde-reference pr. linje, uden for denne "
                       "opgaves scope). Leverandørstamdata (suppliers[]) har fortsat "
                       "ingen sidecar-fil og er derfor uændret altid tom.",
        "beroerte_felter": ["accounts[].account_type", "accounts[].standard_account_id",
                             "accounts[].opening_balance", "accounts[].closing_balance",
                             "transactions[].lines[].account_type",
                             "transactions[].lines[].standard_account_id",
                             "suppliers[]", "customers[]"],
    },
    {
        "id": "GAP-12",
        "status": "delvist_lukket",
        "titel": "Kanonisk vej havde ingen dokument-/bilagsgrupperingsnøgle — kontrol 10 (transaktionsbalance) var strukturelt støjende",
        "beskrivelse": "Byggetrin 8, bekræftet på udviklings-E2E'en mod den "
                       "rigtige BC/NAV-fil (2026-09-17, 125.986 rækker): den "
                       "kanoniske gl_entries-fil har ingen kolonne, der grupperer "
                       "flere linjer til ÉT bilag/dokument (BC's 'Document No.'/"
                       "journal-transaktionsnøgle indgår ikke i den seedede "
                       "mappings producerende felter). canonical_parser pakkede "
                       "derfor HVER RÆKKE som sin egen 1-linjes transaktion (samme "
                       "mønster som data_adapter bruger for et fladt Excel-udtræk "
                       "uden bilagskolonne) — men i modsætning til et typisk fladt "
                       "udtræk, hvor debit/credit ofte allerede er nettet pr. "
                       "linje, er BC/NAV-postering pr. linje ÉNSIDET (enten debit "
                       "eller credit, sjældent begge). Konsekvens: kontrol 10 "
                       "(transaktionsbalance, kategori 1) flagede næsten HVER "
                       "eneste 'transaktion' som ubalanceret — 125.885 kritiske "
                       "fund af 125.986 transaktioner i udviklings-E2E'en, dvs. "
                       "et strukturelt falsk-positivt-mønster, IKKE 125.885 reelle "
                       "bogføringsfejl. "
                       "MITIGERET 2026-09-17, Bal-godkendt: ``canonical_parser`` "
                       "grupperer nu rækker med samme, IKKE-TOMME "
                       "``(invoice_numbers, posting_dates)`` til ÉN transaktion "
                       "med flere ``lines[]``; transaktionens ``total_debit``/"
                       "``total_credit`` summeres over alle linjer i bilaget, "
                       "samme aggregeringskonvention som Excel-/SAF-T-vejen "
                       "(``data_adapter.adapt_excel_to_saft`` / "
                       "``saft_parser.parse_saft``). Ingen ændring af "
                       "kontrollogikken i ``analytics/categories/`` — kun "
                       "parserens transaktionsopbygning. Empirisk verificeret på "
                       "den rigtige BC/NAV-fil (samme fil som ovenfor): "
                       "afstemningsgaten forbliver 208/208 konti afstemt (uændret "
                       "-- afstemning er linje-/kontobaseret, ikke "
                       "transaktionsbaseret, så grupperingen kan pr. konstruktion "
                       "ikke ændre kontosummerne), og kontrol 10-fundene falder "
                       "markant. KENDT BEGRÆNSNING (fortsat åben, bevidst): "
                       "rækker med tomt/manglende ``invoice_numbers`` grupperes "
                       "ALDRIG på tværs af rækker -- der er ingen evidens for en "
                       "sammenhæng uden bilagsnummer, så disse forbliver hver sin "
                       "1-linjes transaktion og kan derfor stadig give kontrol "
                       "10-støj. Et residual af kontrol 10-fund efter gruppering "
                       "kan derudover være ÆGTE (bilag der reelt ikke balancerer) "
                       "-- se rapporten fra E2E-verifikationen for den empiriske "
                       "vurdering af residualets sandsynlige natur og en "
                       "eventuel anbefaling om at justere nøglen yderligere.",
        "beroerte_felter": ["transactions[].total_debit", "transactions[].total_credit",
                             "transactions[].lines[].record_id"],
    },
    {
        "id": "GAP-13",
        "status": "lukket",
        "titel": "Kanonisk vej læste ikke description — kontrol 4 flagede systematisk 50.479 bilag",
        "beskrivelse": "Medium-fund-analysen af den fulde E2E på den rigtige "
                       "BC/NAV-fil (2026-09-17, 114.575 medium-fund) viste at "
                       "kontrol 4 (faktura-feltfuldstændighed) alene stod for "
                       "50.479 fund — ALLE bilag — fordi canonical_parser aldrig "
                       "læste en description-værdi (hardkodet \"\" på både "
                       "linje- og transaktionsniveau), uafhængigt af om kilden "
                       "reelt havde en beskrivelse. LUKKET 2026-09-17, "
                       "Bal-godkendt: canonical_parser læser nu en valgfri "
                       "description-kolonne (fraværende kolonne -> fortsat \"\", "
                       "ingen crash, jf. best-effort-designprincippet), og "
                       "sætter den PRÆCIS som Excel-/SAF-T-vejen på begge "
                       "niveauer. Motor-siden er dermed fuldt lukket; om den "
                       "kanoniske FIL rent faktisk bærer kolonnen er "
                       "vat-extracts mapping-ansvar (uden for denne opgaves "
                       "scope — hovedsessionen orkestrerer synkroniseringen). "
                       "Suppleret af en Del B-gating i analytics/readiness.py: "
                       "så længe kolonnen mangler i kilden (v2-filer), "
                       "rapporterer kontrol 4's Description-delcheck 'ikke "
                       "målbar' ÉN gang i stedet for at generere støj pr. bilag "
                       "— en anden, tests-uafhængig sikkerhedslinje mod at "
                       "denne 50.479-regression kan gentage sig med et andet "
                       "strukturelt fraværende felt.",
        "beroerte_felter": ["transactions[].description", "transactions[].lines[].description"],
    },
]
