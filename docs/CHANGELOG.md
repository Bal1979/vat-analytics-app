# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

## GAP-12 mitigeret: bilagsgruppering i den kanoniske parser — 2026-09-17 (ikke-katalog)
Bal-godkendt opfølgning på byggetrin 8 (se afsnittet nedenfor). Kontrol 10
(transaktionsbalance, kategori 1) gav 125.885 strukturelle falsk-positive fund
af 125.986 kanoniske rækker på den rigtige BC/NAV-fil, fordi hver GL-linje blev
sin egen 1-linjes transaktion — BC/NAV-poster balancerer PR. BILAG, ikke pr.
linje, og bilagsnøglen (Entry No./Transaction No.) er ikke med i den seedede
mapping. Kun `backend/parsers/canonical_parser.py` + rapportmetadata ændret —
ingen ændring i `analytics/categories/*.py`-kontrollogikken.

- **`canonical_parser.py`:** rækker med samme, IKKE-TOMME
  `(invoice_numbers, posting_dates)` samles nu til ÉN transaktion med flere
  `lines[]` (ny `_group_key`/`_build_transaction`). Rækker med tomt/manglende
  `invoice_numbers` grupperes ALDRIG på tværs af rækker (heller ikke ved samme
  dato) — ingen gættet sammenhæng uden evidens; de forbliver hver sin
  1-linjes transaktion med det gamle `ROW-<rækkenr>`-id. En grupperet
  transaktions id er deterministisk (`DOC_<invoice>_<dato>`).
  `total_debit`/`total_credit` summeres over gruppens linjer — samme
  aggregeringskonvention som `data_adapter.adapt_excel_to_saft` og
  `saft_parser.parse_saft`. Hver linje bærer nu et canonical-only
  `source_row`-felt (oprindeligt CSV-rækkenummer) for lineage, uafhængigt af
  gruppering.
- **Empirisk verifikation på den rigtige BC/NAV-fil (125.986 rækker),
  før/efter:**

  | Nøgletal | Før gruppering | Efter gruppering |
  |---|---|---|
  | Transaktioner | 125.986 | 50.479 (Ø 2,5 linjer/bilag) |
  | Kontrol 10 (kritisk) | 125.885 | **0** |
  | Høj/medium/lav (øvrige) | 3.071 / 198.505 / 18.622 | 2.366 / 114.575 / 12.037 |
  | Fund i alt | 346.083 | 128.978 |
  | Afstemningsgate | afstemt, 208/208 | afstemt, 208/208 (uændret) |
  | total_debit/total_credit/total_vat | uændret | uændret (identisk til øre) |
  | Køretid (parsing+analyse) | 6,1 s | 3,7 s |

  Afstemningsgaten er upåvirket, fordi den summerer netto debit−credit PR.
  KONTO over alle linjer uafhængigt af transaktionsgruppering — grupperingen
  kan pr. konstruktion ikke ændre kontosummerne. Residual-analyse: på denne
  fil havde ALLE 125.986 rækker et udfyldt bilagsnummer (0 ungrupperede
  `ROW-`-transaktioner), og samtlige 50.479 grupperede bilag balancerer —
  kontrol 10-residualet er 0 fund, ikke bare reduceret. Den kendte
  begrænsning (tomt bilagsnummer grupperes aldrig) er derfor uprøvet på
  denne konkrete fil, men forbliver dokumenteret som åben i GAP-12.
- **`tools/data_contract_data.py`:** GAP-12 opdateret fra `aaben` til
  `delvist_lukket` — mitigeringen, nøglen og den kendte begrænsning
  (tomme bilagsnumre) dokumenteret. `catalog/data_contract.json`
  regenereret (uændret v0.2.0/69 felter — dette er parser-adfærd, ikke
  kontraktform, så ingen version-bump).
- **Tests:** 5 nye scenarier i `tests/test_canonical_parser.py` (flere linjer
  pr. bilag inkl. korrekt document_date/aggregater, tomme bilagsnumre
  grupperes aldrig, samme bilagsnr. på to datoer = to transaktioner, samt at
  kontrol 10's tidligere falsk-positiv forsvinder efter gruppering). 256/256
  tests grønne (op fra 251), 98/98 uafhængig validering grøn, katalog-/
  kontrakt-drift-gates grønne.

## Kanonisk ingestion-vej + afstemningsgate — 2026-09-17 (ikke-katalog)
Byggetrin 8 i den aftalte rækkefølge (`balai-platform/BALAI-dataflow-arkitektur.md`
§2a/§7, Bal-godkendt 2026-09-17). Tredje input-vej ved siden af Excel/CSV og
SAF-T XML — output fra vat-extracts deterministiske `dataextract.transform`
(mapping-lager, Bal-godkendte mappings pr. schema-fingerprint) kan nu analyseres
uden en mellemliggende SAF-T XML-oversætter. Kun parsere/gate/rapportmetadata/
CLI — ingen ændring af `analytics/categories/*.py`-kontrollogikken.

- **`backend/parsers/canonical_parser.py` (ny):** læser kanonisk gl_entries-CSV
  (+ valgfri `transform_summary.json`-sidecar for lineage) og bygger samme
  kontrakt-struktur som de to andre veje. Separator-agnostisk over for
  `vat_codes` (D1-kombinationen) — behandles altid som en opaque streng, aldrig
  splittet, uanset om vat-extract bruger `/` eller `|`. Én række = én
  transaktion = én linje (samme mønster som `data_adapter` for Excel-vejen).
  `document_date` afledes af `tax_point`-kolonnen (nærmeste kanoniske proxy for
  transaktionsdato adskilt fra bogføringsdato), med fallback til `posting_dates`.
- **`backend/parsers/upload_router.py`:** ny routing-gren `is_canonical()` —
  `.csv`/`.tsv` OG header indeholder de kanoniske markørkolonner
  (`gl_accounts`/`vat_codes`/`posting_dates`). Ingen kollision med et fladt
  Excel/CSV-udtræks kolonnenavne (`excel_parser.COLUMN_ALIASES` bruger andre
  navne). SAF-T-routing uændret, tjekkes først.
- **`catalog/data_contract.json` v0.2.0 (fra v0.1.0):** ny `kilder.canonical`
  pr. felt (69 felter, op fra 67 — to nye lineage-felter på `header`:
  `mapping_version`, `schema_fingerprint`). To nye `known_gaps`: **GAP-10**
  (kanonisk vej har ingen momssats — `tax_table`/`lines[].tax_percentage`
  altid 0.0, da den seedede BC/NAV-mapping ikke leverer en selvstændig
  satskolonne) og **GAP-11** (kanonisk vej har ingen kontoplan-/leverandør-/
  kundestamdata — `suppliers`/`customers` altid tomme, `accounts[]` kun
  `account_id`). En TREDJE ny gap tilføjet efter udviklings-E2E'en (se
  nedenfor): **GAP-12** (kontrol 10/transaktionsbalance er strukturelt
  støjende på denne vej, ingen bilagsgrupperingsnøgle). Alle tre dokumenterer
  reelle, kendte begrænsninger i dagens vat-extract-mapping/kilde-data, ikke
  fejl i denne parser.
- **`tests/test_data_contract_conformance.py`:** udvidet med den kanoniske vej
  (egen fixture + assert-helper, der bevidst SPRINGER objekter over, hvor
  kontrakten lover kanonisk-vejen intet felt — fx `suppliers`/`customers`,
  jf. GAP-11 — frem for at tvinge fabrikerede stamdata ind i fixturen).
- **`backend/analytics/reconciliation_gate.py` (ny):** afstemningsgate, §8.3
  ("afstemt mod kontroltotaler"). Sammenligner NETTO `debit_amount -
  credit_amount` pr. `account_id` mod en ekstern kontroltotal-fil
  (`{reconciliation_version, source, generated, accounts: [{account_id,
  amount}]}` — aftalt snitflade med vat-extract). Konfigurerbar tolerance
  (default 0,01 DKK). Blokerer IKKE analysen ved brud (v1-adfærd) — stempler
  rapporten tydeligt "IKKE_AFSTEMT" med detaljer pr. konto; ingen fil givet =
  "afstemning_ikke_udfoert". Teknisk fejl (ugyldig/manglende fil) rapporteres
  adskilt fra et fagligt afstemningsbrud.
- **`backend/tools/analyze_canonical.py` (ny CLI):** offline-kørsel af HELE
  motoren (`run_all_tests`, alle analyse-moduler som CLI-default — modsat
  webappens produktions-default på kun momskernen) på en kanonisk fil, uden
  webserver. Stempler rapporten med `catalog_version` (fra `catalog/rules.json`),
  `data_contract_version`, `mapping_version` og `schema_fingerprint`. Kør:
  `python tools/analyze_canonical.py <gl_entries.csv> [--reconciliation <json>]
  [--tolerance 0.01] [--modules alle|default|<liste>] --out <rapport.json>`.
- **Udviklings-E2E (2026-09-17):** kørt lokalt mod den rigtige BC/NAV-fil via
  `tools/analyze_canonical.py` (125.986 kanoniske rækker, gammel `/`-separator
  — den friske kørsel med den nye `|`-separator orkestreres separat efter
  vat-extracts parallelspor). Lineage stemplet korrekt (mapping_version 1.0.0,
  fuld schema_fingerprint matcher transform_summary.json). Parsing 0,28 s,
  analyse 5,57 s (alle 5 moduler/103 kontroller, ingen sprunget over). 346.083
  fund i alt — heraf **GAP-12 opdaget og dokumenteret pga. denne kørsel**:
  125.885 af de 125.885 kritiske fund kommer fra kontrol 10
  (transaktionsbalance), fordi hver CSV-række i dag bliver sin egen 1-linjes
  "transaktion" uden modpostering (se GAP-12). Rapport-JSON gemt i scratchpad,
  IKKE i repoet (kundedata i `all_findings`). Ingen kundedata i denne log.
- **Tests:** 29 nye (`test_canonical_parser.py` 12, `test_reconciliation_gate.py`
  10, `test_analyze_canonical_cli.py` 5, `test_data_contract_conformance.py`
  +2). Fuld suite: **251 tests**, alle grønne + uafhængig valideringssuite
  (98/98) grøn.

## Forsoning af input-veje mod datakontrakten — 2026-09-14 (ikke-katalog)
Trin 3 i byggerækkefølgen (`balai-platform/BALAI-dataflow-arkitektur.md` §7),
mod `catalog/data_contract.json` v0.1.0's ni `known_gaps`. Kun parsere/adapter/
kontrakt/tests — ingen ændring af `analytics/categories/*.py`-kontrollogikken.

- **GAP-05 (lukket):** `excel_parser.py` har nye kolonne-aliaser
  `customer_country`/`customer_vat_number` (med fallback til de generiske
  `country`/`vat_number`-kolonner, samme mønster som leverandøren allerede
  brugte) — kundens land/momsnummer kan nu leveres på Excel-vejen, hvilket gør
  kontrol 94-97 (e-handel/OSS/fjernsalg, modulet `ehandel_saerordninger`)
  reelt mulige der. Fravær af kolonner giver stadig tom streng, ikke crash.
  Præcisering undervejs: `cat12._cust_country()` falder rent faktisk allerede
  tilbage til linjens eget `country`-felt før `customers[].country` — det
  oprindeligt dokumenterede gab var smallere end først antaget.
- **GAP-04 (delvist lukket):** nye aliaser `opening_balance`/`closing_balance`
  på Excel-vejen — kontrol 77 (momskonto-afstemning) kan afstemme mod en reel
  saldo, når kildefilen bærer den, i stedet for en konstant nulsaldo. Værdien
  er `None` (ikke en stille 0.0), når kolonnen mangler — uændret kontrol-
  adfærd, da `test_77` allerede har et `or 0`-fallback.
- **GAP-03 (delvist lukket):** nye aliaser `account_type`/`standard_account_id`
  på Excel-vejen (både `accounts[]` og de joinede `transactions[].lines[]`) —
  momsrelevans-scopet (`vat_rules.is_non_vat_account`, kontrol 80) kan
  aktiveres på Excel-import, når kildefilen har kontoplan-kolonnerne. Uændret,
  konservativ default (CLAUDE.md) når kolonnerne mangler.
- **GAP-07 (lukket):** `saft_parser.parse_saft` beregner nu
  `summary.total_debit/total_credit/total_vat` deterministisk af de parsede
  transaktioner/linjer — samme beregning som `data_adapter`. `summary` er nu
  symmetrisk mellem input-vejene.
- **GAP-08/09 (delvist lukket):** `header.source` sat på begge veje (reel
  værdi, ikke placeholder); `tax_table[].rate` afledt af `tax_percentage` på
  SAF-T-vejen (reel værdi); `header.registration_number/saft_version` og
  `tax_table[].standard_tax_code/country` er nu til stede (tom streng) på
  begge veje for nøglesæt-symmetri (ingen KeyError-risiko), men uden reel
  værdi på den vej, hvor det er semantisk umuligt at udlede.
- **GAP-06 (uændret, uløseligt i dag):** dokumentationen er skærpet —
  SAF-T Financial GeneralLedgerEntries har intet selvstændigt dokument-id på
  transaktions-/linjeniveau; en reel lukning kræver en SAF-T-strukturel
  udvidelse (join mod `SourceDocuments`), som er uden for denne opgaves scope.
- **Skema-konformans som test (ny):** `tests/test_data_contract_conformance.py`
  validerer, at BEGGE parser-veje producerer output, der er konformt med
  `catalog/data_contract.json` (felter til stede, typer korrekte) på
  repræsentative fixtures. Ingen runtime-håndhævelse i parserne — kontrakten
  forbliver deskriptiv (jf. §7, byggetrin 2).
- `tools/data_contract_data.py` opdateret: `kilder`-flag flyttet til `true`
  for de nu reelt leverbare felter, `known_gaps` fik et `status`-felt
  (`aaben`/`delvist_lukket`/`lukket`) og opdateret tekst; kontrakten
  regenereret (`python tools/build_data_contract.py`).

## Maskinlæsbar datakontrakt — 2026-09-14 (ikke-katalog)
- **`catalog/data_contract.json` (v0.1.0):** motorens fulde inputkontrakt —
  7 objekter (`header/accounts/tax_table/transactions+lines/suppliers/
  customers/summary`), 67 felter — udledt af en ny hånd-vedligeholdt single
  source `tools/data_contract_data.py` via `tools/build_data_contract.py`
  (samme mønster som `build_rules_catalog.py`). Drift-gated
  (`tests/test_data_contract_fresh.py`), inkl. et krydstjek mod
  `analytics/readiness.py`'s signal-felter og et drift-tjek af
  `MATERIALITY_*`-env-navne mod `analytics/materiality.py`.
- **`balai_extensions`-afsnit:** eksplicit markering af felter der ikke er
  native SAF-T Financial (jf. `balai-platform/BALAI-dataflow-arkitektur.md`
  §2a) — ship_from/to_country, document_date, non_deductible_amount, samt en
  endnu delvist implementeret version-triple (erklæret/strukturelt
  detekteret/mål).
- **`run_config`-afsnit:** `ANALYTICS_MODULES` (introspektion af
  `analytics/modules.py`, aldrig hånd-duplikeret) og `MATERIALITY_*`-tærskler.
- **`known_gaps`-afsnit:** ni konkrete, evidensbaserede uoverensstemmelser
  mellem Excel- og SAF-T-input-vejene, opdaget under kortlægningen (fx
  `customers[].vat_number/country` hårdkodet tomme på Excel-vejen; `summary`
  mangler total_debit/credit/vat på SAF-T-vejen; `source_document_id` betyder
  fakturanummer vs. transaktionsbeskrivelse afhængigt af oprindelse). Ingen af
  disse er rettet — kontrakten er bevidst deskriptiv i denne omgang, ikke
  håndhævende (næste byggetrin i dataflow-arkitekturen).
- Bevidst UDEN FOR scope: ingen ændring af parser-adfærd (fx `or 0.0`-
  fallbacks) og ingen runtime-validering/pydantic — se `known_gaps`.

## Præsentation & scoring — 2026-09-09 (ikke-katalog)
- **Datagrundlag/kørbarhed:** `analytics/readiness.py` afgør pr. kontrol
  kørt / sprunget over (manglende felt) / modul fra / kræver eksterne data;
  vist som panel i UI. Kategorier uden kørte kontroller vises ikke længere som
  grøn «bestået», men som grå «Ikke kørt».
- **SAF-T-input i UI:** filvælger + tekster accepterer nu `.xml` (SAF-T).
- **Distinkt netto som overskrift:** de transaktions-deduplikerede beløb er nu
  hovedtallet; brutto (kan overlappe) vises som kontekst — undgår oppustede tal.
- **«Handling krævet»-panel:** kritiske + høje fund vises prioriteret øverst.
- **Score-kalibrering:** loft pr. severity-tier (`materiality.SEVERITY_PENALTY_CAPS`,
  MATERIALITY_CAP_*), så mange lav-/medium-fund ikke alene tvinger en kategori i
  rød — RØD drives af kritiske/høje fund. Bekræftet: Timing 6 → 69.
- **Auth-fix:** Starlette-session bruger egen cookie (`vat_local`) og central_auth
  prøver alle `session`-cookies — løser 401 på upload.

## Katalog v1.1.0 — 2026-09-08
**Momsrelevans-slankning + scope + central-auth-oprydning.**

- **Analyse-moduler (`analytics/modules.py`):** de 103 kontroller delt i moduler.
  **Momskernen (60) default TIL**; `forensic_statistik` (26), `ehandel_saerordninger`
  (10), `datakvalitet` (4), `dublet_recovery` (3) default FRA. Motoren filtrerer
  findings til aktive moduler før rapporten bygges (intet slettes; alt kan tændes via
  `ANALYTICS_MODULES` eller pr. kørsel). Keep/cut følger berigelsesnotatet.
  Kataloget bærer nu `analyse_modul` + `default_aktiv` pr. kontrol + en
  `analyse_moduler`-oversigt. Valideringssuiten kører med alle moduler tændt.
- **Momsrelevans-scope (fundament):** `vat_rules.is_non_vat_account` undertrykker
  momsfund på balancekonti (SAF-T `AccountType`); kun aktivt ved kendt kontotype
  (uændret for fladt Excel). `data_adapter` bærer `account_type` med på linjen.
  Wiret i kontrol 80. Real-data-fund: klientfiler mislabeler `AccountType="Other"`
  → robust scope kræver `StandardAccountID`/standardkontoplan (SAF-T-parser-sporet).
- **Central auth-tests:** `tests/test_auth.py` omskrevet til den centrale
  BALAI-brugerstyring (redirect/401 i stedet for lokalt setup/login/CSRF), efter at
  login/setup/admin er flyttet til `auth.balai.dk`.
- **Recovery:** `analytics/materiality.py` (og momsrelevans-scope-filerne) var
  utilsigtet ucommitteret; nu bragt i repoet (engine importerede dem allerede).
- **Miljø:** lokal `venv` løftet til Python 3.13 (matcher CI/Railway).
- **SAF-T-parser (produktions-input):** `parsers/saft_parser.py` mapper SAF-T
  Financial (DK v1.0/2.0/2.1) til den kanoniske struktur — best-effort (kører også
  på ugyldig/fejlmærket SAF-T), namespace-agnostisk, XML-hærdet (DOCTYPE/ENTITY
  afvist). `parsers/upload_router.py` router upload på filendelse (`.xml`) eller
  indholds-sniff; `main.py` kalder kun routeren (Excel-sti uændret). Linjen bærer
  nu `standard_account_id` (fundament for robust scope, increment B). Testdækket i
  `tests/test_saft_parser.py`.
- **Robust momsrelevans-scope (increment B):** `analytics/standard_accounts.py`
  klassificerer `StandardAccountID` som balance (≥ 5000) eller resultat (1000–4999)
  ud fra ERST-standardkontoplanens sektions-headere. `vat_rules.is_non_vat_account`
  bruger nu dette signal ud over `AccountType`, så balanceposter undertrykkes korrekt
  på rigtige filer, hvor `AccountType` er fejlmærket "Other". Kontrol 80; bekræftet
  på den fejlmærkede v1.0-fil. Testet i `tests/test_standard_accounts.py` +
  udvidet `tests/test_vat_scope.py`.

## Katalog v1.0.0 — 2026-06-16
Første versionerede regelkatalog, auto-genereret fra de 103 kontrolfunktioner.

**EY-løft gennemført i denne runde:**
- **Sikkerhed/auth:** hardcodede default-credentials fjernet (kode + git-historik håndteret); session-auth porteret til FastAPI (/setup, invitationer, pbkdf2, login-rate-limit, CSRF, timing-sikkert login); stram CSP uden CDN'er + fuld sikkerhedsheader-pakke; UI gjort CSP-rent (self-hostet JS/CSS, event-delegation).
- **Versionsstyring:** hele analysemotoren (cat03–cat12, vat_rules) bragt under git.
- **Regelkatalog & sporbarhed:** versioneret `rules.json` + `rule_notes.json` (kilde/test/beslutninger) auto-genereret fra koden; sporbarhedsmatrix + dækningsrapport.
- **Uafhængig valideringssuite:** ren/defekt-scenarie pr. aktiv kontrol — 98/98 aktive kontroller dækket, gated i CI.
- **Importkontrakt:** udvidet med `ship_from_country`/`ship_to_country`, adskilt `document_date` (faktura) og importerbar `tax_base`; Data Extract-aliaser tilføjet; drevet af delt feltkontrakt.
- **Kontroller aktiveret:** 36 (place-of-supply/trekantshandel) og 46 (faktura/bogførings-lag). 5 kontroller (82, 83, 85, 90, 99) bevidst inaktive med dokumenteret beslutning.
- **CI:** GitHub Actions — pytest + katalogvalidering + valideringssuite (gated) + pip-audit; runtime/test-deps adskilt og pinnet; sårbarheder lukket (fastapi/jinja2/python-multipart/starlette bumpet).
- **Drift:** `railway.json` (1 worker / 1 replica til in-memory jobs); migreret US→EU (EU West); persistent volumen + SECRET_KEY + AUTH/AUDIT_DB_PATH; ikke-arkiv-datapolitik (input slettes pr. kørsel).
- **Dokumentationspakke:** 4 docx + matrix + rapporter (denne pakke).

**Åbent (se Opfoelgningspunkter.md):** faglig pinning af præcis momslov-kilde pr. kontrol; stress-test på rigtige klientdata; EY-platform + DPA; features for 82/83/90; EU-migration af søsterværktøjer.
