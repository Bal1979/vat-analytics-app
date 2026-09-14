# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

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
