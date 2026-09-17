# VAT Analytics — projektkontekst (agent hand-off)

> **Tværgående standarder (LÆS FØRST):** Ved nye værktøjer eller ændringer der
> rører login, design eller drift på tværs, læs `balai-platform/PLATFORM-BUILD-STANDARD.md`
> før du går i gang. Den fastlægger bl.a., at `SECRET_KEY` SKAL være **identisk**
> på tværs af alle værktøjer (delt login), at cookien deles på `.balai.dk`, og
> hvordan et værktøj kobles på den centrale brugerstyring.

Kontinuitets-/hand-off-note: hvor projektet er, hvorfor valgene blev truffet, og
hvad der er åbent. Hold den opdateret ved væsentlige ændringer. Tool-specifik
godkendelses-dokumentation ligger i `docs/` — herunder `docs/TTAR-afklaring-og-mapping.md`,
der mapper EY's TTAR-godkendelsesramme til vores evidens (spejlet på SAF-T-sporet).

## Hvad er det

Web-værktøj (FastAPI-backend + reel UI i `backend/templates/index.html`) der kører
momsanalyse mod et momsdataudtræk (Excel/CSV i dag; SAF-T på vej). 103 kontroller
i 12 kategorier klassificeret efter impact-type, retning og sværhedsgrad. Mål:
EY-godkendt produkt — søsterprojekt til SAF-T Validator, VIES Validator og Data
Extract, som auth/design/dokumentationsmønstre genbruges fra.

Resultatfilosofi (vigtig): **RØD = handling krævet** — ingen falske alarmer
(jf. VIES: 37 røde → 4 reelle). Konservativ mod falske negativer. Prioriteret
handlingsliste, ikke en mur af flag.

## Status (pr. 2026-09-08)

- Regelkatalog **v1.1.0**. 103 kontroller (98 aktive; 82, 83, 85, 90, 99 bevidst
  inaktive med dokumenteret beslutning).
- **Momsrelevans-slankning (analyse-moduler):** de 103 kontroller er delt i moduler.
  **Momskernen (60 kontroller) er default TIL**; resten (43) ligger i moduler der er
  **default FRA**: `forensic_statistik` (26), `ehandel_saerordninger` (10),
  `datakvalitet` (4), `dublet_recovery` (3). En standardkørsel viser altså kun
  momskernen. Intet er slettet — alt kan tændes igen (se Arkitektur).
- **Momsrelevans-scope (robust):** en momskontrol undertrykker kun et fund, når
  linjen positivt er en balancekonto — via TO uafhængige signaler: SAF-T
  `AccountType` (asset/liability/equity) ELLER `StandardAccountID` ≥ 5000
  (standardkontoplanens balance-sektion, `analytics/standard_accounts.py`). Ukendt
  på begge (fladt Excel uden kontoplan) → uændret adfærd. Aktivt for kontrol 80.
  StandardAccountID-signalet løser real-data-fundet: klientfilerne mislabeler
  `AccountType="Other"` på ~93–100% af konti, men `StandardAccountID` er udfyldt og
  pålideligt.
- **SAF-T-input (produktion):** værktøjet accepterer nu også dansk SAF-T Financial
  (`.xml`, v1.0/2.0/2.1) ved siden af Excel/CSV — se `parsers/saft_parser.py` +
  `upload_router.py`. Best-effort, XML-hærdet.
- **Kanonisk CSV-input (byggetrin 8, 2026-09-17):** TREDJE input-vej —
  `parsers/canonical_parser.py` læser output fra vat-extracts deterministiske
  `dataextract.transform` (Bal-godkendt mapping pr. schema-fingerprint) direkte,
  uden en mellemliggende SAF-T-oversætter. Kendte gaps (GAP-10/GAP-11 i
  `catalog/data_contract.json`): ingen momssats, ingen kontoplan-/leverandør-/
  kundestamdata på denne vej i dag. Ny afstemningsgate
  (`analytics/reconciliation_gate.py`, §8.3) + offline-CLI
  (`tools/analyze_canonical.py`) kører hele motoren uden webserver.
- **Bilagsgruppering (GAP-12, 2026-09-17, Bal-godkendt):** `canonical_parser`
  grupperer nu rækker med samme, ikke-tomme `(invoice_numbers, posting_dates)`
  til ÉN transaktion med flere `lines[]` (bilagsnøgle — BC/NAV's Entry
  No./Transaction No. indgår ikke i den seedede mapping). `total_debit`/
  `total_credit` summeres over linjerne, samme konvention som Excel-/SAF-T-vejen.
  Rækker med tomt bilagsnummer grupperes ALDRIG (kendt, bevidst begrænsning —
  gætter ingen sammenhæng uden evidens). Empirisk verificeret på den rigtige
  BC/NAV-fil (125.986 rækker): kontrol 10 (transaktionsbalance) faldt fra
  125.885 kritiske falsk-positive til **0** fund (denne fils rækker havde alle
  udfyldt bilagsnummer); 125.986 → 50.479 transaktioner (Ø 2,5 linjer/bilag);
  afstemningsgaten uændret 208/208 (afstemning er konto-/linjebaseret, ikke
  transaktionsbaseret). GAP-12 status `delvist_lukket` i `data_contract_data.py`.
- **256 automatiserede tests** (251 + 5 nye GAP-12-grupperingstests) +
  uafhængig valideringssuite (**98/98 aktive kontroller**, én plantet defekt
  pr. kontrol, gated i CI).
- Central BALAI-brugerstyring (login/setup/admin ligger IKKE lokalt længere).
- Deployet på Railway (projekt `airy-light`, service → vat.balai.dk, EU West,
  1 worker / 1 replica pga. in-memory jobs).

## Genoptag hurtigt

```bash
cd backend
source venv/bin/activate                       # Python 3.13-baseline
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest -q                            # 169 tests
python tools/build_rules_catalog.py            # catalog/rules.json (drift-gated)
python tools/build_data_contract.py            # catalog/data_contract.json (drift-gated)
python -m validation.run_validation            # 98/98 uafhængig validering
```
Bemærk: Railway auto-deployer ved `git push`. Bal kører pytest lokalt og
committer/pusher (SSH ligger kun på hans Mac).

## Arkitektur

- **`backend/analytics/` er AL forretningslogik** (testbar uden netværk):
  - `engine.py`: orkestrering. `run_all_tests`/`run_analytics` kører hver
    kategori-runner → samler findings → **filtrerer til aktive analyse-moduler** →
    `build_report`. `distinct_amount()` giver transaktions-deduplikerede beløb
    (brutto vs. distinkt — undgår dobbelttælling). Rapporten bærer `moduler`
    (oversigt) + `filtrerede_fund`.
  - `modules.py`: **analyse-modul-registeret.** `CONTROL_MODULE` mapper hver
    kontrol (1–103) til ét modul; `MODULES` har metadata + `default_active`.
    `resolve_active_modules(override)` bestemmer aktive moduler i rækkefølgen
    **override (pr. kørsel) → `ANALYTICS_MODULES` env → default (kun momskernen)**.
    `ANALYTICS_MODULES=alle` tænder alt. Keep/cut følger berigelsesnotatet
    (momskernen beholder 11/16/17, 36/46, 49, 57/59, 72/74, 81, 82/83).
    Valideringssuiten kører med ALLE moduler (`all_module_keys()`), så den
    validerer hver kontrol uafhængigt af produktions-default.
  - `vat_rules.py`: linje-helpers og momsregler. `is_non_vat_account(line)` =
    momsrelevans-scopet: True hvis `AccountType` ∈ {asset,liability,equity} ELLER
    `StandardAccountID` ≥ 5000 (via `standard_accounts.is_balance_account`); begge
    ukendte → False = uændret. `normalize_country`, `is_foreign`, satser m.v.
  - `standard_accounts.py`: StandardAccountID → nature (balance ≥ 5000 / resultat
    1000–4999), fra ERST-standardkontoplanens sektions-headere. Robust scope-signal
    når `AccountType` er fejlmærket.
  - `materiality.py`: severity-vægte + centrale tærskler (kontantgrænse,
    godkendelsesgrænser, købs-/salgsmoms-forhold, faktura-lag, fjernsalgstærskel,
    stort momsbeløb-uden-bilag). Env-overstyrbare via `MATERIALITY_*` (defaults =
    hidtidig adfærd). Wiret i engine + cat05/07/10/12. Note: cat07/cat12 binder
    værdien ved import (env sættes ved opstart).
  - `categories/cat01..cat12`: de 103 `test_NN`-funktioner. `models.make_finding`
    er den fælles finding-konstruktion.
- **`parsers/`**: to input-kilder, samme kanoniske output:
  - `excel_parser.py` (fladt udtræk + kolonne-alias) → `data_adapter.py`
    (adapterer fladt Excel til den SAF-T-lignende struktur motoren forventer:
    `txn["lines"]` med tax_code/percentage/base/amount, `account_type` fra
    kontoplanen båret med på linjen, period/år udledt).
  - `saft_parser.py` (**SAF-T XML → kanonisk struktur, produktion**): best-effort,
    namespace-agnostisk (localname), tolerant over for ugyldig/fejlmærket SAF-T,
    XML-hærdet (DOCTYPE/ENTITY afvises). Bærer `account_type` + `standard_account_id`
    + `non_deductible_amount` med på linjen. Motoren importeres ikke her.
  - `upload_router.py`: `parse_upload()`/`preview_upload()` router på filendelse
    (`.xml`) eller indholds-sniff (AuditFile-rod) — Excel-sti uændret. `main.py`
    kalder kun routeren.
- **`main.py` er kun web-laget.** Beskyttede ruter bruger
  `central_auth.require_tool`. Jobs er in-memory (kræver 1 worker), private pr.
  bruger; `_prune_jobs()` rydder gamle terminale jobs (TTL `JOB_RETENTION_SECONDS`,
  cap `MAX_JOBS`).
- **Auth = central BALAI (`central_auth.py`).** Login/setup/invitationer/admin
  sker KUN centralt på `auth.balai.dk`. Modulet læser den delte Flask-session-cookie
  (`session` på `.balai.dk`, samme `SECRET_KEY`), validerer mod den fælles Postgres
  (`DATABASE_URL`) og håndhæver adgang til slug **"vat"**. `require_tool`:
  API-ruter (`/analyze`,`/preview`,`/status`,`/result`) → **401 JSON**; HTML-ruter →
  **303-redirect** til `AUTH_BASE_URL/login?next=…`. `/logout` rydder den delte cookie.
  `auth.py` er nu en tynd rest (CSRF-verify + audit-db-init). Brugerident er `email`.

## Maskinlæsbar datakontrakt (motorens input)

`catalog/data_contract.json` (v0.2.0) beskriver hele motorens kanoniske
inputstruktur — de 7 objekter `header/accounts/tax_table/transactions
(+lines)/suppliers/customers/summary`, 69 felter i alt (heraf to nye
lineage-felter på `header`: `mapping_version`/`schema_fingerprint`, byggetrin
8). Samme mønster som
regelkataloget: **hånd-vedligeholdt single source** `tools/data_contract_data.py`
→ generator `tools/build_data_contract.py` → `catalog/data_contract.json`,
drift-gated i CI (`tests/test_data_contract_fresh.py`: committet == genereret,
plus et krydstjek mod `analytics/readiness.py`'s signal-felter og et
drift-tjek af `MATERIALITY_*`-env-navne mod `analytics/materiality.py`).
Kontrakten er **deskriptiv, ikke håndhævende** — ingen runtime-validering/
pydantic er indført i parserne (det er et senere byggetrin, jf.
`balai-platform/BALAI-dataflow-arkitektur.md` §7).

Pr. felt: type, obligatorisk/valgfri, format/gyldige værdier, hvilke
kontroller/kategorier/features der kræver det, og om hver af de TRE
input-veje (Excel/CSV, SAF-T XML, kanonisk CSV — byggetrin 8) reelt udfylder
det i dag (`kilder.excel`/`kilder.saft`/`kilder.canonical`). Et eksplicit
`balai_extensions`-afsnit markerer de felter, der IKKE er native SAF-T
Financial-elementer (§2a i dataflow-arkitekturen): ship_from/to_country,
document_date, non_deductible_amount, samt en version-triple
(erklæret/strukturelt detekteret/mål — kun "erklæret" er implementeret i dag).
Et `run_config`-afsnit dokumenterer `ANALYTICS_MODULES` (hentet direkte fra
`analytics/modules.py`, aldrig hånd-duplikeret) og `MATERIALITY_*`-tærsklerne.
Et `known_gaps`-afsnit lister elleve konkrete, evidensbaserede
uoverensstemmelser på tværs af de tre input-veje (fx: `customers[].vat_number/
country` er hårdkodet tomme på Excel-vejen, hvilket gør kontrol 94-97
strukturelt ude af stand til at finde kundens land på Excel-oprindelse;
`summary` mangler total_debit/credit/vat på SAF-T-vejen; `source_document_id`
betyder fakturanummer på Excel-vejen men transaktionsbeskrivelse på
SAF-T-vejen; GAP-10/GAP-11: den kanoniske vej mangler momssats hhv.
kontoplan-/leverandør-/kundestamdata, jf. dagens vat-extract-mapping).
Regenerér efter ændringer i `tools/data_contract_data.py`:
`python tools/build_data_contract.py`.

## Regelkatalog & modul-sporbarhed

`catalog/rules.json` (v1.1.0) auto-genereres af `tools/build_rules_catalog.py`
(statisk AST over `test_NN`-funktionerne — ingen kodekørsel). Hver regel bærer nu
**`analyse_modul`, `analyse_modul_navn`, `default_aktiv`** foruden kategori, impact,
severity, status, kilde/test-noter (`rule_notes.json`-sidecar). Kataloget har også
en top-level `analyse_moduler`-blok (modul → beskrivelse + antal + default). Drift
er CI-gated (`tests/test_catalog_fresh.py`: committet == genereret). Bump
`catalog_version` ved ændringer og regenerér.

## Datapolitik (ikke et arkiv)

Input (kundedata) slettes straks efter kørsel; resultat efter retention; revisionslog
kun metadata (aldrig momsnumre/navne/beløb). Se `docs/VAT-Analytics_Sikkerhed_og_databehandling.docx`.

## Env-variabler

`SECRET_KEY` (påkrævet i prod; SKAL matche central auth), `DATABASE_URL` (delt
Postgres), `AUTH_BASE_URL` (default `https://auth.balai.dk`), `AUTH_DB_PATH`,
`AUDIT_DB_PATH`, `SESSION_COOKIE_SECURE` (default på; `0` til lokal HTTP-dev),
`JOB_RETENTION_SECONDS` (3600) / `MAX_JOBS` (100), **`ANALYTICS_MODULES`**
(komma-liste af aktive moduler; `alle` = alt; default = kun momskernen),
**`MATERIALITY_*`** (tærskler/vægte; defaults = hidtidig adfærd).

## Konventioner

- Ny/ændret kontrol → opdatér koden, regenerér `catalog/rules.json`, opdatér tests,
  valideringssuiten (`validation/scenarios.py`), `docs/CHANGELOG.md` og
  sporbarhedsmatricen. Testsuite + valideringssuite + katalog-drift er CI-porten.
- Ny modul-tilknytning ændres i `analytics/modules.py` (`_OVERRIDES`) — regenerér
  katalog bagefter (drift-gaten fanger glemt regenerering).
- Ny VIES/moms-logik → tests først/samtidig. Tal dansk, klart og konkret.
  **Slet aldrig filer uden Bals tilladelse.**
- **Python 3.13-baseline.** Runtime-deps i `requirements.txt`, test-deps i
  `requirements-dev.txt` (pytest ikke i prod). CI: pytest + katalog + validering +
  `pip-audit --strict`.

## Åbne tråde

- **SAF-T-parser:** ✅ produktions-parser (`parsers/saft_parser.py` + `upload_router.py`)
  landet — mapper SAF-T Financial (DK v1.0/2.0/2.1) til den kanoniske struktur,
  best-effort (kører også på ugyldig SAF-T), routet i `main.py`. Bærer allerede
  `standard_account_id` på linjen. **Increment B (FÆRDIG):** `StandardAccountID` →
  nature (balance ≥ 5000 / resultat 1000–4999) i `analytics/standard_accounts.py`,
  wiret ind i `vat_rules.is_non_vat_account`, så scopet bider på rigtige filer, hvor
  `AccountType` er mislabeled "Other". Bekræftet på den fejlmærkede v1.0-fil.
- **Features 82/83** (besluttet, afklar UI-form): 82 = periode vs. angivelse
  (angivelses-input); 83 = delvis fradragsret (fradragsbrøk + toggle "100%
  momspligtig"). 90 (betalingsmønstre) parkeret.
- **`kilde`-pinning** pr. kontrol i `rule_notes.json` — fagansvarlig bekræfter
  paragraffer; Claude laver kun kategori-udkast.
- **Tophuller fra Fabian-dialogen (G1–G4):** fradragsbegrænsning (§42), udenlandske
  ydelseskøb/RC pr. leverandør, kontoplan-drevet momsforventning, rubrik-afstemning.
- **EU-migration af søsterværktøjer** (VIES/Data Extract volumen), **Next.js-scaffold**
  (`~/Projects/vat-analytics`: byg eller arkivér), **git-historik-scrub** af gamle
  lækkede creds (destruktivt, kræver Bals go), **EY-platformsflytning + DPA**.
