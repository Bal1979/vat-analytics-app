# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

## Byggetrin 9, Del A–D: kontrol 19 mod vat_setup, kontrol 80 pr. konto, HTML-kundedialograpport — 2026-09-17 (catalog v1.3.0, data_contract v0.3.1)

Bal-godkendt opgave (kontekst: `BALAI-dataflow-arkitektur.md` §2a+§7 punkt
8a/8b), tre dele i rækkefølge, verificeret på den rigtige BC/NAV-fil
(v4-datasættet, 50.479 transaktioner/125.986 linjer, 208/208 afstemt).

**Del A — Kontrol 19 valideres mod vat_setup-satser
(`analytics/categories/cat03_vat_rate_validation.py`):** diagnose
(verificeret manuelt): kunden bruger BEVIDSTE delvis-fradragsret-
konstruktioner i sin VAT Posting Setup — `DOMESTIC|REDUCED_PRIVATE_VAT` =
13,63636 % og `DOMESTIC|REDUCED_REP_VAT` = 5,26316 % (en BC-teknik: reduceret
effektiv sats i stedet for 25 % + separat fradragsbegrænsning). Kontrol 19
validerede hidtil ubetinget mod den hardkodede 0/25-liste, så disse to koder
alene stod for 642 af 957 HØJ-fund. Rettelse: når kanonisk `vat_setup.csv` er
indlæst (`header.vat_setup_loaded`, ny nøgle — nøglesæt-symmetri, altid til
stede på den kanoniske vej, default False), validerer kontrol 19 i stedet en
linjes sats mod OPSÆTNINGENS sats for linjens EGEN momskode
(`tax_table[].setup_matched`, ny nøgle, samme symmetri-princip): match = ok
(også 13,64 %), afvigelse fra kodens setup-sats = fund ("Sats afviger fra
vat_setup"), kode helt ukendt i opsætningen = fund ("Ukendt momskode i
opsætning"). UDEN vat_setup (Excel/SAF-T/ældre kanoniske filer uden
sidecar): fuldstændig uændret adfærd (kun 0/25-validering) — ingen
regression. `parsers/canonical_masterdata.enrich_canonical` sætter de to nye
felter; `parsers/canonical_parser.py` initialiserer dem til False for
nøglesæt-symmetri, samme mønster som resten af filen.

**Del B — Kontrol 80 aggregeres pr. konto
(`analytics/categories/cat10_vat_reconciliation.py`):** diagnose: 24.152
per-posterings-fund fordelt på kun 77 konti; top-10 konti (typisk interne
allokeringskonti som 319160/381360) udgjorde 97 %. Den faglige beslutning
("er kontoen håndteret korrekt momsmæssigt?") træffes pr. KONTO, ikke pr.
postering. Kontrollen udsteder nu ÉT fund pr. konto: kontonummer(+navn hvis
`chart_of_accounts.csv` er leveret), antal kvalificerende posteringer, sum
af grundlag, andel af KONTOENS posteringer (alle, ikke kun de
kvalificerende) uden momskode, og op til `materiality.CONTROL_80_MAX_REFS`
(default 10) transaktionsreferencer til drill-down. Severity gradueres efter
kontoens samlede grundlag (`materiality.CONTROL_80_HIGH_THRESHOLD`/
`CONTROL_80_MEDIUM_THRESHOLD`, defaults 500.000/100.000 DKK) i stedet for en
fast "medium" — en bevidst granularitets- OG severity-ændring på tværs af
ALLE input-veje (Bal-godkendt), ikke en ny betingelse for hvornår
kontrollen fyrer. Eksisterende tests i `tests/test_vat_scope.py` er
uændrede i deres forventninger (én kvalificerende linje giver stadig ét
fund); ny dedikeret dækning i `tests/test_control80_aggregation.py`.

**Del C — HTML-rapportgenerator (`backend/tools/generate_report.py`, ny
fil):** CLI (`python tools/generate_report.py <rapport.json> --out
<fil.html>`) der bygger ÉN selvbærende HTML-fil (inline CSS, ingen
CDN/eksterne afhængigheder — kan mailes), dansk UI, BALAI-designsprog
genbrugt fra `static/style.css` (navy #1B365D, sev-farver). Fem sektioner:
(1) **tillidsanker** — kontrol 82 som 12-måneders tabel pr. rubrik
(beregnet/angivet/difference/status, nyt `analytics.build_declaration_
reconciliation_table` i `cat10_vat_reconciliation.py`, wiret ind i
`engine.run_all_tests` som `report["declaration_reconciliation"]`),
afstemningsgatens resultat, og debet==kredit-totalkontrollen; (2)
**ledelsesresumé** — fund pr. severity, transaktioner/bilag, analyseperiode;
(3) **aggregerede fundtabeller** — pr. kontrol → pr. konto (antal, beløb),
sorteret efter væsentlighed, ALDRIG rå fund-dumps (cap ved 25 kontorækker/
60 kontrol-sektioner med "…og N flere"); (4) **datagrundlag & metode** —
ikke-målbare kontroller, relevante `known_gaps` (læst best-effort fra
`catalog/data_contract.json`), lineage-footer (katalog-/kontrakt-/mapping-
version, schema-fingerprint, kørselstidspunkt — ny `generated_at`-nøgle i
`tools/analyze_canonical.py`s lineage) + AI-provenance-note (§2a: analysen
er 100 % deterministisk, AI bruges kun til menneske-godkendte, frosne
mapping-forslag); (5) print-CSS. Al kundedata HTML-escapes. Dækket af
`tests/test_generate_report.py` + `tests/test_declaration_reconciliation_
table.py`.

**Del D — Verifikation (v4-datasættet, samme fil som Del A-diagnosen):**

| Nøgletal | FØR | EFTER |
|---|---|---|
| Fund i alt | 49.308 | 24.612 |
| HØJ | 957 | 375 |
| MEDIUM | 39.957 | 15.830 |
| LAV | 8.394 | 8.407 |
| Kontrol 19-fund | 642 (alle falske — partial-deduction-koder) | 21 (alle ægte "ukendt kode") |
| Kontrol 80-fund | 24.152 (pr. postering) | 77 (pr. konto: 39 høj/25 medium/13 lav) |
| Afstemningsgate | 208/208 | 208/208 (uændret) |

HØJ-faldet (957→375) er STØRRE end det oprindeligt anslåede "~315" i
opgavebeskrivelsen: 642→21 for kontrol 19 (som ventet), kontrol 9/22/7/1
uændrede (143/147/23/2, uden for denne opgaves scope), MEN kontrol 80
bidrager nu 39 nye HØJ-fund (0 før — kontrollen havde tidligere en FAST
"medium"-severity, uanset beløb). Dette er en tilsigtet konsekvens af Del
B's beløbsgraduering (39 konti har hver et akkumuleret uden-moms-grundlag
over 500.000 DKK) — ikke en fejl, men en afvigelse fra den oprindelige
skønnede totalsum, som bør bekræftes eksplicit (se sporbarhedsnotat/
hand-off). HTML-rapport genereret til
`kunde_rapport_2025.html` (53,6 KB) fra den nye rapport-JSON — 5 sektioner,
12-måneders kontrol 82-tabel (output/RC grøn alle 12 måneder, input_vat
"timing" alle 12 måneder, årsresidual -326.212 DKK ≈ 0,56 % — matcher
dokumentationen i kontrol 82-modulet), 29 kontrol-blokke med fund.

Katalog **v1.3.0** (kontrol 19 "navn" ændret til "Sats afviger fra vat_setup"
af den AST-baserede generator — nyt primært make_finding-kald opdaget
først; kontrol 80 "severity" ændret til `["dynamisk"]`, jf. den nye
beløbsgraduering). Datakontrakt **v0.3.1** (to nye felter:
`header.vat_setup_loaded`, `tax_table[].setup_matched`; GAP-10 opdateret).
349 automatiserede tests (op fra 306), valideringssuite fortsat 99/99.

## Byggetrin 8, Del A–D: kontrol 82 aktiveret + tre kanoniske stamdata-filer — 2026-09-17 (catalog v1.2.0, data_contract v0.3.0)

Bal-godkendt opgave: operationalisér kontrol 82 (afstemning mod den
indberettede momsangivelse) og modtagelsen af tre nye valgfrie kanoniske
stamdata-filer. 99/99 aktive kontroller (op fra 98/98) i valideringssuiten;
306 automatiserede tests (op fra 300); afstemningsgaten forbliver 208/208 på
den rigtige BC/NAV-fil.

**Del A — Angivelses-input (`analytics/vat_declarations.py`, ny fil):**
`load_declarations()` læser/validerer `vat_declarations.json`
(`declarations_version: "1.0.0"`, `periods: [{period, output_vat, input_vat,
rc_services, rc_goods, energy_taxes, total}]`) — snitfladen aftalt med
vat-extract. Fejler ALDRIG med exception (samme filosofi som
`reconciliation_gate.py`): en teknisk fejl må aldrig fremstå som et fagligt
afslag.

**Del B — Kontrol 82 aktiveret (`analytics/categories/cat10_vat_reconciliation.py`):**
`test_82_period_declaration` beregner tre rubrikker pr. periode
(`transactions[].period_year`/`period`) og afstemmer dem mod angivelsen:
- **udgående-rubrik** = -sum(vat, sale) + \|sum(vat, DKRC-købslinjer)\|
- **rc_services-rubrik** = \|sum(vat, SERVICE_VAT-købslinjer)\|
- **input-rubrik** = sum(vat, øvrige købslinjer)

Rettelser undervejs, EMPIRISK fanget på den rigtige BC/NAV-fil (Del D):
(1) sale/køb afgøres af den kanoniske CSV's eget `supply_direction`-felt
("sale"/"purchase"), IKKE debet-/kredit-siden — debet/kredit gav en kraftigt
oppustet udgående-rubrik, fordi kilden kopierer invoice-niveau momsmetadata
ud på modpost-/betalingslinjer af samme bilag. Debet/kredit er bevaret som
fallback (`_line_direction`), for Excel-/SAF-T-oprindelse, der ikke bærer
`supply_direction`. (2) hver rubrik summeres MED FORTEGN over linjerne, og
abs()/negering anvendes ÉN GANG på summen (ikke pr. linje), så en
kreditnota/reversering netter korrekt i stedet for at blive lagt oveni.
DKRC-/SERVICE_VAT-kodegenkendelse er konfigurerbar
(`materiality.VAT_DECLARATION_DKRC_PATTERNS`/`..._SERVICE_VAT_PATTERNS`,
defaults `["DKRC"]`/`["SERVICE_VAT"]`) — IKKE hårdkodet til én kundes
kode-taksonomi. V1-håndtering af timing: sammenlignes pr. periode OG
årstotal — resolves differencen over året, er den timing (severity low),
ellers et reelt fund (severity high). Uden en angivelsesfil springer
kontrollen fortsat over, UÆNDRET adfærd (`readiness.EXTERNAL_DATA[82]`,
skærpet med et nyt `external_data_provided`-flag der kun løftes, når en
angivelse faktisk er leveret).

**Del C — Tre valgfrie kanoniske stamdata-filer (`parsers/canonical_masterdata.py`, ny fil):**
Auto-opdaget ved siden af `gl_entries.csv` (samme mønster som
`transform_summary.json`), berigelse EFTER `parse_canonical()`:
- `vat_setup.csv` (join: `vat_codes`-strengen) → reel `tax_percentage`/`rate`
  i `tax_table[]` OG `transactions[].lines[]` — kategori 3 (kontrol 19-26)
  får et reelt grundlag (GAP-10 delvist lukket).
- `chart_of_accounts.csv` (join: kontonummeret/`gl_accounts`) → reel
  `account_type`/`standard_account_id`/saldi i `accounts[]` OG linjerne —
  `vat_rules.is_non_vat_account` (kontrol 80's momsrelevans-scope) kan
  aktiveres (GAP-11 delvist lukket). `vat_rules._NON_VAT_ACCOUNT_TYPES`
  udvidet med BC/NAV's PLURALE konvention (`"assets"`/`"liabilities"`,
  observeret på den rigtige fil) ved siden af SAF-T's ental
  (`"asset"`/`"liability"`/`"equity"`).
- `customers.csv` (fleksible kolonnenavne — bekræftet mod vat-extracts
  reelle transform-output: `ext_customer_id`/`ext_customer_name`/
  `counterparty_country`, samt en simplere `customer_id`/`name`/`country`
  som fallback) → fylder den selvstændige `customers[]`-liste STRUKTURELT.
  **Kendt, fortsat åben begrænsning (ikke skjult):** `gl_entries` bærer
  ingen `customer_id`-kolonne på selve linjerne, så `cat12_ecommerce_special.
  _cust_country()`'s join er altid tomt på den kanoniske vej — `customers.csv`
  aktiverer IKKE kontrol 94-97 alene. `ext_vat_bus_posting_group` (en
  BC-postgruppekode, fx "EU"/"DOMESTIC") mappes bevidst IKKE til
  `vat_number` (ville være misvisende).

`catalog/data_contract.json` **v0.3.0** (fra v0.2.1): `kilder.canonical` for
de berørte felter ændret til `"partial"` (afhænger af om sidecar-filen
leveres); GAP-10/GAP-11 status `delvist_lukket`; tre nye
`MATERIALITY_RUN_CONFIG`-knapper. 69 felter (uændret antal).

**Del D — E2E-verifikation** (den rigtige BC/NAV-fil, 125.986 rækker, 50.479
bilag efter gruppering), før/efter Del A-C:

| Kontrol | Før | Efter | Kommentar |
|---|---|---|---|
| 19 (ugyldig sats) | 0 | 642 | reelt grundlag fra vat_setup.csv |
| 22 (manglende udgående moms) | 0 | 147 | reelt grundlag fra vat_setup.csv |
| 80 (indtægt uden momsbehandling) | 30.863 | 24.152 | chart_of_accounts.csv fjerner falske positive på balancekonti |
| 82 (rubrik-afstemning) | sprunget over | 12 fund | output_vat + rc_services: 0 fund (afstemmer til < 1 kr. alle 12 mdr. 2025); input_vat: 12 fund, ALLE severity high (reel, IKKE timing — se Del B) |
| 94-97 (e-handel/OSS) | 0 | 0 | uændret — kendt gab (customers.csv joiner ikke til linjer) |

Total: 55.216 → 49.308 fund (kritisk 0→0, høj 166→969, medium 46.668→39.957,
lav uændret 8.382). Afstemningsgate uændret **208/208** konti afstemt.
**Input_vat-observationen (12 fund, severity high) er en ægte
reconciliation-observation at forelægge Bal** — ikke en kodefejl: GL-baseret
input-moms og den indberettede input_vat afviger systematisk (~4 mio. DKK
over året), hvilket ligger uden for denne kontrols datagrundlag at forklare
(mulige årsager: delvis fradragsret/§42, manuelle korrektioner i angivelsen,
poster uden for GL-udtrækkets vindue).

## Medium-fund-analysen, punkt 1+2: description-modtagelse + "ikke målbar"-gating — 2026-09-17 (ikke-katalog)
Bal-godkendt opfølgning på GAP-12-mitigeringen (samme dag). Den fulde E2E på
den rigtige BC/NAV-fil (v2, uden description) gav 128.978 fund, heraf 114.575
medium — 80% deraf var datagrundlags-støj: kontrol 4 flagede ALLE 50.479
bilag for manglende Description (feltet blev aldrig læst af
`canonical_parser`, uafhængigt af om kilden havde en beskrivelse), og
kontrol 25 gav 10.671 "ingen udenlandsk modpart"-fund, fordi `country` er
strukturelt fraværende i et GL-udtræk. To ændringer, motorsiden:

**Punkt 1 — Description-modtagelse (GAP-13, lukket):**
- `canonical_parser.py` læser nu en valgfri `description`-kolonne fra den
  kanoniske CSV og fører den ind på BÅDE linje- og transaktionsniveau,
  præcis som Excel-/SAF-T-vejen (`data_adapter.py`/`saft_parser.py`). En
  grupperet flerlinje-transaktion (bilagsgruppering, GAP-12) bruger den
  FØRSTE ikke-tomme linje-description i bilaget. Kolonnen kan mangle på
  ældre kanoniske filer (v2) — da er feltet fortsat `""` (uændret adfærd,
  ingen crash).
- `tools/data_contract_data.py`: `kilder.canonical` for `description`
  (transaction- og line-niveau) rettet til `True`; `kraeves_af` på
  transaktionsniveau rettet (kontrol 4 læser faktisk feltet — tidligere
  fejlagtigt dokumenteret som "ingen kontrol direkte"). Ny known_gaps-post
  **GAP-13** (`lukket`). `catalog/data_contract.json` **v0.2.1** (fra
  v0.2.0) — 69 felter (uændret antal; kun `kilder`-flag + known_gaps
  opdateret, ingen nye felter).

**Punkt 2 — "Ikke målbar"-gating (kernen):**
Bals "ingen falske alarmer"-filosofi udvidet til medium-laget: når et felt,
en kontrol (eller én delcheck i en multi-felt-kontrol) hårdt afhænger af, er
**0% udfyldt i HELE datasættet** — og populationen er stor nok til at
udelukke en enkeltstående/tilfældig tomhed (`MIN_TX_FOR_GATING = 30`, samme
begrundelse som den eksisterende `_MIN_TX_FOR_STATISTIK`-guard) — rapporterer
kontrollen "ikke målbar" ÉN gang i stedet for at generere fund pr.
transaktion.
- `analytics/readiness.py`: ny status `STATUS_IKKE_MAALBART` ("ikke_maalbar")
  — en skærpet variant af den eksisterende `STATUS_SPRUNGET_DATA` (samme
  0%-betingelse, men KUN håndhævet når populationen er stor nok).
  `STATUS_SPRUNGET_DATA` er UÆNDRET (fortsat rent informativ, ingen
  størrelses-guard) — det er derfor valideringssuitens og testsuitens mange
  et-transaktions-scenarier (der bevidst tømmer ét felt for at plante en ægte
  defekt) forbliver upåvirkede. Ny genbrugelig primitiv `field_is_gated()`.
  `CONTROL_REQUIREMENTS[25] = ["country"]` tilføjet (kategori 3's default
  kræver kun tax_code — kontrol 25 tjekker `line["country"]` direkte).
  Ny `SUBCHECK_FIELDS`/`subcheck_gates()` for multi-felt-kontroller, der IKKE
  kan gates som helhed (kontrol 4: TransactionID/TransactionDate/AccountID-
  delcheckene skal blive ved med at køre, selvom Description mangler) —
  eksponeret som `datagrundlag["delkontrol_gates"]`.
- `analytics/categories/cat01_transaction_integrity.py` (`test_04`): kalder
  `readiness.field_is_gated(data, "description", level="transaction")` og
  springer KUN Description-delchecket over, når det er sandt. De øvrige tre
  delcheck er uændrede.
- `analytics/engine.py` (`run_all_tests`): beregner `readiness.assess()` FØR
  rapporten bygges og fjerner fund fra kontroller markeret
  `STATUS_IKKE_MAALBART` (generisk, gælder alle `CATEGORY_REQUIREMENTS`/
  `CONTROL_REQUIREMENTS`-styrede kontroller, ikke kun 25) — erstattet af ÉN
  note i `datagrundlag`. Ny rapport-nøgle `ikke_maalbare_fund_fjernet`
  (transparens, additiv — ændrer ikke `filtrerede_fund`, som fortsat kun
  tæller modul-filtrering).
- **v1-tærskel (bevidst, dokumenteret):** kun 0%-fravær gates — INGEN fuzzy
  mellemtærskler. Et felt der er delvist udfyldt (fx land kun på nogle
  linjer) gates ALDRIG, uanset hvor lav dækningen er.

**Empirisk verifikation på den rigtige BC/NAV-fil (v2, uden description,
125.986 rækker, 50.479 bilag efter GAP-12-gruppering — samme kørsel som
GAP-12-tabellen ovenfor), alle analyse-moduler tændt, før/efter denne opgave:**

| Nøgletal | Før (denne opgave) | Efter |
|---|---|---|
| Kontrol 4 (Faktura-feltfuldstændighed) | 50.479 medium | **0** — status `ikke_maalbar`, delcheck-note |
| Kontrol 25 (Nulsats på indenlandsk handel) | 10.671 medium | **0** — status `ikke_maalbar` |
| Øvrige system-bredt gatede kontroller (32, 71, 73, 86, 92 — kategori 4/9/11, kræver `country`) | 12.613 fund (blandet severity) | **0** |
| Medium i alt | 114.575 | **46.667** |
| Høj / lav i alt | 2.366 / 12.037 | 166 / 8.382 |
| Fund i alt | 128.978 | 55.215 |
| `ikke_maalbare_fund_fjernet` | — | 23.284 |
| Kontroller med status `ikke_maalbar` (`datagrundlag.opsummering`) | 0 (fandtes ikke) | 44 af 103 |
| Afstemningsgate | afstemt, 208/208 | afstemt, 208/208 (uændret) |
| Transaktioner analyseret | 50.479 | 50.479 (uændret) |

De øvrige ~46.667 medium-fund er IKKE rørt af denne opgave — de er reelle
kandidater på de felter, der rent faktisk ER til stede i udtrækket (beløb,
momskoder, datoer m.v.), og skal fortsat vurderes fagligt en for en.

**Opfølgende kørsel på v3 (samme fil, MED description-kolonne tilføjet af
vat-extract parallelt, mapping v1.2.0, 126.035 rækker):** kontrol 4 går fra
"ikke målbar" til **`koert`** med **2** reelle medium-fund (description
faktisk tom på 2 ud af 125.963 udfyldte linjer, dækning 100,0%) — bekræfter
at gatingen kun undertrykker støj, ALDRIG en reel defekt, når feltet rent
faktisk er til stede. Kontrol 25 forbliver `ikke_maalbar` (v3 tilføjede kun
description, ikke `country`). Afstemningsgate fortsat 208/208.

Testdisciplin: 256/256 automatiserede tests + 98/98 uafhængig
valideringssuite grønne uændret (ingen eksisterende scenarie ændrede
resultat — `MIN_TX_FOR_GATING` holder alle valideringssuitens
et-transaktions-scenarier uden for gatingen, jf. `readiness.py`).

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
