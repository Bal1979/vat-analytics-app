# VAT analytics — Review-forberedelse (carry-over fra Data Extract)

> **Status:** Levende dokument. Oprettet under Data Extract-reviewet (2026-06-15)
> for at bære ramme + relevante fund videre til VAT analytics-reviewet, som køres
> i en **separat session**. **Data Extract-reviewet er afsluttet (2026-06-15)** —
> §6 er nu udfyldt med konkrete genbrugs-artefakter, alias-gap og opgaver. Filen er
> klar til overlevering til VAT analytics-sessionen.
>
> **Sådan bruges filen:** Læs §1 (rammen) og §3–§5 (hvad vi allerede ved) før du
> starter VAT analytics-reviewet. Behandl §3-observationer som *let recon* der
> skal verificeres, ikke som færdige konklusioner.

---

## 0. Kilder og reference-repoer

- **Review-playbook (autoritativ ramme):** `~/Projects/vies-validation/docs/Review-playbook_EY-standard.md`
- **Referencearkitektur:** `~/Projects/saf-t-validator` (SAF-T) og `~/Projects/vies-validation` (VIES)
- **Værktøj under review:** `~/Projects/vat-analytics-app/backend` (analysemotor, FastAPI + Python)
  og `~/Projects/vat-analytics` (Next.js-frontend)
- **Søsterværktøj allerede reviewet i denne runde:** `~/Projects/vat-extract` (Data Extract)

VAT analytics nævnes eksplicit i playbook'en som et af de værktøjer, der skal løftes
til samme EY-standard som SAF-T og VIES. Samme skabelon gælder.

---

## 1. Reviewrammen (destilleret fra SAF-T + VIES)

Målet hver gang: et **EY-godkendt-præsentabelt** kontrolværktøj — korrekt, testet,
dokumenteret, sikkert og driftsstabilt. Kør faserne i denne rækkefølge:

1. **Afklar scope FØRST** (AskUserQuestion): separat app vs. delte moduler,
   datapolitik/retention, og den **tool-specifikke kerneudfordring**. Byg intet før.
2. **Fuld gennemgang** (frontend + backend) → find de kritiske blokkere.
3. **Ombyg til referencearkitekturen:** al forretningslogik i én testbar pakke uden
   netværk; eksterne afhængigheder bag interface + mock; portér auth/audit/design/
   sikkerhed/CI fra SAF-T/VIES.
4. **Tests først/samtidig** + **uafhængig valideringssuite** (plantet ÉN defekt pr.
   kontrol, gated i CI) + **versioneret regelkatalog** (JSON, `catalog_version`) +
   **sporbarhedsmatrix** (regel → autoritativ kilde → modul → testdækning).
5. **Godkendelses-dokumentationspakke** (4 docx + matrix.xlsx + valideringsrapport.md)
   efter SAF-T-skabelonen — genereres via `tools/build_approval_docs.js`.
6. **Stress-test på RIGTIGE klientdata** før noget kaldes færdigt. Ret driftsfund ved
   **roden**, ikke symptomet.

**Resultat-/UX-filosofi (hårdt lært):**
- **RØD = ACTION KRÆVET** — ikke "alt der ser skævt ud". Falske alarmer
  underminerer tilliden (VIES: 37 røde → 4 reelle).
- **Konservativ mod falske negativer:** afvis aldrig noget, der kan være gyldigt;
  defer til den autoritative kilde ved tvivl.
- **Sekundære signaler nedgraderer ikke et gyldigt resultat unødigt.**
- **Prioriteret action-liste**, ikke en mur af flag.

**Arbejdsgang:** Bal kører `pytest` lokalt og **pusher selv** (SSH kun på hans Mac).
Giv altid commit/push som en kopiérbar terminal-boks. Dansk, klart og konkret.
Slet aldrig filer uden tilladelse.

---

## 2. Tool-specifik kerneudfordring (afklares i scope-fasen)

Playbook'ens hypotese for VAT analytics: *korrekthed af beregninger/aggregater,
sporbarhed fra tal til kilde, materialitet/tærskler.* Bekræft/justér i scope-fasen.

Tilføjet indsigt fra Data Extract-reviewet: en stor del af VAT analytics' værdi
afhænger af **inputdatakvalitet og -bredde** (se §4). Kerneudfordringen er derfor
sandsynligvis todelt: (a) at kontrollerne er korrekte og falsk-alarm-fri, og
(b) at de faktisk fodres med de felter, de har brug for, frem for at gætte.

---

## 3. VAT analytics — nuværende tilstand (let recon, verificér i reviewet)

- **Arkitektur:** FastAPI-backend (`backend/main.py`) + Python-analysemotor under
  `backend/analytics/`. Frontend er et separat Next.js-projekt (`vat-analytics`).
- **103 kontroller** (`test_id` 1–103) fordelt på **12 kategorier**:
  cat01 transaction_integrity, cat02 duplicate_detection, cat03 vat_rate_validation,
  cat04 cross_border_eu, cat05 timing_period, cat06 party_validation,
  cat07 amount_threshold, cat08 statistical_anomaly, cat09 reverse_charge,
  cat10 vat_reconciliation, cat11 fraud_mtic, cat12 ecommerce_special.
- **Datapipeline:** `parsers/excel_parser.py` (auto-detekterer kolonner via danske
  aliaser) → `parsers/data_adapter.py` (pakker flade rækker til SAF-T-lignende
  txn/lines-struktur, som motoren forventer).
- **Tests findes allerede:** `tests/test_analytics.py`, `tests/test_parser.py`,
  `conftest.py`, `test_data.xlsx`. (Mere end Data Extract havde — men dækningsgrad,
  uafhængig valideringssuite og plantede defekter skal verificeres.)

### Blokkere observeret allerede (verificér + ret i reviewet)

- 🔴 **Hardcodede default-credentials i kode:** `main.py` har
  `AUTH_USERS = "admin:balai2025,Fabian:Salvatore"` som default. Det ligner
  **rigtige** credentials, ikke placeholders → fjern fra koden, tjek git-historik
  for læk, portér SAF-T's session-auth. Samme mønster som Data Extract
  (`admin:change-me-before-deploy`) — men her med reelt udseende passwords.
- 🔴 **Ingen CI fundet** (ingen `.github/workflows`). Hele gated-validerings-baren
  mangler (pytest + pip-audit + valideringssuite).
- 🟡 **Ingen splittede deps** (kun `requirements.txt`), intet `railway.json`,
  Procfile findes. Driftsmodel (workers/persistens) skal fastlægges som på VIES.
- 🟡 **Intet versioneret regelkatalog / sporbarhedsmatrix** for de 103 kontroller
  endnu (verificér).

---

## 4. Carry-over fund fra Data Extract-reviewet (vigtigst)

Under Data Extract-reviewet afstemte vi Data Extracts datapunkter mod de 103
kontroller. Det afslørede tre ting, der er **direkte relevante for VAT analytics**:

### 4.1 Importkontrakten er smal — rige felter droppes lydløst
`excel_parser.py` genkender kun ~22 logiske kolonner:
`transaction_id, date, account_id, account_description, description, debit, credit,
amount, vat_amount, vat_code, vat_rate, supplier_id/name, customer_id/name,
invoice_number, currency, journal_id, period, year, country, vat_number`
(med danske aliaser: momskode, momsbeløb, land, cvr m.fl.).

**Alt uden for listen ignoreres ved import.** Det betyder, at kontroller, der
*refererer* rige begreber (reverse_charge, goods/services, vies-status, tax_point,
margin-scheme), i dag **udleder dem fra `tax_code`/`country`-heuristikker** frem
for fra kildedata — fordi importen aldrig leverer en dedikeret kolonne.
→ **Verificér i reviewet:** hvilke af de 103 kontroller kører reelt på heuristik/
gætværk, og hvilke er reelt inaktive på Excel-importerede data?

### 4.2 ship_from / ship_to bruges IKKE i place-of-supply (prioriteret mangel)
cat04 (cross_border_eu) og cat09 (reverse_charge) afgør place-of-supply ud fra
**modpartens land** + debet/kredit-retning + `tax_code`-navngivning. `ship_from`
og `ship_to` har **0 referencer** i hele backend. Det er præcis den svagere proxy,
som Data Extracts eget katalog kalder *"den hyppigste kilde til fejl i
grænseoverskridende transaktioner."*

**Hvorfor det betyder noget (Bals egne eksempler):**
- **Vare sendt til andet land end debitors:** `ship_to_country` ≠
  `counterparty_country` → mulig trekantshandel / forkert place-of-supply.
  Usynlig hvis man kun ser på modpartslandet.
- **Vare forlader ikke DK, men sælges til fx Tyskland:** `ship_from = DK`,
  `ship_to = DK`, kunde = DE → reelt **indenlandsk dansk moms**, ikke nulrettet
  EU-salg. Klassisk og dyr fejl (uberettiget 0%-rating) — kun detekterbar med
  ship_from/ship_to ved siden af modpartslandet.

→ **Beslutning truffet i Data Extract-reviewet:** `ship_from_country`,
`ship_to_country` og `counterparty_country` skal være **tre selvstændige,
prioriterede felter**. Data Extract indsamler dem allerede (mappet pr. ERP).
**Opgave til VAT analytics-reviewet:** udvid importen til at bære dem, og tilføj/
justér kontroller, der bruger alle tre til place-of-supply og trekantshandel.

### 4.3 Delt kanonisk feltkontrakt (Data Extract ↔ VAT analytics)
Beslutning: ét delt feltkatalog på tværs af de to værktøjer. Data Extracts
spec-output skal garantere de ~22 importerbare felter (med navne/aliaser, importen
genkender) + markere rige felter som "kræver importudvidelse". For VAT analytics
betyder det: **importens kolonnekontrakt = den autoritative kilde**, og den bør
versioneres sammen med regelkataloget.

→ **Navne-faldgrube at tjekke:** Data Extracts generiske header bruger navne som
`gl_account_no`, `vat_amount_lcy`, `ship_from_country`; importens auto-mapping
forventer `account_id`, `vat_amount`/`momsbeløb`, `country`/`land`. Alias-tabellen
i `excel_parser.py::COLUMN_ALIASES` skal udvides, **ellers matcher kolonnerne ikke**.

---

### 4.4 Kolonne-afstemning Data Extract → import (målt under stress-test 2026-06-15)
Afstemning af de kolonner, Data Extracts genererede udtræk faktisk producerer, mod
importens aliaser (`COLUMN_ALIASES`) viste, at kun ~¼ auto-mapper:

- e-conomic-udtræk: **5/17** kolonner matcher (date, amount, currency, text, country).
- Generisk tjekliste: **5/22** matcher.

Resten genkendes ikke (fx `vatCode`, `accountNumber`, `customerNumber`,
`corporateIdentificationNumber`, `vat_amount_lcy`, `gl_account_no`,
`counterparty_country`, `vat_registration_no`), og `ship_from`/`ship_to` har slet
ingen importkolonne. e-conomic-output er desuden relationelt (flere CSV-filer) vs.
importens ene flade tabel.

→ **Rod-fix (3 muligheder):** (1) udvid `COLUMN_ALIASES` så importen genkender
Data Extracts navne; (2) lad Data Extract udskrive de kanoniske navne; (3) byg et
oversætter-/mapping-værktøj, der flad-gør og mapper vilkårlige (også kunde-leverede)
udtræk til det kanoniske format.
→ **Anbefaling:** driv både importens aliaser OG Data Extracts output-navne fra det
delte feltkontrakt, så de ikke kan drifte; oversætteren er det generelle spor til rå
kunde-datasæt.
→ **Opgave i VAT analytics-reviewet:** udvid `COLUMN_ALIASES` og overvej en
mapping-/flad-gørings-fase i importen (parsers/), drevet af feltkontrakten.

## 5. Tjekliste til VAT analytics-reviewet (carry-over spørgsmål)

- [ ] Er alle 103 kontroller reelt aktive på Excel-importerede data, eller falder
      nogle stille igennem pga. manglende inputfelter (§4.1)?
- [ ] Bekræft falsk-alarm-filosofien: betyder RØD reelt "action krævet"? Kør på
      rigtige klientdata og tæl røde → reelle (jf. VIES 37→4).
- [ ] Findes der en uafhængig valideringssuite med ÉN plantet defekt pr. kontrol,
      gated i CI? (Ikke fundet endnu.)
- [ ] Er der et versioneret regelkatalog + sporbarhedsmatrix for de 103 kontroller?
- [ ] Er beregninger/aggregater korrekte og sporbare fra tal → kildetransaktion?
      (Playbook-hypotese for kerneudfordringen.)
- [ ] Udvid importkontrakten med ship_from/ship_to (+ øvrige rige felter) og
      tilføj place-of-supply/trekantshandel-kontroller (§4.2).
- [ ] Fjern hardcodede credentials, portér session-auth, tjek git-historik for læk.
- [ ] Etablér CI (pytest + pip-audit), split runtime/test-deps, fastlæg driftsmodel.

---

## 6. Hvad Data Extract-reviewet efterlod til VAT analytics (FÆRDIG 2026-06-15)

Data Extract-reviewet er gennemført (alle 6 faser, grønt, stress-testet på drift).
Følgende er klar til genbrug/handling i VAT analytics-reviewet.

### 6.1 Det delte feltkontrakt (autoritativ kilde)
Ligger som versioneret JSON:
`~/Projects/vat-extract/dataextract/catalog/field_contract.json` (contract_version 1.0.0).
36 kanoniske momsfelter med: `importable` (kan VAT analytics' import læse feltet i dag),
`va_aliases` (kolonner importen genkender), `priority` (ship_from/ship_to = high) og note.
Nøglen `vat_analytics_importer_columns` lister præcis de 22 kolonner, importen genkender
(kopieret fra `excel_parser.py::COLUMN_ALIASES`).
→ Brug filen som den fælles sandhed; driv både importens aliaser og Data Extracts output
fra den, så de ikke kan drifte.

### 6.2 Konkret alias-gap der skal lukkes i importen (fra §4.4)
Tilføj disse som aliaser i `COLUMN_ALIASES` (Data Extract-navn → kanonisk importkolonne):

| Data Extract udskriver | → importkolonne |
|------------------------|-----------------|
| `vatCode` | `vat_code` |
| `accountNumber`, `gl_account_no` | `account_id` |
| `vat_amount_lcy` | `vat_amount` |
| `counterparty_country` | `country` |
| `vat_registration_no`, `corporateIdentificationNumber` | `vat_number` |
| `customerNumber`, `customer_vendor_no` | `customer_id` |
| `posting_date` | `date` |
| `vat_base_lcy` | (ny: `tax_base` — ingen importkolonne i dag) |
| `ship_from_country`, `ship_to_country` | (NYE kolonner — kræver import + adapter + kontroller) |

(Den fulde maskinlæsbare liste er `field_contract.json`. Husk også at e-conomic-output er
relationelt — flere CSV-filer — mens importen vil have én flad tabel; en flad-gørings-fase
er nødvendig.)

### 6.3 Genbrugbare artefakter fra Data Extract (mønster til at portere)
Alle under `~/Projects/vat-extract/`:
- **Session-auth:** `auth.py` + `audit_log.py` (pbkdf2, invitationer, rate-limit i SQLite,
  CSRF, timing-sikker). BEMÆRK: porteret fra SAF-T's **Flask**; VAT analytics er **FastAPI**
  → ikke copy-paste, skal tilpasses (cookie-session/middleware). Erstatter VAT analytics'
  HTTPBasic + hardcodede creds.
- **Sikkerhedsheaders/CSP:** mønster i `app.py` (stram CSP uden CDN, HSTS, X-Frame-Options).
- **Valideringssuite:** `dataextract/validate.py` + `dataextract/rules.py` (regel-registry) —
  mønster for "plantet defekt pr. kontrol, gated i CI".
- **Sporbarhed:** `tools/build_traceability.py` → matrix.xlsx + rapport.md, auto-genereret
  fra katalog + regel-registry.
- **Dokumentationspakke:** `tools/build_approval_docs.js` (docx-js, 4 docx efter SAF-T-skabelon).
- **CI:** `.github/workflows/ci.yml` (pytest + katalogvalidering gated + pip-audit).

### 6.4 Drifts-/deploy-lektie (verificeret på Railway 2026-06-15)
Samme fælde rammer VAT analytics, hvis den deployes som Data Extract:
- **Persistent volumen** monteret `/data` + `AUTH_DB_PATH=/data/auth.db` +
  `AUDIT_DB_PATH=/data/audit.db` — ellers nulstilles auth-db ved HVER redeploy (admin forsvinder).
- **SECRET_KEY** sat (ellers brudte sessions ved >1 worker).
- **EU-region** for volumenet (det følger servicens region) — auth/audit er personoplysninger.
- Pas på flere Railway-projekter: VIES og Data Extract er separate services — ret kun den rigtige.
- **Lad Procfile styre starten** — undgå `startCommand`-override i `railway.json`; det omgår
  Nixpacks' venv-aktivering, så start-kommandoen (gunicorn/uvicorn/python) ikke er på PATH
  (`command not found`).
- **Hold Node-artefakter ude af git** (`package.json`, `package-lock.json`, `node_modules/`
  fra fx docx-generatoren) — en enkelt `package.json` i repo-roden får Nixpacks til at
  fejldetektere en Node-app og springe Python-deps over (samme fælde ramte Data Extract).

### 6.5 Separat spor: oversætter-værktøjet
Stress-testen (§4.4) bekræftede behovet for et nedstrøms **oversætter-/mapping-værktøj**: tag
et vilkårligt (også kunde-leveret) udtræk og map/flad-gør det til det kanoniske format, VAT
analytics kræver. Adskilt fra Data Extract (opstrøms, leverer specifikationen) og fra selve
analysen. Parkeret som selvstændigt værktøj — kan tages efter VAT analytics-reviewet.

### 6.6 Definition of done for VAT analytics (jf. §1 + playbook §3)
§3-checklisten grøn (auth, CI, valideringssuite m. plantede defekter, versioneret regelkatalog,
sporbarhedsmatrix) + dokumentationspakke (4 docx + matrix + rapport) + stress-test på rigtige
klientdata + alle carry-over-opgaver i §4–§5 adresseret.
