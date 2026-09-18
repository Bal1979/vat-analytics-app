# PoC: LLM-klassifikation af posteringslinjer mod ekspertens fundkatalog

Bal-godkendt opgave (2026-09-18). Analysetrinnet (VAT Analytics' domæne):
kan en lokal LLM klassificere enkelte købsposteringslinjer mod et
fund-katalog (F01-F31), målt op imod en ekspert-gennemgangs egen
klassifikation som facit? Metoden spejler mapping-PoC'en i
`vat-extract/examples/poc_bc_gl_2025/` (godkendt facit → schema-tvunget
LLM-forslag → automatisk scoring på arbejdspapirets §13.4-metrikker) — men
her er enheden en POSTERINGSLINJE og facit ER ekspertens egen
fund-klassifikation pr. linje, ikke en kolonne-mapping.

## Datahygiejne — VIGTIGT

- **Kundefilen (`AI Transaktionsgennemgang.xlsx`) committes ALDRIG.** Den
  ligger lokalt i `~/Kundedata/rapport-ressourcer/` og røres kun læsende
  (§2a-princippet). Alle scripts tager `--xlsx`/`--facit`/`--katalog`/
  `--out-dir` som eksplicitte stier UDENFOR repoet (scratchpad).
- Facit-JSON'en (posteringstekster, leverandørnavne, beløb) og alle
  kørselsartefakter (prompts, rå LLM-svar, predictions) er kundedata og
  skrives KUN til scratchpad — aldrig til repoet.
- Katalog-JSON'en indeholder ingen kundedata (kun ekspertens generelle
  fund-definitioner), men skrives også til scratchpad for at holde metoden
  og kundens konkrete gennemgang adskilt.
- Kørslen sker mod en LOKAL Ollama-model — ingen data forlader maskinen.
- Testene i `tests/` bruger udelukkende syntetiske/opdigtede fixtures.

## Datagrundlag

Kunden: dansk selskab, fuld fradragsret, 2025 købsposteringer (jf.
`Metode og forudsætninger`-arket). `AI Transaktionsgennemgang.xlsx`:

- **"Alle linjer"** (3.010 linjer, 28 kolonner): ekspertens FULDE
  gennemgangspopulation — samtlige linjer, hver med Fund-ID (tom = "OK"),
  risikoniveau, momsrisiko/-mulighed og en fri-tekst kommentar/begrundelse.
  Dette ER facit.
- **"Fundkatalog"** (F01-F31, `F28` findes ikke): navn, status
  ("Fund"/"Ingen fund"), risikoniveau, beskrivelse og ekspertens egen
  identifikationsmetode pr. fund.
- **"Metode og forudsætninger"**: generelle regler (praksis pr.
  konto/leverandør slår ud, absolutte momslovs-kontroller, 60/40-politik på
  it-udstyr, netting af kreditnotaer, risikoniveau-definitioner).

## Facit-statistik (byggetrin 1, kørt 2026-09-18)

3.010 linjer. `build_facit.py` genberegner kontrolsummerne (beløb ekskl.
moms, momsbeløb, ikke-fradragsber. moms) fra "Alle linjer" og udskriver dem
til konsollen ved kørsel — sammenlign selv mod arbejdspapirets §1 ("Metode
og forudsætninger"). Bekræftet byte-for-byte match ved denne kørsel (de
faktiske kr.-beløb er kundedata og gengives derfor ikke her):

| Kategori | Antal |
|---|---|
| OK (intet fund) | 2.770 |
| Fund-linjer i alt | 240 |

Top fund-id'er efter antal linjer: F09 (48), F08 (44), F21 (21), F10 (20),
F29 (18), F22 (12), F18 (11), F23 (9), F14/F16 (8). 19 af 28 aktive
fund-id'er har ≤ 5 linjer — en meget lang hale af sjældne, specifikke fund.

Risikoniveau: OK 2.770, Middel 151, Lav 72, Høj 13, Rubrikfejl 4.

**To fund er markeret "uden for pr.-linje-scope"** i `build_katalog.py`
(`CROSS_LINE_FUNDS`) og holdes ude af recall/præcision i scoringen:

- **F01** (dobbeltbogført faktura): afgøres ved at sammenholde linjer parvis
  på tværs af HELE populationen + netting mod kreditnotaer — en enkelt
  linje bærer ikke i sig selv information om, at en anden linje er en dublet.
- **F10** (kantineordning — metodevalg): et virksomhedsniveau-metodevalg,
  ikke en linjefejl — linjen opfylder blot kriterierne for at være omfattet.

## Byggetrin

1. **`build_facit.py`** — læser "Alle linjer" → facit-JSON (pr. linje:
   id, konto+navn, leverandør, tekst, momskode, beløb, moms,
   `moms_fratrukket`, ekspert-fund-id eller "OK"). Kontrolsum-tjek indbygget.
2. **`build_katalog.py`** — læser "Fundkatalog" + "Metode og
   forudsætninger" → katalog-JSON (pr. fund-id: navn, status, niveauer,
   `regel_kort` destilleret fra Beskrivelse+Lovgrundlag,
   `identifikationsmetode`, `kraever_tvaerlinje_kontekst`-flag).
3. **`dedup.py`** — grupperer linjer på et normaliseret fingeraftryk
   (konto, leverandør, tekst-mønster, momskode, valuta, om moms er
   fratrukket — bevidst UDEN facit-fundet i nøglen). Én LLM-vurdering pr.
   gruppe spredes til alle medlemslinjer.
4. **`run_poc.py`** — vælger en STRATIFICERET population (alle ~240
   fund-linjer + en seedet stikprøve på 300 OK-linjer — den fulde
   population på 3.010 linjer er en senere kørsel), deduplikerer,
   batcher (default 25 grupper/batch) og kalder en lokal Ollama-model
   (temperatur 0, tvungen JSON, `think: false`, `num_ctx=16384`).
5. **`score_poc.py`** — sammenligner predictions mod facit: recall pr.
   fund-id, OK-nøjagtighed, falske positiver (rapporteret separat — IKKE
   automatisk dømt forkerte, kan være reelle nye fund), forkert
   fund-id vs. misset fund, hallucinerede fund-id'er, skema-validitet og
   tid pr. batch fra `batches_meta.json`.

## Deduplikering — resultat (population 2026-09-18, seed 42)

| | |
|---|---|
| Population (240 fund + 300 OK-stikprøve) | 540 linjer |
| Unikke grupper efter dedup | 327 |
| Reduktionsfaktor | 1,65× (39,4 % reduktion) |

Lavere reduktion end mapping-PoC'ens 125.986→30 (kolonnenavne er langt mere
repetitive end posteringstekster) — forventeligt, da fund-linjerne i sagens
natur er en diverse, langhalet population af netop de AFVIGENDE mønstre.

## Røgtest (byggetrin 6, kørt 2026-09-18)

Én batch (25 grupper, 54 linjer, samme batch begge modeller) kørt mod hhv.
`qwen3.8:27b` og `qwen3:14b`:

| Metrik | qwen3.8:27b | qwen3:14b |
|---|---|---|
| Skema-valide svar | 1/1 | 1/1 |
| Andel korrekte (54 scorede linjer) | **92,3 %** | 78,8 % |
| OK-nøjagtighed | 3/3 | 3/3 |
| Falske positiver (OK→fund) | 0 | 0 |
| Hallucinerede fund-id'er | 0 | 0 |
| Forkert fund-id (ikke miss) | 1 | 3 |
| Misset fund som OK | 3 | 8 |
| Tid pr. batch | 187 s | 164 s |

**Kvalitativt:** 27b rammer næsten hele §42-fradragsbegrænsnings-klyngen
(F05 personalefest, F06 teambuilding, F07 kaffe/te) korrekt — kun 1 F05
misset + 1 F05/F06-forveksling (begge er nærtbeslægtede
fradragsbegrænsninger, en rimelig nærmiss). 14b rammer stort set intet i
denne klynge (0/4 F06, 0/2 F07) — en systematisk svaghed, ikke tilfældige
enkeltfejl. Begge modeller misser samme to sværere fund lige meget: F30
(kreditnota bogført brutto — kræver at genkende en UDEBLEVET
momstilbageførsel) og F15 (manglende omvendt betalingspligt, 0/2 begge) —
delt blind vinkel, sandsynligvis fordi identifikationsmetoden i kataloget
her er mere implicit end for de øvrige fund.

**Tidsindsigt:** batch-tiden er næsten ens på tværs af de to modelstørrelser
(187s vs. 164s, kun 14 % forskel) — modsat mapping-PoC'ens ~2× forskel.
Årsag: prompt-evaluering dominerer batch-tiden. Kataloget+reglerne udgør
~10.000 af de ~18.800 tegn i hver prompt (fast pr. batch), mod kun ~340
tegn pr. gruppe — dvs. batch-tid er langt mere styret af den faste
katalog-prompt end af selve klassifikationsarbejdet, og skalerer derfor
dårligt med at gøre batches MINDRE. En større batchstørrelse (fx 40-50
grupper) vil sandsynligvis give bedre gennemløb pr. sekund, men er ikke
verificeret her — batch-størrelse 25 er den eneste testede og bekræftet
skema-sikre indstilling.

**Anbefaling:** `qwen3.8:27b`, batch-størrelse 25. Kvalitetsgevinsten
(13,5 procentpoint, hele fradragsbegrænsnings-klyngen) er det klart
dominerende hensyn — tidsforskellen er marginal. Estimeret fuld kørsel af
denne PoC's stratificerede population (327 grupper ≈ 14 batches à 25):
**14 × ~187 s ≈ 44 minutter** (±20 %, baseret på én batch — reel varians
pr. batch ukendt).

**Den fulde kørsel (alle ~327 grupper) er IKKE kørt af denne PoC** —
hovedsessionen orkestrerer den efterfølgende med den anbefalede model og
batchstørrelse. Kommando:

```bash
cd examples/poc_semantik && source ../../backend/venv/bin/activate
python3 run_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \
    --out-dir <scratch>/run_27b_full --model qwen3.8:27b --batch-size 25
python3 score_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \
    --run-dir <scratch>/run_27b_full
```

## Kør selv

```bash
cd examples/poc_semantik
source ../../backend/venv/bin/activate    # openpyxl + pytest findes her

# Byggetrin 1+2 — output UDENFOR repo, fx til jeres scratchpad
python3 build_facit.py --xlsx <sti>/AI\ Transaktionsgennemgang.xlsx --out <scratch>/facit.json
python3 build_katalog.py --xlsx <sti>/AI\ Transaktionsgennemgang.xlsx --out <scratch>/katalog.json

# Røgtest — én batch
python3 run_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \
    --out-dir <scratch>/smoke_27b --model qwen3.8:27b --max-batches 1

# Fuld kørsel
python3 run_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \
    --out-dir <scratch>/run_27b --model qwen3.8:27b --batch-size 25

# Scoring
python3 score_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \
    --run-dir <scratch>/run_27b

# Harness-tests (syntetiske fixtures, ingen kundedata, ingen netværk)
python -m pytest -q tests/
```

## Metodiske begrænsninger (kendte, bevidste)

- **Repræsentant-beløb:** en gruppe vises til LLM'en med FØRSTE medlems
  beløb/moms (+ antal medlemmer). Tærskelfølsomme fund (fx F31 "over 1.000
  kr.") kan derfor i sjældne tilfælde blive fejlklassificeret for et
  medlem, hvis gruppens linjer spreder sig over tærsklen. `amount_min`/
  `amount_max` er med i `groups.json` til efterfølgende analyse.
- **F01/F10 uden for scope** (se ovenfor) — recall for disse rapporteres
  ikke; linjerne tælles hverken som fund eller miss.
- **Stratificeret, ikke fuld, population** i denne PoC — recall/præcision
  på den fulde 3.010-linje-population kan afvige, særligt for OK-andelen
  (kun 300 af 2.770 OK-linjer er stikprøvet).
- **Falske positiver dømmes ikke automatisk forkerte** — en linje, facit
  siger er OK, men LLM'en flager, kan være et reelt nyt fund eksperten ikke
  fangede (eller omvendt en fejlklassifikation) — kræver menneskelig
  vurdering, jf. samme "gæt aldrig, lad mennesket afgøre"-princip som
  mapping-PoC'en.
