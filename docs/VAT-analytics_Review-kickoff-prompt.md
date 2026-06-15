# VAT Analytics — kickoff-prompt til ny review-session

> Indsæt teksten nedenfor i en ny session for at starte reviewet af VAT Analytics.
> Den samler alt det relevante fra de tidligere reviews (SAF-T, VIES, Data Extract),
> så agenten ikke skal genopdage det. Den læner sig på to filer i repoet:
> review-playbook'en og forberedelses-/carry-over-filen.

---

Review VAT Analytics. Jeg skal have et review af mit værktøj **"VAT Analytics"** —
momsanalyse-motoren, der kører **103 kontroller** mod et momsdataudtræk — som skal
**EY-godkendes**. Vi har netop løftet tre søsterværktøjer til samme standard: SAF-T
Validator, VIES Validator og Data Extract. Jeg vil have VAT Analytics kørt efter
**præcis samme skabelon**.

**ADGANG — bed om adgang til disse mapper med det samme:**
- `~/Projects/vat-analytics-app` (backend = analysemotoren, det vi skal reviewe) og
  `~/Projects/vat-analytics` (Next.js-frontend)
- `~/Projects/vies-validation` (seneste eksempel + indeholder review-playbook'en)
- `~/Projects/saf-t-validator` (referencearkitekturen)
- `~/Projects/vat-extract` (Data Extract — netop færdig-reviewet søsterværktøj med
  konkrete carry-over-fund og genbrugbare generatorer)

**LÆS FØRST, i denne rækkefølge:**
1. Review-playbook'en: `~/Projects/vies-validation/docs/Review-playbook_EY-standard.md`
2. Carry-over-/forberedelsesfilen fra Data Extract-reviewet:
   `~/Projects/vat-analytics-app/docs/VAT-analytics_Review-forberedelse.md` — den samler
   ramme + alle fund og opgaver, der allerede er identificeret for VAT Analytics
   (importkontrakt, ship_from/ship_to, delt feltkontrakt, alias-gap, genbrugsartefakter,
   deploy-lektie). Læs den grundigt; den sparer dig for at genopdage en masse.

**KØR REVIEWET I DENNE RÆKKEFØLGE (som på de andre):**
0. **AFKLAR SCOPE FØRST** med AskUserQuestion (playbook §0/§7): separat app vs. delte
   moduler, datapolitik/retention, og den tool-specifikke **KERNEUDFORDRING**. For VAT
   Analytics er hypotesen (bekræft/justér): korrekthed af beregninger/aggregater,
   sporbarhed fra tal til kilde, materialitet/tærskler — PLUS inputdatakvalitet/-bredde
   (om de 103 kontroller faktisk fodres med de rette felter frem for at gætte). Byg intet før.
1. **Fuld gennemgang** (FastAPI-backend + Next.js-frontend) → find de kritiske blokkere.
   Allerede observeret (verificér selv): hardcodede default-credentials i `main.py`
   (`admin:balai2025,Fabian:Salvatore`), ingen CI, smal importkontrakt (~22 kolonner i
   `parsers/excel_parser.py`), intet versioneret regelkatalog/sporbarhedsmatrix for de
   103 kontroller, FastAPI HTTPBasic-auth.
2. **Foreslå en plan**, og byg på referencearkitekturen: testbar logik-pakke uden netværk,
   mockbare eksterne afhængigheder, porteret auth/audit/design/sikkerhed/CI fra
   SAF-T/VIES/Data Extract. **NB:** auth i de andre er **Flask**; VAT Analytics er
   **FastAPI** → porter mønstret, men tilpas (cookie-session/middleware), ikke copy-paste.
3. **Tests først/samtidig**; uafhængig valideringssuite (plantet ÉN defekt pr. kontrol,
   gated i CI) + versioneret regelkatalog + sporbarhedsmatrix; godkendelses-
   dokumentationspakke (4 docx) til sidst — efter SAF-T-skabelonen. Genbrug Data Extracts
   generatorer som mønster: `~/Projects/vat-extract/tools/build_traceability.py` og
   `tools/build_approval_docs.js`, samt valideringssuiten `dataextract/validate.py` +
   `dataextract/rules.py`.
4. **Luk det vigtigste cross-tool-fund:** udvid importens `COLUMN_ALIASES` + tilføj
   ship_from/ship_to og place-of-supply/trekantshandel-kontroller (se forberedelsesfilen
   §4 + den konkrete alias-gap-tabel i §6.2). Driv import-aliaserne fra det delte
   feltkontrakt: `~/Projects/vat-extract/dataextract/catalog/field_contract.json`.
5. **Stress-test på RIGTIGE klientdata** før vi tror vi er færdige — ret driftsfund ved
   roden. Hvis værktøjet er deployet (Railway): tjek persistent volumen monteret `/data`
   + `AUTH_DB_PATH`/`AUDIT_DB_PATH` + `SECRET_KEY` + EU-region (Data Extract havde præcis
   den fælde: uden volumen nulstilles brugerdatabasen ved hver redeploy). Tjek også build/start: lad Procfile styre starten (ikke `startCommand`-override i `railway.json`, det omgår venv-PATH → `command not found`), og hold Node-artefakter (`package.json`/`-lock`, `node_modules`) gitignored, ellers fejldetekterer Nixpacks en Node-app og springer Python/gunicorn over.

**RESULTAT-FILOSOFI:** RØD = action krævet (ingen falske alarmer; VIES gik fra 37 røde
til 4 reelle), vær konservativ mod falske negativer, og giv en prioriteret action-liste
frem for en mur af flag.

**KONVENTIONER:**
- Tal dansk, klart og konkret. Slet aldrig filer uden min tilladelse. Brug AskUserQuestion
  ved uklarheder, og lav en task-liste for arbejdet.
- Jeg kører `pytest` lokalt og **pusher selv** (SSH ligger kun på min Mac). Når noget skal
  committes/pushes, giv mig den præcise terminal-tekst i en kopiérbar boks — **UDEN
  inline-kommentarer** (jeg bruger zsh, som ikke behandler `#` som kommentar, så
  `pytest # ...` fejler).
- Lokalt miljø: jeg kører i venv; mind mig om `pip install -r requirements.txt -r
  requirements-dev.txt`. docx-generering kræver lokal `npm install docx` (ikke global +
  NODE_PATH); `node_modules` skal være gitignored.
- Railway: jeg har flere projekter. VIES = projekt *efficient-amazement* / vies.balai.dk;
  Data Extract = *powerful-endurance* / vat-extract.balai.dk. VAT Analytics er sin egen —
  vær skarp på hvilken service du rører (jeg kom til at redigere den forkerte sidst).
- Du må gerne tilgå det kørende værktøj via Chrome til stress-test, men du **opretter ikke
  konti og indtaster ikke passwords** — det gør jeg.

**START:** bed om mappeadgang (inkl. de fire repoer ovenfor), læs playbook'en OG
forberedelsesfilen, og kør så scope-afklaringen, før du bygger noget.
