# VAT Analytics — opfølgningspunkter (åbne)

Levende liste over åbne punkter fra EY-løftet. Hold den opdateret.

## Fase B — regelkatalog & sporbarhed
- **Udfyld præcis `kilde` (momslov-paragraf) pr. kontrol.** Mekanikken er på plads
  (`catalog/rules.json` + `catalog/rule_notes.json` → matrix/rapport). Pt. viser
  matricen kategoriens *retsområde* som fallback (0/103 præcise kilder udfyldt).
  Den fagansvarlige skriver de præcise paragraffer ind i `rule_notes.json` (felt
  `"kilde"` pr. test_id); katalog + matrix regenereres af generatorerne.
  Claude kan lave et kategori-udkast til gennemsyn, men opfinder ikke paragraf-numre.

## Fase C — uafhængig valideringssuite (i gang)
- Plantet defekt pr. aktiv kontrol (98), gated i CI. Udfylder samtidig `test`-kolonnen
  i sporbarhedsmatricen.

## Inaktive kontroller — besluttede features
- **82 (periode vs. angivelse):** byg angivelses-input (upload af momsangivelse ELLER
  simuleret dansk momsangivelse) → afstem beregnede totaler mod indberettede tal.
- **83 (delvis fradragsret):** (1) udled/flag anvendt fradragsbrøk fra data; (2) toggle
  "Virksomheden har 100% momspligtig aktivitet" (default til) → slås fra: indtast
  foreløbig + evt. endelig fradragsret (efterregulering, kendes i juni-angivelsen).
- **90 (betalingsmønstre):** parkeret — valgfrit upload af fuld betalingsdata (fraud:
  faktiske overførsler vs. bogførte tal).
- **85, 99:** uden for scope for single-entity-analyse — dokumenterede begrænsninger.

## Drift (tværgående)
- **EU data-residency for søsterværktøjer:** VIES (`efficient-amazement`) og Data
  Extract (`powerful-endurance`) ligger formentlig stadig med volumen i US. Migrér
  til EU som VAT Analytics (slet US-volumen → sæt EU → genopret volumen). Noteret i
  playbook'ens drifts-afsnit.

## Momsrelevans (produktions-tilpasning — 2026-09-08)
- ✅ **Slankning / analyse-moduler (FÆRDIG):** momskernen (60 kontroller) default TIL;
  forensic/statistik (26), e-handel/særordninger (10), datakvalitet (4), dublet-recovery
  (3) default FRA. `analytics/modules.py` + motor-filtrering + katalog v1.1.0
  (`analyse_modul`/`default_aktiv`). Styres af `ANALYTICS_MODULES` (default = kun kerne).
- ✅ **Momsrelevans-scope, fundament (FÆRDIG):** balancekonti (SAF-T `AccountType`)
  undertrykker momsfund; uændret for fladt Excel. Kontrol 80.
- ✅ **SAF-T-parser (FÆRDIG — increment A):** `parsers/saft_parser.py` +
  `parsers/upload_router.py`, routet i `main.py` (Excel-sti uændret). Best-effort,
  namespace-agnostisk, XML-hærdet. Bærer `standard_account_id` på linjen. Testet i
  `tests/test_saft_parser.py`.
- ⬜ **Robust scope via `StandardAccountID`/standardkontoplan (increment B):** nødvendigt
  fordi rigtige filer mislabeler `AccountType="Other"`. Map `StandardAccountID` →
  standardkontoplan-rolle (balance vs. resultat) og før et `account_role`-signal ind i
  `vat_rules.is_non_vat_account`. Genbrug SAF-T Validators `standard_accounts.py` +
  standardkontoplan-referencen (603 konti).

## Internt løft tilbage (kan bygges af os — rækkefølge)
1. ✅ **Aggregat-korrekthed (FÆRDIG 2026-06-17):** `build_report` rapporterer nu både brutto
   (`negative_amount`/`positive_amount`) og distinkt, transaktions-dedupliceret
   (`*_distinct` + `distinct_transactions`) via `engine.distinct_amount`. Testet invariant
   (distinkt ≤ brutto) i `tests/test_aggregate_correctness.py`; dokumenteret i arkitektur-doc'et.
2. ✅ **Jobs-oprydning (FÆRDIG 2026-06-17):** `_prune_jobs()` i `main.py` fjerner gamle terminale
   jobs (TTL via `JOB_RETENTION_SECONDS`, default 3600s) og capper antallet (`MAX_JOBS`, default 100),
   trådsikkert via lock, kaldt lazy i `/analyze`. Testet i `tests/test_jobs_cleanup.py`.
3. ✅ **Katalog-drift-gate (FÆRDIG 2026-06-17):** `tests/test_catalog_fresh.py` asserter at committet
   `rules.json` == `build_catalog()` (+ integritet). Generatoren refaktoreret til `build_catalog()`/`serialize()`/`build()`.
4. ✅ **Materialitet konfigurerbar (FÆRDIG 2026-06-17):** `analytics/materiality.py` samler severity-vægte
   + centrale tærskler (kontantgrænse, godkendelsesgrænser, købs-/salgsmoms-forhold, faktura-lag,
   fjernsalgstærskel, stort momsbeløb-uden-bilag), env-overstyrbare via `MATERIALITY_*` (defaults = hidtidig
   adfærd). Wiret i engine + cat05/07/10/12. Testet i `tests/test_materiality.py`.
   Note: cat07/cat12 binder værdien ved import (env sættes ved opstart); per-run-override ville kræve at
   tråde en config gennem motoren — mulig senere udvidelse.

## Internt, men kræver beslutning/input
- **Features 82 + 83** (besluttet) — afklar UI-form før build. 90 parkeret.
- **`kilde`-pinning** pr. kontrol — Claude laver kategori-udkast; fagansvarlig bekræfter paragraffer.
- **Next.js-scaffold** (`~/Projects/vat-analytics`) — byg som frontend ELLER arkivér (reel UI = backend `index.html`).
- **Git-historik-scrubbing** af lækkede creds — destruktivt, kræver Bals go.

## Senere faser
- **Fase E:** ✅ godkendelses-dokumentationspakke (4 docx + README/CHANGELOG) — færdig.
- **Fase F:** stress-test på rigtige klientdata; tæl røde → reelle (jf. VIES 37→4).
