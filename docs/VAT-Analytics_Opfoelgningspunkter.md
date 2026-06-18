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

## Internt løft tilbage (kan bygges af os — rækkefølge)
1. ✅ **Aggregat-korrekthed (FÆRDIG 2026-06-17):** `build_report` rapporterer nu både brutto
   (`negative_amount`/`positive_amount`) og distinkt, transaktions-dedupliceret
   (`*_distinct` + `distinct_transactions`) via `engine.distinct_amount`. Testet invariant
   (distinkt ≤ brutto) i `tests/test_aggregate_correctness.py`; dokumenteret i arkitektur-doc'et.
2. ✅ **Jobs-oprydning (FÆRDIG 2026-06-17):** `_prune_jobs()` i `main.py` fjerner gamle terminale
   jobs (TTL via `JOB_RETENTION_SECONDS`, default 3600s) og capper antallet (`MAX_JOBS`, default 100),
   trådsikkert via lock, kaldt lazy i `/analyze`. Testet i `tests/test_jobs_cleanup.py`.
3. **Katalog-drift-gate i CI (NÆSTE OP):** assert committet `rules.json` == genereret (fanger forældet katalog).
4. **Materialitet konfigurerbar:** severity-vægte/tærskler er hardcodede i `engine.py`; gør engagement-styrede.

## Internt, men kræver beslutning/input
- **Features 82 + 83** (besluttet) — afklar UI-form før build. 90 parkeret.
- **`kilde`-pinning** pr. kontrol — Claude laver kategori-udkast; fagansvarlig bekræfter paragraffer.
- **Next.js-scaffold** (`~/Projects/vat-analytics`) — byg som frontend ELLER arkivér (reel UI = backend `index.html`).
- **Git-historik-scrubbing** af lækkede creds — destruktivt, kræver Bals go.

## Senere faser
- **Fase E:** ✅ godkendelses-dokumentationspakke (4 docx + README/CHANGELOG) — færdig.
- **Fase F:** stress-test på rigtige klientdata; tæl røde → reelle (jf. VIES 37→4).
