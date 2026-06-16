# VAT Analytics — godkendelses-dokumentationspakke

Indeks over dokumentationen til EY-værktøjsgodkendelsen. Alle artefakter er
versionsstyret og — hvor relevant — auto-genererede fra koden/regelkataloget, så
de ikke kan drifte.

## Start her
- **VAT-Analytics_Godkendelses-overblik.docx** — status pr. område + åbne punkter + næste skridt.

## Dokumenter (docx)
- **VAT-Analytics_Solution_Architecture.docx** — formål, scope, arkitektur, kontrol-/regelmodel, dataflow, QA, hosting, begrænsninger, åbne punkter.
- **VAT-Analytics_Sikkerhed_og_databehandling.docx** — datakategorier, dataflow, GDPR/ikke-arkiv, adgang, trusselsmodel, logning.
- **VAT-Analytics_Hosting_og_drift.docx** — nuværende drift (EU), EY-platform, migration, env-inventar, backup, CI, roller.

## Auto-genererede artefakter
- **VAT-Analytics_Regel-sporbarhedsmatrix.xlsx** — kontrol → kilde → modul → test. Genereres af `backend/tools/build_traceability.py` fra regelkataloget.
- **VAT-Analytics_Sporbarhedsrapport.md** — dækningsrapport (aktive/inaktive, kilde-/testdækning).
- **VAT-Analytics_Valideringsrapport.md** — resultat af den uafhængige valideringssuite. Genereres af `python -m validation.run_validation`.
- **VAT-Analytics_Opfoelgningspunkter.md** — levende liste over åbne punkter.

## Regenerering
```
cd backend
python tools/build_rules_catalog.py        # catalog/rules.json
python tools/build_traceability.py          # matrix.xlsx + sporbarhedsrapport.md
python -m validation.run_validation         # valideringsrapport.md
```
Docx (kræver Node + lokal install i repo-roden):
```
npm install && node backend/tools/build_approval_docs.js
```

## Kilder
Regelkatalog: `backend/catalog/rules.json` (+ `rule_notes.json`). Kontrol-logik:
`backend/analytics/`. Valideringssuite: `backend/validation/`.
