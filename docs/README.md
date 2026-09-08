# VAT Analytics — godkendelses-dokumentationspakke

Indeks over dokumentationen til EY-værktøjsgodkendelsen. Alle artefakter er
versionsstyret og — hvor relevant — auto-genererede fra koden/regelkataloget, så
de ikke kan drifte.

## Start her
- **VAT-Analytics_Godkendelses-overblik.docx** — status pr. område + åbne punkter + næste skridt.
- **TTAR-afklaring-og-mapping.md** — mapper EY's TTAR-godkendelsesramme (Tax Technology
  Approval Roadmap) til vores dokumentation/evidens: TTAR-pligt, jurisdiktion, funktionel
  review, AI (ingen), OSS, data/GDPR, hosting, ændringsstyring + åbne governance-handlinger.

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

## Input: Excel/CSV og SAF-T
Værktøjet accepterer både et fladt Excel/CSV-udtræk og en dansk SAF-T Financial-fil
(`.xml`, v1.0/2.0/2.1). `backend/parsers/upload_router.py` router på filendelse eller
indholds-sniff; begge kilder giver samme kanoniske struktur til motoren. SAF-T-parseren
(`backend/parsers/saft_parser.py`) er best-effort (kører også på ugyldig/fejlmærket SAF-T)
og XML-hærdet.

## Analyse-moduler (momsrelevans-slankning)
Fra katalog v1.1.0 er de 103 kontroller delt i moduler: **momskernen (60) er default
TIL**; forensic/statistik, e-handel/særordninger, datakvalitet og dublet-recovery er
default FRA. Hver kontrol i `rules.json` bærer `analyse_modul` + `default_aktiv`, og
kataloget har en `analyse_moduler`-oversigt. Aktive moduler styres af miljøvariablen
`ANALYTICS_MODULES` (komma-liste; `alle` = alt; default = kun momskernen). Definition:
`backend/analytics/modules.py`.

## Kilder
Regelkatalog: `backend/catalog/rules.json` (+ `rule_notes.json`). Kontrol-logik:
`backend/analytics/` (motor: `engine.py`; moduler: `modules.py`; materialitet:
`materiality.py`). Valideringssuite: `backend/validation/`.
