# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

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
