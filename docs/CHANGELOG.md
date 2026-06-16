# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

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
