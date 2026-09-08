# TTAR-afklaring og mapping — VAT Analytics

TTAR = **Tax Technology Approval Roadmap** (EY Global Tax Quality). Dette dokument
afklarer, om værktøjet kræver en TTAR, og mapper TTAR-rammens krav til vores
konkrete dokumentation og evidens — så grundstrukturen er beskrevet i TTAR-termer
og godkendelsen er velforberedt. Versioneres sammen med koden.

Kilder: TTAR-mappen (Global Tax Quality QRG'er): *Requirement and Exemptions
Guide*, *Functional (Tax Technical content) Review*, *Open-Source Software (OSS)*,
*Data Use and AI-specific Questions (Addendum)*, *How to determine if a Tool needs
a TTAR*. Struktur og fortolkning følger søsterværktøjets afklaring
(`saf-t-validator/docs/TTAR-afklaring-og-mapping.md`), så de to spor er ensartede.

---

## 1. Kræver værktøjet en TTAR? — Ja

GTSLP §27.4 kræver en godkendt TTAR for alle værktøjer i Tax Service Line, **før
der lægges klientdata i værktøjet eller det bruges på engagements**. VAT Analytics
behandler klientdata (virksomhedens momsdataudtræk — Excel/CSV eller SAF-T) og skal
i produktion → TTAR er påkrævet. Ingen undtagelse passer:

| Undtagelse (Requirement & Exemptions Guide) | Gælder? | Hvorfor ikke |
|---|---|---|
| Intern administrativ tool | Nej | Bruges på klientleverancer, tilbageholder klientdata midlertidigt |
| Ekstern referencedata/research | Nej | Udfører kontrol/beregning, ikke ren opslag |
| Datatransport | Nej | Analyserer og beregner momsforhold, flytter ikke bare data |
| EY Empowerment-platform | Nej | Egen FastAPI-app |
| Prototype | Nej | Skal i produktionsbrug med klientdata |
| Workpaper (Excel, single client) | Nej | Web-app, flere klienter, ikke Office |
| Enterprise / TTT single-client / 3.-part SaaS | Nej | EY-udviklet flerklient-værktøj |

**Handling:** Opret TTAR + Tax Solutions Inventory (TSI)-post; TTAR-nummer i
TSI-record.

## 2. Jurisdiktion og deployment (spørgsmål 2p)

Indholdet er **dansk (DK)**: den danske momslov, den danske standardkontoplan
(ERST), danske momskoder og dansk SAF-T Financial. Værktøjet er p.t. et
**single-country (DK) tool**. Bliver det senere flerlands, skal "Global" vælges
først i 2p, og der kræves Global Tax Quality + Risk Management-review.

## 3. Funktionel (skattefaglig) review — den centrale port

Iflg. GTSLP §27.3 behandles Tax SL-værktøjer som *Highly Significant Advice*. Det
jurisdiktions-specifikke indhold skal godkendes af **to PPED'er fra DK**.

For dette værktøj er det jurisdiktions-specifikke indhold: **hele regelkataloget
(103 kontroller; 98 aktive), den danske moms-/reverse charge-logik (`vat_rules.py`),
standardkontoplan-rollemappingen (`standard_accounts.py`), momskoderne og
SAF-T-mappingen (`saft_parser.py`).** Det er præcis det, en Functional Reviewer skal
kunne gennemgå — og vores **regel-sporbarhedsmatrix** (hver kontrol → autoritativ
momslov-kilde → implementering → testdækning) + **regelkatalog** (`catalog/rules.json`,
v1.1.0) er materialet, review'et hviler på. Reviewer kan få et dataudtræk, en rapport
eller en demonstration.

**Momsrelevans-scoping (vigtig for review'et):** kataloget er delt i analyse-moduler.
**Momskernen (60 kontroller) er default TIL** som den egentlige momsgennemgang; 43
kontroller (forensic/statistik, e-handel/særordninger, datakvalitet, dublet-recovery)
er **default FRA** og dokumenteret som valgfrie. Denne afgrænsning — hvilke kontroller
der er momsfaglige vs. andet mandat — er en bevidst faglig beslutning (jf. dialogen
med en momsfaglig kollega) og bør indgå direkte i den funktionelle review. Hver
kontrol bærer `analyse_modul` + `default_aktiv` i kataloget.

**Åbne handlinger:** (a) udpeg to danske PPED'er som Functional Reviewers (angives i
2p); (b) **kilde-pinning: udfyld den præcise momslov-paragraf pr. kontrol** i
`catalog/rule_notes.json` (mekanikken er på plads; p.t. viser matricen kategoriens
retsområde som fallback — 0/103 præcise kilder udfyldt). Det er den funktionelle
review's kerneinput og analogt til SAF-T-sporets ERST-kilde pr. kontrol.

## 4. AI — ingen (addendum ikke relevant)

TTAR's *Data Use and AI-specific Questions* gælder kun ved brug af AI. **Værktøjets
analyse er 100% deterministisk** (regelmotor over de 103 kontroller); også
konklusion/anbefaling (finding-tekst og fix-forslag) er skabelondrevet, bevidst uden
LLM/generativ AI af hensyn til reproducerbarhed og revisorbarhed. Svar på
nøglespørgsmålet *"Does the tool make use of Generative AI or LLM?"* → **Nej.**
Addendummet udfyldes derfor ikke; det bør positivt anføres i TTAR, at der ikke er AI
(ingen bias-/model-drift-risiko).

## 5. Open-Source Software (OSS)

Værktøjet bruger OSS-komponenter — runtime-kernen: **FastAPI, uvicorn, gunicorn,
pydantic, jinja2, itsdangerous, python-multipart, pandas, openpyxl, SQLAlchemy,
psycopg** (fuld, versionslåst liste: `backend/requirements.txt`; test-/dev-deps
adskilt i `requirements-dev.txt`). TTAR kræver, at OSS-godkendelser (GCO,
Architecture, InfoSec, Independence) indhentes via Technology Service Portal, og at
fire beviser vedhæftes TTAR'en (Group approvals-skærmbillede, Catalog Tasks/InfoSec-
skærmbillede, godkendelses-mail, samt den udfyldte Excel *OSS Intake Questionnaire*).

**Vores tekniske grundlag hertil:** `requirements.txt` (versionslåst inventar),
adskilte runtime/test-deps og løbende **`pip-audit --strict`** i CI. **Åben handling:**
kør OSS-intake + indhent de fire godkendelser.

## 6. Data og GDPR

Klientdata = virksomhedens momsudtræk (Excel/CSV) eller SAF-T-fil (finansdata, CVR,
evt. person-/kontaktnavne i header). Behandling, dataflow, retention,
behandlingsgrundlag og adgangskontrol er beskrevet i
**`VAT-Analytics_Sikkerhed_og_databehandling.docx`** (udkast — skal review'es af
jura/databeskyttelse). **Ikke-arkiv-politik:** inputfilen slettes straks efter kørsel;
resultat efter retention; revisionsloggen indeholder KUN metadata (aldrig
momsnumre/navne/beløb).

## 7. Hosting og deployment

P.t. midlertidigt på Railway (vat.balai.dk, EU West). **Før produktionsbrug med
klientdata skal værktøjet migreres til EY-platformen** — plan i
**`VAT-Analytics_Hosting_og_drift.docx`**. Central brugerstyring via BALAI
(`central_auth.py`, delt login på `.balai.dk`, fælles Postgres), HTTP-
sikkerhedsheaders, samt in-memory jobs der er private pr. bruger (1 worker).

## 8. Ændringsstyring, versionering og QA-evidens

- **Regelkatalog-version** (`catalog_version`, nu **1.1.0**) + `docs/CHANGELOG.md`
  pr. version → sporbar ændringsstyring. Katalog-drift-gate i CI sikrer, at det
  committede katalog matcher koden.
- **Uafhængig valideringssuite** (`validation/`) + `VAT-Analytics_Valideringsrapport.md`
  — én plantet defekt pr. aktiv kontrol; **98/98 aktive kontroller** består (ren
  baseline tavs + defekt fanges).
- **190 automatiserede tests** + CI (GitHub Actions: pytest + katalog-generering +
  valideringssuite + `pip-audit --strict` ved hvert push).
- **Verifikation mod reelle klientfiler:** to danske SAF-T-udtræk kørt igennem
  (fejlmærkede v1.0-filer med fuldt datasæt). Fund: `AccountType` er mislabeled
  "Other" på ~93–100% af konti → robust momsrelevans-scope baseret på
  `StandardAccountID`/standardkontoplanen bekræftet (balanceposter holdes ude af
  momsfundene).

## 9. Mapping — TTAR-dimension → vores evidens → status

| TTAR-dimension | Vores dokument/artefakt | Status |
|---|---|---|
| TTAR-pligt + TSI | Dette dokument, afsnit 1 | Afklaret; TSI-post: åben |
| Formål, afgrænsning, arkitektur, dataflow | `Solution_Architecture.docx` + `CLAUDE.md` | Findes (ajourfør til moduler/SAF-T-input) |
| Jurisdiktions-indhold (funktionel review) | `Regel-sporbarhedsmatrix.xlsx` + `catalog/rules.json` | Materiale findes; **2 DK-PPED-review + kilde-pinning: åben** |
| Analyse-scoping (momskerne vs. valgfri) | `analytics/modules.py`, katalogets `analyse_moduler` | Implementeret + dokumenteret; skal med i review |
| AI | Afsnit 4 (ingen AI) | N/A — anføres positivt |
| OSS | `requirements.txt`, `pip-audit` (CI) | Inventar findes; **OSS-godkendelser: åben** |
| Data/GDPR | `Sikkerhed_og_databehandling.docx` | Udkast — jura-/DPO-review: åben |
| Hosting/drift | `Hosting_og_drift.docx` | Udkast; **EY-platform-migration før klientdata: åben** |
| Ændringsstyring/QA | `CHANGELOG.md`, `Valideringsrapport.md`, tests, CI | Dækket |
| Godkendelses-status samlet | `Godkendelses-overblik.docx` | Findes (skal ajourføres) |

## 10. Åbne governance-handlinger (kort)

1. Opret TTAR i TTAR-værktøjet + TSI-post (TTAR-nr. i TSI).
2. Udpeg **2 danske PPED'er** til den funktionelle skattefaglige review (2p).
3. **Kilde-pin de præcise momslov-paragraffer pr. kontrol** i `rule_notes.json` og
   regenerér katalog + sporbarhedsmatrix (funktionel review's kerneinput).
4. Kør **OSS-intake** og indhent GCO/Architecture/InfoSec/Independence-godkendelser
   (4 beviser vedhæftes TTAR'en).
5. Jura/DPO-review af sikkerheds-/databehandlingsdokumentet.
6. **Migrér til EY-platformen før klientdata** i produktion.
7. Ajourfør de fire `.docx`-artefakter til 103 kontroller / v1.1.0 / moduler /
   SAF-T-input (se afsnit 9).
