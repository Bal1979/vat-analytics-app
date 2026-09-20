# Changelog — VAT Analytics

Følger katalogversionen (`backend/catalog/rules.json` → `catalog_version`) og de
væsentlige løft mod EY-standard.

## Kontrol 84-efterforskningen + K5-b: momskode-værn, kollektivnummer-undtagelse og severity pr. fund — 2026-09-20

Baggrund: K4's åbne tråd (kontrol 84's residual domineret af et leverandør-
kartotek-datakvalitetsmønster) + landetabel-rundens 70/84-OBS (præ-eksisterende
momskode-løs støjklasse) + K5-b-anbefalingen fra vagtpost-verifikationen.
Samme disciplin som K1-K4: empirisk fordeling FØRST, på kunde 2s IFS-datasæt.
Katalog **v1.5.1** (regeladfærd + kontrol 84's severity nu "dynamisk" i
kataloget — ingen nye/omnummererede kontroller), datakontrakt v0.5.0 uændret,
**540 automatiserede tests** (11 nye, `tests/test_kontrol84_efterforskning_
2026_09_20.py`), valideringssuite **105/105** (planted defect for kontrol 70
og 84 omdesignet: bærer nu momskode, så scenarierne tester risikofaktor-
logikken og ikke momskode-værnet).

**Empirisk fordeling (kontrol 84's 330 kritiske fund på HEAD):**

- **235 (71%) lå på linjer HELT uden momskode** — kontiene bag dem er
  IC-tilgodehavender, valutakursdifferencer, AR/AP-afregning og
  omsætningskonti (betalinger/afregninger — intet momsfradrag angivet,
  dermed ingen missing trader-eksponering). Samme billede for kontrol 70:
  41.366/50.804 fund (81%) momskode-løse, samme kontoklasser.
- **184 (56%; 71 af dem MED momskode) sad på kun 8 delte, EU-format-ugyldige
  momsnummer-værdier** — hver med ≥ 3 (op til 28) DISTINKTE, tydeligt
  forskellige modpartsnavne i beskrivelsesteksten. Mønstrene er strukturelle
  kartotek-artefakter: landekode alene ("DE"/"BE"/…), pladsholdertekster,
  afkortede numre (8 cifre hvor landet kræver 9) og Excel-notation-
  korruption ("1,0001E+14"). **Kartoteks-efterprøvning bekræfter
  konventionen på masterdata-niveau:** 24 Tax ID'er i kundens eget
  leverandørkartotek deles af ≥ 3 forskellige leverandørnavne (op til 37) —
  en samle-/kollektivkonto-konvention, ikke skjulte handelspartnere.
  **Kontrolgruppe:** 0 fund havde et ugyldigt nummer med < 3
  beskrivelsestekster — en ægte missing trader (ét navn) rammes ikke.
- **23 momskodede fund med TOMT nummer**: reelle, navngivne eksterne
  leverandører (advokater, brancheorganisationer, udenlandske
  serviceleverandører), hvor kartoteket mangler et registreret momsnummer —
  kartotek-datakvalitet/afklaringsspørgsmål, ikke MTIC-mønstre.
- **4 fund med GYLDIGT nummer**: højrisikovare-nøgleordet matchede
  leverandørens NAVN (ikke en vare) — kendt begrænsning, dokumenteret som
  selvstændig observation nedenfor.

**De tre ændringer (alle generiske/strukturelle — INGEN kundenavne eller
momsnummerværdier i koden):**

1. **Momskode-værn (K5-b) for kontrol 70 + 84** (`cat09_reverse_charge.
   test_70_eu_service_no_rc`, `cat11_fraud_mtic.test_84_missing_trader`):
   linjer helt uden momskode springes over — samme princip og
   implementeringsmønster som K2/kontrol 32. KENDT residualrisiko for
   kontrol 70 (bevidst accepteret, dokumenteret i docstring): et EU-køb
   bogført HELT uden momskode kan ikke længere flages — klassen var
   empirisk tom på begge datasæt.
2. **Kollektivnummer-undtagelsen (kontrol 84)**: et udfyldt nummer, der
   fejler EU-formatvalideringen, tæller ikke som "ugyldigt momsnr"-
   risikofaktor, når samme værdi optræder med ≥
   `materiality.CONTROL_84_SHARED_VAT_MIN_DESCS` (default 3, env-
   overstyrbar) distinkte, normaliserede beskrivelsestekster i datasættet
   (`_shared_vat_desc_counts`, bounded optælling). Formatfejlen dækkes
   fortsat af kontrol 28 (3.922 fund, uændret).
3. **Severity pr. fund (kontrol 84)**: "critical" kun når højrisikovare-
   faktoren indgår (den klassiske MTIC-profil); ellers "high". Kataloget
   viser nu "dynamisk" (samme mønster som kontrol 1/80/82/109).

**Empirisk før/efter (kunde 2s IFS-datasæt, alle moduler — eksakt
mmap-tælling af begge rapporter):**

| Kontrol | Før | Efter |
|---|---|---|
| 70 (EU-køb uden RC-markering) | 50.804 høj | 9.438 høj (alle momskodede: E1G/E0G/E0S/E1S m.fl.) |
| 84 (Missing trader) | 330 kritisk | 24 (1 kritisk + 23 høj) |
| 27/30/109 (vagtposter) | 520 / 40 / 402 | 520 / 40 / 402 (UÆNDREDE) |

I alt 1.279.630 → 1.237.958 fund. **Alle øvrige 107 kontroller
fund-identiske** (25: 3.461, 28: 3.922, 32: 147, 33: 12.261, 34: 37, 35:
23.158, 38: 40 m.fl. — uændrede). Kontrol 84's residual (24) er den ægte
kerne: 1 kritisk (højrisikovare-profil) + 23 høj (navngivne leverandører
uden registreret momsnummer — kundespørgsmål, jf. opfølgningsloggen K2-6).

**Regression:** BC-v5-datasættet (125.986 rækker, 23.095 fund) **byte-for-
byte identisk** før/efter (isoleret git-worktree på HEAD `a97ecda` vs.
arbejdstræet; SHA256 med `generated_at`/`catalog_version` strippet — eneste
øvrige difference er `koeretid_sekunder`). OBS: på BC-vejen er kontrol 84
'ikke målbar'-gated (country-feltet findes ikke i datagrundlaget), så
BC-fundene påvirkes hverken af værnet eller severity-ændringen; verificeret
ved direkte kald, at BC's 2 potentielle fund (mobiltelefon-køb, momskodede,
højrisikovare-profil) ville forblive "critical".

**Åbne tråde (uden for denne rundes evidensgrundlag):**

- Kontrol 70's residual (9.438) sidder på momskoderne E1G/E0G/E0S/E1S
  (EU-varer/-ydelser) — om disse koder ER kundens RC-/erhvervelsesmoms-
  koder kan ikke afgøres deterministisk her (kundens vat_setup.csv har
  ingen `vat_calculation_type`-kolonne). Egen kalibreringsrunde mod
  ekspertens tal, evt. via `RC_CODE_PREFIXES`/vat_calculation_type.
- Højrisikovare-nøgleordene matcher også leverandør-NAVNE (fx et navn
  indeholdende "mobile") — gav 4 af de 330 fund. Kandidat til en senere
  præcisering af `MTIC_HIGH_RISK_KEYWORDS`-matchningen.
- K4's åbne tråd om leverandørkartoteket er hermed AFSLUTTET som
  motor-spørgsmål: mønsteret er bekræftet som kartoteks-datakvalitet og
  håndteres nu generisk. Kartoteks-OPRYDNINGEN er fortsat kundens —
  spørgsmålet ligger i opfølgningsloggen (K2-6).

## Landetabel-runden: generisk ISO 3166-1-navnetabel — 2026-09-20 (AFVENTER Bals godkendelse — se "Låste kontroller" nedenfor)

Baggrund: K1-K4's åbne tråd — `vat_rules._COUNTRY_NAME_TO_CODE` (kun ~25
danske/engelske navne) er for lille til ERP-udtræk, der bærer fulde engelske
landenavne. Empirisk fordeling FØRST (K-disciplinen), på kunde 2s IFS-datasæt:
**72 distinkte landenavne, kun 16 genkendt** — 187.024 linjer havde et
udfyldt-men-ukendt landenavn, heriblandt 11 EU-lande (EE/CZ/RO/HU/SI/SK/LV/
LT/HR/BG/MT), som motoren var HELT blind for i alle EU-/tredjelandskontroller.

**Fixet (generisk, IKKE kunde-specifikt):** tabellen udvidet til fuld ISO
3166-1-dækning — 424 navne → 250 alpha-2-koder: officielle engelske kortnavne
(inkl. ISO's kommaformer som "KOREA, REPUBLIC OF"/"TAIWAN, PROVINCE OF
CHINA"), almindelige engelske varianter og danske navne. `normalize_country`
kollapser nu whitespace og stripper et ledende "THE " ("THE NETHERLANDS" →
NL). Alt andet uændret: 2-bogstavskoder passerer stadig uændret igennem, og
udfyldt-men-ukendt tekst er stadig "" (aldrig et gæt). Ingen ny dependency.
Katalog v1.5.0/kontrakt v0.5.0 uændrede (ren regeladfærd via delt helper),
**529 automatiserede tests** (14 nye, `tests/test_landetabel_iso3166.py`),
valideringssuite **105/105** uændret.

**Empirisk før/efter (kunde 2s IFS-datasæt, alle moduler; alle øvrige
kontroller fundtal-identiske — eksakt mmap-tælling af begge rapporter):**

| Kontrol | Før | Efter | Vurdering |
|---|---|---|---|
| 25 (Nulsats indenlandsk) | 3.461 | 3.461 | **UÆNDRET** — K3's konservative landebestemmelse ekskluderede allerede de udenlandske bilag; tabellen er altså IKKE root cause bag 25's rest-støj (hypotesen afkræftet empirisk) |
| 28 (Momsnummer-format) | 3.922 | 3.922 | **UÆNDRET** — K1's præfiks-først-logik er robust |
| 32 (Manglende landekode) | 530 | 147 | −383 ren støjfjernelse (parten HAR et land — motoren kunne bare ikke læse det) |
| 98 (IOSS-lavværdi) | 2 | 0 | −2: linjernes land viste sig at være EU — korrekt ekskluderet |
| 29 (EU-erhvervelse m. moms) | 1.816 | 2.151 | +335 høj, samme klasse som de eksisterende (SI/EE/SK/LV/…) |
| 70 (EU-køb uden RC-markering) | 32.973 | 50.804 | +17.831 høj (EE 5.527, SI, RO, HU, SK, CZ, LV, LT, HR, BG, MT) — se OBS nedenfor |
| 84 (Missing trader) | 247 | 330 | +83 kritisk (AU/IN/HK/AE/…) — se OBS nedenfor |

**Låste kontroller (K1-K4 krævede dem uændrede — status og evidens):**

| Kontrol | Før | Efter | Evidens |
|---|---|---|---|
| 33 (Valuta/land) | 12.261 | 12.261 | UÆNDRET (fund-identisk, også på indhold) |
| 109 (Fradragsprocent) | 402 | 402 | UÆNDRET (landeuafhængig) |
| 27 (EU-handel u. momsnr) | 313 | 520 | +207 høj: EE 85, RO 52, CZ 29 m.fl. — samme fundklasse som de eksisterende 313 (DE/FR/…), blot for de tidligere ukendte EU-lande |
| 30 (Eksport m. moms) | 17 | 40 | +23 høj: CN 12, IN 7, UA/AE/ZA/IS — samme klasse, 240 t.kr. moms |
| 34 (DK-momsnr på udl. part) | 0 | 37 | +37 medium: ALLE er EE-part med DK-præfiks (mønster: udenlandsk enhed med dansk momsregistrering — reelt afklaringsspørgsmål) |
| 35 (Præfiks vs. land) | 19.953 | 23.158 | +3.205 medium: domineret af (HK,CN) 1.700, (SG,CN), (TW,CN), (MX,SA) — samme klasse som kontrollens eksisterende fund. OBS: 50 er (MC,FR) — Monaco anvender lovligt franske momsnumre (kendt undtagelse, kandidat til hvidliste); 8 er (IL,EU) — "EU"-præfiks er non-Union OSS-registrering, ligeledes legitim form |
| 38 (Import u. dok.) | 17 | 40 | +23 medium: spejler kontrol 30 (samme linjer, købssiden) |

**OBS — kontrol 70/84's vækst er overvejende en PRÆ-eksisterende, ukalibreret
støjklasse, ikke tabellens skyld:** stikprøve-karakterisering af de nye fund
viser, at ~66% (70) hhv. 86% (84) sidder på linjer HELT UDEN momskode — den
klasse, K2 fastslog som strukturelt uden for momsscope (bank, løn, koncern-
mellemregning). Kontrol 70/84 har ALDRIG haft K2's momskode-værn; deres
eksisterende fundmasse (32.973 hhv. 247) har samme sammensætning for de
altid-genkendte lande. Tabellen skalerer altså en eksisterende adfærd
konsistent op — den skaber ikke en ny fejlklasse. Et momskode-værn for 70/84
er en selvstændig kalibreringskandidat ("K5"), som IKKE er implementeret her
(kræver egen evidensrunde + Bals godkendelse).

**Regression:** BC-v5-datasættet (125.986 rækker, 23.095 fund) **byte-for-byte
identisk** før/efter (isoleret git-worktree på HEAD `d72fa48` vs. arbejdstræet;
SHA256-sammenligning af rapporterne med kun `generated_at` strippet) — BC-vejen
bærer landekoder, ikke navne. Bemærk: K1-K4-afsnittets "23.549 fund" for BC-v5
kan ikke reproduceres med den dokumenterede CLI-invokation — den giver 23.095
(= F-rundens 22.997 + kontrol 109's 98, konsistent med F-rundens egne tal);
uoverensstemmelsen er i det TIDLIGERE changelog-tal, ikke i denne kørsel.

**Uafhængig vagtpost-verifikation (2026-09-20, efter commit `ab19aeb`) —
svar på de fire kontrolpunkter:**

1. **Kontrol 25 uændret er KORREKT adfærd, ikke en manglende kobling:**
   K3-grenen kalder `vr.normalize_country`/`vr.is_foreign` direkte
   (`cat03_vat_rate_validation.py`, den kontobaserede gren), og fundenes
   referencelinjer er 3.320× eksplicit hjemlandets eget navn + 141× tom
   landetekst — **nul ukendte landenavne**. Vejen mod ekspertens ~146 går
   gennem momskode-populationsafgrænsning: 3.304 af de 3.461 ligger på den
   generiske nulkode "0", som ekspertens population slet ikke medregner
   (ekspertens 146 er alene 0U/E0S/E0G/S0-bilag; motorens fund på netop de
   fire koder: 34). Det er en selvstændig K5-kalibreringsbeslutning — IKKE
   landetabellen.
2. **Kontrol 27 (+207) efterprøvet mod ekspertmaterialet:** 74 fund er en
   EU-offentlig institution uden momsnummer, som ekspertens eget
   bilagsmateriale selv flager — begge ekspertens konkrete bilagsnumre er
   genfundet blandt motorens fund (dokumenteret med navne i kundesagens
   gap-analyse-log, ikke her — kundedata). 78 er kundens eget udenlandske
   repræsentationskontor (koncernintern, uden momsnr — afklaringspunkt,
   næppe compliance-fund), ~44 privatpersoner (B2C-/OSS-spørgsmål), 11
   banklinjer (momskode-løs støjklasse). **Dom: ægte fundklasse med kendte
   støjkomponenter** — samme kalibreringsbehov som 70/84-OBS'en ovenfor.
3. **Kontrol 30 (+23) efterprøvet:** gennemgående reelle samme-klasse-fund —
   tilbagevendende tredjelandsmodparter med bogført dansk moms (240 t.kr. i
   alt); ét fund matcher direkte ekspertens bilagsliste (ekspertens
   population var salgs-only og mindre end motorens). **Dom: ægte.**
4. **Kontrol 84 (+83 kritisk) efterprøvet mod K4-mønstrene:** kun 2/80 er
   koncerninterne — væksten er IKKE K4's koncernklasse. 64/80 er eksterne,
   globalt kendte leverandører uden registreret momsnummer i kartoteket
   (kartoteks-datakvalitetsklassen fra K4's åbne tråd), 16 er banklinjer
   (momskode-løse). Severity "kritisk" er for skarp for denne
   sammensætning — bekræfter K5-kandidaten (momskode-værn +
   kartoteksefterprøvning) FØR nogen rapportvisning.

BC-v5-regressionen er genbekræftet byte-for-byte identisk (metoden ovenfor).
Verifikationen affødte INGEN kodeændringer — punkt 1's hypotese (manglende
kobling til tabellen) er afkræftet empirisk.

**Godkendelsesstatus:** kontrol 27/30/34/35/38 ændrer tal, hvilket K1-K4
eksplicit låste. Ændringerne er dokumenteret ovenfor som samme-klasse-fund
(ingen ny støjklasse ud over den præ-eksisterende 70/84-OBS). Runden er
committet lokalt, IKKE pushet — **produktionsbrug afventer Bals eksplicitte
godkendelse som tilsigtet forbedring.**

## Gap-analyse-runde 2 (kunde 2/IFS) — kalibreringsrunden K1-K4 — 2026-09-20 (Bal-godkendt)

Baggrund: efter F1-F3 (nedenfor) "vågnede" 35 land-/RC-afhængige kontroller til
liv med D-parter-joinet — men fire af dem (28/32/25/84) viste massiv
FØRSTE-AKTIVERINGS-STØJ på kunde 2s IFS-datasæt (samme disciplin som BC-
kalibreringsrunderne: empirisk fordeling FØR fix). Katalog **v1.5.0**
(regelændringer i eksisterende kontroller — ingen nye/omnummererede
kontroller, ingen versionsbump nødvendig), datakontrakt **v0.5.0** uændret
(ny `intercompany`-linjeflag er canonical-only, samme status som
`credit_note_flag`/`supply_direction`/`tax_point` — IKKE et data_contract-
felt), **515 automatiserede tests** (17 nye, `tests/test_kalibrering_k1_k4_
2026_09_20.py`), uafhængig valideringssuite **105/105**.

- **K1 (kontrol 28 "Ugyldigt momsnummer-format", 120.826 → 3.922 høj):**
  hypotesen BEKRÆFTET empirisk. `vat_rules.validate_eu_vat_format` tvang
  ETHVERT momsnummer gennem EU-formatkataloget, uanset landepræfiks. Af
  ca. 1,16 mio. linjer med momsnummer på kunde 2s datasæt var ~1,03 mio.
  EU-præfikserede (heraf kun ~3.900 REELT ugyldige), ~107.000 havde et
  alfabetisk, men ikke-EU-præfiks (CH/NO/CN/GB/US/HK/IS/CA/TW/ZA/RS/UA/…
  — globale Tax ID'er, ikke fejlbehæftede EU-numre), og ~15.000 manglede
  helt et alfabetisk præfiks. Fix: **landeform-bevidst** — valider kun mod
  EU-mønstre når partens land (bestemt af momsnummerets EGET, pålidelige
  præfiks, subsidiært det oplyste land) er ET KENDT EU-LAND; er landet
  udenlandsk-men-ikke-EU eller ubestemmeligt, findes der intet EU-format-
  katalog at holde det op imod — "ikke valideret" (intet fund), IKKE
  "ugyldigt". Et EU-præfikseret nummer, der reelt fejler sit eget lands
  regex, forbliver et fund (uændret sværhedsgrad "high").
- **K2 (kontrol 32 "Manglende landekode", 205.788 → 530 lav):** fordeling
  først bekræftede TO adskilte årsager. 98,7% af de linjer, hvor landekoden
  mangler, mangler OGSÅ momskoden helt (kontantkasse/bank, løn, afskriv-
  ninger, projekter, koncernmellemregninger m.v. — kildesystemets PARTY_TYPE
  COMPANY/blank, strukturelt uden for momsscope). De resterende, momskodede
  linjer ER et reelt datagrundlags-gab, men blev talt PR. LINJE, så samme
  part blæste tallet op. Fix: (1) udelad linjer uden momskode (strukturelt
  ikke momsrelevante), (2) aggregér resten PR. PART-NØGLE (momsnummer,
  subsidiært konto+beskrivelse — samme mønster som kontrol 80's pr.-konto-
  aggregering, `materiality.CONTROL_32_MAX_REFS`), ikke pr. linje.
- **K3 (kontrol 25 "Nulsats indenlandsk", 62.471 → 3.461 medium):** fordeling
  først afslørede at F3's form-hypotese KUN var en DELFORKLARING — 86,9% af
  FØR-fundene var reelt UDENLANDSKE bilag (fx "ROMANIA"/"THE NETHERLANDS"/
  "CHINA"), som `vat_rules.normalize_country`s lille landenavne-tabel ikke
  kunne genkende og derfor (efter den hidtidige "tom = indenlandsk"-
  antagelse) blev fejlklassificeret som indenlandske. Kun ~12% var genuint
  indenlandske grundlagslinjer, hvor moms lå på en separat momskonto-linje i
  samme bilag (den oprindelige F3-hypotese). Fix, ny `_test_25_account_based`
  (dispatcher via `vat_form.is_account_based`, linjebaseret gren 100%
  uændret): (1) bilagsniveau — kun fund når bilagets AGGREGEREDE moms for
  koden er 0 alle steder (genbruger `vat_form.voucher_code_aggregates`), (2)
  konservativ landebestemmelse LOKAL til denne gren — en udfyldt, men
  ikke-normaliserbar landetekst antages IKKE indenlandsk (kun en helt tom
  landetekst gør). Den bredere landenavne-tabel-udvidelse (ville også ramme
  #27/#30/kontrol 33/34/35 m.fl.) er bevidst UDEN FOR denne rundes scope —
  se "Åbne tråde" nedenfor.
- **K4 (kontrol 84 "Missing trader", 309 → 247 kritisk; #86/#92 verificeret):**
  fordeling først viste kun ~20% (62/309) af de kritiske fund var bekræftet
  koncerninterne (kildesystemets egen `intercompany`-boolean, hidtil slet
  ikke læst af `parsers/canonical_parser.py`); resten (~59%) skyldtes en
  SEPARAT, ikke-koncernrelateret data-kvalitetsartefakt i leverandør-
  kartoteket (flere reelt forskellige tyske leverandører deler samme
  generiske/ufuldstændige "momsnummer") — se "Åbne tråde", uden for denne
  rundes evidensgrundlag. Fix, KONSERVATIVT og GENERISK (intet kunde-/
  selskabsnavn i koden): ny `cat11_fraud_mtic._is_intercompany` undtager en
  linje når (a) kildesystemets egen `intercompany`-flag er sat (nyt
  canonical-only linjefelt), ELLER (b) modpartens "momsnummer" har formen af
  en intern koncern-partskode (landepræfiks + 1-3 cifre, fx "US10"/"CA10" —
  et generisk mønster, `vat_rules.looks_like_internal_party_code`, empirisk
  fundet i kundens EGET kartotek, "DK10/FI10-mønstret" fra gap-analysen).
  #86 (32.048) og #92 (9.978) er UÆNDREDE — verificeret at begge hører til
  `forensic_statistik`-modulet, som allerede er default FRA (samme
  pakkevalg som kunde 1) — ingen logikændring uden empirisk belæg, som
  krævet.

**Empirisk før/efter (kunde 2s IFS-datasæt, alle moduler):**

| Kontrol | Før | Efter |
|---|---|---|
| 25 (Nulsats indenlandsk) | 62.471 | 3.461 |
| 28 (Ugyldigt momsnummer-format) | 120.826 | 3.922 |
| 32 (Manglende landekode) | 205.788 | 530 |
| 84 (Missing trader) | 309 | 247 |
| 27/30/109 (kontrol-check) | 313 / 17 / 402 | 313 / 17 / 402 (UÆNDREDE) |

I alt 1.639.505 → 1.258.271 fund. Regression: kontrol 1-108 (bortset fra
25/28/32/84) byte-for-byte identiske, både på kunde 2s datasæt og på
BC-v5-datasættet (125.986 rækker, 23.549 fund — 0 differencer, verificeret
via en isoleret git-worktree på forrige commit). Kunderapport niveau 3
regenereret til kunde 2s eget scratchpad (kundedata, uden for repoet).

**Åbne tråde (uden for denne rundes evidensgrundlag/godkendelse):**

- `vat_rules._COUNTRY_NAME_TO_CODE` (kun ~25 danske/engelske navne) er for
  lille til kunde 2s IFS-udtræk (72 distinkte fulde engelske landenavne,
  kun ~14 genkendt) — root cause bag K3's resterende støj OG en del af
  K1/K2's oprindelige omfang. En generisk ISO-navnetabel-udvidelse er
  IKKE lavet i denne runde, fordi den ville ændre #27/#30 (og evt. #33/34/
  35/38) på en måde, der ikke er efterprøvet mod ekspertens tal endnu — en
  selvstændig, fremtidig kalibreringsrunde.
- Kontrol 84's resterende 247 kritiske fund domineres (~181) af et
  leverandørkartotek-datakvalitetsproblem (flere distinkte tyske
  leverandører deler samme generiske/afkortede "momsnummer", fx et 8-cifret
  "DE"-nummer i stedet for 9) — IKKE koncernintern eller MTIC-lignende.
  Kræver leverandørkartotek-efterforskning, uden for denne rundes scope;
  faglig efterprøvning FØR nogen rapportvisning.

## Gap-analyse-runde 2 (kunde 2/IFS) — fix-runden F1-F3 — 2026-09-20 (Bal-godkendt)

Baggrund: en motorkørsel på et kanonisk IFS-datasæt (180.070 bilag, 1.317.864 rå
fund) afslørede tre navngivne, rettelige BC/NAV-formede antagelser i kontrol-
laget — dataside/afstemning generaliserede perfekt (uændret). Katalog
**v1.5.0** (108→109 kontroller), datakontrakt **v0.5.0** uændret (ingen nye
INPUT-felter — kun nye/ændrede kontroller), **498 automatiserede tests**,
uafhængig valideringssuite **105/105**.

- **F1 (kunde-/stamdata-loader-aliaser):** `parsers/canonical_masterdata.
  load_customers` afviste tidligere ALLE rækker i en IFS-mapping uden
  selvstændig kunde-id-kolonne (kun `vat_registration_numbers`/
  `counterparty_country`) — en reel loader-bug, ikke kun et manglende alias.
  Udvidet til at bruge momsnummeret som id (en ægte forretningsnøgle, IKKE
  den tidligere bevidst afviste `ext_vat_bus_posting_group`) og, hvis selv
  DET mangler, en syntetisk positionel nøgle (`ROW-<n>`) — ingen gyldig række
  tabes længere strukturelt. Derudover fandtes en SEPARAT CLI-begrænsning i
  `tools/analyze_canonical.py`: værktøjet manglede `--vat-setup`/
  `--chart-of-accounts`/`--customers`-flag, så selv en korrekt loader aldrig
  blev kaldt med den rigtige fil, når hver stamdatatabel (som i IFS-
  mappingen) ligger i sin egen undermappe i stedet for ved siden af
  `gl_entries.csv`. Begge dele rettet. Empirisk: customers 0 → **3.173**
  indlæst (0 tomme id'er).
- **F2 (calc-type-/RC-kode-vokabular konfigurerbart pr. ERP):**
  `vat_rules.is_reverse_charge_sale_code` kendte kun BC's
  `vat_calculation_type`-tekst og "Bus.-gruppe|Produktkode"-konventionen —
  begge fraværende i IFS' vat_setup (opake mnemoniske koder som "RC"/"RC50",
  ingen "|"). Ny konfigurerbar `materiality.RC_CODE_PREFIXES` (default
  `["RC"]`, override via `MATERIALITY_RC_CODE_PREFIXES`), samme
  kalibreringsmønster som `NO_VAT_PRODUCT_PATTERNS`, med IFS' egne
  observerede koder (RC/RC50, fra `vat_setup.csv`) som dokumenteret default.
- **F3 (bilagsniveau-momsmodellen — den store):** ny `analytics/vat_form.py`
  — generisk (ikke IFS-hardkodet) statistisk formdetektion pr. datasæt: en
  ERP kan bogføre moms+grundlag på SAMME linje ("linjebaseret", BC/Excel/
  SAF-T — uændret antagelse) eller på FORSKELLIGE linjer i samme bilag
  ("kontobaseret", empirisk bekræftet på IFS: 753.468 momskodede linjer,
  hvoraf momsbeløbet er koncentreret på 11 dedikerede konti mod 207 konti
  for grundlagslinjer). Kontrol **1, 22, 24** er nu FORM-BEVIDSTE: på
  kontobaseret form arbejder de PR. BILAG+MOMSKODE på de aggregerede
  grundlags-/momsbeløb (`vat_form.voucher_code_aggregates`) i stedet for pr.
  linje — linjebaseret form (BC/Excel/SAF-T) er en UÆNDRET separat kodesti
  (regressionstestet byte-for-byte identisk på v5-datasættet, kontrol
  1-108). Kontrol 26 blev GENNEMGÅET men ikke ændret (allerede korrekt på
  begge former, verificeret empirisk). Undervejs fundet og rettet en
  fortegnsbug: IFS' `vat_amount` bærer samme fortegnskonvention som linjens
  netto debet-kredit (negativt på salgssiden) — grundlag og moms sammen-
  lignes derfor konsekvent som magnituder, ellers ville hvert salgsbilag give
  en falsk ~100%/-25%-afvigelse.
- **NY kontrol 109 "Fradragsprocent-afvigelse"** (kategori 13, formuafhængig
  — virker på begge momsformer): pr. bilag+momskode med delvis fradragsret
  (vat_setup's `non_deductible_vat_pct` > 0): forventet moms = grundlag ×
  sats × Deductible%. Gør ekspertens KRITISKE systemfejl-fund (X-Ray s.
  39-42: ET50/R/EI fradragsført med 100% i stedet for 50/25/15%) til en
  deterministisk kontrol — kun muligt efter F3's bilagskobling. Eget
  rapporttema `fradragsret` (altid forfremmet i kunderapporten, jf.
  `tools/report_themes.py`).

**Empirisk før/efter (kunde 2s IFS-datasæt, alle moduler, F1+F2+F3 samlet):**

| Kontrol | Før | Efter | Note |
|---|---|---|---|
| 1 (Moms-genberegning) | 69.564 | 484 | kollapser til reelle bilagsniveau-afvigelser |
| 22 (Manglende salgsmoms) | 33.514 | 78 | domineret af kode '1' — nu korrekt gated pr. bilag |
| 24 (Implicit sats ugyldig) | 79.723 | 854 | samme kollaps, sats vurderes pr. bilag+kode |
| 26 (Momsbeløb uden momskode) | 7.367 | 7.367 | UÆNDRET (gennemgået, ingen form-afhængig fejlkilde) |
| 109 (NY: Fradragsprocent-afvigelse) | — | 402 | 4/4 af ekspertens navngivne eksempler genfundet |

Residual støj i kontrol 1/24/109 er delvist en kendt, allerede-accepteret
bivirkning af GAP-12's bilagsgruppering (`(invoice_numbers, posting_dates)`
kan lejlighedsvis sammenkoble to REELT urelaterede posteringer, der deler
nøgle samme dag — dokumenteret risiko siden GAP-12, ikke ny i denne runde).

Regression: kontrol 1-108 byte-for-byte identiske på BC-v5-datasættet
(22.997 fund uændret); kontrol 109 tilføjer 98 NYE fund på v5 (samme
additive mønster som forrige gap-analyse-runde — v5's eget vat_setup har
delvis-fradragsret-koder, så kontrollen er lige så relevant på BC-data).
Kunderapport niveau 3 regenereret for kunde 2 til kundens eget scratchpad
(kundedata, uden for repoet). Se `GAP-ANALYSE-2-motor-vs-ekspert.md`
(kundedata, uden for repoet) for den fulde ekspert-sammenligning.

## Kunderapportens visuelle løft — 2026-09-19 (katalog/data_contract uændret, ren præsentation)

Bal-godkendt opgave (2026-09-19): "fra internt værktøj til kundeleverance"-
kvalitet for `backend/tools/generate_report.py`. UDELUKKENDE typografi,
layout, grafik og print-kvalitet — de syv sektioners INDHOLD/rækkefølge,
niveau-filtreringen, kurationsmekanikken og "aldrig kontrolnumre uden for
lineage/appendix"-disciplinen er UÆNDREDE (samme tal, samme tekst-kilder).
Referencen for håndværket var kundens X-RAY-præsentationskvalitet
(`GAP-ANALYSE-motor-vs-ekspert.md`, uden for repoet, kun læst for kontekst)
— identiteten er fortsat neutral BALAI, intet kunde-skin.

- **Typografisk system:** CSS custom properties for en konsistent skala
  (forside-titel → sektionsoverskrift → brødtekst → metadata), systemfont-
  stak, opstrammet linjeafstand/marginer. `--content-max: 880px` (var
  1000px) for kortere, mere læsbare linjer.
- **Hero som rigtig forside (sektion 1):** mørk "cover"-panel (navy-
  gradient) med kicker ("Momsanalyse · Resultatrapport"), stor titel,
  kunde-/periode-pladsholder (`[Kundenavn]`, periode fra `summary.period_*`,
  dansk datoformat via ny `_fmt_date()`) og diskret BALAI-afsender. De fire
  nøgletalsfliser er bevaret; "hero-facts"-definitionslisten er erstattet af
  to fremhævede "hero-flow"-fliser (momsgennemstrømning, årets tilsvar) i
  samme store-tal-lille-etiket-stil som nøgletalsfliserne. Den tidligere
  globale `<h1>Momsanalyse — resultatrapport</h1>` + underrubrik uden for
  sektionerne er fjernet — hero ER nu forsiden; niveau-pakkenavnet er
  flyttet ind i hero-meta-linjen.
- **Momsmotoren som grafik (sektion 2):** nyt håndbygget, afhængighedsfrit
  inline-SVG-flowdiagram (`_engine_diagram_html`/`_build_engine_flow_svg`-
  logik) — tre kolonner (Momskoder → Momskonti → rubrikker i angivelsen)
  med streger, hvis tykkelse er proportional med antal momskoder pr.
  rubrik, plus en farvelegend med de fulde rubrik-betegnelser. Bevidst
  FORENKLET (proportionalt pr. rubrik, ikke en literal kode-for-kode-
  tegning, jf. figurteksten) — den eksisterende, præcise tabel står
  uændret lige under som kilden til sandhed.
- **Afstemningen som tillidsanker, visuelt (sektion 3):** uændret 12-
  måneders/periode-tabel med status-chips: nyt håndbygget SVG-søjle-
  diagram PR. RUBRIK (`_recon_barchart_svg`/`_build_recon_charts`) —
  beregnet vs. angivet side om side pr. periode, søjlefarve følger
  celle-status (grøn=match/blå=timing/rød=afvigelse), fælles
  status-legend. Vises kun når der er ≥2 perioder at sammenligne.
- **Observationer & spørgsmål (sektion 4):** tema-etiketten er nu en
  rigtig "chip" (`obs-theme-badge`, pilleform); spørgsmålet har fået en
  "Spørgsmål"-eyebrow og større skrift som visuelt anker
  (`obs-question`); bevis-tabellen er zebra-stribet (CSS); anbefalings-
  udkast (`obs-recommendation`, grøn venstre-accent) og rådgiver-note
  (`advisor-note`, gul/amber venstre-accent) er nu tydeligt visuelt
  adskilt fra hinanden og fra brødteksten.
- **Print/PDF-kvalitet:** `@page { size: A4; margin: 16mm 14mm; }`,
  sideskift PR. HOVEDSEKTION (`section:not(.hero) { break-before: page; }`
  — hero er side 1), `break-inside: avoid` på kort/rækker/diagrammer så de
  ikke deles over et sideskift, samt en ny (kun i print synlig)
  `.print-footer` med kort kørselsinfo/kunde-pladsholder.
- **Selvbærende, uændret:** stadig ét `<style>`-tag, ingen CDN/eksterne
  scripts — de nye diagrammer er 100 % håndskrevet inline-SVG (ingen
  chart-bibliotek). Excel-arbejdsbilaget (`tools/report_workbook.py`) er
  IKKE rørt.
- **Tests:** alle 27 eksisterende `test_generate_report.py`-tests grønne
  uændret (ingen af de tekst-/struktur-baserede assertions krævede
  opdatering — markup-tilføjelserne bryder ikke de checkede substrings).
  Fulde suite **494/494** grøn. Verificeret ved at regenerere niveau 1/2/3
  fra en FRISK `analyze_canonical.py`-kørsel på v5-datasættet (samme
  22.997 fund/gate 208/208 som byggetrin ~11's statuspunkt) + eksisterende
  `kuration_v2.json` → `kunderapport_niveau{1,2,3}_v3.html` i scratchpad
  (uden for repoet). Filstørrelser 51-73 KB pr. niveau (mod 26-44 KB før
  redesignet) — fortsat langt under 300 KB-loftet. HTML-struktur
  verificeret programmatisk (afbalancerede tags, alle indlejrede SVG'er
  parser som gyldig XML) — ingen browser tilgængelig i denne session.
- **Bevidst udeladt (uden for opgavens scope):** kunde-specifikt farve-
  skin (skal være en senere template-parameter, jf. opgaven — identiteten
  er fortsat neutral BALAI), reelle sidetal i print (Chromium understøtter
  ikke CSS Paged Media-margin-bokse til `counter(page)`; en fast
  `.print-footer` bruges i stedet), og motorens 31-perioders periode-liste
  i testdatasættet (data-artefakt fra v5-datasættets tidsspand, ikke en
  del af denne rent visuelle opgave).

## Semantik-PoC: LLM-klassifikation af posteringslinjer — 2026-09-18 (katalog v1.4.0 uændret)

Bal-godkendt opgave (2026-09-18). Ny `examples/poc_semantik/` (spejler
mapping-PoC'en i `vat-extract/examples/poc_bc_gl_2025/`): kan en lokal LLM
klassificere enkelte købsposteringslinjer mod et ekspert-fundkatalog
(F01-F31), målt mod en ekspert-gennemgangs egen linje-klassifikation som
facit? Ingen ændring i motoren/kontrolkataloget — selvstændig PoC-harness.

- **`build_facit.py`/`build_katalog.py`**: læser kundens Excel-gennemgang
  (UDENFOR repo, committes aldrig) til generiske facit-/katalog-JSON'er
  (UDENFOR repo, scratchpad). Indbygget kontrolsum-tjek bekræftede
  byte-for-byte match mod kundens egen afstemning i "Metode og
  forudsætninger" §1 (beløb er kundedata, gengives ikke her). 3.010 linjer,
  240 fund-linjer i 28 aktive fund-id'er.
- **`dedup.py`**: gruppering på normaliseret (konto, leverandør,
  tekst-mønster, momskode, valuta, moms-fratrukket) — ALDRIG facit-fundet i
  nøglen. Stratificeret population (240 fund-linjer + seedet stikprøve på
  300 OK-linjer, seed 42) = 540 linjer → 327 unikke grupper (1,65×
  reduktion).
- **`run_poc.py`**: lokal Ollama, temperatur 0, tvungen JSON, `think: false`,
  batches à 25 grupper. To fund (F01 dobbeltbogføring, F10
  kantine-metodevalg) er markeret `kraever_tvaerlinje_kontekst` og holdt
  uden for pr.-linje-scoring (kan ikke afgøres fair fra én linje).
- **`score_poc.py`**: recall pr. fund-id, OK-nøjagtighed, falske positiver
  rapporteret separat (ikke automatisk dømt forkerte), hallucinerede
  fund-id'er, skema-validitet/tid pr. batch.
- **Røgtest (1 batch, 25 grupper/54 linjer, begge modeller allerede lokalt
  installeret):** `qwen3.8:27b` **92,3 %** korrekte (0 hallucinationer, 0
  falske positiver, rammer næsten hele §42-fradragsbegrænsnings-klyngen
  F05/F06/F07), `qwen3:14b` 78,8 % (samme 0/0 på hallucination/falsk
  positiv, men systematisk svag på F05/F06/F07-klyngen). Batch-tid næsten
  ens (187s vs. 164s — prompt-evaluering af den faste katalog-tekst
  dominerer, ikke modelstørrelsen). **Anbefaling:** 27b, batchstørrelse 25;
  estimeret fuld PoC-kørsel (327 grupper, 14 batches) ≈ 44 min.
- **35 nye harness-tests** (syntetiske fixtures, ingen kundedata, ingen
  netværk) — se `examples/poc_semantik/tests/`. Eksisterende 494 tests
  upåvirket.
- **Fuld kørsel (327 grupper) er IKKE kørt i dette byggetrin** — hoved-
  sessionen orkestrerer den efterfølgende. Se `examples/poc_semantik/README.md`.

## A-listens krydskontroller fra gap-analysen (byggetrin ~11) — 2026-09-18 (catalog v1.3.0 → v1.4.0, data_contract v0.4.4 → v0.5.0)

Bal-godkendt opgave (2026-09-18). Fire nye deterministiske kontroller fra
gap-analysen (BALAI-motoren vs. ekspertleverancen) — A-listens
"billige, høj værdi"-punkter (kundedatafilen `GAP-ANALYSE-motor-vs-ekspert.md`
er ikke i repoet; her kun kontrolnumre/antal/mekanik, ingen kundedata). Ny
kategori 13 "Krydsdimensionelle kontroller" (**kontrol 104-108**, alle i
momskernen) tilføjet ADDITIVT — ingen af de 103 eksisterende kontroller er
omnummereret eller ændret.

- **`backend/analytics/categories/cat13_cross_dimension.py` (ny):**
  - **Kontrol 104** — udenlandsk valuta (`currency` ≠ tom/DKK) bogført med
    den danske standardmomskode (Bus.-gruppe `DOMESTIC`, produktkode
    indeholdende `STANDARD_VAT`, ikke reverse charge). Stilles altid som
    SPØRGSMÅL (severity medium) — en dansk leverandør kan lovligt fakturere
    i udenlandsk valuta.
  - **Kontrol 105** — EU-/3.-landskøb (Bus.-gruppe `EU`/`OUTSIDE DK/EU`)
    med intet bogført momsbeløb. To sikkerhedsniveauer: en reverse
    charge-kode (`vat_calculation_type`, med navnemønster-fallback) giver
    et HØJT fund ("RC-beregning mangler"); en `NO_VAT`-familiekode
    (`vat_rules.is_no_vat_product`, konfigurerbar via
    `materiality.NO_VAT_PRODUCT_PATTERNS`) giver et MEDIUM
    spørgsmålsfund ("kan være en ægte fritaget ydelse, fx pension/
    forsikring — bør bekræftes").
  - **Kontrol 106** — varekøb fra 3.-land (Bus.-gruppe `OUTSIDE DK/EU`,
    produktkode der matcher BÅDE `GOODS_VAT` og `NOT_EU`). Informativt fund
    (severity low) der beder om at bekræfte importørregistrering/
    toldbehandling — uafhængigt af om momsen ser korrekt beregnet ud.
  - **Kontrol 107** — atypisk moms på bilagstype: lærer filens EGEN
    fordeling (ingen hardkodede bilagstype-navne). En bilagstype hvor højst
    `materiality.CONTROL_107_MAX_TYPICAL_VAT_SHARE` (default 5%) af
    linjerne har moms, og som har mindst
    `materiality.CONTROL_107_MIN_LINES_PER_SOURCE_CODE` (default 20)
    linjer, er "typisk momsfri" — de få linjer der alligevel har moms
    flages enkeltvis.
  - **Kontrol 108** — salg/køb spredt over mange bilagstyper: ÉT
    aggregeret fund pr. retning (ikke pr. linje) når en retning bruger
    mindst `materiality.CONTROL_108_MIN_SOURCE_CODES` (default 3)
    forskellige bilagstyper over mindst `materiality.CONTROL_108_MIN_LINES`
    (default 30) linjer. Procesobservation, ikke en fejlpåstand.
  - Kontrol 107-108 kræver det NYE felt `transactions[].lines[].source_code`
    (BC/NAV "Source Code"/bilagstype) — se GAP-14 nedenfor. INGEN af de 5
    kontroller hardkoder kunde-/leverandørnavne; alle grænser er
    strukturelle (Bus.-gruppe, produktkode-mønster, valuta, calc type,
    bilagstype).
  - Ny helpers i `vat_rules.py`: `vat_bus_group`/`vat_product_code`
    (opaque split af "Bus.-gruppe|Produktkode"), `is_rc_calc_type`,
    `is_no_vat_product`. Nye tærskler i `materiality.py`:
    `NO_VAT_PRODUCT_PATTERNS`, `CONTROL_107_MIN_LINES_PER_SOURCE_CODE`,
    `CONTROL_107_MAX_TYPICAL_VAT_SHARE`, `CONTROL_108_MIN_LINES`,
    `CONTROL_108_MIN_SOURCE_CODES` — alle env-overstyrbare
    (`MATERIALITY_*`), defaults konservative.
- **`transactions[].lines[].source_code` (data_contract v0.5.0, GAP-14,
  status ÅBEN):** ny balai_extensions-linjefelt, nøglesæt-symmetrisk på
  alle tre input-veje (canonical: valgfri kolonne, samme best-effort-mønster
  som `description`/GAP-13; Excel: valgfri alias-kolonne
  `source_code`/`bilagstype`/`document_type`/…; SAF-T Financial: intet
  nativt element, altid `""`). Uden feltet (0% udfyldt på hele datasættet)
  'ikke målbar'-gates kontrol 107/108 via `analytics/readiness.py`
  (`CONTROL_REQUIREMENTS[107]`/`[108] = ["source_code"]`) — samme mekanik
  som kontrol 25 uden landekolonne. Ingen falske alarmer i mellemtiden.
- **Kategori 13** tilføjet i `analytics/engine.py::CATEGORIES` (test_range
  104-108) og `tools/build_rules_catalog.py`; `analytics/modules.py` (nye
  kontroller uangivet i `_OVERRIDES` → forbliver i momskernen, default TIL);
  `analytics/readiness.py` (`CATEGORY_REQUIREMENTS[13] = []`,
  `range(1, 104)` → `range(1, 109)` alle steder). Alle "103
  kontroller"/"12 kategorier"-referencer i kode/docstrings opdateret til
  108/13.
- **`tools/report_themes.py`:** nyt tema **`udlandshandel`** → kontrol
  104/105/106 ("er den udenlandske handel momsbehandlet korrekt?") —
  begrundelse: adskilt fra `kodeopsaetning` (som handler om den
  INDENLANDSKE kodeopsætnings korrekthed, ikke grænsen til udlandet).
  Kontrol 107/108 tilføjet til det EKSISTERENDE tema **`proces`** (nu
  {46,14,107,108}) — begrundelse: begge er PROCESOBSERVATIONER
  (bilagstype-/journalbrug), samme karakter som fakturanummer-genbrug/
  faktura-bogføringslag, ikke momsberegningsfejl.
- **Valideringssuite:** 5 nye clean/defect-scenarier (`validation/scenarios.py`)
  — **104/104 scenarier bestået** (99 → 104). **Tests: 494 automatiserede
  tests** (+ ny `tests/test_cat13_cross_dimension.py`, 25 tests). Alle
  eksisterende tests opdateret til de nye totaler (108 kontroller,
  13 kategorier, 6 rapport-temaer) — ingen svækkede assertions.

**Empirisk verificeret** (v5-datasættet — v4 + den nye `source_code`-kolonne,
byte-for-byte identisk på alle andre felter; kørt med alle analyse-moduler,
`tools/analyze_canonical.py`):

| Kontrol | Fund | Acceptkriterium (gap-analysen) | Vurdering |
|---|---|---|---|
| 104 | 100 (alle medium) | Fanger F29-leverandørfamilien (~18 linjer) | 72 af 100 fund matcher F29-familiens beskrivelsesmønster (verificeret i scratchpad, ikke gengivet her — kundedata) — familien er fanget. Antallet af fund (100) er BREDERE end den med vilje: kontrollen er en generel valuta×kode-krydskontrol, ikke en leverandør-specifik regel. |
| 105 | 105 linjer / **59 distinkte bilag** (31 høj, 74 medium) | ~57 bilag (side 24 pkt. 3) | Bilagstallet (59) matcher tæt. Severity splittet: 31 er RC-koder uden beregnet moms (høj), 74 er NO_VAT-familiekoder (medium, spørgsmålsform — pension/forsikring-forbeholdet). |
| 106 | 2 (alle low) | ~10,5 t.kr. moms | **Eksakt match**: samlet momsbeløb 10.561,55 DKK. |
| 107 | 180 (alle medium, i praksis udelukkende bilagstypen `EXPENSE`) | Side 22-tabellen (kvalitativt) | 0 fund på v4 (source_code mangler — 'ikke målbar', jf. datagrundlag), 180 på v5. Ingen hardkodet bilagstype-navn i koden. |
| 108 | 2 (køb: 22 bilagstyper/25.440 linjer; salg: 22 bilagstyper/21.726 linjer) | Side 32/36 (kvalitativt, "23 typer" nævnt i gap-analysen) | 0 fund på v4 ('ikke målbar'), 2 aggregerede procesobservationer på v5 — antal bilagstyper (22) ligger tæt på ekspertens observerede 23. |

**Regressionsgaranti:** de 103 eksisterende kontroller er BYTE-FOR-BYTE
uændrede — pr.-kontrol-fundtal identisk mellem den forrige baseline
(22.608 fund, alle moduler, v4-datasættet) og denne kørsel efter
tilføjelsen (22.608 for kontrol 1-103 + 389 nye fund fra kontrol 104-108 =
22.997 i alt). Afstemningsgaten uændret **208/208** på både v4 og v5.
Kørt på v4 (uden `source_code`): kontrol 104-106 kører uændret (207 nye
fund), kontrol 107-108 'ikke målbar'-gates korrekt til 0 fund — bekræftet
via `analytics/readiness.py`s `datagrundlag`.

Kunderapport niveau 3 + Excel-arbejdsbilag regenereret på v5-datasættet til
scratchpad (ikke i repoet — indeholder kundedata): nyt tema `udlandshandel`
(207 fund, forfremmet) vises i sektion 4; `proces`-temaet (5.524 fund,
inkl. 107/108) er IKKE auto-forfremmet i denne kørsel (under
`materiality.REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD`) — rådgiveren kan
forfremme manuelt i kurationsfilen ved behov.

## Kunderapport-redesign (byggetrin ~10) — 2026-09-18 (catalog v1.3.0 uændret, data_contract v0.4.3 → v0.4.4)

Bal-godkendt opgave (2026-09-18, designoplæg med alle fire spørgsmål
godkendt). `backend/tools/generate_report.py` bygget om fra bunden til det
nye kunderapport-design — **syv sektioner** (hero "Jeres moms — set gennem
data", Momsmotoren, Afstemningen, Observationer & spørgsmål, Datagrundlag &
metode, Anbefalinger, lineage-footer), og en helt ny **kuraterings-
mekanisme** der medierer sektion 4/6 mellem motor og rådgiver.

- **`backend/tools/report_themes.py` (ny):** den faste tema<->kontrol-
  mapping (`kodeopsaetning`→19, `momsbehandling_pr_konto`→80,
  `dataanomalier`→{1,7,24,60}, `proces`→{46,14}, `timing`→{82,5}) +
  deterministiske tekst-udkast (spørgsmål/hvorfor/anbefaling) pr. tema.
  INGEN sprogmodel — ren skabelon + simple frekvensoptællinger på fundenes
  egne transaktionsreferencer/beskrivelser.
- **`backend/tools/report_curation.py` (ny):** kurationsfilens format +
  livscyklus (seed → merge). Findes filen ikke: seedes med auto-forslag
  (alle høj-fund-grupper + timen-temaet forfremmet; medium forfremmes over
  `materiality.REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD`, default 100.000
  kr., env-konfigurerbar). Findes den: rådgiverens indhold vinder for
  kendte temaer (kun den auto-genererede evidens-/nøgletalsblok
  regenereres); et tema uden fund før men med fund nu tilføjes som
  `ny_ikke_kurateret` (auto-forfremmelsesreglen anvendes stadig — en ægte
  ny høj-fund-observation må ikke forsvinde stille efter en
  motorkalibrering); et tema med fund før men ingen nu beholder
  rådgiverens tekst men nulstilles til status `ingen_fund_i_seneste_koersel`
  og vises ikke. Verificeret end-to-end på v4-datasættet (se nedenfor).
- **`backend/tools/report_workbook.py` (ny, openpyxl):** rådgiverens Excel-
  arbejdsbilag — oversigtsark (tema/kontrolnumre/antal/beløb/medtaget),
  "Alle fund" (fladt, ALLE severities, kontrol-id'er), ét ark pr. tema.
  Her (og i HTML-rapportens lineage-footer, som et kompakt tema→kontrol-
  revisionsspor) må kontrolnumre optræde — ALDRIG i kundens narrative
  sektioner 1-6.
- **`generate_report.py`s CLI** udvidet: `--curation <fil.json>`,
  `--niveau 1|2|3` (default 3; 1=Basis {1,3,5,7}, 2=Standard {+2,4, UDEN 6},
  3=Fuld rådgivning {alt} — dokumenteret kobling til produktpakkerne i
  modulets docstring), `--workbook <fil.xlsx>`, `--appendix` (det
  tidligere aggregerede fundtabel-format, nu et VALGFRIT teknisk bilag,
  default FRA).
- **Bevidste afvigelser** (dokumenteret i modulets docstring): den
  tidligere "Ledelsesresumé"-sektion (severity-kort) er UDGÅET af
  kundenarrativet — erstattet af de kuraterede tema-observationer
  (severity-fordelingen findes stadig i Excel-arbejdsbilagets
  oversigtsark); kontrolnumre fjernet fra ALLE kundevendte sektioner,
  inkl. den tidligere "Kontrol N: ..."-formulering i "ikke-målbar"-listen
  (nu kun årsagsteksten, deduplikeret).
- **Datakontrakt v0.4.4:** tre nye `tax_table[]`-felter
  (`sales_vat_account`/`purchase_vat_account`/`reverse_charge_vat_account`,
  balai_extension, kilde: `vat_setup.csv`s `ext_*_vat_account`-kolonner) til
  Momsmotoren-sektionens kode→konto-kobling; ingen aktiv kontrol
  konsumerer dem. `tools/analyze_canonical.py`s rapport bærer nu også et
  letvægts `tax_table_oversigt`-udtræk (samme filosofi som `konto_navne`).
  Ny `analytics.categories.cat10_vat_reconciliation.classify_purchase_rubric`
  — offentlig alias for `_purchase_rubric`, så rapport-laget genbruger
  kontrol 82's rubrik-logik i stedet for at gendanne en kopi af den.
- **Ny materialitetsknap:** `MATERIALITY_REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD`
  (default 100.000 kr.) — se ovenfor.
- **54 nye automatiserede tests** (`tests/test_report_themes.py`,
  `tests/test_report_curation.py`, `tests/test_report_workbook.py` +
  udvidelser af `tests/test_generate_report.py`/
  `tests/test_canonical_masterdata.py`/`tests/test_analyze_canonical_cli.py`).
  Fuld testsuite: 462 grønne, valideringssuite 99/99.
- **Verificeret end-to-end på v4-datasættet** (frisk `analyze_canonical`-
  kørsel, 50.479 transaktioner, 22.608 fund, afstemningsgate 208/208, alle
  5 analyse-moduler): kurationsfil seedet (5 tema-grupper, 4 auto-
  forfremmet), rapporter genereret i niveau 1 (25 KB, 4 sektioner), niveau 2
  (39 KB, 6 sektioner) og niveau 3 (40 KB, 7 sektioner) + Excel-arbejdsbilag
  (1,7 MB, oversigt + "Alle fund" + 5 temaark). Genkørsels-flowet testet
  ved at redigere ét felt (spørgsmål/status/rådgivernote) plus en manuel
  `medtag`-forfremmelse programmatisk i kurationsfilen og genkøre hele
  pipelinen (ny `analyze_canonical`-kørsel + `generate_report`): den
  redigerede tekst og den manuelle forfremmelse overlevede ordret, mens
  hver gruppes auto-blok (fundantal m.v.) blev opdateret — ingen
  eksisterende rådgiverindhold blev overskrevet.

## Kalibrering af kontrol 24 og 60 — 2026-09-18 (catalog v1.3.0 uændret, data_contract v0.4.2 → v0.4.3)

Bal-godkendt opgave (2026-09-18), sidste oprydning fra gap-analysen før
kunderapport-designet. Begge hypoteser blev verificeret EMPIRISK på
v4-datasættet FØR ændring, som krævet — se fordelingerne nedenfor.

**Fix D1 — kontrol 24 mod vat_setup (samme princip som kontrol 19).**
Hypotesen (525 fund domineret af delvis-fradragsret-koderne) blev bekræftet:
454 fund var `DOMESTIC|REDUCED_PRIVATE_VAT` (13,63636 %), 55 var
`DOMESTIC|REDUCED_REP_VAT` (5,26316 %) — 507 af 525 (96,6 %). Kontrollen
sammenlignede den implicitte sats (moms/grundlag) mod den hardkodede 0/25-
liste, uanset hvilken momskode linjen faktisk havde. Rettet: når kundens
vat_setup er indlæst, er den gyldige implicitte sats nu SETUP-satsen for
LINJENS EGEN kode (± `vr.RATE_TOLERANCE`) — ikke retningsbevidst som kontrol
22 (dette handler ikke om salgs-/købsside, men om at en kode kan have sin
egen gyldige sats). Ukendt/umatchet kode: falder tilbage til 0/25-listen
(kontrol 19 flager selve "ukendt kode" separat). Uden vat_setup: uændret.

**Fix D2 — kontrol 60 struktureret reversal-/allokeringsnetting.**
Hypotesen (1.303 fund domineret af allokerings-/tilbageførselsbilag) blev
bekræftet EMPIRISK, IKKE ved at hardkode bilagspræfikser: af 1.303 negative
momslinjer var 1.181 (90,6 %) beviseligt nettet af en positiv modpost på
SAMME konto+momskode — 40 i samme bilag (sum ≈ 0), 1.141 som et
"reversal-par" (modsat beløb, >90 % bogført SAMME dag som modposten, resten
inden for kalendermåneden). Verificeret manuelt: `DOC_PA001344` (Purchase
Allocation, Invoice 1005938) reverserer nøjagtigt `DOC_1005938`s
+716,31 kr. udgående moms på konto 402200/`DOMESTIC|STANDARD_VAT`, samme
dag — en ægte modpost, ikke et tilfælde. Rettet: en negativ momslinje
undertrykkes NU (ikke nedgraderet til "info" — den severity-værdi findes
ikke i dag i `models.make_finding`/scoringslaget, og at indføre den ville
kræve bredere skema-/rapportændringer uden for denne opgaves evidensgrundlag;
fuld undertrykkelse matcher desuden præcedensen fra kontrol 22's Fix A)
når den nettes — strukturelt via `_has_offsetting_vat_entry`
(`cat07_amount_threshold.py`): samme bilag ELLER et reversal-par inden for
`materiality.CONTROL_60_REVERSAL_WINDOW_DAYS` (default 31 dage — dækker den
empiriske spredning på v4). INGEN kunde-specifikke bilagspræfikser (fx "PA")
indgår i logikken — den virker på ethvert konto+kode-par uanset kilde-system.
Negative momslinjer UDEN modpost forbliver fund (ekspertens F30).

**Verifikation — v4-datasættet (worktree-diff mod commit `03d79a7`, samme fil):**

| Kontrol | Før (alle moduler) | Efter (alle moduler) | Før (default: momskerne) | Efter (default: momskerne) |
|---|---|---|---|---|
| 24 (Implicit sats ugyldig) | 525 medium | **16 medium** | 525 medium | **16 medium** |
| 60 (Negativt momsbeløb) | 1.303 medium | **98 medium** | 1.303 medium | **98 medium** |
| **Total** | **24.322** (høj 85) | **22.608** (høj 85) | **18.865** (høj 85) | **17.151** (høj 85) |

Delta −1.714 = præcis 509 (kontrol 24: 525−16) + 1.205 (kontrol 60:
1.303−98). Høj-fund uændret (85) — begge kontroller er `medium`-severity.
Afstemningsgate uændret 208/208. Alle øvrige 25 kontroller med fund
byte-for-byte uændret (verificeret pr. test_id, ikke kun i totalen).

**Residual-fund (kandidater til kunderapporten):**
- Kontrol 24: 16 tilbage. 14 er cent-niveau-afrundinger på
  `DOMESTIC|STANDARD_VAT`-linjer med et meget lille momsgrundlag (< 1 kr.,
  fx moms 0,12 af grundlag 0,47 ⇒ 25,53 %) — lav væsentlighed, men reelle
  afvigelser fra kodens egen setup-sats. 2 er en ægte datakvalitetsanomali:
  et momsgrundlag på 0,01 kr. mod et momsbeløb på hhv. 15.382,74 kr. og
  16.361,65 kr. på `EU|SERVICE_VAT_EU` — momsgrundlaget er tydeligvis ikke
  registreret korrekt for disse to linjer. Værd at fremhæve i
  kunderapporten som en konkret datakvalitetsobservation.
- Kontrol 60: 98 tilbage, alle `medium`. Ingen materialitetsfiltrering er
  tilføjet her (i modsætning til kontrol 22's Fix B) — beløbene spænder fra
  få hundrede til titusindvis af kroner og bør gennemgås som en liste, ikke
  antages ensartet lavt væsentlige.

**Discipliner:** 408 automatiserede tests (var 388; 20 nye —
`tests/test_control24_setup_rate.py` + `tests/test_control60_reversal_netting.py`),
99/99 uafhængig validering, `catalog/rules.json` uændret (v1.3.0 — ingen
literal test_name/impact_type/severity ændret), `catalog/data_contract.json`
v0.4.2 → **v0.4.3** (to nye `MATERIALITY_RUN_CONFIG`-poster:
`MATERIALITY_CONTROL_60_NET_TOLERANCE`, `MATERIALITY_CONTROL_60_REVERSAL_WINDOW_DAYS`
— ingen nye kontraktfelter). HTML-kundedialograpport regenereret (uden for
repoet, jf. datapolitikken).

## Tre motor-fixes fra gap-analysen mod ekspertleverancen — 2026-09-18 (catalog v1.3.0 uændret, data_contract v0.4.2)

Bal-godkendt opgave (2026-09-18). Tre uafhængige rettelser identificeret ved en
intern gap-analyse af motorens seneste kørsel (kundedataen selv ligger uden for
repoet — kun kontrolnumre/antal/beløbstotaler er evidensen her).

**Fix A — retningsbevidste setup-satser (kontrol 22).** BC-semantik (generel
platform-egenskab, ikke kunde-specifik, jf. `vat_rules.is_reverse_charge_sale_code`):
i Business Centrals VAT Posting Setup er satsen på en reverse charge-kode
BEREGNINGSSATSEN FOR KØBSSIDEN. På SALGSSIDEN er samme kode nulsats-eksport —
0 kr. udgående moms er korrekt. Kontrol 22 brugte tidligere den setup-arvede
sats retningsløst og flagede derfor lovligt eksportsalg som "manglende
salgsmoms". Rettelsen bruger `vat_calculation_type` (balai_extensions,
deterministisk, når vat_setup.csv er indlæst) FØR et fallback på Bus.-gruppens
første led i `vat_codes`-strengen (≠ "DOMESTIC" ⇒ udenlandsk handel). Kontrol
19/24/25 gennemgået for samme fejlkilde (dokumenteret i
`cat03_vat_rate_validation.py`'s moduldocstring) — ingen af de tre er
retningsafhængige på samme måde, ingen kodeændring der.

**Fix B — materialitets-gulv (kontrol 22).** Ny `MATERIALITY_CONTROL_22_MIN_BASE`
(default 1,00 kr., env-overstyrbar, `analytics/materiality.py`) undertrykker
rene afrundingslinjer (0,01 kr.), der aldrig var reelle "manglende
salgsmoms"-fund.

**Fix C — kontrol 9-bug (off-by-one i periodeafgrænsningen).** For en periode,
der slutter i december, blev `period_end` sat til `datetime(end_year, 12, 31)`
— en INKLUSIV øvre grænse — mens sammenligningen (`txn_date >= period_end`)
forudsætter en EKSKLUSIV grænse. Konsekvens: enhver transaktion bogført
PRÆCIS periodens sidste dag (fx 31/12) blev fejlagtigt rapporteret som
liggende uden for perioden — en selvmodsigende fundtekst ("... har dato
2025-12-31 der ligger uden for ... (1/2025 - 12/2025)"). Rettet til
`datetime(end_year + 1, 1, 1)` (samme eksklusive-øvre-grænse-mønster som den
generelle, ikke-december gren).

**Verifikation — v4-datasættet, alle moduler (worktree-diff mod forrige
commit, samme fil):**

| Kontrol | Før | Efter | Kommentar |
|---|---|---|---|
| 9 (Leveringstidspunkt) | 143 høj | 0 | Alle 143 var dateret periodens sidste dag (bugsymptomet) — ingen genuine periode-overskridelser tilbage i datasættet. |
| 19 (Ugyldig momssats) | 21 høj | 21 høj | Uændret, som krævet (matcher ekspertens "ukendt kode"-fund). |
| 22 (Manglende salgsmoms) | 147 høj | 0 | Fordelt før: 113 EU-eksportkode + 32 OUTSIDE DK/EU-eksportkode (Fix A) + 2 afrundingslinjer à 0,01 kr. (Fix B). |
| 24 (Implicit sats ugyldig) | 525 medium | 525 medium | Gennemgået, ingen retningsafhængig fejlkilde — uændret. |
| 25 (Nulsats indenlandsk) | 0 | 0 | Gennemgået, ingen retningsafhængig fejlkilde — uændret. |
| **Total (alle moduler)** | **24.612** (høj 375) | **24.322** (høj 85) | Delta −290 = præcis 143 (kontrol 9) + 147 (kontrol 22). Afstemningsgate uændret 208/208. |
| **Total (default: kun momskerne)** | 19.155 (høj 375) | 18.865 (høj 85) | Samme delta, pakke-billedet for produktniveau 1-2. |

**Discipliner:** 388 automatiserede tests (was 372), 99/99 uafhængig
validering, `catalog/rules.json` uændret (v1.3.0 — ingen literal
test_name/impact_type/severity ændret), `catalog/data_contract.json` v0.4.1 →
**v0.4.2** (ny `MATERIALITY_CONTROL_22_MIN_BASE`-metadata i
`MATERIALITY_RUN_CONFIG`, ingen nye kontraktfelter). Ny HTML-kundedialograpport
regenereret fra den nye kørsel (uden for repoet, jf. datapolitikken).

## Kontrol 82 hærdet med vat_calculation_type + alias-bugfix (description/navn) + kontrol 77 vat-match — 2026-09-17 (catalog v1.3.0 uændret, data_contract v0.4.1)

Bal-godkendt opgave, opfølgning på forrige punkt (`non_deductible_vat_pct`/
`allow_non_deductible_vat`/`vat_calculation_type` optaget i kontrakten, men
"kun kontrakt+parser — ingen kontrol konsumerer felterne endnu"). Denne runde
aktiverer `vat_calculation_type` i kontrol 82 og retter et alias-bug, der
gjorde konto-/momskodebeskrivelser strukturelt tomme på den kanoniske vej.

- **Kontrol 82 (`cat10._purchase_rubric`) bruger nu `vat_calculation_type`
  FØR DKRC/SERVICE_VAT-navnemønstrene, når feltet er til stede** (kun på den
  kanoniske vej, når `vat_setup.csv` er indlæst — joinet BÅDE på `tax_table[]`
  og `transactions[].lines[]`, da kontrol 82 klassificerer pr. LINJE):
  siger feltet ikke "reverse charge" (Normal/Full VAT) → `input`,
  deterministisk. Siger det "reverse charge", kan feltet ALENE ikke skelne
  indenlandsk RC fra RC fra udlandet (begge bruger samme beregningstype i
  BC/NAV) — Bus.-gruppen (`vat_codes`-strengens FØRSTE led, fx
  `DOMESTIC`/`EU`/`OUTSIDE DK/EU`) løser den skelnen: `DOMESTIC` → `dkrc`
  (udgående rubrik), UDEN krav om "dkrc" i selve kodenavnet (hærdning).
  Ikke-domestic RC kan hverken beregningstype eller Bus.-gruppe skelne
  ydelse fra vare (fx `EU|SERVICE_VAT_EU` vs. `EU|GOODS_VAT_EU` deler begge
  værdier) — den sidste skelnen falder fortsat tilbage til
  SERVICE_VAT-navnemønstret (bevidst, dokumenteret, ikke et overset hul).
  Mangler feltet helt (Excel-/SAF-T-oprindelse, eller kanonisk uden
  `vat_setup.csv`): uændret ren navnemønster-klassifikation.
  **Regressionskriterium bekræftet empirisk** (worktree-sammenligning af
  commit `013ca1f` mod denne ændring, samme v4-datasæt, samme
  reconciliation/declarations-input): fund-antal PR. TEST-ID er
  byte-for-byte identisk før/efter (24.612 fund i alt, 0 kritisk/375 høj/
  15.830 medium/8.407 lav; kontrol 82's 12 fund — alle LAV, alle
  "Indgående moms"/timing, udgående moms + RC-ydelser fortsat <1 kr. for
  alle 12 måneder 2025 — er felt-for-felt identiske JSON-objekter før/efter).
- **Alias-bugfix (`parsers/canonical_masterdata.py`):** loaderen læste
  `description` (vat_setup.csv) og `description` (chart_of_accounts.csv),
  men vat-extracts REELLE transform-output navngiver kolonnerne
  `ext_description` hhv. `ext_name` — kodebeskrivelser og kontonavne var
  derfor strukturelt ALTID tomme på den kanoniske vej, uafhængigt af om
  sidecar-filerne var leveret. Rettet til at acceptere `ext_description`/
  `ext_name` FØRST, med de upræfiksede navne som fallback (ingen
  adfærdsændring for en fil der allerede brugte det upræfiksede navn).
  Bekræftet empirisk på v4: kontrol 80's finding-tekster viser nu reelle
  engelske BC/NAV-kontonavne (fx "Konto 711100 (Income tax)" i stedet for
  blot "Konto 711100") — finding-ANTALLET er uændret (kun teksten beriget).
  **OBS, rapportérbart:** den mailbare HTML-rapport
  (`tools/generate_report.py`) viser i dag KUN kontonummeret i sin
  aggregerede pr.-konto-tabel (`_finding_account()` læser udelukkende
  `account_id` fra finding-referencerne, aldrig kontoens `description`) —
  alias-fixet forbedrer altså finding-teksten i rå rapport-JSON, men slår
  IKKE automatisk igennem i HTML-rapportens kontotabel. At vise kontonavne
  dér er en selvstændig, efterfølgende ændring i `generate_report.py`
  (uden for denne opgaves scope).
- **Kontrol 77 (`test_77_vat_account_reconciliation`) matcher nu ALSO 'vat'**
  (case-insensitivt), ikke kun det danske 'moms' — kontrollen kunne
  tidligere ALDRIG ramme en engelsksproget kontoplan (BC/NAV m.fl.). Danske
  kontoplaner matcher fortsat uændret. **Vågnede IKKE på v4-datasættet**:
  chart_of_accounts.csv har ingen `opening_balance`/`closing_balance`-
  kolonner (Trial Balance-saldi er endnu ikke i pipelinen, jf.
  `BALAI-dataflow-arkitektur.md`), så `account_balance` summer til 0 og
  kontrollen springer over PRÆCIS som før udvidelsen — bekræftet 0 fund
  før/efter i worktree-sammenligningen. 14 konti i v4's kontoplan
  indeholder faktisk 'vat'/'moms' i navnet (fx "VAT Related to Sales
  (Salgsmoms)"), så kontrollen VIL vågne, når Trial Balance-saldi
  operationaliseres.
- **`tools/data_contract_data.py`:** `vat_calculation_type`s `kraeves_af`/
  `noter` opdateret fra "ingen kontrol konsumerer feltet endnu" til AKTIV
  (kontrol 82) + hele beslutningstræet inkl. den dokumenterede Bus.-gruppe-
  begrænsning. `description`-felterne på `accounts[]` og `tax_table[]`
  opdateret `kilder.canonical` fra `false` til `"partial"` (alias-fixet gør
  dem reelt udfyldte, når sidecar-filen er leveret) + `kraeves_af` for
  `accounts[].description` udvidet til at nævne kontrol 77/80.
  `data_contract.json` **v0.4.1** (fra v0.4.0, felt- og ekstensionsantal
  uændret: 74 felter, 12 ekstensioner — kun beskrivelser/kilder-flag
  opdateret, ingen nye felter).
- Testsuite **353 → 372** (19 nye tests: alias-fallback for begge loaders,
  linje-niveau `vat_calculation_type`-berigelse, `_purchase_rubric`s fulde
  beslutningstræ inkl. Bus.-gruppe-hærdningen, kontrol 77's nye
  'vat'-matching). Valideringssuite uændret **99/99**. `catalog/rules.json`
  uændret **v1.3.0** (ingen ny/ændret kontrol-signatur, kun intern
  klassifikationslogik i en eksisterende, allerede aktiv kontrol).

## To nye balai_extensions fra vat_setup: non_deductible_vat_pct (+flag) og vat_calculation_type — 2026-09-17 (catalog v1.3.0 uændret, data_contract v0.4.0)

Bal-godkendt tværgående beslutning (§2a-disciplin, jf. analysen af vat-extracts
tre seedede stamdata-mappings samme dag): to af de dokumenterede
ekstensionskolonner i `vat-extract/tools/seed_master_data_mappings.py`
optages i `balai_extensions`, fordi en eksisterende/planlagt kontrol reelt
kræver dem — resten forbliver bevidst udenfor (kandidater: pr.-kode
momskonti + `ext_bc_account_type`, først relevante når Trial Balance-saldi
operationaliseres).

- **`tax_table[].non_deductible_vat_pct`** (+ ledsage-flaget
  **`allow_non_deductible_vat`**): pr.-kode ikke-fradragsprocent fra kundens
  egen VAT Posting Setup (`ext_non_deductible_vat_pct`/
  `ext_allow_non_deductible_vat` i `vat_setup.csv`). Kræves af feature 83
  (delvis fradragsret) + BAL-055-familien (§42-fradragsbegrænsning, G1) —
  den kanoniske CSV-vej havde ellers INTET fradragsbegrænsnings-signal
  (linje-niveau `non_deductible_amount` findes kun på SAF-T-vejen). Bevidst
  IKKE det kanoniske `pro_rata` (virksomhedsbred brøk) — tre granulariteter
  af samme feature er nu alle i §2a-blokken.
- **`tax_table[].vat_calculation_type`**: deterministisk mekanisme-flag pr.
  kode (Normal/Reverse Charge/Full VAT, `ext_vat_calculation_type`). Kræves
  af kontrol 82 som hærdning af DKRC/SERVICE_VAT-mønstergenkendelsen i
  `cat10._purchase_rubric` (i dag kalibreret til én kundes kodenavngivning).
  Dokumenteret forbehold: skelner RC fra normal, ikke alene indenlandsk RC
  fra RC-ydelser fra udlandet. SAF-T-nativt alternativ på sigt: eksplicit
  besluttet StandardTaxCode-mapping — migreringsomfanget dermed dokumenteret
  på forhånd, som §2a kræver.

Implementeret: `parsers/canonical_parser.py` (nøglesæt-symmetri: de tre nye
nøgler ALTID til stede på den kanoniske vej, defaults None/""/""),
`parsers/canonical_masterdata.load_vat_setup`/`enrich_canonical` (læser
`ext_`-præfikserede kolonner med upræfikset fallback, bærer værdierne RÅT
ind på matchede tax_table-poster), `tools/data_contract_data.py` (tre
tax_table-felter med `ekstension=True` + tre §2a-entries, kontrakt v0.3.1 →
**v0.4.0**, 71 → 74 felter, 9 → 12 ekstensioner). INGEN kontrol konsumerer
felterne endnu (kun kontrakt+parser) — regelkataloget er derfor uændret
v1.3.0. Testsuite 349 → **353** (nye loader-/berigelsestests), valideringssuite
uændret **99/99**.

## Byggetrin 9, Del A–D: kontrol 19 mod vat_setup, kontrol 80 pr. konto, HTML-kundedialograpport — 2026-09-17 (catalog v1.3.0, data_contract v0.3.1)

Bal-godkendt opgave (kontekst: `BALAI-dataflow-arkitektur.md` §2a+§7 punkt
8a/8b), tre dele i rækkefølge, verificeret på den rigtige BC/NAV-fil
(v4-datasættet, 50.479 transaktioner/125.986 linjer, 208/208 afstemt).

**Del A — Kontrol 19 valideres mod vat_setup-satser
(`analytics/categories/cat03_vat_rate_validation.py`):** diagnose
(verificeret manuelt): kunden bruger BEVIDSTE delvis-fradragsret-
konstruktioner i sin VAT Posting Setup — `DOMESTIC|REDUCED_PRIVATE_VAT` =
13,63636 % og `DOMESTIC|REDUCED_REP_VAT` = 5,26316 % (en BC-teknik: reduceret
effektiv sats i stedet for 25 % + separat fradragsbegrænsning). Kontrol 19
validerede hidtil ubetinget mod den hardkodede 0/25-liste, så disse to koder
alene stod for 642 af 957 HØJ-fund. Rettelse: når kanonisk `vat_setup.csv` er
indlæst (`header.vat_setup_loaded`, ny nøgle — nøglesæt-symmetri, altid til
stede på den kanoniske vej, default False), validerer kontrol 19 i stedet en
linjes sats mod OPSÆTNINGENS sats for linjens EGEN momskode
(`tax_table[].setup_matched`, ny nøgle, samme symmetri-princip): match = ok
(også 13,64 %), afvigelse fra kodens setup-sats = fund ("Sats afviger fra
vat_setup"), kode helt ukendt i opsætningen = fund ("Ukendt momskode i
opsætning"). UDEN vat_setup (Excel/SAF-T/ældre kanoniske filer uden
sidecar): fuldstændig uændret adfærd (kun 0/25-validering) — ingen
regression. `parsers/canonical_masterdata.enrich_canonical` sætter de to nye
felter; `parsers/canonical_parser.py` initialiserer dem til False for
nøglesæt-symmetri, samme mønster som resten af filen.

**Del B — Kontrol 80 aggregeres pr. konto
(`analytics/categories/cat10_vat_reconciliation.py`):** diagnose: 24.152
per-posterings-fund fordelt på kun 77 konti; top-10 konti (typisk interne
allokeringskonti som 319160/381360) udgjorde 97 %. Den faglige beslutning
("er kontoen håndteret korrekt momsmæssigt?") træffes pr. KONTO, ikke pr.
postering. Kontrollen udsteder nu ÉT fund pr. konto: kontonummer(+navn hvis
`chart_of_accounts.csv` er leveret), antal kvalificerende posteringer, sum
af grundlag, andel af KONTOENS posteringer (alle, ikke kun de
kvalificerende) uden momskode, og op til `materiality.CONTROL_80_MAX_REFS`
(default 10) transaktionsreferencer til drill-down. Severity gradueres efter
kontoens samlede grundlag (`materiality.CONTROL_80_HIGH_THRESHOLD`/
`CONTROL_80_MEDIUM_THRESHOLD`, defaults 500.000/100.000 DKK) i stedet for en
fast "medium" — en bevidst granularitets- OG severity-ændring på tværs af
ALLE input-veje (Bal-godkendt), ikke en ny betingelse for hvornår
kontrollen fyrer. Eksisterende tests i `tests/test_vat_scope.py` er
uændrede i deres forventninger (én kvalificerende linje giver stadig ét
fund); ny dedikeret dækning i `tests/test_control80_aggregation.py`.

**Del C — HTML-rapportgenerator (`backend/tools/generate_report.py`, ny
fil):** CLI (`python tools/generate_report.py <rapport.json> --out
<fil.html>`) der bygger ÉN selvbærende HTML-fil (inline CSS, ingen
CDN/eksterne afhængigheder — kan mailes), dansk UI, BALAI-designsprog
genbrugt fra `static/style.css` (navy #1B365D, sev-farver). Fem sektioner:
(1) **tillidsanker** — kontrol 82 som 12-måneders tabel pr. rubrik
(beregnet/angivet/difference/status, nyt `analytics.build_declaration_
reconciliation_table` i `cat10_vat_reconciliation.py`, wiret ind i
`engine.run_all_tests` som `report["declaration_reconciliation"]`),
afstemningsgatens resultat, og debet==kredit-totalkontrollen; (2)
**ledelsesresumé** — fund pr. severity, transaktioner/bilag, analyseperiode;
(3) **aggregerede fundtabeller** — pr. kontrol → pr. konto (antal, beløb),
sorteret efter væsentlighed, ALDRIG rå fund-dumps (cap ved 25 kontorækker/
60 kontrol-sektioner med "…og N flere"); (4) **datagrundlag & metode** —
ikke-målbare kontroller, relevante `known_gaps` (læst best-effort fra
`catalog/data_contract.json`), lineage-footer (katalog-/kontrakt-/mapping-
version, schema-fingerprint, kørselstidspunkt — ny `generated_at`-nøgle i
`tools/analyze_canonical.py`s lineage) + AI-provenance-note (§2a: analysen
er 100 % deterministisk, AI bruges kun til menneske-godkendte, frosne
mapping-forslag); (5) print-CSS. Al kundedata HTML-escapes. Dækket af
`tests/test_generate_report.py` + `tests/test_declaration_reconciliation_
table.py`.

**Del D — Verifikation (v4-datasættet, samme fil som Del A-diagnosen):**

| Nøgletal | FØR | EFTER |
|---|---|---|
| Fund i alt | 49.308 | 24.612 |
| HØJ | 957 | 375 |
| MEDIUM | 39.957 | 15.830 |
| LAV | 8.394 | 8.407 |
| Kontrol 19-fund | 642 (alle falske — partial-deduction-koder) | 21 (alle ægte "ukendt kode") |
| Kontrol 80-fund | 24.152 (pr. postering) | 77 (pr. konto: 39 høj/25 medium/13 lav) |
| Afstemningsgate | 208/208 | 208/208 (uændret) |

HØJ-faldet (957→375) er STØRRE end det oprindeligt anslåede "~315" i
opgavebeskrivelsen: 642→21 for kontrol 19 (som ventet), kontrol 9/22/7/1
uændrede (143/147/23/2, uden for denne opgaves scope), MEN kontrol 80
bidrager nu 39 nye HØJ-fund (0 før — kontrollen havde tidligere en FAST
"medium"-severity, uanset beløb). Dette er en tilsigtet konsekvens af Del
B's beløbsgraduering (39 konti har hver et akkumuleret uden-moms-grundlag
over 500.000 DKK) — ikke en fejl, men en afvigelse fra den oprindelige
skønnede totalsum, som bør bekræftes eksplicit (se sporbarhedsnotat/
hand-off). HTML-rapport genereret til
`kunde_rapport_2025.html` (53,6 KB) fra den nye rapport-JSON — 5 sektioner,
12-måneders kontrol 82-tabel (output/RC grøn alle 12 måneder, input_vat
"timing" alle 12 måneder, årsresidual -326.212 DKK ≈ 0,56 % — matcher
dokumentationen i kontrol 82-modulet), 29 kontrol-blokke med fund.

Katalog **v1.3.0** (kontrol 19 "navn" ændret til "Sats afviger fra vat_setup"
af den AST-baserede generator — nyt primært make_finding-kald opdaget
først; kontrol 80 "severity" ændret til `["dynamisk"]`, jf. den nye
beløbsgraduering). Datakontrakt **v0.3.1** (to nye felter:
`header.vat_setup_loaded`, `tax_table[].setup_matched`; GAP-10 opdateret).
349 automatiserede tests (op fra 306), valideringssuite fortsat 99/99.

## Byggetrin 8, Del A–D: kontrol 82 aktiveret + tre kanoniske stamdata-filer — 2026-09-17 (catalog v1.2.0, data_contract v0.3.0)

Bal-godkendt opgave: operationalisér kontrol 82 (afstemning mod den
indberettede momsangivelse) og modtagelsen af tre nye valgfrie kanoniske
stamdata-filer. 99/99 aktive kontroller (op fra 98/98) i valideringssuiten;
306 automatiserede tests (op fra 300); afstemningsgaten forbliver 208/208 på
den rigtige BC/NAV-fil.

**Del A — Angivelses-input (`analytics/vat_declarations.py`, ny fil):**
`load_declarations()` læser/validerer `vat_declarations.json`
(`declarations_version: "1.0.0"`, `periods: [{period, output_vat, input_vat,
rc_services, rc_goods, energy_taxes, total}]`) — snitfladen aftalt med
vat-extract. Fejler ALDRIG med exception (samme filosofi som
`reconciliation_gate.py`): en teknisk fejl må aldrig fremstå som et fagligt
afslag.

**Del B — Kontrol 82 aktiveret (`analytics/categories/cat10_vat_reconciliation.py`):**
`test_82_period_declaration` beregner tre rubrikker pr. periode
(`transactions[].period_year`/`period`) og afstemmer dem mod angivelsen:
- **udgående-rubrik** = -sum(vat, sale) + \|sum(vat, DKRC-købslinjer)\|
- **rc_services-rubrik** = \|sum(vat, SERVICE_VAT-købslinjer)\|
- **input-rubrik** = sum(vat, øvrige købslinjer)

Rettelser undervejs, EMPIRISK fanget på den rigtige BC/NAV-fil (Del D):
(1) sale/køb afgøres af den kanoniske CSV's eget `supply_direction`-felt
("sale"/"purchase"), IKKE debet-/kredit-siden — debet/kredit gav en kraftigt
oppustet udgående-rubrik, fordi kilden kopierer invoice-niveau momsmetadata
ud på modpost-/betalingslinjer af samme bilag. Debet/kredit er bevaret som
fallback (`_line_direction`), for Excel-/SAF-T-oprindelse, der ikke bærer
`supply_direction`. (2) hver rubrik summeres MED FORTEGN over linjerne, og
abs()/negering anvendes ÉN GANG på summen (ikke pr. linje), så en
kreditnota/reversering netter korrekt i stedet for at blive lagt oveni.
DKRC-/SERVICE_VAT-kodegenkendelse er konfigurerbar
(`materiality.VAT_DECLARATION_DKRC_PATTERNS`/`..._SERVICE_VAT_PATTERNS`,
defaults `["DKRC"]`/`["SERVICE_VAT"]`) — IKKE hårdkodet til én kundes
kode-taksonomi. V1-håndtering af timing: sammenlignes pr. periode OG
årstotal — resolves differencen over året, er den timing (severity low),
ellers et reelt fund (severity high). Uden en angivelsesfil springer
kontrollen fortsat over, UÆNDRET adfærd (`readiness.EXTERNAL_DATA[82]`,
skærpet med et nyt `external_data_provided`-flag der kun løftes, når en
angivelse faktisk er leveret).

**Del C — Tre valgfrie kanoniske stamdata-filer (`parsers/canonical_masterdata.py`, ny fil):**
Auto-opdaget ved siden af `gl_entries.csv` (samme mønster som
`transform_summary.json`), berigelse EFTER `parse_canonical()`:
- `vat_setup.csv` (join: `vat_codes`-strengen) → reel `tax_percentage`/`rate`
  i `tax_table[]` OG `transactions[].lines[]` — kategori 3 (kontrol 19-26)
  får et reelt grundlag (GAP-10 delvist lukket).
- `chart_of_accounts.csv` (join: kontonummeret/`gl_accounts`) → reel
  `account_type`/`standard_account_id`/saldi i `accounts[]` OG linjerne —
  `vat_rules.is_non_vat_account` (kontrol 80's momsrelevans-scope) kan
  aktiveres (GAP-11 delvist lukket). `vat_rules._NON_VAT_ACCOUNT_TYPES`
  udvidet med BC/NAV's PLURALE konvention (`"assets"`/`"liabilities"`,
  observeret på den rigtige fil) ved siden af SAF-T's ental
  (`"asset"`/`"liability"`/`"equity"`).
- `customers.csv` (fleksible kolonnenavne — bekræftet mod vat-extracts
  reelle transform-output: `ext_customer_id`/`ext_customer_name`/
  `counterparty_country`, samt en simplere `customer_id`/`name`/`country`
  som fallback) → fylder den selvstændige `customers[]`-liste STRUKTURELT.
  **Kendt, fortsat åben begrænsning (ikke skjult):** `gl_entries` bærer
  ingen `customer_id`-kolonne på selve linjerne, så `cat12_ecommerce_special.
  _cust_country()`'s join er altid tomt på den kanoniske vej — `customers.csv`
  aktiverer IKKE kontrol 94-97 alene. `ext_vat_bus_posting_group` (en
  BC-postgruppekode, fx "EU"/"DOMESTIC") mappes bevidst IKKE til
  `vat_number` (ville være misvisende).

`catalog/data_contract.json` **v0.3.0** (fra v0.2.1): `kilder.canonical` for
de berørte felter ændret til `"partial"` (afhænger af om sidecar-filen
leveres); GAP-10/GAP-11 status `delvist_lukket`; tre nye
`MATERIALITY_RUN_CONFIG`-knapper. 69 felter (uændret antal).

**Del D — E2E-verifikation** (den rigtige BC/NAV-fil, 125.986 rækker, 50.479
bilag efter gruppering), før/efter Del A-C:

| Kontrol | Før | Efter | Kommentar |
|---|---|---|---|
| 19 (ugyldig sats) | 0 | 642 | reelt grundlag fra vat_setup.csv |
| 22 (manglende udgående moms) | 0 | 147 | reelt grundlag fra vat_setup.csv |
| 80 (indtægt uden momsbehandling) | 30.863 | 24.152 | chart_of_accounts.csv fjerner falske positive på balancekonti |
| 82 (rubrik-afstemning) | sprunget over | 12 fund | output_vat + rc_services: 0 fund (afstemmer til < 1 kr. alle 12 mdr. 2025); input_vat: 12 fund, ALLE severity high (reel, IKKE timing — se Del B) |
| 94-97 (e-handel/OSS) | 0 | 0 | uændret — kendt gab (customers.csv joiner ikke til linjer) |

Total: 55.216 → 49.308 fund (kritisk 0→0, høj 166→969, medium 46.668→39.957,
lav uændret 8.382). Afstemningsgate uændret **208/208** konti afstemt.
**Input_vat-observationen (12 fund, severity high) er en ægte
reconciliation-observation at forelægge Bal** — ikke en kodefejl: GL-baseret
input-moms og den indberettede input_vat afviger systematisk (~4 mio. DKK
over året), hvilket ligger uden for denne kontrols datagrundlag at forklare
(mulige årsager: delvis fradragsret/§42, manuelle korrektioner i angivelsen,
poster uden for GL-udtrækkets vindue).

## Medium-fund-analysen, punkt 1+2: description-modtagelse + "ikke målbar"-gating — 2026-09-17 (ikke-katalog)
Bal-godkendt opfølgning på GAP-12-mitigeringen (samme dag). Den fulde E2E på
den rigtige BC/NAV-fil (v2, uden description) gav 128.978 fund, heraf 114.575
medium — 80% deraf var datagrundlags-støj: kontrol 4 flagede ALLE 50.479
bilag for manglende Description (feltet blev aldrig læst af
`canonical_parser`, uafhængigt af om kilden havde en beskrivelse), og
kontrol 25 gav 10.671 "ingen udenlandsk modpart"-fund, fordi `country` er
strukturelt fraværende i et GL-udtræk. To ændringer, motorsiden:

**Punkt 1 — Description-modtagelse (GAP-13, lukket):**
- `canonical_parser.py` læser nu en valgfri `description`-kolonne fra den
  kanoniske CSV og fører den ind på BÅDE linje- og transaktionsniveau,
  præcis som Excel-/SAF-T-vejen (`data_adapter.py`/`saft_parser.py`). En
  grupperet flerlinje-transaktion (bilagsgruppering, GAP-12) bruger den
  FØRSTE ikke-tomme linje-description i bilaget. Kolonnen kan mangle på
  ældre kanoniske filer (v2) — da er feltet fortsat `""` (uændret adfærd,
  ingen crash).
- `tools/data_contract_data.py`: `kilder.canonical` for `description`
  (transaction- og line-niveau) rettet til `True`; `kraeves_af` på
  transaktionsniveau rettet (kontrol 4 læser faktisk feltet — tidligere
  fejlagtigt dokumenteret som "ingen kontrol direkte"). Ny known_gaps-post
  **GAP-13** (`lukket`). `catalog/data_contract.json` **v0.2.1** (fra
  v0.2.0) — 69 felter (uændret antal; kun `kilder`-flag + known_gaps
  opdateret, ingen nye felter).

**Punkt 2 — "Ikke målbar"-gating (kernen):**
Bals "ingen falske alarmer"-filosofi udvidet til medium-laget: når et felt,
en kontrol (eller én delcheck i en multi-felt-kontrol) hårdt afhænger af, er
**0% udfyldt i HELE datasættet** — og populationen er stor nok til at
udelukke en enkeltstående/tilfældig tomhed (`MIN_TX_FOR_GATING = 30`, samme
begrundelse som den eksisterende `_MIN_TX_FOR_STATISTIK`-guard) — rapporterer
kontrollen "ikke målbar" ÉN gang i stedet for at generere fund pr.
transaktion.
- `analytics/readiness.py`: ny status `STATUS_IKKE_MAALBART` ("ikke_maalbar")
  — en skærpet variant af den eksisterende `STATUS_SPRUNGET_DATA` (samme
  0%-betingelse, men KUN håndhævet når populationen er stor nok).
  `STATUS_SPRUNGET_DATA` er UÆNDRET (fortsat rent informativ, ingen
  størrelses-guard) — det er derfor valideringssuitens og testsuitens mange
  et-transaktions-scenarier (der bevidst tømmer ét felt for at plante en ægte
  defekt) forbliver upåvirkede. Ny genbrugelig primitiv `field_is_gated()`.
  `CONTROL_REQUIREMENTS[25] = ["country"]` tilføjet (kategori 3's default
  kræver kun tax_code — kontrol 25 tjekker `line["country"]` direkte).
  Ny `SUBCHECK_FIELDS`/`subcheck_gates()` for multi-felt-kontroller, der IKKE
  kan gates som helhed (kontrol 4: TransactionID/TransactionDate/AccountID-
  delcheckene skal blive ved med at køre, selvom Description mangler) —
  eksponeret som `datagrundlag["delkontrol_gates"]`.
- `analytics/categories/cat01_transaction_integrity.py` (`test_04`): kalder
  `readiness.field_is_gated(data, "description", level="transaction")` og
  springer KUN Description-delchecket over, når det er sandt. De øvrige tre
  delcheck er uændrede.
- `analytics/engine.py` (`run_all_tests`): beregner `readiness.assess()` FØR
  rapporten bygges og fjerner fund fra kontroller markeret
  `STATUS_IKKE_MAALBART` (generisk, gælder alle `CATEGORY_REQUIREMENTS`/
  `CONTROL_REQUIREMENTS`-styrede kontroller, ikke kun 25) — erstattet af ÉN
  note i `datagrundlag`. Ny rapport-nøgle `ikke_maalbare_fund_fjernet`
  (transparens, additiv — ændrer ikke `filtrerede_fund`, som fortsat kun
  tæller modul-filtrering).
- **v1-tærskel (bevidst, dokumenteret):** kun 0%-fravær gates — INGEN fuzzy
  mellemtærskler. Et felt der er delvist udfyldt (fx land kun på nogle
  linjer) gates ALDRIG, uanset hvor lav dækningen er.

**Empirisk verifikation på den rigtige BC/NAV-fil (v2, uden description,
125.986 rækker, 50.479 bilag efter GAP-12-gruppering — samme kørsel som
GAP-12-tabellen ovenfor), alle analyse-moduler tændt, før/efter denne opgave:**

| Nøgletal | Før (denne opgave) | Efter |
|---|---|---|
| Kontrol 4 (Faktura-feltfuldstændighed) | 50.479 medium | **0** — status `ikke_maalbar`, delcheck-note |
| Kontrol 25 (Nulsats på indenlandsk handel) | 10.671 medium | **0** — status `ikke_maalbar` |
| Øvrige system-bredt gatede kontroller (32, 71, 73, 86, 92 — kategori 4/9/11, kræver `country`) | 12.613 fund (blandet severity) | **0** |
| Medium i alt | 114.575 | **46.667** |
| Høj / lav i alt | 2.366 / 12.037 | 166 / 8.382 |
| Fund i alt | 128.978 | 55.215 |
| `ikke_maalbare_fund_fjernet` | — | 23.284 |
| Kontroller med status `ikke_maalbar` (`datagrundlag.opsummering`) | 0 (fandtes ikke) | 44 af 103 |
| Afstemningsgate | afstemt, 208/208 | afstemt, 208/208 (uændret) |
| Transaktioner analyseret | 50.479 | 50.479 (uændret) |

De øvrige ~46.667 medium-fund er IKKE rørt af denne opgave — de er reelle
kandidater på de felter, der rent faktisk ER til stede i udtrækket (beløb,
momskoder, datoer m.v.), og skal fortsat vurderes fagligt en for en.

**Opfølgende kørsel på v3 (samme fil, MED description-kolonne tilføjet af
vat-extract parallelt, mapping v1.2.0, 126.035 rækker):** kontrol 4 går fra
"ikke målbar" til **`koert`** med **2** reelle medium-fund (description
faktisk tom på 2 ud af 125.963 udfyldte linjer, dækning 100,0%) — bekræfter
at gatingen kun undertrykker støj, ALDRIG en reel defekt, når feltet rent
faktisk er til stede. Kontrol 25 forbliver `ikke_maalbar` (v3 tilføjede kun
description, ikke `country`). Afstemningsgate fortsat 208/208.

Testdisciplin: 256/256 automatiserede tests + 98/98 uafhængig
valideringssuite grønne uændret (ingen eksisterende scenarie ændrede
resultat — `MIN_TX_FOR_GATING` holder alle valideringssuitens
et-transaktions-scenarier uden for gatingen, jf. `readiness.py`).

## GAP-12 mitigeret: bilagsgruppering i den kanoniske parser — 2026-09-17 (ikke-katalog)
Bal-godkendt opfølgning på byggetrin 8 (se afsnittet nedenfor). Kontrol 10
(transaktionsbalance, kategori 1) gav 125.885 strukturelle falsk-positive fund
af 125.986 kanoniske rækker på den rigtige BC/NAV-fil, fordi hver GL-linje blev
sin egen 1-linjes transaktion — BC/NAV-poster balancerer PR. BILAG, ikke pr.
linje, og bilagsnøglen (Entry No./Transaction No.) er ikke med i den seedede
mapping. Kun `backend/parsers/canonical_parser.py` + rapportmetadata ændret —
ingen ændring i `analytics/categories/*.py`-kontrollogikken.

- **`canonical_parser.py`:** rækker med samme, IKKE-TOMME
  `(invoice_numbers, posting_dates)` samles nu til ÉN transaktion med flere
  `lines[]` (ny `_group_key`/`_build_transaction`). Rækker med tomt/manglende
  `invoice_numbers` grupperes ALDRIG på tværs af rækker (heller ikke ved samme
  dato) — ingen gættet sammenhæng uden evidens; de forbliver hver sin
  1-linjes transaktion med det gamle `ROW-<rækkenr>`-id. En grupperet
  transaktions id er deterministisk (`DOC_<invoice>_<dato>`).
  `total_debit`/`total_credit` summeres over gruppens linjer — samme
  aggregeringskonvention som `data_adapter.adapt_excel_to_saft` og
  `saft_parser.parse_saft`. Hver linje bærer nu et canonical-only
  `source_row`-felt (oprindeligt CSV-rækkenummer) for lineage, uafhængigt af
  gruppering.
- **Empirisk verifikation på den rigtige BC/NAV-fil (125.986 rækker),
  før/efter:**

  | Nøgletal | Før gruppering | Efter gruppering |
  |---|---|---|
  | Transaktioner | 125.986 | 50.479 (Ø 2,5 linjer/bilag) |
  | Kontrol 10 (kritisk) | 125.885 | **0** |
  | Høj/medium/lav (øvrige) | 3.071 / 198.505 / 18.622 | 2.366 / 114.575 / 12.037 |
  | Fund i alt | 346.083 | 128.978 |
  | Afstemningsgate | afstemt, 208/208 | afstemt, 208/208 (uændret) |
  | total_debit/total_credit/total_vat | uændret | uændret (identisk til øre) |
  | Køretid (parsing+analyse) | 6,1 s | 3,7 s |

  Afstemningsgaten er upåvirket, fordi den summerer netto debit−credit PR.
  KONTO over alle linjer uafhængigt af transaktionsgruppering — grupperingen
  kan pr. konstruktion ikke ændre kontosummerne. Residual-analyse: på denne
  fil havde ALLE 125.986 rækker et udfyldt bilagsnummer (0 ungrupperede
  `ROW-`-transaktioner), og samtlige 50.479 grupperede bilag balancerer —
  kontrol 10-residualet er 0 fund, ikke bare reduceret. Den kendte
  begrænsning (tomt bilagsnummer grupperes aldrig) er derfor uprøvet på
  denne konkrete fil, men forbliver dokumenteret som åben i GAP-12.
- **`tools/data_contract_data.py`:** GAP-12 opdateret fra `aaben` til
  `delvist_lukket` — mitigeringen, nøglen og den kendte begrænsning
  (tomme bilagsnumre) dokumenteret. `catalog/data_contract.json`
  regenereret (uændret v0.2.0/69 felter — dette er parser-adfærd, ikke
  kontraktform, så ingen version-bump).
- **Tests:** 5 nye scenarier i `tests/test_canonical_parser.py` (flere linjer
  pr. bilag inkl. korrekt document_date/aggregater, tomme bilagsnumre
  grupperes aldrig, samme bilagsnr. på to datoer = to transaktioner, samt at
  kontrol 10's tidligere falsk-positiv forsvinder efter gruppering). 256/256
  tests grønne (op fra 251), 98/98 uafhængig validering grøn, katalog-/
  kontrakt-drift-gates grønne.

## Kanonisk ingestion-vej + afstemningsgate — 2026-09-17 (ikke-katalog)
Byggetrin 8 i den aftalte rækkefølge (`balai-platform/BALAI-dataflow-arkitektur.md`
§2a/§7, Bal-godkendt 2026-09-17). Tredje input-vej ved siden af Excel/CSV og
SAF-T XML — output fra vat-extracts deterministiske `dataextract.transform`
(mapping-lager, Bal-godkendte mappings pr. schema-fingerprint) kan nu analyseres
uden en mellemliggende SAF-T XML-oversætter. Kun parsere/gate/rapportmetadata/
CLI — ingen ændring af `analytics/categories/*.py`-kontrollogikken.

- **`backend/parsers/canonical_parser.py` (ny):** læser kanonisk gl_entries-CSV
  (+ valgfri `transform_summary.json`-sidecar for lineage) og bygger samme
  kontrakt-struktur som de to andre veje. Separator-agnostisk over for
  `vat_codes` (D1-kombinationen) — behandles altid som en opaque streng, aldrig
  splittet, uanset om vat-extract bruger `/` eller `|`. Én række = én
  transaktion = én linje (samme mønster som `data_adapter` for Excel-vejen).
  `document_date` afledes af `tax_point`-kolonnen (nærmeste kanoniske proxy for
  transaktionsdato adskilt fra bogføringsdato), med fallback til `posting_dates`.
- **`backend/parsers/upload_router.py`:** ny routing-gren `is_canonical()` —
  `.csv`/`.tsv` OG header indeholder de kanoniske markørkolonner
  (`gl_accounts`/`vat_codes`/`posting_dates`). Ingen kollision med et fladt
  Excel/CSV-udtræks kolonnenavne (`excel_parser.COLUMN_ALIASES` bruger andre
  navne). SAF-T-routing uændret, tjekkes først.
- **`catalog/data_contract.json` v0.2.0 (fra v0.1.0):** ny `kilder.canonical`
  pr. felt (69 felter, op fra 67 — to nye lineage-felter på `header`:
  `mapping_version`, `schema_fingerprint`). To nye `known_gaps`: **GAP-10**
  (kanonisk vej har ingen momssats — `tax_table`/`lines[].tax_percentage`
  altid 0.0, da den seedede BC/NAV-mapping ikke leverer en selvstændig
  satskolonne) og **GAP-11** (kanonisk vej har ingen kontoplan-/leverandør-/
  kundestamdata — `suppliers`/`customers` altid tomme, `accounts[]` kun
  `account_id`). En TREDJE ny gap tilføjet efter udviklings-E2E'en (se
  nedenfor): **GAP-12** (kontrol 10/transaktionsbalance er strukturelt
  støjende på denne vej, ingen bilagsgrupperingsnøgle). Alle tre dokumenterer
  reelle, kendte begrænsninger i dagens vat-extract-mapping/kilde-data, ikke
  fejl i denne parser.
- **`tests/test_data_contract_conformance.py`:** udvidet med den kanoniske vej
  (egen fixture + assert-helper, der bevidst SPRINGER objekter over, hvor
  kontrakten lover kanonisk-vejen intet felt — fx `suppliers`/`customers`,
  jf. GAP-11 — frem for at tvinge fabrikerede stamdata ind i fixturen).
- **`backend/analytics/reconciliation_gate.py` (ny):** afstemningsgate, §8.3
  ("afstemt mod kontroltotaler"). Sammenligner NETTO `debit_amount -
  credit_amount` pr. `account_id` mod en ekstern kontroltotal-fil
  (`{reconciliation_version, source, generated, accounts: [{account_id,
  amount}]}` — aftalt snitflade med vat-extract). Konfigurerbar tolerance
  (default 0,01 DKK). Blokerer IKKE analysen ved brud (v1-adfærd) — stempler
  rapporten tydeligt "IKKE_AFSTEMT" med detaljer pr. konto; ingen fil givet =
  "afstemning_ikke_udfoert". Teknisk fejl (ugyldig/manglende fil) rapporteres
  adskilt fra et fagligt afstemningsbrud.
- **`backend/tools/analyze_canonical.py` (ny CLI):** offline-kørsel af HELE
  motoren (`run_all_tests`, alle analyse-moduler som CLI-default — modsat
  webappens produktions-default på kun momskernen) på en kanonisk fil, uden
  webserver. Stempler rapporten med `catalog_version` (fra `catalog/rules.json`),
  `data_contract_version`, `mapping_version` og `schema_fingerprint`. Kør:
  `python tools/analyze_canonical.py <gl_entries.csv> [--reconciliation <json>]
  [--tolerance 0.01] [--modules alle|default|<liste>] --out <rapport.json>`.
- **Udviklings-E2E (2026-09-17):** kørt lokalt mod den rigtige BC/NAV-fil via
  `tools/analyze_canonical.py` (125.986 kanoniske rækker, gammel `/`-separator
  — den friske kørsel med den nye `|`-separator orkestreres separat efter
  vat-extracts parallelspor). Lineage stemplet korrekt (mapping_version 1.0.0,
  fuld schema_fingerprint matcher transform_summary.json). Parsing 0,28 s,
  analyse 5,57 s (alle 5 moduler/103 kontroller, ingen sprunget over). 346.083
  fund i alt — heraf **GAP-12 opdaget og dokumenteret pga. denne kørsel**:
  125.885 af de 125.885 kritiske fund kommer fra kontrol 10
  (transaktionsbalance), fordi hver CSV-række i dag bliver sin egen 1-linjes
  "transaktion" uden modpostering (se GAP-12). Rapport-JSON gemt i scratchpad,
  IKKE i repoet (kundedata i `all_findings`). Ingen kundedata i denne log.
- **Tests:** 29 nye (`test_canonical_parser.py` 12, `test_reconciliation_gate.py`
  10, `test_analyze_canonical_cli.py` 5, `test_data_contract_conformance.py`
  +2). Fuld suite: **251 tests**, alle grønne + uafhængig valideringssuite
  (98/98) grøn.

## Forsoning af input-veje mod datakontrakten — 2026-09-14 (ikke-katalog)
Trin 3 i byggerækkefølgen (`balai-platform/BALAI-dataflow-arkitektur.md` §7),
mod `catalog/data_contract.json` v0.1.0's ni `known_gaps`. Kun parsere/adapter/
kontrakt/tests — ingen ændring af `analytics/categories/*.py`-kontrollogikken.

- **GAP-05 (lukket):** `excel_parser.py` har nye kolonne-aliaser
  `customer_country`/`customer_vat_number` (med fallback til de generiske
  `country`/`vat_number`-kolonner, samme mønster som leverandøren allerede
  brugte) — kundens land/momsnummer kan nu leveres på Excel-vejen, hvilket gør
  kontrol 94-97 (e-handel/OSS/fjernsalg, modulet `ehandel_saerordninger`)
  reelt mulige der. Fravær af kolonner giver stadig tom streng, ikke crash.
  Præcisering undervejs: `cat12._cust_country()` falder rent faktisk allerede
  tilbage til linjens eget `country`-felt før `customers[].country` — det
  oprindeligt dokumenterede gab var smallere end først antaget.
- **GAP-04 (delvist lukket):** nye aliaser `opening_balance`/`closing_balance`
  på Excel-vejen — kontrol 77 (momskonto-afstemning) kan afstemme mod en reel
  saldo, når kildefilen bærer den, i stedet for en konstant nulsaldo. Værdien
  er `None` (ikke en stille 0.0), når kolonnen mangler — uændret kontrol-
  adfærd, da `test_77` allerede har et `or 0`-fallback.
- **GAP-03 (delvist lukket):** nye aliaser `account_type`/`standard_account_id`
  på Excel-vejen (både `accounts[]` og de joinede `transactions[].lines[]`) —
  momsrelevans-scopet (`vat_rules.is_non_vat_account`, kontrol 80) kan
  aktiveres på Excel-import, når kildefilen har kontoplan-kolonnerne. Uændret,
  konservativ default (CLAUDE.md) når kolonnerne mangler.
- **GAP-07 (lukket):** `saft_parser.parse_saft` beregner nu
  `summary.total_debit/total_credit/total_vat` deterministisk af de parsede
  transaktioner/linjer — samme beregning som `data_adapter`. `summary` er nu
  symmetrisk mellem input-vejene.
- **GAP-08/09 (delvist lukket):** `header.source` sat på begge veje (reel
  værdi, ikke placeholder); `tax_table[].rate` afledt af `tax_percentage` på
  SAF-T-vejen (reel værdi); `header.registration_number/saft_version` og
  `tax_table[].standard_tax_code/country` er nu til stede (tom streng) på
  begge veje for nøglesæt-symmetri (ingen KeyError-risiko), men uden reel
  værdi på den vej, hvor det er semantisk umuligt at udlede.
- **GAP-06 (uændret, uløseligt i dag):** dokumentationen er skærpet —
  SAF-T Financial GeneralLedgerEntries har intet selvstændigt dokument-id på
  transaktions-/linjeniveau; en reel lukning kræver en SAF-T-strukturel
  udvidelse (join mod `SourceDocuments`), som er uden for denne opgaves scope.
- **Skema-konformans som test (ny):** `tests/test_data_contract_conformance.py`
  validerer, at BEGGE parser-veje producerer output, der er konformt med
  `catalog/data_contract.json` (felter til stede, typer korrekte) på
  repræsentative fixtures. Ingen runtime-håndhævelse i parserne — kontrakten
  forbliver deskriptiv (jf. §7, byggetrin 2).
- `tools/data_contract_data.py` opdateret: `kilder`-flag flyttet til `true`
  for de nu reelt leverbare felter, `known_gaps` fik et `status`-felt
  (`aaben`/`delvist_lukket`/`lukket`) og opdateret tekst; kontrakten
  regenereret (`python tools/build_data_contract.py`).

## Maskinlæsbar datakontrakt — 2026-09-14 (ikke-katalog)
- **`catalog/data_contract.json` (v0.1.0):** motorens fulde inputkontrakt —
  7 objekter (`header/accounts/tax_table/transactions+lines/suppliers/
  customers/summary`), 67 felter — udledt af en ny hånd-vedligeholdt single
  source `tools/data_contract_data.py` via `tools/build_data_contract.py`
  (samme mønster som `build_rules_catalog.py`). Drift-gated
  (`tests/test_data_contract_fresh.py`), inkl. et krydstjek mod
  `analytics/readiness.py`'s signal-felter og et drift-tjek af
  `MATERIALITY_*`-env-navne mod `analytics/materiality.py`.
- **`balai_extensions`-afsnit:** eksplicit markering af felter der ikke er
  native SAF-T Financial (jf. `balai-platform/BALAI-dataflow-arkitektur.md`
  §2a) — ship_from/to_country, document_date, non_deductible_amount, samt en
  endnu delvist implementeret version-triple (erklæret/strukturelt
  detekteret/mål).
- **`run_config`-afsnit:** `ANALYTICS_MODULES` (introspektion af
  `analytics/modules.py`, aldrig hånd-duplikeret) og `MATERIALITY_*`-tærskler.
- **`known_gaps`-afsnit:** ni konkrete, evidensbaserede uoverensstemmelser
  mellem Excel- og SAF-T-input-vejene, opdaget under kortlægningen (fx
  `customers[].vat_number/country` hårdkodet tomme på Excel-vejen; `summary`
  mangler total_debit/credit/vat på SAF-T-vejen; `source_document_id` betyder
  fakturanummer vs. transaktionsbeskrivelse afhængigt af oprindelse). Ingen af
  disse er rettet — kontrakten er bevidst deskriptiv i denne omgang, ikke
  håndhævende (næste byggetrin i dataflow-arkitekturen).
- Bevidst UDEN FOR scope: ingen ændring af parser-adfærd (fx `or 0.0`-
  fallbacks) og ingen runtime-validering/pydantic — se `known_gaps`.

## Præsentation & scoring — 2026-09-09 (ikke-katalog)
- **Datagrundlag/kørbarhed:** `analytics/readiness.py` afgør pr. kontrol
  kørt / sprunget over (manglende felt) / modul fra / kræver eksterne data;
  vist som panel i UI. Kategorier uden kørte kontroller vises ikke længere som
  grøn «bestået», men som grå «Ikke kørt».
- **SAF-T-input i UI:** filvælger + tekster accepterer nu `.xml` (SAF-T).
- **Distinkt netto som overskrift:** de transaktions-deduplikerede beløb er nu
  hovedtallet; brutto (kan overlappe) vises som kontekst — undgår oppustede tal.
- **«Handling krævet»-panel:** kritiske + høje fund vises prioriteret øverst.
- **Score-kalibrering:** loft pr. severity-tier (`materiality.SEVERITY_PENALTY_CAPS`,
  MATERIALITY_CAP_*), så mange lav-/medium-fund ikke alene tvinger en kategori i
  rød — RØD drives af kritiske/høje fund. Bekræftet: Timing 6 → 69.
- **Auth-fix:** Starlette-session bruger egen cookie (`vat_local`) og central_auth
  prøver alle `session`-cookies — løser 401 på upload.

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
- **Robust momsrelevans-scope (increment B):** `analytics/standard_accounts.py`
  klassificerer `StandardAccountID` som balance (≥ 5000) eller resultat (1000–4999)
  ud fra ERST-standardkontoplanens sektions-headere. `vat_rules.is_non_vat_account`
  bruger nu dette signal ud over `AccountType`, så balanceposter undertrykkes korrekt
  på rigtige filer, hvor `AccountType` er fejlmærket "Other". Kontrol 80; bekræftet
  på den fejlmærkede v1.0-fil. Testet i `tests/test_standard_accounts.py` +
  udvidet `tests/test_vat_scope.py`.

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
