"""
Materialitets-/tærskelkonfiguration — ét sted for de engagement-kalibrerbare
knapper i analysen.

Defaults matcher den hidtidige adfærd, så intet ændrer sig uden konfiguration.
Hver knap kan overstyres via en miljøvariabel (MATERIALITY_*), så «RØD = action»
kan kalibreres pr. kunde/engagement uden kodeændring.

Bevidst IKKE her: rent metodiske konstanter (fx Benford-kritisk værdi, rund-tal-
basis), som ikke er materialitets-/engagement-valg.
"""

import os


def _f(env, default):
    try:
        return float(os.environ[env])
    except (KeyError, ValueError):
        return float(default)


def _i(env, default):
    try:
        return int(os.environ[env])
    except (KeyError, ValueError):
        return int(default)


def _list(env, default):
    raw = os.environ.get(env)
    if not raw:
        return list(default)
    try:
        return [float(x) for x in raw.split(",") if x.strip()]
    except ValueError:
        return list(default)


def _strlist(env, default):
    raw = os.environ.get(env)
    if not raw:
        return list(default)
    return [p.strip() for p in raw.split(",") if p.strip()]


# Severity-vægte til kategori-/overordnet score (engine.build_report).
SEVERITY_WEIGHTS = {
    "critical": _i("MATERIALITY_WEIGHT_CRITICAL", 25),
    "high": _i("MATERIALITY_WEIGHT_HIGH", 15),
    "medium": _i("MATERIALITY_WEIGHT_MEDIUM", 8),
    "low": _i("MATERIALITY_WEIGHT_LOW", 3),
}

# Loft pr. severity-tier på det samlede score-fradrag i en kategori. Uden loft
# kunne mange lav-/medium-fund alene presse en kategori i rød — men RØD skal
# betyde «handling krævet», dvs. drevet af kritiske/høje fund. Kritisk/høj har et
# højt loft (kan reelt give rød); medium/lav er begrænset, så medium-only højst
# giver gul og lav-only forbliver ~grøn. Kalibrerbart pr. engagement.
SEVERITY_PENALTY_CAPS = {
    "critical": _i("MATERIALITY_CAP_CRITICAL", 100),
    "high": _i("MATERIALITY_CAP_HIGH", 100),
    "medium": _i("MATERIALITY_CAP_MEDIUM", 30),
    "low": _i("MATERIALITY_CAP_LOW", 15),
}

# Kontantbetalingsgrænse (cat07): erhvervsdrivende må ikke modtage ≥ 20.000 DKK kontant.
CASH_LIMIT = _f("MATERIALITY_CASH_LIMIT", 20000.0)

# Interne godkendelses-/indberetningsgrænser (cat07): runde tal lige under disse flagges.
APPROVAL_THRESHOLDS = _list("MATERIALITY_APPROVAL_THRESHOLDS",
                            [10000, 25000, 50000, 100000, 250000, 500000])

# Stort momsbeløb uden bilag (cat07 test_59).
LARGE_VAT_NO_DOCUMENT = _f("MATERIALITY_LARGE_VAT_NO_DOCUMENT", 5000.0)

# Forhold købsmoms/salgsmoms der udløser flag (cat10 test_76).
INPUT_OUTPUT_RATIO = _f("MATERIALITY_INPUT_OUTPUT_RATIO", 3.0)

# Materialitets-gulv for kontrol 22 (manglende salgsmoms, 2026-09-18,
# Bal-godkendt gap-analyse-fix B): en kreditlinje under denne grænse
# undertrykkes ALTID, uanset momskode/-sats -- formålet er udelukkende at
# fjerne rene afrundingslinjer (fx 0,01 kr. øre-korrektioner), IKKE at skjule
# reelle fund. Sat lavt (1 krone) med vilje: enhver reel salgstransaktion
# ligger langt over denne grænse.
CONTROL_22_MIN_BASE = _f("MATERIALITY_CONTROL_22_MIN_BASE", 1.0)

# Kontrol 60 (negativt momsbeløb, 2026-09-18, Bal-godkendt gap-analyse-fix D):
# en negativ momslinje undertrykkes, når den beviseligt nettes af en
# matchende positiv modpost på SAMME konto+momskode -- enten i samme bilag,
# eller som et "reversal-par" (modsat beløb inden for denne tolerance,
# bogført inden for dette antal dage). Strukturel regel -- INGEN
# kunde-specifikke bilagspræfikser indgår. Tolerance sat lavt (øre-niveau);
# vinduet sat til én kalendermåned (dækker den typiske allokerings-/
# tilbageførselsafstand fundet empirisk på v4-datasættet: >90% af parrene
# er bogført SAMME dag, resten typisk inden for samme momsperiode).
CONTROL_60_NET_TOLERANCE = _f("MATERIALITY_CONTROL_60_NET_TOLERANCE", 0.02)
CONTROL_60_REVERSAL_WINDOW_DAYS = _i("MATERIALITY_CONTROL_60_REVERSAL_WINDOW_DAYS", 31)

# Kontrol 80 (indtægt uden momsbehandling, byggetrin ~9, Del B, Bal-godkendt
# 2026-09-17): fundet aggregeres PR. KONTO (én postering-population, ikke én
# postering ad gangen — se cat10_vat_reconciliation.test_80_revenue_without_
# output_vat). Severity gradueres efter kontoens samlede uden-moms-grundlag
# (sum af de kvalificerende posteringer), så en konto med et lille beløb
# ikke automatisk vejer lige så tungt som en konto med millionbeløb.
CONTROL_80_HIGH_THRESHOLD = _f("MATERIALITY_CONTROL_80_HIGH", 500000.0)
CONTROL_80_MEDIUM_THRESHOLD = _f("MATERIALITY_CONTROL_80_MEDIUM", 100000.0)

# Maks. antal transaktionsreferencer vist pr. kontrol-80-fund (drill-down-
# udsnit). Resten opsummeres i beskrivelsen ("…og N flere").
CONTROL_80_MAX_REFS = _i("MATERIALITY_CONTROL_80_MAX_REFS", 10)

# Kontrol 32 (manglende landekode på udenlandsk part, K2, gap-analyse-runde 2/
# kunde 2, Bal-godkendt 2026-09-20): fundet aggregeres PR. PART-NØGLE (samme
# princip som kontrol 80's pr.-konto-aggregering), ikke pr. linje — se
# cat04_cross_border_eu.test_32_missing_country_on_foreign. Samme
# ref-begrænsning/opsummeringsmønster som kontrol 80.
CONTROL_32_MAX_REFS = _i("MATERIALITY_CONTROL_32_MAX_REFS", 10)

# Kontrol 84 (missing trader, kontrol 84-efterforskningen/K5-b, 2026-09-20):
# "kollektivnummer"-undtagelsen. Et udfyldt momsnummer, der fejler EU-format-
# valideringen, tæller IKKE som "ugyldigt momsnr"-risikofaktor, når præcis
# samme værdi optræder med mindst dette antal DISTINKTE (normaliserede)
# beskrivelsestekster i datasættet — så er værdien empirisk en samle-/
# kollektivkonto-konvention i leverandørkartoteket (mange reelt forskellige,
# tydeligt navngivne modparter deler ét generisk/afkortet nummer), ikke én
# skjult handelspartner. Strukturelt signal — INGEN kundenavne/-numre i koden.
# Formatfejlen selv dækkes fortsat af kontrol 28 (datakvalitet).
CONTROL_84_SHARED_VAT_MIN_DESCS = _i("MATERIALITY_CONTROL_84_SHARED_VAT_MIN_DESCS", 3)

# Lag mellem faktura- og bogføringsdato i dage (cat05 test_46).
INVOICE_POSTING_LAG_DAYS = _i("MATERIALITY_INVOICE_POSTING_LAG_DAYS", 30)

# Fjernsalgstærskel for EU B2C i DKK (~10.000 EUR) (cat12 test_95).
DISTANCE_SELLING_THRESHOLD_DKK = _f("MATERIALITY_DISTANCE_SELLING_DKK", 74500.0)

# Kontrol 82 (periode-/rubrikafstemning mod momsangivelsen, byggetrin 8/Del B,
# Bal-godkendt 2026-09-17): mønstre der klassificerer en KØBSLINJES momskode
# til henholdsvis indenlandsk omvendt betalingspligt (DKRC — tælles med i
# UDGÅENDE moms, jf. rubrik-logikken i cat10_vat_reconciliation.py) og
# RC-ydelser fra udlandet (egen rubrik). Delstrengs-match, case-insensitivt
# (vat_rules.text_matches_any — mønstrene holdes lowercase her).
# KENDT BEGRÆNSNING (bevidst, ikke skjult): mønstrene er kalibreret til den
# observerede BC/NAV-kode-taksonomi (fx "DOMESTIC|REDUCED_PRIVATE_DKRC",
# "*SERVICE_VAT_EU*"/"*SERVICE_VAT_NOT_EU*") — IKKE en universel standard for
# alle kunders momskode-navngivning. En klient med en anden taksonomi kræver
# en engagement-specifik override via env (komma-separeret liste).
VAT_DECLARATION_DKRC_PATTERNS = [
    p.lower() for p in _strlist("MATERIALITY_VAT_DECLARATION_DKRC_PATTERNS", ["DKRC"])
]
VAT_DECLARATION_SERVICE_VAT_PATTERNS = [
    p.lower() for p in _strlist("MATERIALITY_VAT_DECLARATION_SERVICE_VAT_PATTERNS", ["SERVICE_VAT"])
]

# Fix-runde 2026-09-22 (Bal-godkendt, FEJL 1 — empirisk påvist mod Nordic
# RCC's TastSelv-angivelse og ekspertens 3-vejs-afstemning): kode-navnemønster
# der identificerer en AFGIFTSKODE (fx elafgift, "DOMESTIC|ELECTRICITY_TAX")
# frem for en ægte momskode, i BC/NAVs "Bus.-gruppe|Produktkode"-konvention.
# BC/NAV navngiver afgiftskoder med suffikset "_TAX" (modsat momskoders
# "_VAT"-suffiks) — et GENERISK navngivningsmønster, ikke en kundespecifik
# værdi/liste af afgiftstyper. Sådanne linjer hører til momsangivelsens EGEN
# "energy_taxes"-rubrik (afgifter, ikke moms), som v1 bevidst ikke afstemmer
# (se cat10_vat_reconciliation.py) — de skal derfor UDELADES af den
# beregnede input_vat-rubrik, ikke tælles med som fradragsberettiget
# købsmoms. Delstrengs-match, case-insensitivt (vat_rules.text_matches_any),
# samme kalibreringsfilosofi som DKRC-/SERVICE_VAT-mønstrene ovenfor —
# override via env for en anden ERP-konvention. Se
# ``vat_rules.is_energy_tax_code`` for det sekundære, strukturelle
# fallback-signal (tax_percentage=0 + vat_calculation_type "Full VAT").
VAT_DECLARATION_ENERGY_TAX_PATTERNS = [
    p.lower() for p in _strlist("MATERIALITY_VAT_DECLARATION_ENERGY_TAX_PATTERNS", ["_TAX"])
]

# Tolerance (DKK) for periode-/rubrikafstemningen i kontrol 82 — under denne
# betragtes en beregnet rubrik og den angivne værdi som matchende (afrundings-
# differencer, ikke et reelt fund).
VAT_DECLARATION_TOLERANCE = _f("MATERIALITY_VAT_DECLARATION_TOLERANCE", 1.0)

# Relativ årsgrænse (procent af angivet årstotal pr. rubrik) for kontrol 82's
# timing-klassifikation: en årsdifference inden for denne grænse vurderes som
# TIMING (fx købsmoms angivet på settlement- frem for vat_period-basis, med
# spillover hen over årsgrænsen) og giver LAV i stedet for HØJ pr. periode.
VAT_DECLARATION_ANNUAL_TIMING_PCT = _f("MATERIALITY_VAT_DECLARATION_ANNUAL_TIMING_PCT", 1.0)

# Selvkonsistens-gaten / "momskonto-krydstjekket" (byggetrin ~12, Bal-godkendt
# 2026-09-23, analytics/self_consistency_gate.py): motorens egen kontrol af,
# om dens beregnede rubrikker (samme kilde-af-sandhed som kontrol 82's
# _compute_period_rubrics) stemmer med de FAKTISKE posteringer på kundens
# egne momskonti (tax_table[].sales_vat_account/purchase_vat_account/
# reverse_charge_vat_account) — UDEN et ekspert-facit at holde det op imod
# (baggrund: kontrol 82-sagen 2026-09-22, se CHANGELOG).

# Øre-/afrundingstolerance (DKK) for én rubrik i én periode/år — under denne
# betragtes beregnet og bogført som identiske (samme filosofi som
# VAT_DECLARATION_TOLERANCE ovenfor).
SELF_CONSISTENCY_TOLERANCE = _f("MATERIALITY_SELF_CONSISTENCY_TOLERANCE", 1.0)

# Relativ materialitetsgrænse (procent af den beregnede ÅRSTOTAL for rubrikken)
# der afgør, om en årsdifference er stor nok til at flage gaten som
# "afvigelse" — under grænsen (og under den absolutte tolerance ovenfor)
# er differencen støj, ikke et signal om en klassifikationsfejl. Samme
# "max(absolut, relativ)"-mønster som VAT_DECLARATION_ANNUAL_TIMING_PCT.
SELF_CONSISTENCY_MATERIALITY_PCT = _f("MATERIALITY_SELF_CONSISTENCY_MATERIALITY_PCT", 1.0)

# BC/NAV's kildekode (``source_code``) for VAT-afregningsbatchen ("moms-
# afregning") — disse posteringer NULSTILLER momskontiene ved periodeafslutning
# og skal holdes ude af krydstjekket (de er ikke en del af periodens
# beregnede rubrik, kun kontoens periodiske nulstilling). Empirisk bekræftet
# på BC-datasættet: batchen bærer IKKE altid ``supply_direction="settlement"``
# (kun på nogle af de berørte konti) — ``source_code="MOMSAFREGN"`` er det
# pålidelige, gennemgående signal. Sammenlignes case-insensitivt. Kalibreret
# til BC/NAV's observerede vokabular, ikke universel — override via env
# (komma-separeret liste) for en anden ERP-konvention.
SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES = [
    c.upper() for c in _strlist("MATERIALITY_SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES", ["MOMSAFREGN"])
]

# Kontrol 104-108 (krydsdimensionelle kontroller, gap-analysen, Bal-godkendt
# 2026-09-18, analytics/categories/cat13_cross_dimension.py):

# Produktkode-mønstre der identificerer en "ægte nulsats/fritaget"-kode i
# BC/NAV's "Bus.-gruppe|Produktkode"-konvention (fx "NO_VAT", "NO_VAT_EU",
# "NO_VAT_NOT_EU") -- modsat en reverse charge-kode (GOODS_VAT_*/SERVICE_VAT_*).
# Delstrengs-match, case-insensitivt. Samme kalibrerings-filosofi som kontrol
# 82's DKRC/SERVICE_VAT-mønstre: kalibreret til den observerede BC/NAV-
# taksonomi, ikke en universel standard — override via env for en klient med
# en anden navnekonvention.
NO_VAT_PRODUCT_PATTERNS = [
    p.lower() for p in _strlist("MATERIALITY_NO_VAT_PRODUCT_PATTERNS", ["NO_VAT"])
]

# Kontrol 107 (atypisk moms på bilagstype): en bilagstype (source_code) skal
# have mindst dette antal linjer, før dens moms-andel overhovedet kan siges at
# være "typisk" (undgår at dømme et mønster ud fra en håndfuld linjer).
CONTROL_107_MIN_LINES_PER_SOURCE_CODE = _i("MATERIALITY_CONTROL_107_MIN_LINES", 20)

# Kontrol 107: en bilagstype er "typisk momsfri", når højst denne andel af
# dens linjer bærer et momsbeløb. De ENKELTE linjer der alligevel har moms på
# en sådan bilagstype, er kontrollens fund.
CONTROL_107_MAX_TYPICAL_VAT_SHARE = _f("MATERIALITY_CONTROL_107_MAX_VAT_SHARE", 0.05)

# Kontrol 108 (salg/køb spredt over mange bilagstyper): minimumspopulation
# for en retning (salg/køb), før spredningen overhovedet vurderes.
CONTROL_108_MIN_LINES = _i("MATERIALITY_CONTROL_108_MIN_LINES", 30)

# Kontrol 108: minimum antal DISTINKTE bilagstyper i én retning, før det
# tæller som "spredt" (procesobservation, ikke en fejlpåstand).
CONTROL_108_MIN_SOURCE_CODES = _i("MATERIALITY_CONTROL_108_MIN_SOURCE_CODES", 3)

# F2 (gap-analyse-runde 2, Bal-godkendt 2026-09-20): calc-type-/RC-kode-
# vokabular konfigurerbart pr. ERP (analytics/vat_rules.is_reverse_charge_
# sale_code). BC/NAV signalerer reverse charge enten via et
# vat_calculation_type-tekstfelt ("Reverse Charge VAT") eller via "Bus.-
# gruppe|Produktkode"-konventionen (håndteret separat, ingen ændring).
# IFS' VAT Code-opsætning (kunde 2s IFS-datasæts vat_setup.csv, empirisk
# verificeret 2026-09-20) har INGEN af delene — koderne er opake mnemonics
# uden "|"-separator, og vat_calculation_type-kolonnen findes slet ikke i
# den leverede vat_setup.csv. Observerede IFS-koder: "RC" (Tax percentage
# 25, fuldt fradragsberettiget — indenlandsk omvendt betalingspligt) og
# "RC50" (samme sats, 50% ikke-fradragsberettiget — defineret i opsætningen,
# ikke observeret i transaktionsdata). Prefiks-match, case-insensitivt,
# samme kalibreringsfilosofi som NO_VAT_PRODUCT_PATTERNS ovenfor — override
# for en klient med en anden kode-konvention (fx Oracle/SAP).
RC_CODE_PREFIXES = [p.upper() for p in _strlist("MATERIALITY_RC_CODE_PREFIXES", ["RC"])]

# Kalibrering "chip 2's tråd A" (2026-09-20, Bal-godkendt): vat-extract
# udvidede efterfølgende IFS' vat_setup-mapping (mapping v1.1.0) med
# ``ext_vat_calculation_type``, der bærer IFS' rå "Tax Type"-kolonne igennem
# UÆNDRET (samme "opak streng, ingen fortolkning"-disciplin som resten af
# canonical_masterdata.py). Empirisk værdirum på kunde 2s regenererede
# vat_setup.csv (21 koder): "Tax" (14), "No Tax" (1), "Calculated Tax"
# (PRÆCIS {E0G, E0S, E1G, E1S, RC, RC50}) — IFS grupperer selv de fire
# tidligere uafklarede E-koder sammen med de kendte RC-koder. "Calculated
# Tax" betyder ENTYDIGT omvendt betalingspligt i IFS' egen taksonomi og
# tilføjes derfor til det genkendte vat_calculation_type-vokabular
# (analytics/vat_rules.is_rc_calc_type), ud over BC/NAVs "reverse charge"-
# substring-familie (håndteret separat, ingen ændring). INGEN fuzzy-match —
# eksplicit værdiliste, lowercase-normaliseret. Engagement-overstyrbar for
# andre ERP'ers tilsvarende, entydige RC-beregningstype-værdier.
RC_CALC_TYPE_VALUES = [v.strip().lower() for v in
                       _strlist("MATERIALITY_RC_CALC_TYPE_VALUES", ["calculated tax"])]

# Kontrol 109 (Fradragsprocent-afvigelse, gap-analyse-runde 2, Bal-godkendt
# 2026-09-20, cat13_cross_dimension.py): tolerance i DKK mellem bogført og
# forventet (grundlag × sats × Deductible%) moms pr. bilag+momskode, før det
# tæller som et fund — undertrykker afrundingsstøj, samme princip som
# CONTROL_22_MIN_BASE.
CONTROL_109_TOLERANCE = _f("MATERIALITY_CONTROL_109_TOLERANCE", 1.0)

# Kontrol 109: difference (DKK) hvorover fundet er HØJ i stedet for MEDIUM —
# samme kalibrering som kontrol 1's høj-grænse.
CONTROL_109_HIGH_THRESHOLD = _f("MATERIALITY_CONTROL_109_HIGH_THRESHOLD", 100.0)

# Kunderapportens kuraterings-seed (byggetrin ~10, Bal-godkendt 2026-09-18,
# tools/report_curation.py): et tema-gruppe uden kritiske/høje fund
# forfremmes ("medtag": true i den auto-genererede kuration) hvis dens
# samlede estimerede beløb når denne tærskel. Høj-fund-grupper og
# timing-temaet forfremmes ALTID (jf. designoplægget) uanset beløb.
REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD = _f(
    "MATERIALITY_REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD", 100000.0
)
