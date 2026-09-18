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

# Tolerance (DKK) for periode-/rubrikafstemningen i kontrol 82 — under denne
# betragtes en beregnet rubrik og den angivne værdi som matchende (afrundings-
# differencer, ikke et reelt fund).
VAT_DECLARATION_TOLERANCE = _f("MATERIALITY_VAT_DECLARATION_TOLERANCE", 1.0)

# Relativ årsgrænse (procent af angivet årstotal pr. rubrik) for kontrol 82's
# timing-klassifikation: en årsdifference inden for denne grænse vurderes som
# TIMING (fx købsmoms angivet på settlement- frem for vat_period-basis, med
# spillover hen over årsgrænsen) og giver LAV i stedet for HØJ pr. periode.
VAT_DECLARATION_ANNUAL_TIMING_PCT = _f("MATERIALITY_VAT_DECLARATION_ANNUAL_TIMING_PCT", 1.0)
