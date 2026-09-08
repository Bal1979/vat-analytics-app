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


# Severity-vægte til kategori-/overordnet score (engine.build_report).
SEVERITY_WEIGHTS = {
    "critical": _i("MATERIALITY_WEIGHT_CRITICAL", 25),
    "high": _i("MATERIALITY_WEIGHT_HIGH", 15),
    "medium": _i("MATERIALITY_WEIGHT_MEDIUM", 8),
    "low": _i("MATERIALITY_WEIGHT_LOW", 3),
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

# Lag mellem faktura- og bogføringsdato i dage (cat05 test_46).
INVOICE_POSTING_LAG_DAYS = _i("MATERIALITY_INVOICE_POSTING_LAG_DAYS", 30)

# Fjernsalgstærskel for EU B2C i DKK (~10.000 EUR) (cat12 test_95).
DISTANCE_SELLING_THRESHOLD_DKK = _f("MATERIALITY_DISTANCE_SELLING_DKK", 74500.0)
