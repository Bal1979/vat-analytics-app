"""
SAF-T Analytics Engine
Kører alle 103 momsanalysetests og returnerer struktureret rapport
med findings klassificeret efter impact-type, retning og sværhedsgrad.
"""

import logging
from typing import Optional, Iterable
from analytics.models import make_finding
from analytics import materiality
from analytics import modules

logger = logging.getLogger(__name__)

# Import test-kategorier
from analytics.categories.cat01_transaction_integrity import run_transaction_integrity_tests
from analytics.categories.cat02_duplicate_detection import run_duplicate_detection_tests
from analytics.categories.cat03_vat_rate_validation import run_vat_rate_tests
from analytics.categories.cat04_cross_border_eu import run_cross_border_tests
from analytics.categories.cat05_timing_period import run_timing_tests
from analytics.categories.cat06_party_validation import run_party_tests
from analytics.categories.cat07_amount_threshold import run_amount_tests
from analytics.categories.cat08_statistical_anomaly import run_statistical_tests
from analytics.categories.cat09_reverse_charge import run_reverse_charge_tests
from analytics.categories.cat10_vat_reconciliation import run_reconciliation_tests
from analytics.categories.cat11_fraud_mtic import run_fraud_tests
from analytics.categories.cat12_ecommerce_special import run_ecommerce_tests


# === KATEGORI-DEFINITIONER ===

CATEGORIES = [
    {"id": 1, "name": "Transaktionsintegritet & Datakvalitet", "test_range": (1, 10)},
    {"id": 2, "name": "Dubletdetektion", "test_range": (11, 18)},
    {"id": 3, "name": "Momssats-validering", "test_range": (19, 26)},
    {"id": 4, "name": "Grænseoverskridende & EU-compliance", "test_range": (27, 38)},
    {"id": 5, "name": "Timing & Periodetest", "test_range": (39, 46)},
    {"id": 6, "name": "Leverandør- & Kundevalidering", "test_range": (47, 54)},
    {"id": 7, "name": "Beløbs- & Tærskeltest", "test_range": (55, 62)},
    {"id": 8, "name": "Statistisk Anomalidetektion", "test_range": (63, 69)},
    {"id": 9, "name": "Reverse Charge & Selvangivelse", "test_range": (70, 75)},
    {"id": 10, "name": "Indgående/Udgående Moms Afstemning", "test_range": (76, 83)},
    {"id": 11, "name": "Svindeldetektion & Karrusel/MTIC", "test_range": (84, 93)},
    {"id": 12, "name": "E-handel, Digitale Ydelser & Særordninger", "test_range": (94, 103)},
]


# === TEST RUNNER ===

def run_all_tests(data: dict, active_modules: Optional[Iterable[str]] = None) -> dict:
    """
    Kør alle implementerede tests mod parsed SAF-T data.
    Returnerer en fuld analyserapport.

    ``active_modules``: valgfrit eksplicit sæt af analyse-modulnøgler (se
    analytics/modules.py). Er det ikke sat, bestemmes de aktive moduler af
    miljøvariablen ANALYTICS_MODULES, ellers default (kun momskernen).
    Deaktiverede moduler kører teknisk stadig, men deres fund filtreres fra før
    rapporten bygges — så aggregater/scores kun afspejler de aktive kontroller.
    """
    all_findings = []

    # Kør implementerede kategorier
    logger.info("Running transaction integrity tests (cat01)")
    cat01_findings = run_transaction_integrity_tests(data)
    logger.info("Transaction integrity tests: %d findings", len(cat01_findings))
    all_findings.extend(cat01_findings)

    logger.info("Running duplicate detection tests (cat02)")
    cat02_findings = run_duplicate_detection_tests(data)
    logger.info("Duplicate detection tests: %d findings", len(cat02_findings))
    all_findings.extend(cat02_findings)

    for label, runner in (
        ("vat rate validation (cat03)", run_vat_rate_tests),
        ("cross-border & EU (cat04)", run_cross_border_tests),
        ("timing & period (cat05)", run_timing_tests),
        ("party validation (cat06)", run_party_tests),
        ("amount & threshold (cat07)", run_amount_tests),
        ("statistical anomaly (cat08)", run_statistical_tests),
        ("reverse charge (cat09)", run_reverse_charge_tests),
        ("vat reconciliation (cat10)", run_reconciliation_tests),
        ("fraud & MTIC (cat11)", run_fraud_tests),
        ("e-commerce & special schemes (cat12)", run_ecommerce_tests),
    ):
        logger.info("Running %s", label)
        cat_findings = runner(data)
        logger.info("%s: %d findings", label, len(cat_findings))
        all_findings.extend(cat_findings)

    logger.info("All tests complete: %d total findings", len(all_findings))

    # Momsrelevans-slankning: behold kun fund fra aktive analyse-moduler.
    active = modules.resolve_active_modules(active_modules)
    kept = [f for f in all_findings if modules.is_control_active(f["test_id"], active)]
    suppressed = len(all_findings) - len(kept)
    logger.info("Aktive moduler: %s — beholdt %d fund, filtreret %d fra",
                sorted(active), len(kept), suppressed)

    # Byg rapport på de aktive fund
    report = build_report(data, kept)
    report["moduler"] = modules.module_summary(active)
    report["filtrerede_fund"] = suppressed
    return report


def distinct_amount(findings: list) -> tuple:
    """Transaktions-dedupliceret beløb for en delmængde af findings.

    Samme transaktion kan udløse flere kontroller. En ren sum af findings'
    `estimated_amount` dobbelttæller derfor pengene. Denne funktion henfører
    hvert findings beløb til de transaktioner det refererer (estimated_amount delt
    ligeligt på de refererede transaktioner) og tager pr. transaktion det STØRSTE
    bidrag på tværs af kontroller — et defensivt, ikke-dobbelttællende nedre estimat.

    Fund uden transaktions-id (aggregerede fund som forhold/ratioer) kan ikke
    henføres til en enkelt transaktion og tælles helt.

    Returnerer (distinkt_beløb, antal_distinkte_transaktioner).
    """
    by_txn = {}            # transaction_id -> største henførte bidrag
    aggregate_total = 0.0  # fund uden transaktions-id
    for f in findings:
        amt = f.get("estimated_amount") or 0.0
        if amt <= 0:
            continue
        txn_ids = [t.get("transaction_id") for t in (f.get("transactions") or [])
                   if t.get("transaction_id")]
        if not txn_ids:
            aggregate_total += amt
            continue
        per = amt / len(txn_ids)
        for tid in txn_ids:
            if per > by_txn.get(tid, 0.0):
                by_txn[tid] = per
    return round(sum(by_txn.values()) + aggregate_total, 2), len(by_txn)


def build_report(data: dict, findings: list) -> dict:
    """Byg den fulde analyserapport med scores og klassificering."""

    # Klassificér findings per impact-type
    economic = [f for f in findings if f["impact_type"] == "economic"]
    interest_risk = [f for f in findings if f["impact_type"] == "interest_risk"]
    compliance = [f for f in findings if f["impact_type"] == "compliance"]

    # Brutto-beløb per retning: sum af per-kontrol-estimater (KAN overlappe, dvs.
    # samme transaktion talt af flere kontroller). Suppleres af distinkte tal nedenfor.
    economic_negative = sum(f["estimated_amount"] for f in economic if f["direction"] == "negative")
    economic_positive = sum(f["estimated_amount"] for f in economic if f["direction"] == "positive")
    interest_negative = sum(f["estimated_amount"] for f in interest_risk if f["direction"] == "negative")
    interest_positive = sum(f["estimated_amount"] for f in interest_risk if f["direction"] == "positive")

    # Distinkte (transaktions-deduplikerede) beløb — undgår dobbelttælling.
    econ_neg_distinct, econ_neg_txns = distinct_amount([f for f in economic if f["direction"] == "negative"])
    econ_pos_distinct, econ_pos_txns = distinct_amount([f for f in economic if f["direction"] == "positive"])
    int_neg_distinct, int_neg_txns = distinct_amount([f for f in interest_risk if f["direction"] == "negative"])
    int_pos_distinct, int_pos_txns = distinct_amount([f for f in interest_risk if f["direction"] == "positive"])

    # Beregn scores per kategori
    category_results = []
    for cat in CATEGORIES:
        cat_findings = [f for f in findings if cat["test_range"][0] <= f["test_id"] <= cat["test_range"][1]]
        total_tests = cat["test_range"][1] - cat["test_range"][0] + 1

        # Score: 100 - (findings med severity-vægt). Vægtene er engagement-kalibrerbare.
        severity_weights = materiality.SEVERITY_WEIGHTS
        penalty = sum(severity_weights.get(f["severity"], 5) for f in cat_findings)
        score = max(0, min(100, 100 - penalty))

        category_results.append({
            "id": cat["id"],
            "name": cat["name"],
            "score": score,
            "total_tests": total_tests,
            "findings_count": len(cat_findings),
            "critical_count": len([f for f in cat_findings if f["severity"] == "critical"]),
            "high_count": len([f for f in cat_findings if f["severity"] == "high"]),
            "medium_count": len([f for f in cat_findings if f["severity"] == "medium"]),
            "low_count": len([f for f in cat_findings if f["severity"] == "low"]),
            "findings": cat_findings,
        })

    # Samlet score (gennemsnit af kategori-scores, vægtet efter antal tests)
    total_weight = sum(c["total_tests"] for c in category_results)
    overall_score = round(
        sum(c["score"] * c["total_tests"] for c in category_results) / total_weight
    ) if total_weight > 0 else 100

    return {
        "overall_score": overall_score,
        "summary": data.get("summary", {}),
        "impact_summary": {
            "economic": {
                "total_findings": len(economic),
                "negative_amount": round(economic_negative, 2),
                "positive_amount": round(economic_positive, 2),
                "net_amount": round(economic_positive - economic_negative, 2),
                # Distinkte (transaktions-deduplikerede) tal — undgår dobbelttælling.
                "negative_amount_distinct": econ_neg_distinct,
                "positive_amount_distinct": econ_pos_distinct,
                "net_amount_distinct": round(econ_pos_distinct - econ_neg_distinct, 2),
                "distinct_transactions": econ_neg_txns + econ_pos_txns,
                "currency": data["header"].get("currency", "DKK"),
            },
            "interest_risk": {
                "total_findings": len(interest_risk),
                "negative_amount": round(interest_negative, 2),
                "positive_amount": round(interest_positive, 2),
                "net_amount": round(interest_positive - interest_negative, 2),
                "negative_amount_distinct": int_neg_distinct,
                "positive_amount_distinct": int_pos_distinct,
                "net_amount_distinct": round(int_pos_distinct - int_neg_distinct, 2),
                "distinct_transactions": int_neg_txns + int_pos_txns,
                "currency": data["header"].get("currency", "DKK"),
            },
            "compliance": {
                "total_findings": len(compliance),
                "critical_count": len([f for f in compliance if f["severity"] == "critical"]),
                "high_count": len([f for f in compliance if f["severity"] == "high"]),
                "medium_count": len([f for f in compliance if f["severity"] == "medium"]),
                "low_count": len([f for f in compliance if f["severity"] == "low"]),
            },
        },
        "categories": category_results,
        "all_findings": findings,
        "total_findings": len(findings),
        "severity_summary": {
            "critical": len([f for f in findings if f["severity"] == "critical"]),
            "high": len([f for f in findings if f["severity"] == "high"]),
            "medium": len([f for f in findings if f["severity"] == "medium"]),
            "low": len([f for f in findings if f["severity"] == "low"]),
        },
    }


def run_analytics(data: dict, active_modules: Optional[Iterable[str]] = None) -> dict:
    """
    Hovedfunktion: Kør alle analytics tests mod parsed data.
    Alias for run_all_tests — bruges af main.py. ``active_modules`` sendes videre
    (ellers styres modulerne af ANALYTICS_MODULES / default).
    """
    return run_all_tests(data, active_modules=active_modules)
