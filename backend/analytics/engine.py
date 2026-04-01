"""
SAF-T Analytics Engine
Kører alle 103 momsanalysetests og returnerer struktureret rapport
med findings klassificeret efter impact-type, retning og sværhedsgrad.
"""

from typing import Optional
from analytics.parser import parse_saft_file
from analytics.models import make_finding

# Import test-kategorier
from analytics.categories.cat01_transaction_integrity import run_transaction_integrity_tests
from analytics.categories.cat02_duplicate_detection import run_duplicate_detection_tests


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

def run_all_tests(data: dict) -> dict:
    """
    Kør alle implementerede tests mod parsed SAF-T data.
    Returnerer en fuld analyserapport.
    """
    all_findings = []

    # Kør implementerede kategorier
    all_findings.extend(run_transaction_integrity_tests(data))
    all_findings.extend(run_duplicate_detection_tests(data))

    # TODO: Tilføj flere kategorier her efterhånden
    # all_findings.extend(run_vat_rate_tests(data))
    # all_findings.extend(run_cross_border_tests(data))
    # ...

    # Byg rapport
    report = build_report(data, all_findings)
    return report


def build_report(data: dict, findings: list) -> dict:
    """Byg den fulde analyserapport med scores og klassificering."""

    # Klassificér findings per impact-type
    economic = [f for f in findings if f["impact_type"] == "economic"]
    interest_risk = [f for f in findings if f["impact_type"] == "interest_risk"]
    compliance = [f for f in findings if f["impact_type"] == "compliance"]

    # Beregn beløb per retning
    economic_negative = sum(f["estimated_amount"] for f in economic if f["direction"] == "negative")
    economic_positive = sum(f["estimated_amount"] for f in economic if f["direction"] == "positive")
    interest_negative = sum(f["estimated_amount"] for f in interest_risk if f["direction"] == "negative")
    interest_positive = sum(f["estimated_amount"] for f in interest_risk if f["direction"] == "positive")

    # Beregn scores per kategori
    category_results = []
    for cat in CATEGORIES:
        cat_findings = [f for f in findings if cat["test_range"][0] <= f["test_id"] <= cat["test_range"][1]]
        total_tests = cat["test_range"][1] - cat["test_range"][0] + 1

        # Score: 100 - (findings med severity-vægt)
        severity_weights = {"critical": 25, "high": 15, "medium": 8, "low": 3}
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
        "summary": data["summary"],
        "impact_summary": {
            "economic": {
                "total_findings": len(economic),
                "negative_amount": round(economic_negative, 2),
                "positive_amount": round(economic_positive, 2),
                "net_amount": round(economic_positive - economic_negative, 2),
                "currency": data["header"].get("currency", "DKK"),
            },
            "interest_risk": {
                "total_findings": len(interest_risk),
                "negative_amount": round(interest_negative, 2),
                "positive_amount": round(interest_positive, 2),
                "net_amount": round(interest_positive - interest_negative, 2),
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


def analyze_file(file_path: str) -> Optional[dict]:
    """
    Hovedfunktion: Parser en SAF-T fil og kører alle analytics tests.
    Returnerer fuld rapport eller None ved fejl.
    """
    data = parse_saft_file(file_path)
    if data is None:
        return None

    report = run_all_tests(data)
    return report
