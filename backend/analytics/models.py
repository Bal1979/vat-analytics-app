"""
Datamodeller for SAF-T Analytics findings.
"""


def make_finding(
    test_id: int,
    test_name: str,
    impact_type: str,       # "economic" | "interest_risk" | "compliance"
    direction: str,          # "negative" | "positive" | "neutral"
    severity: str,           # "critical" | "high" | "medium" | "low"
    description: str,
    fix_suggestion: str = "",
    estimated_amount: float = 0.0,
    currency: str = "DKK",
    transactions: list = None,
) -> dict:
    """Opret et standardiseret finding-objekt."""
    return {
        "test_id": test_id,
        "test_name": test_name,
        "impact_type": impact_type,
        "direction": direction,
        "severity": severity,
        "description": description,
        "fix_suggestion": fix_suggestion,
        "estimated_amount": estimated_amount,
        "currency": currency,
        "transactions": transactions or [],
    }
