"""
Tests for the analytics engine.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analytics.engine import run_all_tests
from analytics.models import make_finding


def _make_minimal_data(transactions=None):
    """Build a minimal SAF-T-style data dict for testing."""
    return {
        "header": {
            "company_name": "Test Company",
            "currency": "DKK",
            "period": {
                "start": "1",
                "start_year": "2024",
                "end": "12",
                "end_year": "2024",
            },
        },
        "accounts": [
            {"account_id": "4000", "description": "Varekøb", "account_type": "Expense",
             "opening_balance": 0.0, "closing_balance": 0.0},
        ],
        "tax_table": [
            {"tax_code": "I25", "description": "Indgående 25%", "tax_percentage": 25.0},
            {"tax_code": "U25", "description": "Udgående 25%", "tax_percentage": 25.0},
        ],
        "transactions": transactions or [],
        "suppliers": [],
        "customers": [],
        "summary": {},
    }


class TestRunAllTests:
    """Tests for run_all_tests() return structure."""

    def test_returns_expected_top_level_keys(self):
        data = _make_minimal_data()
        result = run_all_tests(data)
        assert "overall_score" in result
        assert "categories" in result
        assert "all_findings" in result
        assert "total_findings" in result
        assert "severity_summary" in result
        assert "impact_summary" in result

    def test_returns_all_12_categories(self):
        data = _make_minimal_data()
        result = run_all_tests(data)
        assert len(result["categories"]) == 12

    def test_empty_data_returns_no_findings(self):
        data = _make_minimal_data()
        result = run_all_tests(data)
        assert result["total_findings"] == 0
        assert len(result["all_findings"]) == 0

    def test_overall_score_is_100_for_clean_data(self):
        data = _make_minimal_data()
        result = run_all_tests(data)
        assert result["overall_score"] == 100

    def test_severity_summary_has_all_levels(self):
        data = _make_minimal_data()
        result = run_all_tests(data)
        summary = result["severity_summary"]
        assert "critical" in summary
        assert "high" in summary
        assert "medium" in summary
        assert "low" in summary

    def test_with_transactions_produces_structured_output(self):
        txn = {
            "transaction_id": "T001",
            "date": "2024-01-15",
            "description": "Test purchase",
            "journal_id": "IMPORT",
            "period": "01",
            "period_year": "2024",
            "total_debit": 10000.0,
            "total_credit": 10000.0,
            "lines": [{
                "record_id": "L1",
                "account_id": "4000",
                "description": "Test",
                "debit_amount": 10000.0,
                "credit_amount": 0,
                "tax_code": "I25",
                "tax_percentage": 25.0,
                "tax_base": 10000.0,
                "tax_amount": 2500.0,
                "currency": "DKK",
                "supplier_id": "S001",
                "supplier_name": "Test Supplier",
                "customer_id": "",
                "customer_name": "",
                "source_document_id": "INV-001",
            }],
        }
        data = _make_minimal_data(transactions=[txn])
        result = run_all_tests(data)
        assert isinstance(result["all_findings"], list)
        assert isinstance(result["categories"], list)


class TestMakeFinding:
    """Tests for make_finding() helper."""

    def test_creates_valid_finding_dict(self):
        finding = make_finding(
            test_id=1,
            test_name="Test finding",
            impact_type="economic",
            direction="negative",
            severity="high",
            description="A test finding description",
        )
        assert finding["test_id"] == 1
        assert finding["test_name"] == "Test finding"
        assert finding["impact_type"] == "economic"
        assert finding["direction"] == "negative"
        assert finding["severity"] == "high"
        assert finding["description"] == "A test finding description"

    def test_default_values(self):
        finding = make_finding(
            test_id=1,
            test_name="Test",
            impact_type="compliance",
            direction="neutral",
            severity="low",
            description="Desc",
        )
        assert finding["fix_suggestion"] == ""
        assert finding["estimated_amount"] == 0.0
        assert finding["currency"] == "DKK"
        assert finding["transactions"] == []

    def test_custom_values(self):
        finding = make_finding(
            test_id=5,
            test_name="Custom test",
            impact_type="interest_risk",
            direction="positive",
            severity="critical",
            description="Critical finding",
            fix_suggestion="Fix this immediately",
            estimated_amount=50000.0,
            currency="EUR",
            transactions=[{"id": "T1"}],
        )
        assert finding["fix_suggestion"] == "Fix this immediately"
        assert finding["estimated_amount"] == 50000.0
        assert finding["currency"] == "EUR"
        assert len(finding["transactions"]) == 1

    def test_finding_has_all_required_keys(self):
        finding = make_finding(
            test_id=1,
            test_name="T",
            impact_type="economic",
            direction="negative",
            severity="medium",
            description="D",
        )
        required_keys = [
            "test_id", "test_name", "impact_type", "direction",
            "severity", "description", "fix_suggestion",
            "estimated_amount", "currency", "transactions",
        ]
        for key in required_keys:
            assert key in finding, f"Missing key: {key}"
