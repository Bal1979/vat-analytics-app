"""
Tests for the Excel/CSV parser.
"""

import os
import sys
import pytest

# Ensure the backend package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from parsers.excel_parser import parse_excel, _detect_columns
import pandas as pd


class TestParseExcel:
    """Tests for parse_excel() with a real Excel file."""

    def test_parse_returns_expected_keys(self, test_excel_path):
        result = parse_excel(test_excel_path)
        assert "header" in result
        assert "accounts" in result
        assert "tax_table" in result
        assert "transactions" in result
        assert "suppliers" in result
        assert "customers" in result
        assert "parse_info" in result

    def test_parse_returns_correct_row_count(self, test_excel_path):
        result = parse_excel(test_excel_path)
        assert result["parse_info"]["rows"] == 8

    def test_parse_detects_transactions(self, test_excel_path):
        result = parse_excel(test_excel_path)
        assert len(result["transactions"]) == 8

    def test_parse_extracts_amounts(self, test_excel_path):
        result = parse_excel(test_excel_path)
        first_txn = result["transactions"][0]
        assert first_txn["debit_amount"] == 10000.00
        assert first_txn["credit_amount"] == 0

    def test_parse_extracts_dates(self, test_excel_path):
        result = parse_excel(test_excel_path)
        first_txn = result["transactions"][0]
        assert first_txn["date"] == "2024-01-15"

    def test_parse_detects_suppliers(self, test_excel_path):
        result = parse_excel(test_excel_path)
        supplier_ids = [s["supplier_id"] for s in result["suppliers"]]
        assert "L001" in supplier_ids
        assert "L002" in supplier_ids


class TestColumnAutoDetectionDanish:
    """Tests for auto-detection of Danish column names."""

    def test_detects_danish_bilagsnr(self, test_excel_path):
        result = parse_excel(test_excel_path)
        col_map = result["parse_info"]["detected_columns"]
        assert "transaction_id" in col_map
        assert col_map["transaction_id"] == "Bilagsnr"

    def test_detects_danish_dato(self, test_excel_path):
        result = parse_excel(test_excel_path)
        col_map = result["parse_info"]["detected_columns"]
        assert "date" in col_map
        assert col_map["date"] == "Dato"

    def test_detects_danish_debet_kredit(self, test_excel_path):
        result = parse_excel(test_excel_path)
        col_map = result["parse_info"]["detected_columns"]
        assert "debit" in col_map
        assert "credit" in col_map

    def test_detects_danish_moms(self, test_excel_path):
        result = parse_excel(test_excel_path)
        col_map = result["parse_info"]["detected_columns"]
        assert "vat_amount" in col_map
        assert "vat_code" in col_map


class TestColumnAutoDetectionEnglish:
    """Tests for auto-detection of English column names."""

    def test_detects_english_transaction_id(self, english_columns_excel):
        result = parse_excel(english_columns_excel)
        col_map = result["parse_info"]["detected_columns"]
        assert "transaction_id" in col_map
        assert col_map["transaction_id"] == "transaction_id"

    def test_detects_english_date(self, english_columns_excel):
        result = parse_excel(english_columns_excel)
        col_map = result["parse_info"]["detected_columns"]
        assert "date" in col_map
        assert col_map["date"] == "date"

    def test_detects_english_amounts(self, english_columns_excel):
        result = parse_excel(english_columns_excel)
        col_map = result["parse_info"]["detected_columns"]
        assert "debit" in col_map
        assert "credit" in col_map

    def test_detects_english_vat_fields(self, english_columns_excel):
        result = parse_excel(english_columns_excel)
        col_map = result["parse_info"]["detected_columns"]
        assert "vat_amount" in col_map
        assert "vat_code" in col_map
        assert "vat_rate" in col_map


class TestEmptyFile:
    """Tests for handling empty files."""

    def test_empty_excel_returns_no_transactions(self, empty_excel_path):
        result = parse_excel(empty_excel_path)
        assert len(result["transactions"]) == 0

    def test_empty_excel_has_zero_rows(self, empty_excel_path):
        result = parse_excel(empty_excel_path)
        # Either parse_info has rows=0 or an error message
        rows = result["parse_info"].get("rows", 0)
        error = result["parse_info"].get("error")
        assert rows == 0 or error is not None
