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


class TestImportContractExpansion:
    """Fase D: de nye aliaser (Data Extract-navne) + felter (ship_from/ship_to,
    adskilt faktura-/bogføringsdato, importeret tax_base) skal flyde igennem."""

    def _xlsx(self, tmp_path, headers, rows):
        import openpyxl
        path = str(tmp_path / "import.xlsx")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(headers)
        for r in rows:
            ws.append(r)
        wb.save(path)
        return path

    def test_data_extract_aliases_map(self, tmp_path):
        path = self._xlsx(
            tmp_path,
            ["transaction_id", "posting_date", "accountNumber", "vatCode",
             "vat_amount_lcy", "counterparty_country", "customerNumber",
             "vat_registration_no"],
            [["T1", "2024-01-15", "4000", "I25", 2500.0, "DE", "K1", "DE123456789"]],
        )
        cm = parse_excel(path)["parse_info"]["detected_columns"]
        assert cm.get("account_id") == "accountNumber"
        assert cm.get("vat_code") == "vatCode"
        assert cm.get("vat_amount") == "vat_amount_lcy"
        assert cm.get("country") == "counterparty_country"
        assert cm.get("customer_id") == "customerNumber"
        assert cm.get("vat_number") == "vat_registration_no"

    def test_ship_and_separate_document_date(self, tmp_path):
        path = self._xlsx(
            tmp_path,
            ["Bilagsnr", "Bogføringsdato", "Fakturadato", "Konto", "Debet",
             "Kredit", "ship_from_country", "ship_to_country"],
            [["B1", "2024-03-10", "2024-02-25", "4000", 1000.0, 0, "DK", "DE"]],
        )
        res = parse_excel(path)
        cm = res["parse_info"]["detected_columns"]
        assert cm.get("date") == "Bogføringsdato"          # bogføring foretrækkes som primær dato
        assert cm.get("document_date") == "Fakturadato"     # faktura adskilt
        assert cm.get("ship_from_country") == "ship_from_country"
        assert cm.get("ship_to_country") == "ship_to_country"
        txn = res["transactions"][0]
        assert txn["date"] == "2024-03-10"
        assert txn["document_date"] == "2024-02-25"
        assert txn["ship_from_country"] == "DK"
        assert txn["ship_to_country"] == "DE"

    def test_adapter_carries_new_fields_and_imported_tax_base(self, tmp_path):
        from parsers.data_adapter import adapt_excel_to_saft
        path = self._xlsx(
            tmp_path,
            ["Bilagsnr", "Dato", "Konto", "Debet", "Moms", "Momssats",
             "tax_base", "ship_from_country", "ship_to_country", "Fakturadato"],
            [["B1", "2024-03-10", "4000", 1000.0, 250.0, 25.0, 4242.0, "DK", "SE", "2024-03-01"]],
        )
        adapted = adapt_excel_to_saft(parse_excel(path))
        txn = adapted["transactions"][0]
        line = txn["lines"][0]
        assert line["ship_from_country"] == "DK"
        assert line["ship_to_country"] == "SE"
        # Importeret momsgrundlag vinder over det udledte (250/0.25 = 1000).
        assert line["tax_base"] == 4242.0
        assert txn["document_date"] == "2024-03-01"
