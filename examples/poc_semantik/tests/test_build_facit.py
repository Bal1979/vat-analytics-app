"""Tests for build_facit.py mod en syntetisk (opdigtet) Excel-fixture.

Ingen kundedata anvendes — alle vaerdier er fiktive og opfundet til testen.
"""
import datetime

import openpyxl

from build_facit import COLUMN_MAP, build_meta, load_lines

HEADER = list(COLUMN_MAP.keys())


def _write_fixture(path, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Alle linjer"
    ws.append(HEADER)
    for row in rows:
        ws.append(row)
    wb.save(path)


def _row(**overrides):
    base = {
        "Bilagsnr.": "9000001",
        "Posteringsdato": datetime.datetime(2025, 1, 15),
        "Momsdato": datetime.datetime(2025, 1, 15),
        "Konto": "100000",
        "Kontonavn": "Testkonto",
        "Posteringstekst": "Fiktiv postering",
        "Leverandør": "Fiktiv Leverandoer ApS",
        "Eksternt bilagsnr.": "EXT-1",
        "Valuta": "",
        "Beløb ekskl. moms": 1000.0,
        "Momsbeløb": 250.0,
        "Ikke-fradragsber. moms": 0,
        "Effektiv momssats": 0.25,
        "Momsvirksomhedsgruppe": "DOMESTIC",
        "Momsvaregruppe": "STANDARD_VAT",
        "Risikoniveau": "OK",
        "Fund-ID": "",
        "Kategori": "",
        "Lovgrundlag": "",
        "Kommentar": "Fiktiv begrundelse",
        "Anbefalet handling": "",
    }
    base.update(overrides)
    return [base[h] for h in HEADER]


def test_load_lines_defaults_blank_fund_id_to_ok(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row()])
    lines = load_lines(xlsx)
    assert len(lines) == 1
    assert lines[0]["fund_id"] == "OK"


def test_load_lines_keeps_explicit_fund_id(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(**{"Fund-ID": "F02", "Risikoniveau": "Høj"})])
    lines = load_lines(xlsx)
    assert lines[0]["fund_id"] == "F02"
    assert lines[0]["risk_level"] == "Høj"


def test_vat_code_is_composed_from_both_groups(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(**{"Momsvirksomhedsgruppe": "EU", "Momsvaregruppe": "REDUCED_REP_VAT"})])
    lines = load_lines(xlsx)
    assert lines[0]["vat_code"] == "EU/REDUCED_REP_VAT"


def test_moms_fratrukket_false_when_vat_amount_zero(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(**{"Momsbeløb": 0})])
    lines = load_lines(xlsx)
    assert lines[0]["moms_fratrukket"] is False


def test_moms_fratrukket_true_when_vat_amount_nonzero(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(**{"Momsbeløb": 42.0})])
    lines = load_lines(xlsx)
    assert lines[0]["moms_fratrukket"] is True


def test_line_ids_are_stable_and_track_excel_row(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(), _row()])
    lines = load_lines(xlsx)
    assert lines[0]["line_id"] == "L0002"  # header er raekke 1
    assert lines[1]["line_id"] == "L0003"


def test_blank_rows_are_skipped(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row(), [None] * len(HEADER), _row()])
    lines = load_lines(xlsx)
    assert len(lines) == 2


def test_dates_serialize_to_iso_strings(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [_row()])
    lines = load_lines(xlsx)
    assert lines[0]["posting_date"] == "2025-01-15T00:00:00"


def test_build_meta_computes_control_totals_and_distribution(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fixture(xlsx, [
        _row(**{"Fund-ID": "F02", "Beløb ekskl. moms": 100.0, "Momsbeløb": 25.0}),
        _row(**{"Beløb ekskl. moms": 200.0, "Momsbeløb": 50.0}),
    ])
    lines = load_lines(xlsx)
    meta = build_meta(lines, xlsx)
    assert meta["total_lines"] == 2
    assert meta["fund_distribution"] == {"OK": 1, "F02": 1}
    assert meta["kontrolsummer"]["beloeb_ekskl_moms"] == 300.0
    assert meta["kontrolsummer"]["momsbeloeb"] == 75.0
