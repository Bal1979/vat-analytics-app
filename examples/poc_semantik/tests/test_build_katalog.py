"""Tests for build_katalog.py mod en syntetisk (opdigtet) Excel-fixture.

Ingen kundedata anvendes — fund-tekster er fiktive.
"""
import openpyxl

from build_katalog import CROSS_LINE_FUNDS, load_fundkatalog, load_generelle_regler

FUNDKATALOG_HEADER = [
    "Fund-ID", "Fund", "Status", "Risikoniveau", "Antal linjer", "Momsrisiko",
    "Momsmulighed", "Rubrikfejl (0 kr.)", "Elafgift", "Lovgrundlag", "Beskrivelse",
    "Sådan er kontrollen udført",
]


def _write_fundkatalog(path, rows, footer_text=None):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Fundkatalog"
    ws.append(["EY", "Fundkatalog"])
    ws.append([None, "Alle udførte kontroller"])
    ws.append([])
    ws.append(FUNDKATALOG_HEADER)
    for row in rows:
        ws.append(row)
    ws.append([None] * len(FUNDKATALOG_HEADER))
    if footer_text:
        ws.append([footer_text] + [None] * (len(FUNDKATALOG_HEADER) - 1))

    metode_ws = wb.create_sheet("Metode og forudsætninger")
    metode_ws.append(["EY", "Metode og forudsætninger"])
    metode_ws.append(["Fremgangsmåde, aftalte forudsætninger"])
    metode_ws.append([])
    metode_ws.append(["1. Datagrundlag"])
    metode_ws.append(["Testregel om fiktiv 60/40-fordeling og netting."])
    wb.save(path)


def test_load_fundkatalog_parses_basic_fields(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(xlsx, [
        ["F01", "Fiktivt fund et", "Fund", "Høj", 1, 100, 0, 0, 0, "ML § 1", "Beskrivelse et", "Metode et"],
    ])
    funds = load_fundkatalog(xlsx)
    assert len(funds) == 1
    f = funds[0]
    assert f["id"] == "F01"
    assert f["navn"] == "Fiktivt fund et"
    assert f["niveauer"] == ["Høj"]
    assert "Beskrivelse et" in f["regel_kort"]
    assert "ML § 1" in f["regel_kort"]
    assert f["identifikationsmetode"] == "Metode et"


def test_duplicate_fund_id_merges_niveauer(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(xlsx, [
        ["F16", "Fiktiv gruppefejl", "Fund", "Lav", 4, 0, 0, 0, 0, "ML § 2", "Beskrivelse", "Metode"],
        ["F16", "Fiktiv gruppefejl", "Fund", "Rubrikfejl", 4, 0, 0, 100, 0, "ML § 2", "Beskrivelse", "Metode"],
    ])
    funds = load_fundkatalog(xlsx)
    assert len(funds) == 1
    assert funds[0]["niveauer"] == ["Lav", "Rubrikfejl"]


def test_footer_free_text_row_is_not_parsed_as_fund(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(
        xlsx,
        [["F01", "Fiktivt fund", "Fund", "Lav", 1, 0, 0, 0, 0, "ML § 1", "Besk", "Metode"]],
        footer_text="Denne note beskriver kontrollernes omfang og skal ikke tolkes som et fund-id.",
    )
    funds = load_fundkatalog(xlsx)
    assert len(funds) == 1
    assert all(f["id"] == "F01" for f in funds)


def test_ingen_fund_status_preserved(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(xlsx, [
        ["F24", "Fiktiv kontrol uden fund", "Ingen fund", None, 0, 0, 0, 0, 0, None, "Beskrivelse", "Metode"],
    ])
    funds = load_fundkatalog(xlsx)
    assert funds[0]["status"] == "Ingen fund"


def test_cross_line_funds_flagged_by_constant(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(xlsx, [
        ["F01", "Fiktiv dublet", "Fund", "Middel", 2, 0, 0, 0, 0, "ML § 1", "Besk", "Metode"],
        ["F02", "Fiktivt andet fund", "Fund", "Høj", 1, 0, 0, 0, 0, "ML § 2", "Besk", "Metode"],
    ])
    funds = load_fundkatalog(xlsx)
    by_id = {f["id"]: f for f in funds}
    assert by_id["F01"]["kraever_tvaerlinje_kontekst"] is True
    assert by_id["F01"]["tvaerlinje_begrundelse"] == CROSS_LINE_FUNDS["F01"]
    assert by_id["F02"]["kraever_tvaerlinje_kontekst"] is False
    assert by_id["F02"]["tvaerlinje_begrundelse"] is None


def test_load_generelle_regler_skips_title_rows(tmp_path):
    xlsx = tmp_path / "fixture.xlsx"
    _write_fundkatalog(xlsx, [
        ["F01", "Fiktivt fund", "Fund", "Lav", 1, 0, 0, 0, 0, "ML § 1", "Besk", "Metode"],
    ])
    regler = load_generelle_regler(xlsx)
    assert "EY" not in regler
    assert any("60/40" in r for r in regler)
