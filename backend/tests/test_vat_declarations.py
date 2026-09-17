"""
vat_declarations.py (byggetrin 8, Del A, Bal-godkendt 2026-09-17).

Dækker: gyldig fil, manglende fil, ugyldig JSON, forkert/manglende
declarations_version, manglende 'periods'-liste, manglende periode-felter —
load_declarations() må ALDRIG kaste en exception.
"""

import json

from analytics import vat_declarations as decl


def _write(tmp_path, doc, name="vat_declarations.json"):
    path = tmp_path / name
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path)


_VALID_DOC = {
    "declarations_version": "1.0.0",
    "source": "test",
    "generated": "2026-09-17T00:00:00",
    "periods": [
        {"period": "2025-01", "output_vat": 100.0, "input_vat": 50.0,
         "rc_services": 0.0, "rc_goods": 0.0, "energy_taxes": 0.0, "total": 50.0},
    ],
}


def test_missing_file_returns_error_not_exception(tmp_path):
    doc, error = decl.load_declarations(str(tmp_path / "does_not_exist.json"))
    assert doc is None
    assert error


def test_valid_declarations_file_loads(tmp_path):
    path = _write(tmp_path, _VALID_DOC)
    doc, error = decl.load_declarations(path)
    assert error is None
    assert doc["periods"][0]["period"] == "2025-01"


def test_invalid_json_returns_error(tmp_path):
    path = tmp_path / "vat_declarations.json"
    path.write_text("{not valid json", encoding="utf-8")
    doc, error = decl.load_declarations(str(path))
    assert doc is None
    assert "JSON" in error


def test_wrong_version_rejected(tmp_path):
    bad = {**_VALID_DOC, "declarations_version": "2.0.0"}
    path = _write(tmp_path, bad)
    doc, error = decl.load_declarations(path)
    assert doc is None
    assert "declarations_version" in error


def test_missing_version_rejected(tmp_path):
    bad = dict(_VALID_DOC)
    del bad["declarations_version"]
    path = _write(tmp_path, bad)
    doc, error = decl.load_declarations(path)
    assert doc is None


def test_missing_periods_list_rejected(tmp_path):
    bad = dict(_VALID_DOC)
    del bad["periods"]
    path = _write(tmp_path, bad)
    doc, error = decl.load_declarations(path)
    assert doc is None
    assert "periods" in error


def test_period_missing_required_field_rejected(tmp_path):
    bad_period = dict(_VALID_DOC["periods"][0])
    del bad_period["rc_goods"]
    bad = {**_VALID_DOC, "periods": [bad_period]}
    path = _write(tmp_path, bad)
    doc, error = decl.load_declarations(path)
    assert doc is None
    assert "rc_goods" in error


def test_period_with_empty_period_key_rejected(tmp_path):
    bad_period = {**_VALID_DOC["periods"][0], "period": ""}
    bad = {**_VALID_DOC, "periods": [bad_period]}
    path = _write(tmp_path, bad)
    doc, error = decl.load_declarations(path)
    assert doc is None


def test_top_level_not_an_object_rejected(tmp_path):
    path = tmp_path / "vat_declarations.json"
    path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    doc, error = decl.load_declarations(str(path))
    assert doc is None
    assert "JSON-objekt" in error


def test_empty_periods_list_is_valid():
    """En tom 'periods'-liste er strukturelt gyldig (kontrol 82 springer så
    bare over -- ingen perioder at afstemme mod)."""
    import os
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "vat_declarations.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({**_VALID_DOC, "periods": []}, f)
        doc, error = decl.load_declarations(path)
        assert error is None
        assert doc["periods"] == []
