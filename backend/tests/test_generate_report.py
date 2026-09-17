"""
generate_report.py (byggetrin ~9, Del C, Bal-godkendt 2026-09-17): HTML-
kundedialog-rapporten. Dækker: sektionsopbygning med og uden valgfrie data
(kontrol 82/afstemningsgate), aggregering pr. kontrol/konto (cap ved
_MAX_ACCOUNT_ROWS), HTML-escaping af kundedata, og CLI'en end-to-end.
"""

import json
import os

from tools import generate_report as gr


def _finding(test_id, test_name, severity, account_id=None, amount=0.0, direction="neutral"):
    txns = [{"account_id": account_id}] if account_id else []
    return {
        "test_id": test_id, "test_name": test_name, "impact_type": "economic",
        "direction": direction, "severity": severity, "description": "x",
        "fix_suggestion": "", "estimated_amount": amount, "currency": "DKK",
        "transactions": txns,
    }


def _minimal_analytics(findings=None):
    findings = findings or []
    return {
        "overall_score": 80,
        "summary": {"total_transactions": 10, "total_debit": 1000.0, "total_credit": 1000.0,
                    "total_vat": 0.0, "currency": "DKK", "period_start": "2024-01-01",
                    "period_end": "2024-12-31"},
        "impact_summary": {},
        "categories": [],
        "all_findings": findings,
        "total_findings": len(findings),
        "severity_summary": {
            "critical": sum(1 for f in findings if f["severity"] == "critical"),
            "high": sum(1 for f in findings if f["severity"] == "high"),
            "medium": sum(1 for f in findings if f["severity"] == "medium"),
            "low": sum(1 for f in findings if f["severity"] == "low"),
        },
        "moduler": [], "filtrerede_fund": 0, "ikke_maalbare_fund_fjernet": 0,
        "datagrundlag": {"opsummering": {"koert": 1, "i_alt": 103, "ikke_maalbar": 0,
                                          "modul_fra": 0, "kraever_eksterne_data": 0},
                          "kontroller": [], "delkontrol_gates": []},
        "declaration_reconciliation": None,
    }


def _wrap(analytics, **overrides):
    report = {
        "kanonisk_vej": True,
        "lineage": {"catalog_version": "1.2.0", "data_contract_version": "0.3.1",
                    "mapping_version": "1.0.0", "schema_fingerprint": "sha256:abc",
                    "source_erp": "Test ERP", "generated_at": "2026-09-17T10:00:00+00:00"},
        "afstemning": {},
        "angivelse": {"brugt": False, "antal_perioder": 0},
        "parse_info": {"warnings": [], "sections": {}, "stamdata": {}},
        "analytics": analytics,
        "koeretid_sekunder": {},
    }
    report.update(overrides)
    return report


# --- aggregate_findings_by_control ------------------------------------------

def test_aggregate_groups_by_test_id_and_account():
    findings = [
        _finding(80, "Indtægt uden momsbehandling", "medium", account_id="4000", amount=1000.0),
        _finding(80, "Indtægt uden momsbehandling", "medium", account_id="4000", amount=500.0),
        _finding(80, "Indtægt uden momsbehandling", "medium", account_id="5000", amount=200.0),
    ]
    controls = gr.aggregate_findings_by_control(findings)
    assert len(controls) == 1
    c = controls[0]
    assert c["test_id"] == 80
    assert c["findings"] == 3
    assert c["amount"] == 1700.0
    accounts = dict(c["accounts"])
    assert accounts["4000"] == {"count": 2, "amount": 1500.0}
    assert accounts["5000"] == {"count": 1, "amount": 200.0}


def test_aggregate_sorts_controls_by_worst_severity_then_amount():
    findings = [
        _finding(1, "Lav kontrol", "low", amount=10.0),
        _finding(2, "Kritisk kontrol", "critical", amount=1.0),
        _finding(3, "Høj kontrol", "high", amount=5.0),
    ]
    controls = gr.aggregate_findings_by_control(findings)
    assert [c["test_id"] for c in controls] == [2, 3, 1]


def test_finding_without_account_falls_back_to_placeholder():
    findings = [_finding(76, "Forhold", "medium")]
    controls = gr.aggregate_findings_by_control(findings)
    accounts = dict(controls[0]["accounts"])
    assert "(ingen kontoreference)" in accounts


# --- render_html: robusthed uden valgfrie sektioner --------------------------

def test_render_html_without_declaration_reconciliation():
    report = _wrap(_minimal_analytics())
    out = gr.render_html(report, None)
    assert "Ingen momsangivelse var tilgængelig" in out
    assert "<html" in out and "</html>" in out


def test_render_html_without_afstemning():
    report = _wrap(_minimal_analytics())
    out = gr.render_html(report, None)
    assert "Ingen afstemningsgate-kørsel" in out


def test_render_html_shows_afstemning_when_present():
    report = _wrap(_minimal_analytics(), afstemning={
        "gate_status": "afstemt", "message": "Afstemt: alle 5 konti matcher.",
        "tolerance": 0.01, "summary": {"total_accounts_checked": 5, "reconciled_count": 5,
                                        "not_reconciled_count": 0},
    })
    out = gr.render_html(report, None)
    assert "AFSTEMT" in out
    assert "5 af 5 konti afstemt" in out


def test_render_html_flags_unbalanced_debit_credit():
    analytics = _minimal_analytics()
    analytics["summary"]["total_debit"] = 1000.0
    analytics["summary"]["total_credit"] = 900.0
    out = gr.render_html(_wrap(analytics), None)
    assert "BALANCERER IKKE" in out


# --- HTML-escaping af kundedata -----------------------------------------------

def test_account_id_and_test_name_are_html_escaped():
    findings = [_finding(80, "Fund <script>alert(1)</script>", "high",
                         account_id="<img src=x onerror=alert(1)>", amount=100.0)]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None)
    assert "<script>alert(1)</script>" not in out
    assert "<img src=x onerror=alert(1)>" not in out
    assert "&lt;script&gt;" in out


# --- Cap ved mange konti pr. kontrol -----------------------------------------

def test_account_rows_capped_with_more_note():
    findings = [_finding(80, "Indtægt uden momsbehandling", "medium",
                         account_id=f"K{i}", amount=100.0) for i in range(40)]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None)
    assert "…og 15 flere konti." in out


# --- Talformattering -----------------------------------------------------------

def test_fmt_amount_danish_style():
    assert gr.fmt_amount(1234567.891) == "1.234.567,89 DKK"
    assert gr.fmt_amount(-500.5) == "-500,50 DKK"
    assert gr.fmt_amount(None) == "–"


def test_fmt_int_danish_style():
    assert gr.fmt_int(1234567) == "1.234.567"
    assert gr.fmt_int(None) == "–"


# --- CLI end-to-end -----------------------------------------------------------

def test_cli_writes_html_file(tmp_path):
    report_path = tmp_path / "rapport.json"
    report_path.write_text(json.dumps(_wrap(_minimal_analytics())), encoding="utf-8")
    out_path = tmp_path / "ud" / "rapport.html"
    rc = gr.main([str(report_path), "--out", str(out_path)])
    assert rc == 0
    assert out_path.exists()
    content = out_path.read_text(encoding="utf-8")
    assert "<html" in content
    assert "BALAI" in content


def test_cli_missing_input_file_returns_error(tmp_path, capsys):
    rc = gr.main([str(tmp_path / "does_not_exist.json"), "--out", str(tmp_path / "out.html")])
    assert rc == 1
    captured = capsys.readouterr()
    assert "FEJL" in captured.err
