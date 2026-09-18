"""
generate_report.py (byggetrin ~9, Del C, Bal-godkendt 2026-09-17): HTML-
kundedialog-rapporten. Dækker: sektionsopbygning med og uden valgfrie data
(kontrol 82/afstemningsgate), aggregering pr. kontrol/konto (cap ved
_MAX_ACCOUNT_ROWS), HTML-escaping af kundedata, og CLI'en end-to-end.
"""

import json
import os

from tools import generate_report as gr


def _finding(test_id, test_name, severity, account_id=None, amount=0.0, direction="neutral",
             description="x"):
    txns = [{"account_id": account_id}] if account_id else []
    return {
        "test_id": test_id, "test_name": test_name, "impact_type": "economic",
        "direction": direction, "severity": severity, "description": description,
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
    """Kontrol 80 (momsbehandling_pr_konto) er tema-mappet, så fundet indgår i
    en observationsgruppe (sektion 4) — kontoen dukker op i evidenstabellen,
    og beskrivelsen dukker op i appendixets kontonavn-/testnavne-kolonner
    (``--appendix``). Begge veje skal escape kundedata."""
    findings = [_finding(80, "Fund <script>alert(1)</script>", "high",
                         account_id="<img src=x onerror=alert(1)>", amount=100.0,
                         description="<b>ond</b> beskrivelse")]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None, appendix=True)
    assert "<script>alert(1)</script>" not in out
    assert "<img src=x onerror=alert(1)>" not in out
    assert "<b>ond</b>" not in out
    assert "&lt;script&gt;" in out
    assert "&lt;b&gt;ond&lt;/b&gt;" in out


# --- Cap ved mange konti pr. kontrol -----------------------------------------

def test_account_rows_capped_with_more_note():
    findings = [_finding(80, "Indtægt uden momsbehandling", "medium",
                         account_id=f"K{i}", amount=100.0) for i in range(40)]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None, appendix=True)
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


# --- Redesign (byggetrin ~10): niveau-filtrering ----------------------------

def test_niveau_1_shows_only_hero_trust_data_lineage():
    out = gr.render_html(_wrap(_minimal_analytics()), None, niveau=1)
    assert "Jeres moms — set gennem data" in out
    assert "Afstemningen</h2>" in out
    assert "Datagrundlag &amp; metode" in out
    assert "Sporbarhed</h2>" in out
    assert "Momsmotoren</h2>" not in out
    assert "Observationer &amp; spørgsmål</h2>" not in out
    assert "Anbefalinger</h2>" not in out


def test_niveau_2_adds_engine_and_observations_but_not_recommendations():
    out = gr.render_html(_wrap(_minimal_analytics()), None, niveau=2)
    assert "Momsmotoren</h2>" in out
    assert "Observationer &amp; spørgsmål</h2>" in out
    assert "Anbefalinger</h2>" not in out


def test_niveau_3_shows_all_seven_sections():
    out = gr.render_html(_wrap(_minimal_analytics()), None, niveau=3)
    for heading in ("Jeres moms — set gennem data", "Momsmotoren</h2>", "Afstemningen</h2>",
                    "Observationer &amp; spørgsmål</h2>", "Datagrundlag &amp; metode",
                    "Anbefalinger</h2>", "Sporbarhed</h2>"):
        assert heading in out


def test_appendix_is_off_by_default_and_on_when_requested():
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=1000.0)]
    report = _wrap(_minimal_analytics(findings))
    without = gr.render_html(report, None)
    assert "Appendix: aggregeret fundoversigt" not in without
    with_appendix = gr.render_html(report, None, appendix=True)
    assert "Appendix: aggregeret fundoversigt" in with_appendix
    assert "Kontrol 80:" in with_appendix


def test_kontrolnumre_never_appear_outside_lineage_and_appendix():
    """Byggetrin ~10-disciplinen: sektion 1-6 må ALDRIG vise et kontrolnummer
    — kun sektion 7 (kompakt tema->kontrol-revisionsspor) og appendixet."""
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=600000.0)]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None, niveau=3, appendix=False)
    # Sektion 4/6 skal indeholde temaets NAVN, ikke kontrolnummeret, i selve teksten:
    assert "Momsbehandling pr. konto" in out
    before_footer = out.split("<h2>Sporbarhed</h2>")[0]
    assert "Kontrol 80" not in before_footer
    assert "kontrol 80" not in before_footer.lower()


# --- Redesign: 'Momsmotoren' (sektion 2) ------------------------------------

def test_engine_section_renders_tax_table_row():
    report = _wrap(_minimal_analytics())
    report["tax_table_oversigt"] = [{
        "tax_code": "DOMESTIC|REDUCED_PRIVATE_DKRC", "description": "Test", "tax_percentage": 25.0,
        "vat_calculation_type": "Reverse Charge VAT", "sales_vat_account": "961100",
        "purchase_vat_account": "963100", "reverse_charge_vat_account": "961400",
        "setup_matched": True,
    }]
    report["konto_navne"] = {"961100": "Udgående moms"}
    out = gr.render_html(report, None, niveau=2)
    assert "DOMESTIC|REDUCED_PRIVATE_DKRC" in out
    assert "Omvendt betalingspligt" in out
    assert "961100" in out and "Udgående moms" in out
    assert "Udgående moms (omvendt betalingspligt, indenlandsk)" in out


def test_engine_section_missing_data_is_graceful():
    out = gr.render_html(_wrap(_minimal_analytics()), None, niveau=2)
    assert "Momsmotoren</h2>" in out
    assert "Ingen detaljeret momsopsætning" in out


# --- Redesign: kuration/observationer (sektion 4) ---------------------------

def test_observations_section_uses_curation_group():
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=600000.0)]
    out = gr.render_html(_wrap(_minimal_analytics(findings)), None, niveau=3)
    assert "Momsbehandling pr. konto" in out
    assert "4000" in out  # evidens-konto
    assert "Vores anbefaling" in out


def test_observations_section_empty_without_findings():
    out = gr.render_html(_wrap(_minimal_analytics()), None, niveau=3)
    assert "Ingen kuraterede observationer" in out


def test_explicit_curation_overrides_seed_text():
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=600000.0)]
    analytics = _minimal_analytics(findings)
    curation = gr.report_curation.seed_curation(findings)
    curation["grupper"]["momsbehandling_pr_konto"]["spoergsmaal"] = "Rådgiverens eget spørgsmål?"
    out = gr.render_html(_wrap(analytics), None, niveau=3, curation=curation)
    assert "Rådgiverens eget spørgsmål?" in out


# --- Redesign: CLI med kuration/niveau/workbook -----------------------------

def test_cli_seeds_curation_file_when_missing(tmp_path):
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=600000.0)]
    report_path = tmp_path / "rapport.json"
    report_path.write_text(json.dumps(_wrap(_minimal_analytics(findings))), encoding="utf-8")
    curation_path = tmp_path / "kuration.json"
    rc = gr.main([str(report_path), "--out", str(tmp_path / "out.html"), "--curation", str(curation_path)])
    assert rc == 0
    assert curation_path.exists()
    data = json.loads(curation_path.read_text(encoding="utf-8"))
    assert "momsbehandling_pr_konto" in data["grupper"]
    assert data["grupper"]["momsbehandling_pr_konto"]["medtag"] is True


def test_cli_rerun_preserves_advisor_edit(tmp_path):
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=600000.0)]
    report_path = tmp_path / "rapport.json"
    report_path.write_text(json.dumps(_wrap(_minimal_analytics(findings))), encoding="utf-8")
    curation_path = tmp_path / "kuration.json"

    gr.main([str(report_path), "--out", str(tmp_path / "out1.html"), "--curation", str(curation_path)])
    data = json.loads(curation_path.read_text(encoding="utf-8"))
    data["grupper"]["momsbehandling_pr_konto"]["spoergsmaal"] = "Rådgiverens redigerede spørgsmål?"
    data["grupper"]["momsbehandling_pr_konto"]["medtag"] = True
    curation_path.write_text(json.dumps(data), encoding="utf-8")

    # Genkør på et NYT fund (samme tema) — den redigerede tekst skal overleve.
    findings2 = findings + [_finding(80, "Indtægt uden momsbehandling", "high",
                                      account_id="5000", amount=10000.0)]
    report_path.write_text(json.dumps(_wrap(_minimal_analytics(findings2))), encoding="utf-8")
    rc = gr.main([str(report_path), "--out", str(tmp_path / "out2.html"), "--curation", str(curation_path)])
    assert rc == 0

    data_after = json.loads(curation_path.read_text(encoding="utf-8"))
    grp = data_after["grupper"]["momsbehandling_pr_konto"]
    assert grp["spoergsmaal"] == "Rådgiverens redigerede spørgsmål?"
    assert grp["auto"]["fund_antal"] == 2  # auto-blokken ER opdateret

    out2 = (tmp_path / "out2.html").read_text(encoding="utf-8")
    assert "Rådgiverens redigerede spørgsmål?" in out2


def test_cli_niveau_1_excludes_observations(tmp_path):
    report_path = tmp_path / "rapport.json"
    report_path.write_text(json.dumps(_wrap(_minimal_analytics())), encoding="utf-8")
    out_path = tmp_path / "out.html"
    rc = gr.main([str(report_path), "--out", str(out_path), "--niveau", "1"])
    assert rc == 0
    content = out_path.read_text(encoding="utf-8")
    assert "Observationer &amp; spørgsmål</h2>" not in content


def test_cli_writes_workbook(tmp_path):
    findings = [_finding(80, "Indtægt uden momsbehandling", "high", account_id="4000", amount=1000.0)]
    report_path = tmp_path / "rapport.json"
    report_path.write_text(json.dumps(_wrap(_minimal_analytics(findings))), encoding="utf-8")
    workbook_path = tmp_path / "arbejdsbilag.xlsx"
    rc = gr.main([str(report_path), "--out", str(tmp_path / "out.html"),
                  "--workbook", str(workbook_path)])
    assert rc == 0
    assert workbook_path.exists()
    assert workbook_path.stat().st_size > 0
