#!/usr/bin/env python3
"""
generate_report.py — kundedialog-laget (byggetrin ~9, Del C,
BALAI-dataflow-arkitektur.md §2a+§7 punkt 8a/8b, Bal-godkendt 2026-09-17).

Bygger ÉN selvbærende HTML-fil (inline CSS, INGEN eksterne afhængigheder/CDN
— den skal kunne mailes og åbnes offline) fra en analyserapport-JSON
(``tools/analyze_canonical.py``s output, eller webappens tilsvarende
``analytics.engine.run_all_tests``-rapport). Dansk UI, BALAI-designsprog
(farver/typografi genbrugt fra ``static/style.css`` — IKKE noget EY-agtigt).

Rapporten er bevidst et KUNDEDIALOG-lag, ikke en rå fund-dump:
  1. Tillidsanker øverst: kontrol 82-afstemningen (12 måneder x rubrik),
     afstemningsgatens resultat, og debet==kredit-totalerne.
  2. Ledelsesresumé: fund pr. severity, transaktioner/bilag, analyseperiode.
  3. Aggregerede fundtabeller: pr. kontrol -> pr. konto, sorteret efter
     væsentlighed, cap'et (aldrig rå fund-dumps).
  4. Datagrundlag & metode: ikke-målbare kontroller, relevante known_gaps,
     lineage-footer + AI-provenance-note (§2a: analysen er 100 %
     deterministisk; AI bruges kun til mapping-forslag, menneske-godkendt og
     frosset).
  5. Print-CSS til PDF/print.

Kør (fra backend/):
    python tools/generate_report.py <rapport.json> --out <fil.html>
    # eller, som modul:
    python -m tools.generate_report <rapport.json> --out <fil.html>

INGEN kundedata logges til stdout her — kun nøgletal og filstier. Selve
HTML-filen KAN indeholde kundedata (kontonumre, beløb, beskrivelser fra
bogføringen, hentet direkte fra rapport-JSON'en) — del/gem den derfor med
samme forsigtighed som selve rapport-JSON'en. Teknisk fejl (manglende felt,
tomt datagrundlag) må aldrig fremstå som et fagligt resultat — samme
disciplin som resten af den kanoniske vej: manglende sektioner vises som
"ikke tilgængelig i denne rapport", kaster aldrig en exception.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
from collections import defaultdict

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
_DATA_CONTRACT_PATH = os.path.join(_BACKEND, "catalog", "data_contract.json")

_MAX_ACCOUNT_ROWS = 25       # cap pr. kontrol-tabel (Del C, punkt 3)
_MAX_CONTROLS_SHOWN = 60     # cap på antal kontrol-sektioner (undgår en side på 100+ tabeller)
_MAX_KNOWN_GAPS_SHOWN = 6

_SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
_SEVERITY_LABELS = {"critical": "Kritisk", "high": "Høj", "medium": "Medium", "low": "Lav"}
_SEVERITY_COLORS = {
    "critical": ("#f8d7da", "#721c24", "#dc3545"),
    "high": ("#fff3cd", "#856404", "#ffc107"),
    "medium": ("#d1ecf1", "#0c5460", "#17a2b8"),
    "low": ("#e2e3e5", "#383d41", "#6c757d"),
}

_RUBRIC_STATUS_LABELS = {
    "match": "Match",
    "timing": "Timing",
    "afvigelse": "Afvigelse",
    "ingen_angivelse": "Ingen angivelse",
}


# --- Hjælpere ------------------------------------------------------------

def _esc(value) -> str:
    """HTML-escape enhver værdi (kundedata kan indeholde <, >, & m.v.)."""
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


def fmt_amount(value, currency: str = "DKK") -> str:
    """Dansk talformat (punktum som tusindtalsseparator, komma som decimal)."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "–"
    s = f"{abs(v):,.2f}"
    s = s.replace(",", "§").replace(".", ",").replace("§", ".")
    sign = "-" if v < 0 else ""
    return f"{sign}{s} {currency}".strip()


def fmt_int(value) -> str:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return "–"
    return f"{v:,}".replace(",", ".")


def fmt_pct(value, decimals: int = 0) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "–"
    return f"{v:.{decimals}f}%".replace(".", ",")


def load_report(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _analytics(report: dict) -> dict:
    """Rapporten kan enten være analyze_canonical.py's indpakkede form
    (``{"analytics": {...}, "lineage": {...}, ...}``) eller selve
    analytics-rapporten direkte (webappens/engine.run_all_tests' output).
    Tolerant over for begge — best-effort, aldrig en KeyError."""
    if isinstance(report.get("analytics"), dict):
        return report["analytics"]
    return report


def _load_data_contract() -> dict | None:
    """Best-effort: known_gaps-teksten hentes fra catalog/data_contract.json
    ved siden af scriptet. Findes filen ikke (fx kørt uden for repoet),
    springes sektionen bare over — ingen exception."""
    try:
        with open(_DATA_CONTRACT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


# --- Sektion 1: Tillidsanker ----------------------------------------------

def _rubric_status_badge(status: str) -> str:
    colors = {
        "match": ("#d4edda", "#155724"),
        "timing": ("#d1ecf1", "#0c5460"),
        "afvigelse": ("#f8d7da", "#721c24"),
        "ingen_angivelse": ("#e2e3e5", "#383d41"),
    }
    bg, fg = colors.get(status, ("#e2e3e5", "#383d41"))
    label = _RUBRIC_STATUS_LABELS.get(status, status)
    return f'<span class="status-badge" style="background:{bg};color:{fg};">{_esc(label)}</span>'


def build_trust_anchor(report: dict, analytics: dict, currency: str) -> str:
    parts = ['<section class="trust-anchor">', "<h2>Tillidsanker: afstemning og totalkontrol</h2>"]

    # --- Kontrol 82: periode-/rubrikafstemning -------------------------------
    decl = analytics.get("declaration_reconciliation")
    if decl and decl.get("perioder"):
        labels = decl.get("rubrik_labels", {})
        rubrics = list(labels.keys()) or ["output_vat", "rc_services", "input_vat"]
        parts.append('<h3>Momsangivelse pr. periode (kontrol 82)</h3>')
        parts.append('<div class="table-scroll"><table class="recon-table"><thead><tr><th>Periode</th>')
        for r in rubrics:
            parts.append(f"<th colspan='4'>{_esc(labels.get(r, r))}</th>")
        parts.append("</tr><tr><th></th>")
        for _ in rubrics:
            parts.append("<th>Beregnet</th><th>Angivet</th><th>Diff.</th><th>Status</th>")
        parts.append("</tr></thead><tbody>")
        for p in decl["perioder"]:
            parts.append(f"<tr><td>{_esc(p['periode'])}</td>")
            for r in rubrics:
                cell = p["rubrikker"].get(r, {})
                beregnet = cell.get("beregnet")
                angivet = cell.get("angivet")
                diff = cell.get("difference")
                status = cell.get("status", "ingen_angivelse")
                parts.append(
                    f"<td>{fmt_amount(beregnet, currency) if beregnet is not None else '–'}</td>"
                    f"<td>{fmt_amount(angivet, currency) if angivet is not None else '–'}</td>"
                    f"<td>{fmt_amount(diff, currency) if diff is not None else '–'}</td>"
                    f"<td>{_rubric_status_badge(status)}</td>"
                )
            parts.append("</tr>")
        parts.append("</tbody>")
        if decl.get("aarstotaler"):
            parts.append("<tfoot><tr><td><strong>Årstotal</strong></td>")
            for r in rubrics:
                at = decl["aarstotaler"].get(r, {})
                parts.append(
                    f"<td><strong>{fmt_amount(at.get('beregnet'), currency)}</strong></td>"
                    f"<td><strong>{fmt_amount(at.get('angivet'), currency)}</strong></td>"
                    f"<td><strong>{fmt_amount(at.get('difference'), currency)}</strong></td>"
                    f"<td></td>"
                )
            parts.append("</tr></tfoot>")
        parts.append("</table></div>")
        parts.append(
            '<p class="anchor-note">Udgående moms og RC-ydelser er GRØNNE (match), når de bogførte '
            "beløb stemmer med den indberettede momsangivelse for perioden. Købsmoms angives på "
            "settlement-basis i angivelsen, mens beregningen her følger bogføringsperioden — en "
            'difference her markeret <span class="status-badge" style="background:#d1ecf1;color:#0c5460;">Timing</span> '
            "nulstilles over årstotalen og er ikke en reel fejl (se metodenoten nedenfor).</p>"
        )
    else:
        parts.append(
            '<p class="anchor-missing">Ingen momsangivelse var tilgængelig for denne kørsel — '
            "kontrol 82-afstemningen (periode/rubrik mod den indberettede angivelse) indgår derfor "
            "ikke i denne rapport.</p>"
        )

    # --- Afstemningsgate ------------------------------------------------------
    af = report.get("afstemning") or {}
    if af:
        status = af.get("gate_status", "ukendt")
        ok = status == "afstemt"
        badge_bg, badge_fg = ("#d4edda", "#155724") if ok else ("#f8d7da", "#721c24")
        summ = af.get("summary", {}) or {}
        parts.append("<h3>Afstemningsgate</h3>")
        parts.append(
            f'<p><span class="status-badge" style="background:{badge_bg};color:{badge_fg};">'
            f"{_esc(status.upper())}</span> {_esc(af.get('message', ''))}</p>"
        )
        if summ:
            parts.append(
                "<p class='anchor-note'>"
                f"{fmt_int(summ.get('reconciled_count'))} af {fmt_int(summ.get('total_accounts_checked'))} "
                f"konti afstemt (tolerance {af.get('tolerance', '–')})."
                "</p>"
            )
    else:
        parts.append(
            '<p class="anchor-missing">Ingen afstemningsgate-kørsel var tilgængelig for denne rapport.</p>'
        )

    # --- Debet == kredit --------------------------------------------------
    summary = analytics.get("summary", {}) or {}
    debit = summary.get("total_debit")
    credit = summary.get("total_credit")
    if debit is not None and credit is not None:
        balanced = abs(float(debit) - float(credit)) < 0.01
        badge_bg, badge_fg = ("#d4edda", "#155724") if balanced else ("#f8d7da", "#721c24")
        parts.append("<h3>Totalkontrol</h3>")
        parts.append(
            f"<p>Debet {fmt_amount(debit, currency)} — Kredit {fmt_amount(credit, currency)} "
            f'<span class="status-badge" style="background:{badge_bg};color:{badge_fg};">'
            f"{'BALANCERER' if balanced else 'BALANCERER IKKE'}</span></p>"
        )

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 2: Ledelsesresumé ---------------------------------------------

def build_executive_summary(report: dict, analytics: dict) -> str:
    sev = analytics.get("severity_summary", {}) or {}
    summary = analytics.get("summary", {}) or {}
    parts = ['<section class="exec-summary">', "<h2>Ledelsesresumé</h2>"]

    parts.append('<div class="sev-cards">')
    for key in ("critical", "high", "medium", "low"):
        count = sev.get(key, 0)
        bg, fg, _ = _SEVERITY_COLORS[key]
        parts.append(
            f'<div class="sev-card" style="background:{bg};color:{fg};">'
            f'<div class="sev-card-count">{fmt_int(count)}</div>'
            f'<div class="sev-card-label">{_SEVERITY_LABELS[key]}</div></div>'
        )
    parts.append("</div>")

    total_tx = summary.get("total_transactions")
    period_start = summary.get("period_start", "")
    period_end = summary.get("period_end", "")
    parts.append('<dl class="exec-facts">')
    if total_tx is not None:
        parts.append(f"<dt>Transaktioner/bilag analyseret</dt><dd>{fmt_int(total_tx)}</dd>")
    lines = (report.get("parse_info", {}) or {}).get("sections", {}).get("lines")
    if lines is not None:
        parts.append(f"<dt>Bogføringslinjer</dt><dd>{fmt_int(lines)}</dd>")
    if period_start or period_end:
        parts.append(f"<dt>Analyseperiode</dt><dd>{_esc(period_start)} – {_esc(period_end)}</dd>")
    total_findings = analytics.get("total_findings")
    if total_findings is not None:
        parts.append(f"<dt>Fund i alt (efter modul-/gating-filtrering)</dt><dd>{fmt_int(total_findings)}</dd>")
    parts.append("</dl>")
    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 3: Aggregerede fundtabeller ------------------------------------

def _finding_account(finding: dict) -> str:
    for ref in finding.get("transactions") or []:
        acc = ref.get("account_id")
        if acc:
            return str(acc)
    return "(ingen kontoreference)"


def aggregate_findings_by_control(all_findings: list) -> list:
    """Grupper ALLE fund pr. kontrol -> pr. konto. Aldrig rå fund-dumps —
    kun antal + beløb pr. gruppe, sorteret efter væsentlighed."""
    by_test = {}
    for f in all_findings:
        tid = f.get("test_id")
        bucket = by_test.setdefault(tid, {
            "test_id": tid, "test_name": f.get("test_name", ""),
            "findings": 0, "amount": 0.0, "severity_rank": 4,
            "severity_counts": defaultdict(int),
            "accounts": defaultdict(lambda: {"count": 0, "amount": 0.0}),
        })
        amt = f.get("estimated_amount") or 0.0
        bucket["findings"] += 1
        bucket["amount"] += amt
        sev = f.get("severity", "low")
        bucket["severity_counts"][sev] += 1
        rank = _SEVERITY_ORDER.get(sev, 4)
        if rank < bucket["severity_rank"]:
            bucket["severity_rank"] = rank
        acc = _finding_account(f)
        ab = bucket["accounts"][acc]
        ab["count"] += 1
        ab["amount"] += amt

    controls = []
    for tid, b in by_test.items():
        accounts_sorted = sorted(
            b["accounts"].items(),
            key=lambda kv: (-kv[1]["amount"], -kv[1]["count"]),
        )
        controls.append({
            "test_id": tid,
            "test_name": b["test_name"],
            "findings": b["findings"],
            "amount": round(b["amount"], 2),
            "severity_rank": b["severity_rank"],
            "severity_counts": dict(b["severity_counts"]),
            "accounts": accounts_sorted,
        })
    controls.sort(key=lambda c: (c["severity_rank"], -c["amount"], -c["findings"]))
    return controls


def build_findings_tables(analytics: dict, currency: str, konto_navne: dict | None = None) -> str:
    """``konto_navne``: valgfrit opslagskort {account_id: navn} fra rapportens
    top-level (analyze_canonical) — viser 'nummer · navn' i konto-tabellerne,
    når kontoplan-stamdata var indlæst; ellers kun nummeret (uændret)."""
    konto_navne = konto_navne or {}
    all_findings = analytics.get("all_findings") or []
    if not all_findings:
        return (
            '<section class="findings-tables"><h2>Fund pr. kontrol</h2>'
            '<p class="no-findings">Ingen fund i denne rapport.</p></section>'
        )

    controls = aggregate_findings_by_control(all_findings)
    total_controls = len(controls)
    shown = controls[:_MAX_CONTROLS_SHOWN]

    parts = ['<section class="findings-tables">', "<h2>Fund pr. kontrol (aggregeret pr. konto)</h2>"]
    parts.append(
        '<p class="section-hint">Hver kontrol vises som ÉT sammendrag pr. konto (antal, samlet beløb) '
        "— ikke en liste af enkeltposteringer. Sorteret efter væsentlighed (værste severity, "
        "derefter beløb).</p>"
    )

    for c in shown:
        sev_key = next((k for k, v in _SEVERITY_ORDER.items() if v == c["severity_rank"]), "low")
        bg, fg, border = _SEVERITY_COLORS[sev_key]
        parts.append(f'<div class="control-block" style="border-left-color:{border};">')
        parts.append(
            f'<h3>Kontrol {c["test_id"]}: {_esc(c["test_name"])} '
            f'<span class="status-badge" style="background:{bg};color:{fg};">'
            f'{_SEVERITY_LABELS[sev_key]}</span></h3>'
        )
        sev_bits = ", ".join(
            f"{_SEVERITY_LABELS[k]}: {fmt_int(c['severity_counts'].get(k, 0))}"
            for k in ("critical", "high", "medium", "low") if c["severity_counts"].get(k)
        )
        parts.append(
            f'<p class="control-meta">{fmt_int(c["findings"])} fund'
            + (f" ({sev_bits})" if sev_bits else "")
            + (f" — estimeret {fmt_amount(c['amount'], currency)}" if c["amount"] else "")
            + "</p>"
        )

        accounts = c["accounts"]
        rows = accounts[:_MAX_ACCOUNT_ROWS]
        more = len(accounts) - len(rows)
        parts.append(
            "<table class='account-table'><thead><tr><th>Konto</th><th>Antal</th>"
            "<th>Beløb</th></tr></thead><tbody>"
        )
        for acc_id, agg in rows:
            navn = konto_navne.get(acc_id, "")
            label = f"{_esc(acc_id)} <span class='account-name'>· {_esc(navn)}</span>" if navn else _esc(acc_id)
            parts.append(
                f"<tr><td>{label}</td><td>{fmt_int(agg['count'])}</td>"
                f"<td>{fmt_amount(agg['amount'], currency) if agg['amount'] else '–'}</td></tr>"
            )
        parts.append("</tbody></table>")
        if more > 0:
            parts.append(f'<p class="more-note">…og {fmt_int(more)} flere konti.</p>')
        parts.append("</div>")

    if total_controls > len(shown):
        parts.append(
            f'<p class="more-note">…og {fmt_int(total_controls - len(shown))} flere kontroller med fund '
            "(vist i rå rapport-JSON, ikke gengivet her).</p>"
        )

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 4: Datagrundlag & metode ---------------------------------------

def build_data_foundation(report: dict, analytics: dict, data_contract: dict | None) -> str:
    parts = ['<section class="data-foundation">', "<h2>Datagrundlag &amp; metode</h2>"]

    datagrundlag = analytics.get("datagrundlag") or {}
    opsummering = datagrundlag.get("opsummering") or {}
    if opsummering:
        parts.append(
            "<p>"
            f"{fmt_int(opsummering.get('koert'))} af {fmt_int(opsummering.get('i_alt'))} kontroller kørte. "
            f"{fmt_int(opsummering.get('ikke_maalbar'))} kunne ikke måles på dette datagrundlag, "
            f"{fmt_int(opsummering.get('modul_fra'))} ligger i fravalgte analysemoduler, og "
            f"{fmt_int(opsummering.get('kraever_eksterne_data'))} kræver eksterne data, der ikke blev leveret."
            "</p>"
        )

    ikke_maalbare = [k for k in (datagrundlag.get("kontroller") or []) if k.get("status") == "ikke_maalbar"]
    if ikke_maalbare:
        parts.append("<h3>Ikke-målbare kontroller</h3>")
        parts.append("<ul class='gate-list'>")
        for k in ikke_maalbare[:_MAX_CONTROLS_SHOWN]:
            parts.append(f"<li>Kontrol {k['test_id']}: {_esc(k.get('aarsag', ''))}</li>")
        parts.append("</ul>")

    delkontrol = datagrundlag.get("delkontrol_gates") or []
    if delkontrol:
        parts.append("<h3>Delkontrol-noter</h3>")
        parts.append("<ul class='gate-list'>")
        for g in delkontrol[:_MAX_CONTROLS_SHOWN]:
            parts.append(f"<li>Kontrol {g['test_id']}: {_esc(g.get('besked', ''))}</li>")
        parts.append("</ul>")

    if data_contract and data_contract.get("known_gaps"):
        keywords = ("vat_number", "kunde", "customer", "leverandør", "supplier", "land", "country")
        gaps = [g for g in data_contract["known_gaps"] if g.get("status") != "lukket"]
        relevant = [
            g for g in gaps
            if any(kw in (g.get("titel", "") + " " + " ".join(g.get("beroerte_felter", []))).lower()
                   for kw in keywords)
        ]
        chosen = relevant[:_MAX_KNOWN_GAPS_SHOWN] or gaps[:_MAX_KNOWN_GAPS_SHOWN]
        if chosen:
            parts.append("<h3>Kendte begrænsninger i datagrundlaget</h3>")
            parts.append("<ul class='gate-list'>")
            for g in chosen:
                parts.append(f"<li><strong>{_esc(g.get('id', ''))}</strong>: {_esc(g.get('titel', ''))}</li>")
            parts.append("</ul>")

    parts.append("</section>")
    return "\n".join(parts)


def build_lineage_footer(report: dict) -> str:
    lineage = report.get("lineage") or {}
    parts = ['<footer class="lineage-footer">']
    parts.append("<h2>Sporbarhed</h2>")
    parts.append("<dl class='lineage-facts'>")
    fields = [
        ("Katalogversion", lineage.get("catalog_version")),
        ("Datakontrakt-version", lineage.get("data_contract_version")),
        ("Mapping-version", lineage.get("mapping_version")),
        ("Skema-fingerprint", lineage.get("schema_fingerprint")),
        ("Kildesystem", lineage.get("source_erp")),
        ("Kørselstidspunkt (UTC)", lineage.get("generated_at")),
    ]
    for label, value in fields:
        if value:
            parts.append(f"<dt>{_esc(label)}</dt><dd>{_esc(value)}</dd>")
    parts.append("</dl>")
    parts.append(
        '<p class="ai-provenance">Analysen er 100&nbsp;% deterministisk — samme input giver altid samme '
        "resultat, og ingen sprogmodel fortolker bogføringen eller afgør fund. AI anvendes ALENE i et "
        "tidligere, adskilt trin til at FORESLÅ en kolonne-mapping fra kildesystemet til den kanoniske "
        "datastruktur; et menneske godkender og fryser mappingen (versioneret ovenfor), før analysen "
        "kører. Se BALAI-dataflow-arkitektur.md §2a.</p>"
    )
    parts.append("</footer>")
    return "\n".join(parts)


# --- Samlet dokument ---------------------------------------------------------

_STYLE = """
:root {
  --navy: #1B365D;
  --blue: #2d5aa0;
  --bg: #f0f2f5;
  --card: #ffffff;
  --text: #1a1a2e;
  --muted: #6B7280;
  --border: #e5e7eb;
}
* { box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--bg); color: var(--text); margin: 0; padding: 0;
}
.report-banner { background: #fff; border-bottom: 1px solid var(--border); padding: 14px 20px; }
.report-banner-inner { max-width: 1000px; margin: 0 auto; display: flex; align-items: center; gap: 12px; }
.report-mark { width: 32px; height: 32px; border-radius: 6px; background: var(--navy); color: #fff;
  display: inline-flex; align-items: center; justify-content: center; font-weight: 700; font-size: 15px; flex: none; }
.report-brand-top { font-weight: 600; font-size: 14px; color: var(--navy); display: block; }
.report-brand-sub { font-weight: 400; font-size: 11px; color: var(--muted); }
.container { max-width: 1000px; margin: 0 auto; padding: 32px 20px 60px; }
h1 { font-size: 1.8rem; color: var(--navy); margin: 0 0 6px; }
.report-subtitle { color: var(--muted); margin: 0 0 28px; }
section, footer { background: var(--card); border-radius: 14px; padding: 24px 26px; margin-bottom: 20px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
h2 { font-size: 1.15rem; color: var(--navy); margin: 0 0 14px; border-bottom: 2px solid var(--border);
  padding-bottom: 8px; }
h3 { font-size: 1rem; color: var(--blue); margin: 18px 0 8px; }
p { line-height: 1.55; }
.section-hint, .anchor-note, .control-meta { color: var(--muted); font-size: 0.88rem; }
.anchor-missing, .no-findings { color: var(--muted); font-style: italic; }
.status-badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 0.72rem;
  font-weight: 700; }
.table-scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 0.85rem; }
th, td { padding: 6px 10px; text-align: right; border-bottom: 1px solid var(--border); white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
thead th { color: var(--muted); font-weight: 600; font-size: 0.78rem; text-transform: uppercase; }
tfoot td { border-top: 2px solid var(--border); border-bottom: none; }
.recon-table th, .recon-table td { font-size: 0.8rem; }
.sev-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 18px; }
.sev-card { border-radius: 10px; padding: 14px; text-align: center; }
.sev-card-count { font-size: 1.6rem; font-weight: 800; }
.sev-card-label { font-size: 0.8rem; font-weight: 600; text-transform: uppercase; }
.exec-facts { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px 24px; }
.exec-facts dt { color: var(--muted); font-size: 0.82rem; }
.exec-facts dd { margin: 0 0 8px; font-weight: 600; }
.control-block { border-left: 4px solid var(--blue); background: #fafbfc; border-radius: 8px;
  padding: 14px 16px; margin-bottom: 14px; }
.control-block h3 { margin-top: 0; display: flex; align-items: center; gap: 10px; }
.account-table { font-size: 0.82rem; }
.account-name { color: #6B7280; font-weight: normal; }
.more-note { color: var(--muted); font-size: 0.82rem; font-style: italic; }
.gate-list { padding-left: 20px; line-height: 1.7; }
.lineage-facts { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px 24px; font-size: 0.85rem; }
.lineage-facts dt { color: var(--muted); }
.lineage-facts dd { margin: 0 0 6px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.ai-provenance { font-size: 0.82rem; color: var(--muted); margin-top: 14px; border-top: 1px solid var(--border);
  padding-top: 12px; }

@media print {
  body { background: #fff; }
  .report-banner { display: none; }
  section, footer { box-shadow: none; border: 1px solid var(--border); break-inside: avoid; }
  .control-block { break-inside: avoid; }
}
@media (max-width: 640px) {
  .sev-cards, .exec-facts, .lineage-facts { grid-template-columns: 1fr; }
  .container { padding: 20px 12px 40px; }
}
"""


def render_html(report: dict, data_contract: dict | None) -> str:
    analytics = _analytics(report)
    currency = (analytics.get("summary", {}) or {}).get("currency", "DKK")

    body = [
        '<!DOCTYPE html>',
        '<html lang="da">',
        '<head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
        '<title>VAT Analytics — Kunderapport</title>',
        f"<style>{_STYLE}</style>",
        '</head>',
        '<body>',
        '<div class="report-banner"><div class="report-banner-inner">'
        '<span class="report-mark" aria-hidden="true">B</span>'
        '<span><span class="report-brand-top">BALAI</span>'
        '<span class="report-brand-sub">VAT Analytics</span></span>'
        '</div></div>',
        '<div class="container">',
        '<h1>Momsanalyse — resultatrapport</h1>',
        '<p class="report-subtitle">Deterministisk gennemgang af bogføringen mod de danske momsregler. '
        'Se afsnittet "Datagrundlag &amp; metode" for forbehold og forudsætninger.</p>',
        build_trust_anchor(report, analytics, currency),
        build_executive_summary(report, analytics),
        build_findings_tables(analytics, currency, report.get("konto_navne")),
        build_data_foundation(report, analytics, data_contract),
        build_lineage_footer(report),
        '</div>',
        '</body>',
        '</html>',
    ]
    return "\n".join(body)


# --- CLI ---------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("rapport_json", help="Sti til analyserapport-JSON (analyze_canonical.py's output).")
    parser.add_argument("--out", required=True, help="Sti til output-HTML-fil.")
    args = parser.parse_args(argv)

    try:
        report = load_report(args.rapport_json)
    except (OSError, json.JSONDecodeError) as e:
        print(f"FEJL: kunne ikke læse {args.rapport_json}: {e}", file=sys.stderr)
        return 1

    data_contract = _load_data_contract()
    html_out = render_html(report, data_contract)

    out_dir = os.path.dirname(os.path.abspath(args.out))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_out)

    size_kb = os.path.getsize(args.out) / 1024
    print(f"Skrev {args.out} ({size_kb:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
