#!/usr/bin/env python3
"""
generate_report.py — kundedialog-laget, REDESIGNET (byggetrin ~10,
Bal-godkendt 2026-09-18: designoplæg med alle fire spørgsmål godkendt).
Afløser den tidligere "aggregeret fundtabel"-rapport (byggetrin ~9, Del C,
2026-09-17) — den gamle visning lever videre som et VALGFRIT appendix
(``--appendix``), default FRA i kundevisningen.

Bygger ÉN selvbærende HTML-fil (inline CSS, INGEN eksterne afhængigheder/CDN
— skal kunne mailes og åbnes offline) fra en analyserapport-JSON
(``tools/analyze_canonical.py``s output, eller webappens tilsvarende
``analytics.engine.run_all_tests``-rapport). Dansk UI, spørgende/vi-
bemærker tone (ALDRIG anklagende — observationer er ikke konklusioner).

DE SYV SEKTIONER (designoplægget, ikke konfigurerbare i indhold — kun i
hvilke der VISES, jf. ``--niveau`` nedenfor):

  1. **Hero** ("Jeres moms — set gennem data"): antal linjer/bilag/konti/
     perioder, estimerede datapunkter, momsgennemstrømning
     (|udgående|+|indgående|), årets tilsvar — beregnet af rapport-JSON'en.
  2. **Momsmotoren** (pædagogisk): momskode -> momskonto -> angivelsens
     rubrik, auto-genereret fra ``tax_table_oversigt`` (vat_setup-data) +
     kontrol 82's rubrik-logik (``cat10.classify_purchase_rubric`` — samme
     kilde-af-sandhed, ikke en gendannet kopi).
  3. **Afstemningen** (tillidsanker): 12-måneders rubrik-tabel, gate-
     resultat, klartekst-konklusion.
  4. **Observationer & spørgsmål** (kernen): kuraterede fund-GRUPPER efter
     TEMA — ALDRIG kontrolnumre i kundens view. Medieret 100% gennem
     ``tools/report_curation.py``s kurationsfil (se dens docstring for
     formatet og genkørsels-mekanikken).
  5. **Datagrundlag & metode**: analyseret omfang, ikke-målbare kontroller
     i klartekst (INGEN kontrolnumre — kun årsagsteksten, deduplikeret),
     og "yderligere analyser mulige med disse data" (mersalg, fra
     ``catalog/data_contract.json``s ``known_gaps``, oversat til
     forretningssprog).
  6. **Anbefalinger** i horisonter (0-3/3-12/12-24 mdr.) — udledt af de
     FORFREMMEDE tema-grupper (``medtag: true`` i kurationsfilen).
  7. **Lineage-footer**: versioner, fingerprint, AI-provenance-note. HER
     (og i Excel-arbejdsbilaget, ``tools/report_workbook.py``) må
     kontrol-id'er optræde — som et kompakt tema->kontrolnummer-
     revisionsspor, IKKE i den løbende tekst.

BEVIDSTE AFVIGELSER fra den forrige rapportversion (dokumenteret, jf.
opgavens "afvig ikke uden at notere det eksplicit"-krav):
  - Den tidligere "Ledelsesresumé"-sektion (severity-kort: kritisk/høj/
    medium/lav) er UDGÅET af kundenarrativet — den var netop "en mur af
    flag", som designfilosofien (RØD = handling krævet, prioriteret
    handlingsliste) bevidst erstatter med de kuraterede tema-observationer
    i sektion 4. Severity-fordelingen findes stadig i Excel-arbejdsbilagets
    oversigtsark.
  - Kontrolnumre er fjernet fra ALLE sektioner undtagen 7 (lineage-footer,
    kompakt tema-liste) og Excel-arbejdsbilaget — inkl. "ikke-målbar"-
    listen i sektion 5, der tidligere viste "Kontrol N: ...".

NIVEAU <-> PRODUKTPAKKE (dokumenteret kobling, jf. opgaven):
    niveau 1 ("Basis")     — sektion 1+3+5+7. Et hurtigt datadrevet
                              sundhedstjek uden pædagogik eller anbefalinger.
    niveau 2 ("Standard")  — niveau 1 + sektion 2+4 (UDEN sektion 6). Den
                              løbende rådgivningsrelation: vi viser motoren
                              og observationerne, men forpligter ikke til
                              en handlingsplan.
    niveau 3 ("Fuld rådgivning", DEFAULT) — alle syv sektioner, inkl.
                              prioriterede anbefalinger. En egentlig
                              gennemgangs-/advisory-leverance.

KURATIONSFIL: se ``tools/report_curation.py`` (format + seed/merge-mekanik).
EXCEL-ARBEJDSBILAG: se ``tools/report_workbook.py`` (rådgiverens værktøj —
her ER kontrolnumre, alle severities, ingen kuratering).

Kør (fra backend/):
    python tools/generate_report.py <rapport.json> --out <fil.html> \\
        [--curation <kuration.json>] [--niveau 1|2|3] \\
        [--workbook <arbejdsbilag.xlsx>] [--appendix]

INGEN kundedata logges til stdout her — kun nøgletal og filstier. Selve
HTML-filen/kurationsfilen/Excel-arbejdsbilaget KAN indeholde kundedata —
del/gem dem derfor med samme forsigtighed som rapport-JSON'en. Teknisk fejl
må aldrig fremstå som et fagligt resultat — manglende sektioner vises som
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
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)  # så analytics.*/tools.* kan importeres uanset cwd

from analytics.categories import cat10_vat_reconciliation as _cat10  # noqa: E402
from tools import report_curation  # noqa: E402
from tools import report_themes  # noqa: E402

_MAX_ACCOUNT_ROWS = 25       # cap pr. kontrol-tabel i appendixet
_MAX_CONTROLS_SHOWN = 60     # cap på antal kontrol-sektioner i appendixet
_MAX_KNOWN_GAPS_SHOWN = 6
_MAX_TAX_CODES_SHOWN = 60
_ESTIMATED_FIELDS_PER_LINE = 12  # groft, EKSPLICIT mærket estimat — se build_hero

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

_CALC_TYPE_LABELS = {
    "normal vat": "Almindelig moms",
    "reverse charge vat": "Omvendt betalingspligt",
    "full vat": "Fuld moms (intet fradrag)",
}

_PURCHASE_RUBRIC_LABELS = {
    "dkrc": "Udgående moms (omvendt betalingspligt, indenlandsk)",
    "rc_services": "RC-ydelser fra udlandet (eget felt i angivelsen)",
    "input": "Alm. moms (udgående ved salg / indgående ved køb)",
}

# Niveau -> hvilke sektionsnøgler der vises, i den faste visningsrækkefølge.
_SECTION_ORDER = ("hero", "engine", "trust_anchor", "observations", "data_foundation",
                   "recommendations", "lineage")
NIVEAU_SECTIONS = {
    1: {"hero", "trust_anchor", "data_foundation", "lineage"},
    2: {"hero", "engine", "trust_anchor", "observations", "data_foundation", "lineage"},
    3: set(_SECTION_ORDER),
}
NIVEAU_PRODUKTPAKKE = {
    1: "Basis — hurtigt datadrevet sundhedstjek, ingen pædagogik/anbefalinger.",
    2: "Standard — + momsmotor-pædagogik og kuraterede observationer, uden forpligtende anbefalinger.",
    3: "Fuld rådgivning — alle syv sektioner inkl. prioriterede anbefalinger i horisont.",
}

# Yderligere analyser, der bliver mulige med mere data (sektion 5, mersalg).
# Knyttet til konkrete known_gaps-id'er i catalog/data_contract.json, så
# listen automatisk krymper, når et gab lukkes (status "lukket") — uden at
# vise interne felt-/kontrolreferencer til kunden.
_UPSELL_OPPORTUNITIES = (
    {"gap_ids": ("GAP-05",),
     "tekst": "Med kundernes momsnumre og lande fuldt struktureret kan analysen "
              "udvides til at kontrollere EU-salgsangivelsen (listesystemet)."},
    {"gap_ids": ("GAP-10", "GAP-11"),
     "tekst": "Med en fuld kontoplan og saldobalance fra jeres ERP kan vi tilføje "
              "en uafhængig kontoafstemning ud over den nuværende momsafstemning."},
    {"gap_ids": ("GAP-06",),
     "tekst": "Med et konsistent bilagsnummer på tværs af systemet kan flere "
              "kontroller kobles direkte til det underliggende dokument."},
)


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


# --- Sektion 1: Hero --------------------------------------------------------

def build_hero(report: dict, analytics: dict, currency: str) -> str:
    sections = (report.get("parse_info") or {}).get("sections") or {}
    summary = analytics.get("summary", {}) or {}
    decl = analytics.get("declaration_reconciliation")
    konto_navne = report.get("konto_navne") or {}

    linjer = sections.get("lines")
    bilag = sections.get("transactions") or summary.get("total_transactions")
    konti = sections.get("accounts") or (len(konto_navne) if konto_navne else None)
    perioder = len(decl["perioder"]) if decl and decl.get("perioder") else None
    datapunkter = linjer * _ESTIMATED_FIELDS_PER_LINE if linjer else None

    gennemstroemning = None
    tilsvar = None
    if decl and decl.get("aarstotaler"):
        at = decl["aarstotaler"]
        out_ = (at.get("output_vat") or {}).get("beregnet")
        in_ = (at.get("input_vat") or {}).get("beregnet")
        if out_ is not None and in_ is not None:
            gennemstroemning = abs(out_) + abs(in_)
            tilsvar = round(out_ - in_, 2)

    cards = []

    def _card(label, value):
        cards.append(
            f'<div class="hero-card"><div class="hero-card-value">{value}</div>'
            f'<div class="hero-card-label">{_esc(label)}</div></div>'
        )

    if linjer is not None:
        _card("Bogføringslinjer", fmt_int(linjer))
    if bilag is not None:
        _card("Bilag", fmt_int(bilag))
    if konti is not None:
        _card("Konti", fmt_int(konti))
    if perioder is not None:
        _card("Perioder med angivelse", fmt_int(perioder))

    parts = ['<section class="hero">', "<h2>Jeres moms — set gennem data</h2>"]
    if cards:
        parts.append('<div class="hero-cards">' + "".join(cards) + "</div>")
    else:
        parts.append('<p class="anchor-missing">Intet datagrundlag tilgængeligt for denne rapport.</p>')

    facts = []
    if datapunkter is not None:
        facts.append(("Estimerede datapunkter analyseret", fmt_int(datapunkter),
                       "Groft estimat (linjer × typiske felter pr. linje) — ikke en præcis optælling."))
    if gennemstroemning is not None:
        facts.append(("Momsgennemstrømning i perioden", fmt_amount(gennemstroemning, currency),
                       "Udgående moms + købsmoms (numerisk sum, uanset fortegn)."))
    if tilsvar is not None:
        fortegn = "tilsvar (betales)" if tilsvar >= 0 else "til gode (refusion)"
        facts.append(("Årets momstilsvar", f"{fmt_amount(tilsvar, currency)} ({fortegn})",
                       "Udgående moms minus indgående moms (inkl. omvendt betalingspligt), beregnet af bogføringen."))

    if facts:
        parts.append('<dl class="hero-facts">')
        for label, value, note in facts:
            parts.append(f"<dt>{_esc(label)}</dt><dd>{value}<span class='hero-fact-note'>{_esc(note)}</span></dd>")
        parts.append("</dl>")
    elif not cards:
        pass
    else:
        parts.append(
            '<p class="anchor-note">Momsgennemstrømning og årets tilsvar kræver en indberettet '
            "momsangivelse — se afsnittet \"Afstemningen\".</p>"
        )

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 2: Momsmotoren --------------------------------------------------

def _calc_type_label(raw: str) -> str:
    key = (raw or "").strip().lower()
    return _CALC_TYPE_LABELS.get(key, raw or "Ukendt/ikke oplyst")


def _rubric_label_for_code(tax_code: str, vat_calculation_type: str) -> str:
    rubric = _cat10.classify_purchase_rubric(tax_code, vat_calculation_type)
    return _PURCHASE_RUBRIC_LABELS.get(rubric, "Ukendt")


def build_engine_section(report: dict, konto_navne: dict) -> str:
    tax_rows = report.get("tax_table_oversigt") or []
    parts = ['<section class="engine">', "<h2>Momsmotoren</h2>"]
    parts.append(
        "<p>Jeres momsopsætning kobler hver momskode til én eller flere momskonti og til en "
        "rubrik i momsangivelsen (udgående moms, RC-ydelser fra udlandet, eller almindelig "
        "moms ved salg/køb). Tabellen nedenfor er genereret automatisk ud fra jeres egen "
        "opsætning — ikke skrevet i hånden — så den altid afspejler den faktiske konfiguration.</p>"
    )
    if not tax_rows:
        parts.append(
            '<p class="anchor-missing">Ingen detaljeret momsopsætning var tilgængelig for denne '
            "rapport — dette afsnit kræver kundens egen momskode-opsætning som sidecar-data.</p>"
        )
        parts.append("</section>")
        return "\n".join(parts)

    shown = tax_rows[:_MAX_TAX_CODES_SHOWN]
    parts.append(
        "<div class='table-scroll'><table class='engine-table'><thead><tr>"
        "<th>Momskode</th><th>Sats</th><th>Beregningstype</th><th>Konti</th>"
        "<th>Rubrik i angivelsen</th></tr></thead><tbody>"
    )
    for row in shown:
        code = row.get("tax_code", "")
        matched = row.get("setup_matched", False)
        calc_type = row.get("vat_calculation_type", "")
        accounts = [
            (label, acc) for label, acc in (
                ("Salg", row.get("sales_vat_account", "")),
                ("Køb", row.get("purchase_vat_account", "")),
                ("RC", row.get("reverse_charge_vat_account", "")),
            ) if acc
        ]
        if accounts:
            acc_html = "; ".join(
                f"{_esc(label)}: {_esc(acc)}"
                + (f" ({_esc(konto_navne[acc])})" if acc in konto_navne else "")
                for label, acc in accounts
            )
        else:
            acc_html = "–"
        rate = fmt_pct(row.get("tax_percentage"), decimals=2) if matched else "–"
        calc_label = _calc_type_label(calc_type) if matched else "Ukendt/mangler i jeres opsætning"
        rubric_label = _rubric_label_for_code(code, calc_type)
        parts.append(
            f"<tr><td>{_esc(code)}</td><td>{rate}</td><td>{_esc(calc_label)}</td>"
            f"<td>{acc_html}</td><td>{_esc(rubric_label)}</td></tr>"
        )
    parts.append("</tbody></table></div>")
    if len(tax_rows) > len(shown):
        parts.append(f'<p class="more-note">…og {fmt_int(len(tax_rows) - len(shown))} flere momskoder.</p>')
    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 3: Afstemningen (tillidsanker) --------------------------------

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


def _reconciliation_conclusion(decl: dict | None, gate: dict | None) -> str:
    bits = []
    if decl and decl.get("perioder"):
        counts = defaultdict(int)
        total = 0
        for p in decl["perioder"]:
            for cell in (p.get("rubrikker") or {}).values():
                counts[cell.get("status", "ingen_angivelse")] += 1
                total += 1
        afvigelser = counts.get("afvigelse", 0)
        if afvigelser == 0 and total > 0:
            bits.append("Ingen reelle afvigelser fundet — eventuelle periodeforskelle er ren "
                        "timing, der udlignes over årstotalen.")
        elif afvigelser > 0:
            bits.append(f"{afvigelser} celle(r) i tabellen viser en reel afvigelse, der bør afklares.")
    if gate:
        status = gate.get("gate_status")
        if status == "afstemt":
            bits.append("Den samlede kontoafstemning stemmer.")
        elif status and status != "afstemning_ikke_udfoert":
            bits.append("Den samlede kontoafstemning stemmer IKKE fuldt ud — se afstemningsgatens besked.")
    if not bits:
        return ""
    return " ".join(bits)


def build_trust_anchor(report: dict, analytics: dict, currency: str) -> str:
    parts = ['<section class="trust-anchor">', "<h2>Afstemningen</h2>",
             '<p class="section-hint">Dette er rapportens tillidsanker: en fuld, genkørbar '
             "afstemning af jeres bogføring mod den indberettede momsangivelse og mod jeres egne "
             "kontosaldi — ikke et udsnit.</p>"]

    decl = analytics.get("declaration_reconciliation")
    if decl and decl.get("perioder"):
        labels = decl.get("rubrik_labels", {})
        rubrics = list(labels.keys()) or ["output_vat", "rc_services", "input_vat"]
        parts.append('<h3>Momsangivelse pr. periode</h3>')
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
            "nulstilles over årstotalen og er ikke en reel fejl.</p>"
        )
    else:
        parts.append(
            '<p class="anchor-missing">Ingen momsangivelse var tilgængelig for denne kørsel — '
            "periode-/rubrikafstemningen indgår derfor ikke i denne rapport.</p>"
        )

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

    conclusion = _reconciliation_conclusion(decl, af)
    if conclusion:
        parts.append(f'<p class="conclusion"><strong>Konklusion:</strong> {_esc(conclusion)}</p>')

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 4: Observationer & spørgsmål -----------------------------------

def build_observations_section(curation: dict) -> str:
    groups = report_curation.promoted_groups(curation)
    parts = ['<section class="observations">', "<h2>Observationer &amp; spørgsmål</h2>"]
    if not groups:
        parts.append(
            '<p class="no-findings">Ingen kuraterede observationer i denne rapport.</p>'
        )
        parts.append("</section>")
        return "\n".join(parts)

    parts.append(
        '<p class="section-hint">Dette er OBSERVATIONER — ikke konklusioner. Vi bemærker mønstre '
        "i data og stiller spørgsmål; vurderingen af, om noget kræver handling, tager vi bedst "
        "sammen med jer.</p>"
    )
    for g in groups:
        parts.append('<div class="obs-group">')
        parts.append(f'<span class="obs-theme-label">{_esc(g.get("tema_navn", ""))}</span>')
        parts.append(f'<h3>{_esc(g.get("spoergsmaal", ""))}</h3>')
        if g.get("hvorfor"):
            parts.append(f"<p>{_esc(g['hvorfor'])}</p>")

        auto = g.get("auto") or {}
        evidens = auto.get("evidens") or []
        if evidens:
            parts.append(
                "<div class='table-scroll'><table class='evidence-table'><thead><tr>"
                "<th>Konto</th><th>Bilag</th><th>Dato</th><th>Beløb</th><th>Beskrivelse</th>"
                "</tr></thead><tbody>"
            )
            for row in evidens:
                parts.append(
                    f"<tr><td>{_esc(row.get('konto'))}</td><td>{_esc(row.get('bilag'))}</td>"
                    f"<td>{_esc(row.get('dato'))}</td><td>{fmt_amount(row.get('beloeb'))}</td>"
                    f"<td>{_esc(row.get('beskrivelse'))}</td></tr>"
                )
            parts.append("</tbody></table></div>")
            more = auto.get("fund_antal", 0) - len(evidens)
            if more > 0:
                parts.append(f'<p class="more-note">…og {fmt_int(more)} flere posteringer i denne gruppe.</p>')

        if g.get("anbefaling"):
            parts.append(f'<p class="obs-recommendation"><strong>Vores anbefaling:</strong> {_esc(g["anbefaling"])}</p>')
        if g.get("advisor_notes"):
            parts.append(f'<p class="advisor-note"><strong>Rådgivers note:</strong> {_esc(g["advisor_notes"])}</p>')
        parts.append("</div>")

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 5: Datagrundlag & metode ---------------------------------------

def _relevant_known_gaps(data_contract: dict | None) -> list:
    if not data_contract or not data_contract.get("known_gaps"):
        return []
    return [g for g in data_contract["known_gaps"] if g.get("status") != "lukket"]


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

    # INGEN kontrolnumre her (byggetrin ~10-disciplinen) — kun årsagsteksten,
    # deduplikeret (flere kontroller kan dele samme "feltet mangler"-årsag).
    ikke_maalbare = [k for k in (datagrundlag.get("kontroller") or []) if k.get("status") == "ikke_maalbar"]
    if ikke_maalbare:
        seen = []
        for k in ikke_maalbare:
            text = (k.get("aarsag") or "").strip()
            if text and text not in seen:
                seen.append(text)
        if seen:
            parts.append("<h3>Forhold, der ikke kunne måles på dette datagrundlag</h3>")
            parts.append("<ul class='gate-list'>")
            for text in seen[:_MAX_CONTROLS_SHOWN]:
                parts.append(f"<li>{_esc(text)}</li>")
            parts.append("</ul>")

    delkontrol = datagrundlag.get("delkontrol_gates") or []
    if delkontrol:
        seen = []
        for g in delkontrol:
            text = (g.get("besked") or "").strip()
            if text and text not in seen:
                seen.append(text)
        if seen:
            parts.append("<h3>Yderligere forbehold</h3>")
            parts.append("<ul class='gate-list'>")
            for text in seen[:_MAX_CONTROLS_SHOWN]:
                parts.append(f"<li>{_esc(text)}</li>")
            parts.append("</ul>")

    open_gap_ids = {g.get("id") for g in _relevant_known_gaps(data_contract)}
    upsell = [o for o in _UPSELL_OPPORTUNITIES if open_gap_ids & set(o["gap_ids"])] if data_contract else list(_UPSELL_OPPORTUNITIES)
    if upsell:
        parts.append("<h3>Yderligere analyser mulige med disse data</h3>")
        parts.append("<ul class='gate-list'>")
        for o in upsell[:_MAX_KNOWN_GAPS_SHOWN]:
            parts.append(f"<li>{_esc(o['tekst'])}</li>")
        parts.append("</ul>")

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 6: Anbefalinger -------------------------------------------------

_HORISONT_LABELS = (("0-3", "0-3 måneder"), ("3-12", "3-12 måneder"), ("12-24", "12-24 måneder"))


def build_recommendations_section(curation: dict) -> str:
    groups = report_curation.promoted_groups(curation)
    parts = ['<section class="recommendations">', "<h2>Anbefalinger</h2>"]
    if not groups:
        parts.append('<p class="no-findings">Ingen anbefalinger at vise i denne rapport.</p>')
        parts.append("</section>")
        return "\n".join(parts)

    buckets = {key: [] for key, _ in _HORISONT_LABELS}
    other = []
    for g in groups:
        buckets.get(g.get("horisont"), other).append(g) if g.get("horisont") in buckets else other.append(g)

    any_shown = False
    for key, label in _HORISONT_LABELS:
        items = buckets[key]
        if not items:
            continue
        any_shown = True
        parts.append(f"<h3>{_esc(label)}</h3><ul class='reco-list'>")
        for g in items:
            parts.append(f"<li><strong>{_esc(g.get('tema_navn', ''))}:</strong> {_esc(g.get('anbefaling', ''))}</li>")
        parts.append("</ul>")
    if other:
        any_shown = True
        parts.append("<h3>Øvrigt</h3><ul class='reco-list'>")
        for g in other:
            parts.append(f"<li><strong>{_esc(g.get('tema_navn', ''))}:</strong> {_esc(g.get('anbefaling', ''))}</li>")
        parts.append("</ul>")

    if not any_shown:
        parts.append('<p class="no-findings">Ingen anbefalinger at vise i denne rapport.</p>')

    parts.append("</section>")
    return "\n".join(parts)


# --- Sektion 7: Lineage-footer -----------------------------------------------

def build_lineage_footer(report: dict, curation: dict | None = None) -> str:
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
        "datastruktur, og til at UDKASTE spørgsmål/forklaringer i denne rapports observationer — et "
        "menneske (jeres rådgiver) godkender/redigerer altid udkastet før udsendelse (se "
        "kurationsfilen). Ingen af disse trin fortolker selve bogføringen eller afgør et fund. "
        "Se BALAI-dataflow-arkitektur.md §2a.</p>"
    )

    if curation and curation.get("grupper"):
        refs = []
        for tk in report_themes.THEME_ORDER:
            g = curation["grupper"].get(tk)
            if not g:
                continue
            kontroller = (g.get("auto") or {}).get("kilde_kontroller") or []
            if kontroller:
                refs.append(f"{_esc(g.get('tema_navn', tk))} → kontrol {', '.join(str(c) for c in kontroller)}")
        if refs:
            parts.append("<h3>Interne referencer (revisionsspor)</h3>")
            parts.append(f"<p class='lineage-refs'>{' · '.join(refs)}</p>")

    parts.append("</footer>")
    return "\n".join(parts)


# --- Appendix (tidligere hovedvisning, byggetrin ~9) ------------------------

def _finding_account(finding: dict) -> str:
    for ref in finding.get("transactions") or []:
        acc = ref.get("account_id")
        if acc:
            return str(acc)
    return "(ingen kontoreference)"


def aggregate_findings_by_control(all_findings: list) -> list:
    """Grupper ALLE fund pr. kontrol -> pr. konto. Bruges KUN af det
    valgfrie appendix (``--appendix``) — kontrolnumre er tilladt her, jf.
    appendixets teknisk-orienterede karakter (bevaret uændret fra
    byggetrin ~9's version)."""
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


def build_appendix(analytics: dict, currency: str, konto_navne: dict | None = None) -> str:
    konto_navne = konto_navne or {}
    all_findings = analytics.get("all_findings") or []
    parts = ['<section class="findings-tables appendix">',
             "<h2>Appendix: aggregeret fundoversigt (teknisk)</h2>",
             '<p class="section-hint">Dette er det tidligere rapportformat, bevaret som valgfrit '
             "teknisk bilag. Modsat resten af rapporten VISER dette afsnit kontrolnumre og alle "
             "fund pr. kontrol/konto, ikke kun de kuraterede observationer.</p>"]
    if not all_findings:
        parts.append('<p class="no-findings">Ingen fund i denne rapport.</p></section>')
        return "\n".join(parts)

    controls = aggregate_findings_by_control(all_findings)
    total_controls = len(controls)
    shown = controls[:_MAX_CONTROLS_SHOWN]

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


# Bagudkompatibelt alias — den forrige rapportversions offentlige navn.
build_findings_tables = build_appendix


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
.hero-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 18px; }
.hero-card { border-radius: 10px; padding: 14px; text-align: center; background: #eef2f8; }
.hero-card-value { font-size: 1.5rem; font-weight: 800; color: var(--navy); }
.hero-card-label { font-size: 0.78rem; font-weight: 600; color: var(--muted); text-transform: uppercase; }
.hero-facts { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px 24px; }
.hero-facts dt { color: var(--muted); font-size: 0.82rem; }
.hero-facts dd { margin: 0 0 10px; font-weight: 700; font-size: 1.05rem; }
.hero-fact-note { display: block; font-weight: 400; color: var(--muted); font-size: 0.78rem; }
.engine-table th, .engine-table td { font-size: 0.82rem; white-space: normal; }
.obs-group { border-left: 4px solid var(--blue); background: #fafbfc; border-radius: 8px;
  padding: 14px 16px; margin-bottom: 18px; }
.obs-theme-label { display: inline-block; font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
  color: var(--blue); letter-spacing: 0.03em; margin-bottom: 4px; }
.obs-group h3 { margin-top: 2px; }
.evidence-table { font-size: 0.8rem; }
.obs-recommendation, .advisor-note { background: #eef2f8; border-radius: 8px; padding: 10px 14px;
  font-size: 0.88rem; }
.advisor-note { background: #fff8e1; }
.reco-list, .gate-list { padding-left: 20px; line-height: 1.7; }
.conclusion { background: #eef2f8; border-radius: 8px; padding: 12px 14px; }
.control-block { border-left: 4px solid var(--blue); background: #fafbfc; border-radius: 8px;
  padding: 14px 16px; margin-bottom: 14px; }
.control-block h3 { margin-top: 0; display: flex; align-items: center; gap: 10px; }
.account-table { font-size: 0.82rem; }
.account-name { color: #6B7280; font-weight: normal; }
.more-note { color: var(--muted); font-size: 0.82rem; font-style: italic; }
.lineage-facts { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px 24px; font-size: 0.85rem; }
.lineage-facts dt { color: var(--muted); }
.lineage-facts dd { margin: 0 0 6px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.ai-provenance, .lineage-refs { font-size: 0.82rem; color: var(--muted); margin-top: 14px; border-top: 1px solid var(--border);
  padding-top: 12px; }

@media print {
  body { background: #fff; }
  .report-banner { display: none; }
  section, footer { box-shadow: none; border: 1px solid var(--border); break-inside: avoid; }
  .control-block, .obs-group { break-inside: avoid; }
}
@media (max-width: 640px) {
  .hero-cards, .hero-facts, .lineage-facts { grid-template-columns: 1fr; }
  .container { padding: 20px 12px 40px; }
}
"""


def render_html(report: dict, data_contract: dict | None, *,
                 niveau: int = 3, curation: dict | None = None, appendix: bool = False) -> str:
    analytics = _analytics(report)
    currency = (analytics.get("summary", {}) or {}).get("currency", "DKK")
    konto_navne = report.get("konto_navne") or {}

    if curation is None:
        curation = report_curation.seed_curation(analytics.get("all_findings") or [])

    wanted = NIVEAU_SECTIONS.get(niveau, NIVEAU_SECTIONS[3])
    builders = {
        "hero": lambda: build_hero(report, analytics, currency),
        "engine": lambda: build_engine_section(report, konto_navne),
        "trust_anchor": lambda: build_trust_anchor(report, analytics, currency),
        "observations": lambda: build_observations_section(curation),
        "data_foundation": lambda: build_data_foundation(report, analytics, data_contract),
        "recommendations": lambda: build_recommendations_section(curation),
        "lineage": lambda: build_lineage_footer(report, curation),
    }
    sections_html = [builders[key]() for key in _SECTION_ORDER if key in wanted]
    if appendix:
        sections_html.append(build_appendix(analytics, currency, konto_navne))

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
        '<p class="report-subtitle">Deterministisk gennemgang af bogføringen mod de danske momsregler '
        f'(rapportniveau {niveau}). Se afsnittet "Datagrundlag &amp; metode" for forbehold og '
        'forudsætninger.</p>',
        *sections_html,
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
    parser.add_argument("--curation", default=None,
                         help="Sti til kurationsfil (JSON). Findes filen ikke: seedes med "
                              "auto-forslag og skrives. Findes den: rådgiverens indhold vinder, "
                              "nye tema-grupper tilføjes uden at røre eksisterende (se "
                              "tools/report_curation.py). Udelades: kuration bruges kun in-memory "
                              "(intet gemmes).")
    parser.add_argument("--niveau", type=int, choices=(1, 2, 3), default=3,
                         help="1=Basis, 2=Standard, 3=Fuld rådgivning (default). Se modulets docstring.")
    parser.add_argument("--workbook", default=None,
                         help="Sti til Excel-arbejdsbilag (.xlsx) — rådgiverens fulde fundgrundlag "
                              "med kontrolnumre, alle severities. Udelades: intet workbook skrives.")
    parser.add_argument("--appendix", action="store_true",
                         help="Vedhæft det tidligere aggregerede fundtabel-appendix (viser "
                              "kontrolnumre) efter lineage-footeren. Default FRA i kundevisningen.")
    args = parser.parse_args(argv)

    try:
        report = load_report(args.rapport_json)
    except (OSError, json.JSONDecodeError) as e:
        print(f"FEJL: kunne ikke læse {args.rapport_json}: {e}", file=sys.stderr)
        return 1

    data_contract = _load_data_contract()
    analytics = _analytics(report)
    all_findings = analytics.get("all_findings") or []

    curation, curation_written = report_curation.resolve_curation(args.curation, all_findings)

    html_out = render_html(report, data_contract, niveau=args.niveau, curation=curation,
                            appendix=args.appendix)

    out_dir = os.path.dirname(os.path.abspath(args.out))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_out)

    size_kb = os.path.getsize(args.out) / 1024
    print(f"Skrev {args.out} ({size_kb:.1f} KB) — niveau {args.niveau} ({NIVEAU_PRODUKTPAKKE[args.niveau]})")
    n_groups = len(curation.get("grupper", {}))
    n_promoted = len(report_curation.promoted_groups(curation))
    print(f"Kuration: {n_groups} tema-grupper ({n_promoted} medtaget)"
          + (f" — gemt til {args.curation}" if curation_written else " — ikke gemt (ingen --curation angivet)"))

    if args.workbook:
        from tools import report_workbook
        report_workbook.build_workbook(report, curation, args.workbook)
        wb_size_kb = os.path.getsize(args.workbook) / 1024
        print(f"Skrev {args.workbook} ({wb_size_kb:.1f} KB)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
