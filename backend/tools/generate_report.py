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
from collections import Counter, defaultdict

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
    # Fix-runde 2026-09-22 (Bal-godkendt, FEJL 1): afgiftskoder (fx elafgift)
    # -- egen "energy_taxes"-rubrik i angivelsen, som v1 bevidst ikke
    # afstemmer (se cat10_vat_reconciliation.py).
    "energy_tax": "Afgift (fx elafgift — egen rubrik, afstemmes ikke i v1)",
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


def _fmt_date(value) -> str:
    """ISO-dato ('YYYY-MM-DD', evt. med klokkeslæt) -> dansk 'dd.mm.åååå'.
    Rent visuelt (byggetrin ~10-designløft) — ukendt/uparsérbart format
    vises som modtaget i stedet for at kaste en exception."""
    if not value:
        return ""
    s = str(value)
    try:
        y, m, d = s[:10].split("-")
        if len(y) == 4 and len(m) == 2 and len(d) == 2:
            return f"{d}.{m}.{y}"
    except ValueError:
        pass
    return s


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

def build_hero(report: dict, analytics: dict, currency: str, niveau: int = 3) -> str:
    """Rapportens forside (byggetrin ~10-designløft): en mørk 'cover'-panel
    med titel, kunde-/periode-pladsholder og diskret BALAI-afsender, fire
    nøgletals-fliser og — når data findes — to fremhævede fliser for
    momsgennemstrømning og årets tilsvar. Rent typografi/layout — samme
    tal/logik som før redesignet."""
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

    parts = ['<section class="hero">']
    parts.append('<div class="hero-kicker">Momsanalyse &middot; Resultatrapport</div>')
    parts.append('<h2 class="hero-title">Jeres moms — set gennem data</h2>')

    period_label = " – ".join(b for b in (_fmt_date(summary.get("period_start")),
                                           _fmt_date(summary.get("period_end"))) if b)
    meta_bits = ["[Kundenavn]"]
    if period_label:
        meta_bits.append(f"Periode: {period_label}")
    meta_bits.append(NIVEAU_PRODUKTPAKKE.get(niveau, "").split(" — ")[0] or f"Niveau {niveau}")
    meta_bits.append("Udarbejdet af BALAI VAT Analytics")
    parts.append(f'<p class="hero-meta">{" &middot; ".join(_esc(b) for b in meta_bits)}</p>')

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

    if cards:
        parts.append('<div class="hero-cards">' + "".join(cards) + "</div>")
    else:
        parts.append('<p class="anchor-missing">Intet datagrundlag tilgængeligt for denne rapport.</p>')

    flow_tiles = []
    if datapunkter is not None:
        flow_tiles.append(("Estimerede datapunkter analyseret", fmt_int(datapunkter),
                            "Groft estimat (linjer × typiske felter pr. linje) — ikke en præcis optælling."))
    if gennemstroemning is not None:
        flow_tiles.append(("Momsgennemstrømning i perioden", fmt_amount(gennemstroemning, currency),
                            "Udgående moms + købsmoms (numerisk sum, uanset fortegn)."))
    if tilsvar is not None:
        fortegn = "tilsvar (betales)" if tilsvar >= 0 else "til gode (refusion)"
        flow_tiles.append(("Årets momstilsvar", f"{fmt_amount(tilsvar, currency)} ({fortegn})",
                            "Udgående moms minus indgående moms (inkl. omvendt betalingspligt), beregnet af bogføringen."))

    if flow_tiles:
        parts.append('<div class="hero-flow">')
        for label, value, note in flow_tiles:
            parts.append(
                '<div class="hero-flow-tile">'
                f'<div class="hero-flow-value">{value}</div>'
                f'<div class="hero-flow-label">{_esc(label)}</div>'
                f'<div class="hero-flow-note">{_esc(note)}</div>'
                '</div>'
            )
        parts.append('</div>')
    elif cards:
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


def _rubric_label_for_code(tax_code: str, vat_calculation_type: str,
                            tax_percentage=None) -> str:
    rubric = _cat10.classify_purchase_rubric(tax_code, vat_calculation_type, tax_percentage)
    return _PURCHASE_RUBRIC_LABELS.get(rubric, "Ukendt")


# "energy_tax" (fix-runde 2026-09-22) medtages BEVIDST ikke i
# _RUBRIC_FLOW_ORDER — diagrammet illustrerer kun de rubrikker kontrol 82
# rent faktisk AFSTEMMER (se _PURCHASE_RUBRIC_LABELS ovenfor for at koden
# stadig får en meningsfuld label i den detaljerede tabel nedenfor).
_RUBRIC_FLOW_ORDER = ("dkrc", "rc_services", "input")
_RUBRIC_FLOW_COLORS = {"dkrc": "#2d5aa0", "rc_services": "#17a2b8", "input": "#1B365D"}
_RUBRIC_DIAGRAM_LABELS = {"dkrc": "Udg. moms (RC)", "rc_services": "RC-ydelser, udland", "input": "Alm. moms"}


def _engine_diagram_html(tax_rows: list) -> str:
    """Håndbygget, pædagogisk flowdiagram (ingen eksterne biblioteker):
    momskoder -> momskonti -> rubrikker i angivelsen. Forenklet MED VILJE
    (proportionalt pr. rubrik, ikke en literal kode-for-kode-tegning) — den
    fulde, præcise kode-for-kode-opsætning står i tabellen ved siden af.
    Tom streng hvis der intet er at tegne (0 kendte rubrikker) — kaldestedet
    springer da diagrammet helt over."""
    rubric_counts: dict = {}
    accounts: set = set()
    for row in tax_rows:
        rubric = _cat10.classify_purchase_rubric(row.get("tax_code", ""), row.get("vat_calculation_type", ""),
                                                   row.get("tax_percentage"))
        rubric_counts[rubric] = rubric_counts.get(rubric, 0) + 1
        for key in ("sales_vat_account", "purchase_vat_account", "reverse_charge_vat_account"):
            acc = row.get(key)
            if acc:
                accounts.add(str(acc))

    present = [(r, rubric_counts[r]) for r in _RUBRIC_FLOW_ORDER if rubric_counts.get(r)]
    total_codes = len(tax_rows)
    if not present or total_codes == 0:
        return ""

    node_w, node_h, gap, top = 148, 56, 22, 18
    n = len(present)
    height = max(190, n * (node_h + gap) - gap + 2 * top)
    width = 700
    xa, xb, xc = 24, 276, 528
    ya = yb = height / 2 - node_h / 2
    c_ys = [top + i * (node_h + gap) for i in range(n)]

    def _box(x, y, label, sub, color):
        return (
            f'<rect x="{x}" y="{y:.1f}" width="{node_w}" height="{node_h}" rx="10" fill="{color}"/>'
            f'<text x="{x + node_w / 2:.1f}" y="{y + node_h / 2 - 5:.1f}" text-anchor="middle" '
            f'font-size="12.5" font-weight="700" fill="#ffffff">{_esc(label)}</text>'
            f'<text x="{x + node_w / 2:.1f}" y="{y + node_h / 2 + 13:.1f}" text-anchor="middle" '
            f'font-size="11" fill="#dbe4f2">{_esc(sub)}</text>'
        )

    def _flow(x1, y1, x2, y2, w, color):
        mx = (x1 + x2) / 2
        return (
            f'<path d="M{x1:.1f},{y1:.1f} C{mx:.1f},{y1:.1f} {mx:.1f},{y2:.1f} {x2:.1f},{y2:.1f}" '
            f'fill="none" stroke="{color}" stroke-width="{w:.1f}" stroke-linecap="round" opacity="0.5"/>'
        )

    svg = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" '
        'aria-label="Momsmotoren: momskoder til momskonti til rubrikker i momsangivelsen" '
        'xmlns="http://www.w3.org/2000/svg">'
    ]
    svg.append(_box(xa, ya, "Momskoder", f"{fmt_int(total_codes)} koder", "#1B365D"))
    svg.append(_box(xb, yb, "Momskonti", f"{fmt_int(len(accounts))} konti", "#2d5aa0"))
    svg.append(_flow(xa + node_w, ya + node_h / 2, xb, yb + node_h / 2,
                      max(3.0, min(20.0, total_codes)), "#2d5aa0"))
    legend_rows = []
    for (rubric, count), cy in zip(present, c_ys):
        color = _RUBRIC_FLOW_COLORS.get(rubric, "#6B7280")
        w = max(3.0, min(20.0, 3 + 17 * count / total_codes))
        svg.append(_flow(xb + node_w, yb + node_h / 2, xc, cy + node_h / 2, w, color))
        svg.append(_box(xc, cy, _RUBRIC_DIAGRAM_LABELS.get(rubric, rubric),
                         f"{fmt_int(count)} kode" + ("r" if count != 1 else ""), color))
        full_label = _PURCHASE_RUBRIC_LABELS.get(rubric, rubric)
        legend_rows.append(
            f'<li><span class="legend-dot" style="background:{color};"></span>'
            f'{_esc(full_label)} — {fmt_int(count)} momskode' + ("r" if count != 1 else "") + '</li>'
        )
    svg.append("</svg>")

    return (
        '<div class="engine-diagram">'
        + "".join(svg)
        + '<ul class="engine-legend">' + "".join(legend_rows) + "</ul>"
        + '<p class="diagram-caption">Forenklet oversigt — stregernes tykkelse afspejler antallet af '
          "momskoder pr. rubrik, ikke den enkelte kodes beløb. Se tabellen nedenfor for den præcise, "
          "kode-for-kode-opsætning.</p>"
        + "</div>"
    )


def build_engine_section(report: dict, konto_navne: dict) -> str:
    tax_rows = report.get("tax_table_oversigt") or []
    parts = ['<section class="engine">', "<h2>Momsmotoren</h2>"]
    parts.append(
        "<p>Jeres momsopsætning kobler hver momskode til én eller flere momskonti og til en "
        "rubrik i momsangivelsen (udgående moms, RC-ydelser fra udlandet, eller almindelig "
        "moms ved salg/køb). Diagrammet og tabellen nedenfor er genereret automatisk ud fra "
        "jeres egen opsætning — ikke skrevet i hånden — så de altid afspejler den faktiske "
        "konfiguration.</p>"
    )
    if not tax_rows:
        parts.append(
            '<p class="anchor-missing">Ingen detaljeret momsopsætning var tilgængelig for denne '
            "rapport — dette afsnit kræver kundens egen momskode-opsætning som sidecar-data.</p>"
        )
        parts.append("</section>")
        return "\n".join(parts)

    parts.append(_engine_diagram_html(tax_rows))

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
        rubric_label = _rubric_label_for_code(code, calc_type, row.get("tax_percentage"))
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


_RECON_STATUS_COLORS = {
    "match": "#28a745",
    "timing": "#17a2b8",
    "afvigelse": "#dc3545",
    "ingen_angivelse": "#adb5bd",
}


def _recon_barchart_svg(rubric_label: str, rubric_key: str, periods: list, currency: str) -> str:
    """Håndbygget søjlediagram (beregnet vs. angivet) pr. periode for én
    rubrik — ingen eksterne biblioteker. Søjlefarven følger cellens status
    (grøn=match/gul-blå=timing/rød=afvigelse), så 'alt stemmer' kan aflæses
    på ét blik uden at læse tabellen celle for celle. Tom streng hvis
    rubrikken ikke har nogen udfyldt celle i nogen periode."""
    cells = []
    for p in periods:
        cell = (p.get("rubrikker") or {}).get(rubric_key) or {}
        ber, ang = cell.get("beregnet"), cell.get("angivet")
        if ber is None and ang is None:
            continue
        cells.append((p.get("periode", ""), ber, ang, cell.get("status", "ingen_angivelse")))
    if not cells:
        return ""

    vmax = max((abs(v) for _, ber, ang, _ in cells for v in (ber, ang) if v is not None), default=0.0)
    vmax = vmax or 1.0
    bar_w, bar_gap, group_gap, chart_h, top = 9, 2, 16, 90, 8
    group_w = bar_w * 2 + bar_gap + group_gap
    width = max(260, len(cells) * group_w + 20)
    height = top + chart_h + 30
    baseline = top + chart_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" role="img" '
        f'aria-label="Beregnet vs. angivet pr. periode for {_esc(rubric_label)}" '
        'xmlns="http://www.w3.org/2000/svg">',
        f'<line x1="6" y1="{baseline}" x2="{width - 6}" y2="{baseline}" stroke="#d7dbe3" stroke-width="1"/>',
    ]
    for i, (label, ber, ang, status) in enumerate(cells):
        color = _RECON_STATUS_COLORS.get(status, "#adb5bd")
        gx = 10 + i * group_w
        for j, val in enumerate((ber, ang)):
            if val is None:
                continue
            h = max(1.0, (abs(val) / vmax) * chart_h)
            x = gx + j * (bar_w + bar_gap)
            y = baseline - h
            opacity = "1" if j == 0 else "0.5"
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w}" height="{h:.1f}" rx="2" '
                f'fill="{color}" opacity="{opacity}"/>'
            )
        parts.append(
            f'<text x="{gx + bar_w:.1f}" y="{baseline + 12}" font-size="8.5" fill="#6B7280" '
            f'text-anchor="middle">{_esc(label)}</text>'
        )
    parts.append("</svg>")
    return (
        '<div class="recon-chart-block">'
        f'<div class="recon-chart-title">{_esc(rubric_label)}</div>'
        + "".join(parts)
        + '<p class="recon-chart-hint">Mørk søjle = beregnet af bogføringen, lys søjle = angivet. '
          "Farven følger status i tabellen ovenfor.</p>"
        + "</div>"
    )


def _build_recon_charts(decl: dict, rubrics: list, labels: dict, currency: str) -> str:
    periods = decl.get("perioder") or []
    if len(periods) < 2:
        return ""
    blocks = [_recon_barchart_svg(labels.get(r, r), r, periods, currency) for r in rubrics]
    blocks = [b for b in blocks if b]
    if not blocks:
        return ""
    legend = "".join(
        f'<li><span class="legend-dot" style="background:{color};"></span>{_esc(_RUBRIC_STATUS_LABELS.get(key, key))}</li>'
        for key, color in _RECON_STATUS_COLORS.items()
    )
    return (
        '<h3>Beregnet vs. angivet, pr. rubrik hen over året</h3>'
        '<ul class="engine-legend recon-legend">' + legend + '</ul>'
        '<div class="recon-charts">' + "".join(blocks) + '</div>'
    )


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
        udeladt = decl.get("udeladte_nul_perioder") or {}
        if udeladt.get("antal"):
            parts.append(
                f"<p class='section-hint'>{udeladt['antal']} perioder uden angivelse og uden "
                f"beregnet moms ({_esc(udeladt.get('foerste'))} – {_esc(udeladt.get('sidste'))}) "
                "er udeladt af tabellen — de stammer fra enkeltposteringer med momsdato uden for "
                "analyseåret (typisk korrektioner) og påvirker ikke afstemningen.</p>"
            )
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
        parts.append(_build_recon_charts(decl, rubrics, labels, currency))
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
        parts.append(f'<span class="obs-theme-badge">{_esc(g.get("tema_navn", ""))}</span>')
        parts.append('<p class="obs-eyebrow">Spørgsmål</p>')
        parts.append(f'<h3 class="obs-question">{_esc(g.get("spoergsmaal", ""))}</h3>')
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
  --navy-dark: #142944;
  --blue: #2d5aa0;
  --blue-light: #4a7fc4;
  --bg: #f0f2f5;
  --card: #ffffff;
  --text: #1a1a2e;
  --muted: #6B7280;
  --border: #e5e7eb;
  --border-strong: #d7dbe3;
  --green: #1e7e34;
  --green-bg: #eaf5ee;
  --amber: #8a6d3b;
  --amber-bg: #fff8e1;
  --red: #b02a37;
  --content-max: 880px;

  /* Typografisk skala (byggetrin ~visuelt-loeft): forside-titel -> sektion -> brødtekst -> metadata */
  --fs-hero-title: 2.05rem;
  --fs-hero-kicker: 0.74rem;
  --fs-h2: 1.2rem;
  --fs-h3: 1.02rem;
  --fs-body: 0.93rem;
  --fs-meta: 0.78rem;
}
* { box-sizing: border-box; }
html { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  background: var(--bg); color: var(--text); margin: 0; padding: 0;
  font-size: var(--fs-body); line-height: 1.6; -webkit-font-smoothing: antialiased;
}
.report-banner { background: #fff; border-bottom: 1px solid var(--border); padding: 12px 20px; }
.report-banner-inner { max-width: var(--content-max); margin: 0 auto; display: flex; align-items: center; gap: 12px; }
.report-mark { width: 30px; height: 30px; border-radius: 6px; background: var(--navy); color: #fff;
  display: inline-flex; align-items: center; justify-content: center; font-weight: 700; font-size: 14px; flex: none; }
.report-brand-top { font-weight: 600; font-size: 13px; color: var(--navy); display: block; }
.report-brand-sub { font-weight: 400; font-size: 11px; color: var(--muted); }
.container { max-width: var(--content-max); margin: 0 auto; padding: 28px 20px 64px; }
section, footer { background: var(--card); border-radius: 14px; padding: 26px 28px; margin-bottom: 22px;
  box-shadow: 0 2px 10px rgba(20,30,50,0.07); border: 1px solid rgba(20,30,50,0.03); }
h2 { font-size: var(--fs-h2); font-weight: 700; color: var(--navy); margin: 0 0 16px; border-bottom: 2px solid var(--border);
  padding-bottom: 10px; letter-spacing: -0.01em; }
h3 { font-size: var(--fs-h3); font-weight: 700; color: var(--blue); margin: 20px 0 8px; }
p { line-height: 1.6; margin: 0 0 10px; }
p:last-child { margin-bottom: 0; }
.section-hint, .anchor-note, .control-meta { color: var(--muted); font-size: var(--fs-meta); }
.anchor-missing, .no-findings { color: var(--muted); font-style: italic; }
.status-badge { display: inline-block; padding: 3px 11px; border-radius: 20px; font-size: 0.71rem;
  font-weight: 700; letter-spacing: 0.01em; }

/* --- Tabeller: kompakt, zebra-ramt hvor det giver overblik --- */
.table-scroll { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 0.85rem; }
th, td { padding: 7px 10px; text-align: right; border-bottom: 1px solid var(--border); white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
thead th { color: var(--muted); font-weight: 600; font-size: var(--fs-meta); text-transform: uppercase;
  letter-spacing: 0.02em; border-bottom-color: var(--border-strong); }
tfoot td { border-top: 2px solid var(--border-strong); border-bottom: none; }
.recon-table th, .recon-table td { font-size: 0.8rem; }
.evidence-table { font-size: 0.8rem; }
.evidence-table tbody tr:nth-child(even) { background: #f6f7f9; }
.account-table tbody tr:nth-child(even) { background: #f6f7f9; }

/* --- Hero: forside --- */
.hero { background: linear-gradient(135deg, var(--navy) 0%, var(--navy-dark) 100%); color: #fff;
  padding: 40px 34px 32px; border: none; }
.hero-kicker { text-transform: uppercase; letter-spacing: 0.14em; font-size: var(--fs-hero-kicker);
  font-weight: 700; color: #aebedb; margin-bottom: 10px; }
h2.hero-title { color: #fff; font-size: var(--fs-hero-title); font-weight: 800; border-bottom: none;
  padding-bottom: 0; margin: 0 0 10px; letter-spacing: -0.02em; line-height: 1.15; }
.hero-meta { color: #c7d2e6; font-size: 0.86rem; margin-bottom: 26px; }
.hero .anchor-missing, .hero .anchor-note { color: #c7d2e6; }
.hero-cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 18px; }
.hero-card { border-radius: 10px; padding: 16px 12px; text-align: center; background: rgba(255,255,255,0.08);
  border: 1px solid rgba(255,255,255,0.14); }
.hero-card-value { font-size: 1.55rem; font-weight: 800; color: #fff; }
.hero-card-label { font-size: var(--fs-meta); font-weight: 600; color: #c7d2e6; text-transform: uppercase;
  letter-spacing: 0.02em; margin-top: 2px; }
.hero-flow { display: grid; grid-template-columns: repeat(2, 1fr); gap: 14px; margin-top: 4px; }
.hero-flow-tile { background: rgba(255,255,255,0.12); border: 1px solid rgba(255,255,255,0.18);
  border-radius: 12px; padding: 18px 20px; }
.hero-flow-value { font-size: 1.7rem; font-weight: 800; color: #fff; line-height: 1.1; }
.hero-flow-label { font-size: var(--fs-meta); font-weight: 600; color: #d7e0f0; text-transform: uppercase;
  letter-spacing: 0.02em; margin-top: 4px; }
.hero-flow-note { font-size: 0.74rem; color: #a9b8d6; margin-top: 6px; }

/* --- Momsmotoren: diagram + legend --- */
.engine-diagram { background: #f6f8fb; border: 1px solid var(--border); border-radius: 12px;
  padding: 18px 18px 14px; margin: 4px 0 20px; }
.engine-diagram svg { display: block; }
.engine-legend { list-style: none; margin: 12px 0 0; padding: 0; display: flex; flex-wrap: wrap; gap: 8px 20px; }
.engine-legend li { font-size: 0.82rem; color: var(--text); display: flex; align-items: center; gap: 7px; }
.legend-dot { display: inline-block; width: 10px; height: 10px; border-radius: 3px; flex: none; }
.diagram-caption { font-size: var(--fs-meta); color: var(--muted); font-style: italic; margin-top: 10px; }
.engine-table th, .engine-table td { font-size: 0.82rem; white-space: normal; }

/* --- Afstemningen: søjlediagrammer --- */
.recon-legend { margin-bottom: 6px; }
.recon-charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; margin-top: 10px; }
.recon-chart-block { background: #f6f8fb; border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
.recon-chart-block svg { display: block; }
.recon-chart-title { font-size: 0.85rem; font-weight: 700; color: var(--navy); margin-bottom: 6px; }
.recon-chart-hint { font-size: 0.72rem; color: var(--muted); font-style: italic; margin: 8px 0 0; }

/* --- Observationer & spørgsmål --- */
.obs-group { border-left: 4px solid var(--blue); background: #fafbfc; border-radius: 8px;
  padding: 16px 18px; margin-bottom: 18px; }
.obs-theme-badge { display: inline-block; font-size: 0.68rem; font-weight: 700; text-transform: uppercase;
  color: var(--blue); letter-spacing: 0.04em; background: #e7edf7; padding: 3px 10px; border-radius: 20px;
  margin-bottom: 10px; }
.obs-eyebrow { font-size: var(--fs-meta); font-weight: 600; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; margin: 0 0 2px; }
.obs-question { margin-top: 0; font-size: 1.08rem; }
.obs-recommendation, .advisor-note { border-radius: 8px; padding: 12px 16px; font-size: 0.88rem;
  border-left: 4px solid; margin-top: 12px; }
.obs-recommendation { background: var(--green-bg); border-left-color: var(--green); }
.advisor-note { background: var(--amber-bg); border-left-color: var(--amber); }
.reco-list, .gate-list { padding-left: 20px; line-height: 1.8; }
.conclusion { background: #eef2f8; border-radius: 8px; padding: 12px 16px; border-left: 4px solid var(--blue); }
.control-block { border-left: 4px solid var(--blue); background: #fafbfc; border-radius: 8px;
  padding: 14px 16px; margin-bottom: 14px; }
.control-block h3 { margin-top: 0; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
.account-table { font-size: 0.82rem; }
.account-name { color: #6B7280; font-weight: normal; }
.more-note { color: var(--muted); font-size: 0.82rem; font-style: italic; }

/* --- Sporbarhed --- */
.lineage-facts { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px 24px; font-size: 0.85rem; }
.lineage-facts dt { color: var(--muted); }
.lineage-facts dd { margin: 0 0 6px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.82rem; }
.ai-provenance, .lineage-refs { font-size: 0.82rem; color: var(--muted); margin-top: 14px; border-top: 1px solid var(--border);
  padding-top: 12px; }

.print-footer { display: none; }

/* --- Print/PDF-kvalitet --- */
@page { size: A4; margin: 16mm 14mm; }
@media print {
  body { background: #fff; font-size: 11.5px; }
  .report-banner { display: none; }
  .container { padding: 0; max-width: none; }
  section, footer { box-shadow: none; border: 1px solid var(--border); break-inside: avoid; }
  section:not(.hero) { break-before: page; }
  .hero { break-inside: avoid; -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  .control-block, .obs-group, .hero-card, .hero-flow-tile, .recon-chart-block, .engine-diagram { break-inside: avoid; }
  table { break-inside: auto; }
  tr { break-inside: avoid; }
  .print-footer { display: flex; position: fixed; bottom: 6mm; left: 0; right: 0; justify-content: space-between;
    font-size: 8px; color: #8a93a6; border-top: 1px solid var(--border-strong); padding-top: 4px; }
}
@media (max-width: 640px) {
  .hero-cards, .hero-flow, .lineage-facts { grid-template-columns: 1fr; }
  .hero { padding: 28px 20px 24px; }
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
        "hero": lambda: build_hero(report, analytics, currency, niveau),
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

    lineage = report.get("lineage") or {}
    footer_bits = ["BALAI VAT Analytics — Momsanalyse, resultatrapport"]
    if lineage.get("generated_at"):
        footer_bits.append(str(lineage["generated_at"])[:10])
    print_footer = (
        '<div class="print-footer">'
        f'<span>{_esc(" · ".join(footer_bits))}</span>'
        '<span>[Kundenavn]</span>'
        '</div>'
    )

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
        *sections_html,
        '</div>',
        print_footer,
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
