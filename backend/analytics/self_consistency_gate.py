"""
Selvkonsistens-gaten — "momskonto-krydstjekket" (byggetrin ~12, Bal-godkendt
2026-09-23).

Baggrund (kontrol 82-sagen, 2026-09-22, se docs/CHANGELOG.md): motorens
rubrik-sum afveg 326 t.kr. fra virkeligheden, og det blev kun opdaget, fordi
KUNDENS EGEN 3-vejs-afstemning fandtes som facit at holde motoren op imod. Den
metode, der fandt fejlen dengang, kan automatiseres, fordi momskodeopsætningen
allerede bærer kontoreferencerne (``tax_table[].sales_vat_account``/
``purchase_vat_account``/``reverse_charge_vat_account``): motoren kan
krydstjekke sine EGNE beregnede rubrikker mod de FAKTISKE posteringer på
kundens momskonti — uden et ekspert-facit.

Kilde-af-sandhed-disciplin (ingen parallel logik):
  - De beregnede rubrik-summer kommer PRÆCIS fra
    ``cat10_vat_reconciliation.compute_period_rubrics`` (samme funktion
    kontrol 82 selv bruger, offentliggjort som alias til formålet) — ikke en
    gendannet kopi.
  - Rubrik-klassifikationen pr. momskode kommer PRÆCIS fra
    ``cat10_vat_reconciliation.classify_purchase_rubric`` (samme funktion
    kunderapportens 'Momsmotoren'-sektion allerede bruger).

Denne gate er IKKE en ny nummereret kontrol (ingen finding, intet fund) —
samme filosofi som ``analytics/reconciliation_gate.py``: et selvstændigt,
informativt lag, der ALDRIG blokerer analysen og ALDRIG ændrer fund-billedet.
Den er wiret ind i ``analytics.engine.run_all_tests`` som en rapport-blok
(``report["intern_momskonto_afstemning"]``), samme sted som
``declaration_reconciliation`` — se ``engine.py``.

"Ikke målbar"-disciplin (samme som resten af motoren, jf.
``analytics/readiness.py``): mangler kontoreferencerne i vat_setup (fx en
IFS-mapping uden ext_sales_vat_account/ext_purchase_vat_account/
ext_reverse_charge_vat_account), er der intet at krydstjekke mod — gaten
rapporterer ÉN ærlig "ikke målbar"-status, ingen støj, intet gæt. Aktiveres
AUTOMATISK, den dag felterne leveres (ingen kodeændring nødvendig) — samme
"nøglesæt-symmetri før værdi-symmetri"-mønster som resten af canonical_
masterdata.py's berigelser.

VAT-afregningsbatchen (empirisk undersøgt på BC-datasættet FØR reglen blev
fastlagt, jf. opgavens "empirisk-først"-krav): kunder posterer periodisk en
batch, der NULSTILLER momskontiene ved periodeafslutning. Denne batch bærer
IKKE konsekvent ``supply_direction="settlement"`` på tværs af alle berørte
konti (kun på nogle) — det pålidelige, gennemgående signal er
``source_code`` (BC/NAV's "MOMSAFREGN", se materiality.
SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES). Begge signaler bruges (OR),
så en linje udelades hvis ENTEN er til stede — verificeret empirisk til at
give et øre-nøjagtigt match for alle 12 måneder 2025 på BC-datasættet.
"""

from __future__ import annotations

from analytics import materiality
from analytics.categories.cat10_vat_reconciliation import (
    compute_period_rubrics,
    classify_purchase_rubric,
)

STATUS_OK = "bestaaet"
STATUS_DEVIATION = "afvigelse"
STATUS_NOT_MEASURABLE = "ikke_maalbar"

_CELL_MATCH = "match"
_CELL_DEVIATION = "afvigelse"
_CELL_NOT_MEASURABLE = "ikke_maalbar"

# Rækkefølge/nøgler -- samme fire nøgler som
# cat10_vat_reconciliation.compute_period_rubrics returnerer pr. periode
# (de tre deklarations-rubrikker + den rå energy_tax-sum, byggetrin ~12).
RUBRICS = ("output_vat", "rc_services", "input_vat", "energy_tax")

RUBRIC_LABELS = {
    "output_vat": "Udgående moms (salgsmoms + indenlandsk omvendt betalingspligt)",
    "rc_services": "RC-ydelser fra udlandet (omvendt betalingspligt, ydelser)",
    "input_vat": "Indgående moms (købsmoms)",
    "energy_tax": "Afgifter (fx elafgift)",
}

# Kredit-naturede rubrikker (liability -- BC/NAV-konventionen: debet positiv,
# kredit negativ på ét signeret beløbsfelt, samme antagelse som
# reconciliation_gate.py's _net_amount_per_account dokumenterer). Bogført
# nettobeløb på disse rubrikkers konti er derfor NEGATIVT og skal negeres for
# at sammenlignes med den positive, beregnede rubrik-sum. De øvrige (debet-
# naturede: input_vat/energy_tax) bruges som de er.
_CREDIT_NATURED = {"output_vat", "rc_services"}


def _rubric_accounts(data: dict) -> dict:
    """{rubrik: {kontonumre}} -- udledt AF tax_table's egne kontoreference-
    felter, klassificeret med PRÆCIS samme funktion som kontrol 82/
    Momsmotoren (``classify_purchase_rubric``). Ingen kontoplan-gæt: et
    tomt felt bidrager intet.

    - ``sales_vat_account`` hører ALTID til output_vat (en salgslinje
      klassificeres ikke af _purchase_rubric -- den rammer sales_vat_account
      uanset koden, jf. cat10's ``_line_direction``/rubrik-formel).
    - ``reverse_charge_vat_account`` hører til output_vat for 'dkrc'-koder
      (indenlandsk omvendt betalingspligt -- liability-siden er en del af
      UDGÅENDE moms) og til rc_services for 'rc_services'-koder. For
      RC-VAREKØB (klassificeret 'input', jf. _purchase_rubric's dokumenterede
      begrænsning) medtages reverse_charge_vat_account BEVIDST IKKE her --
      empirisk bekræftet immateriel/urelateret konto på BC-datasættet, og
      koden er allerede fuldt repræsenteret i input_vat via dens
      purchase_vat_account.
    - ``purchase_vat_account`` hører til input_vat for alle ikke-energy_tax-
      koder (input/dkrc/rc_services deler samme fradragskonto i BC/NAV, jf.
      _compute_period_rubrics's "*_fradrag"-summer), og til energy_tax for
      'energy_tax'-koder."""
    accounts: dict = {r: set() for r in RUBRICS}
    for entry in data.get("tax_table", []):
        code = entry.get("tax_code", "")
        rubric = classify_purchase_rubric(
            code, entry.get("vat_calculation_type", ""), entry.get("tax_percentage")
        )
        sale_acc = (entry.get("sales_vat_account") or "").strip()
        purchase_acc = (entry.get("purchase_vat_account") or "").strip()
        rc_acc = (entry.get("reverse_charge_vat_account") or "").strip()

        if sale_acc:
            accounts["output_vat"].add(sale_acc)

        if rubric == "energy_tax":
            if purchase_acc:
                accounts["energy_tax"].add(purchase_acc)
            continue

        if purchase_acc:
            accounts["input_vat"].add(purchase_acc)
        if rubric == "dkrc" and rc_acc:
            accounts["output_vat"].add(rc_acc)
        elif rubric == "rc_services" and rc_acc:
            accounts["rc_services"].add(rc_acc)
    return accounts


def _is_settlement_line(line: dict) -> bool:
    """VAT-afregningsbatch-postering -- udelades af krydstjekket (se
    modulets docstring for den empiriske begrundelse for de to signaler)."""
    if (line.get("supply_direction") or "").strip().lower() == "settlement":
        return True
    source = (line.get("source_code") or "").strip().upper()
    return source in materiality.SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES


def _gl_totals_per_period(data: dict, rubric_accounts: dict) -> dict:
    """{periode: {rubrik: nettobeløb}} -- netto (debet-kredit) summeret over
    ikke-afregnings-linjer, for linjer hvis ``account_id`` er en af rubrikkens
    konti. Samme periode-nøgle-konvention ("YYYY-MM" fra
    transactions[].period_year/period) som compute_period_rubrics, så de to
    sider kan sammenlignes 1:1 pr. periode."""
    totals: dict = {}
    any_accounts = any(rubric_accounts.values())
    if not any_accounts:
        return totals
    for txn in data.get("transactions", []):
        year = (txn.get("period_year") or "").strip()
        month = (txn.get("period") or "").strip()
        if not year or not month:
            continue
        key = f"{year}-{month.zfill(2)}"
        for line in txn.get("lines", []):
            acc = line.get("account_id") or ""
            if not acc or _is_settlement_line(line):
                continue
            net = (line.get("debit_amount") or 0.0) - (line.get("credit_amount") or 0.0)
            if not net:
                continue
            for rubric, accs in rubric_accounts.items():
                if acc in accs:
                    bucket = totals.setdefault(key, {r: 0.0 for r in RUBRICS})
                    bucket[rubric] += net
    return totals


def _cell(computed: float, booked_net: float | None, rubric: str, tolerance: float,
          measurable: bool) -> dict:
    if not measurable:
        return {"beregnet": round(computed, 2), "bogfoert": None, "difference": None,
                "status": _CELL_NOT_MEASURABLE}
    booked = -booked_net if rubric in _CREDIT_NATURED else booked_net
    booked = booked or 0.0
    diff = round(computed - booked, 2)
    status = _CELL_MATCH if abs(diff) <= tolerance else _CELL_DEVIATION
    return {"beregnet": round(computed, 2), "bogfoert": round(booked, 2),
            "difference": diff, "status": status}


def evaluate(data: dict) -> dict:
    """Kør selvkonsistens-gaten. Fejler ALDRIG -- returnerer altid et dict,
    uanset datagrundlag (samme filosofi som reconciliation_gate.reconcile).

    Returnerer:
        {
          "status": STATUS_OK | STATUS_DEVIATION | STATUS_NOT_MEASURABLE,
          "message": "<menneskelæsbar status, dansk>",
          "tolerance": <float>, "materiality_pct": <float>,
          "rubrik_labels": {...},
          "rubrikker": {
              rubrik: {"konti": [...], "maalbar": bool,
                       "beregnet_aar", "bogfoert_aar", "difference_aar",
                       "status"},
              ...
          },
          "perioder": [{"periode": "2025-01", "rubrikker": {rubrik: _cell(...)}}],
        }
    """
    rubric_accounts = _rubric_accounts(data)
    measurable_rubrics = {r for r, accs in rubric_accounts.items() if accs}

    computed = compute_period_rubrics(data)
    all_periods = sorted(computed)

    tolerance = materiality.SELF_CONSISTENCY_TOLERANCE
    materiality_pct = materiality.SELF_CONSISTENCY_MATERIALITY_PCT

    if not measurable_rubrics:
        return {
            "status": STATUS_NOT_MEASURABLE,
            "message": (
                "Kan ikke krydstjekkes: jeres momsopsætning (vat_setup) bærer ikke "
                "kontoreferencer (sales_vat_account/purchase_vat_account/"
                "reverse_charge_vat_account) — selvkonsistens-gaten aktiveres "
                "automatisk, den dag de leveres."
            ),
            "tolerance": tolerance,
            "materiality_pct": materiality_pct,
            "rubrik_labels": dict(RUBRIC_LABELS),
            "rubrikker": {
                r: {"konti": [], "maalbar": False, "beregnet_aar": None,
                    "bogfoert_aar": None, "difference_aar": None,
                    "status": STATUS_NOT_MEASURABLE}
                for r in RUBRICS
            },
            "perioder": [],
        }

    gl_totals = _gl_totals_per_period(data, rubric_accounts)

    annual_computed = {r: 0.0 for r in RUBRICS}
    annual_booked_net = {r: 0.0 for r in RUBRICS}
    for key in all_periods:
        c = computed.get(key, {})
        g = gl_totals.get(key, {})
        for r in RUBRICS:
            annual_computed[r] += c.get(r, 0.0)
            annual_booked_net[r] += g.get(r, 0.0)

    rubrikker_out = {}
    deviating = []
    for r in RUBRICS:
        measurable = r in measurable_rubrics
        cell = _cell(annual_computed[r], annual_booked_net[r] if measurable else None,
                     r, tolerance, measurable)
        if measurable:
            cap = max(tolerance, materiality_pct / 100.0 * abs(annual_computed[r]))
            status = _CELL_MATCH if abs(cell["difference"]) <= cap else _CELL_DEVIATION
            cell["status"] = status
            if status == _CELL_DEVIATION:
                deviating.append(r)
        rubrikker_out[r] = {
            "konti": sorted(rubric_accounts[r]),
            "maalbar": measurable,
            "beregnet_aar": cell["beregnet"],
            "bogfoert_aar": cell["bogfoert"],
            "difference_aar": cell["difference"],
            "status": cell["status"],
        }

    periods_out = []
    for key in all_periods:
        c = computed.get(key, {})
        g = gl_totals.get(key, {})
        cells = {}
        for r in RUBRICS:
            measurable = r in measurable_rubrics
            cells[r] = _cell(c.get(r, 0.0), g.get(r, 0.0) if measurable else None,
                              r, tolerance, measurable)
        periods_out.append({"periode": key, "rubrikker": cells})

    not_measurable = [r for r in RUBRICS if r not in measurable_rubrics]
    if deviating:
        status = STATUS_DEVIATION
        labels = ", ".join(RUBRIC_LABELS[r] for r in deviating)
        message = (
            f"AFVIGELSE: motorens egne beregnede tal stemmer ikke med de bogførte "
            f"momskonti for: {labels}. Tallene for disse rubrikker bør ikke "
            "publiceres uden forbehold, før differencen er forklaret."
        )
    else:
        status = STATUS_OK
        message = (
            "Bestået: motorens beregnede rubrikker stemmer med de bogførte momskonti "
            f"(tolerance {tolerance:.2f}, materialitet {materiality_pct:.1f}%)."
        )
    if not_measurable:
        note_labels = ", ".join(RUBRIC_LABELS[r] for r in not_measurable)
        message += f" (Ikke målbar: {note_labels}.)"

    return {
        "status": status,
        "message": message,
        "tolerance": tolerance,
        "materiality_pct": materiality_pct,
        "rubrik_labels": dict(RUBRIC_LABELS),
        "rubrikker": rubrikker_out,
        "perioder": periods_out,
    }
