"""
Kategori 10: Indgående/Udgående Moms Afstemning (Tests 76-83)

Afstemmer købsmoms (indgående) og salgsmoms (udgående) på tværs af
regnskabet og mod momskonti, og afdækker usædvanlige forhold mellem dem.

Kontrol 80 (byggetrin ~9, Del B, Bal-godkendt 2026-09-17): aggregeret PR.
KONTO, ikke pr. postering — se test_80_revenue_without_output_vat.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr
from analytics import materiality


def run_reconciliation_tests(data: dict, declarations: dict = None) -> list:
    """``declarations``: den indberettede momsangivelse (valgfri, se
    ``analytics.vat_declarations.load_declarations``) — kun kontrol 82 bruger
    den. Uændret adfærd for alle andre kald (default None)."""
    findings = []
    findings.extend(test_76_input_output_ratio(data))
    findings.extend(test_77_vat_account_reconciliation(data))
    findings.extend(test_78_negative_vat_liability(data))
    findings.extend(test_79_input_vat_no_purchase(data))
    findings.extend(test_80_revenue_without_output_vat(data))
    findings.extend(test_81_zero_rated_share(data))
    findings.extend(test_82_period_declaration(data, declarations))
    findings.extend(test_83_partial_deduction(data))
    return findings


def _ref(txn, line, **extra):
    ref = {
        "transaction_id": txn["transaction_id"],
        "journal_id": txn["journal_id"],
        "date": txn["date"],
        "account_id": line["account_id"],
        "description": txn["description"],
        "amount": line["debit_amount"] + line["credit_amount"],
    }
    ref.update(extra)
    return ref


def _input_output_vat(data):
    """Returnér (input_vat, output_vat) ud fra debet-/kredit-siden af linjerne."""
    input_vat = 0.0
    output_vat = 0.0
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            if vat == 0:
                continue
            if (line.get("debit_amount", 0) or 0) > 0:
                input_vat += vat
            elif (line.get("credit_amount", 0) or 0) > 0:
                output_vat += vat
    return round(input_vat, 2), round(output_vat, 2)


# === TEST 76: Usædvanligt forhold købsmoms/salgsmoms ===

def test_76_input_output_ratio(data):
    findings = []
    input_vat, output_vat = _input_output_vat(data)
    if output_vat <= 0 and input_vat <= 0:
        return findings
    if output_vat > 0:
        ratio = input_vat / output_vat
        # Købsmoms over den kalibrerbare faktor x salgsmoms = vedvarende negativ position
        if ratio > materiality.INPUT_OUTPUT_RATIO:
            findings.append(make_finding(
                test_id=76, test_name="Højt købsmoms/salgsmoms-forhold",
                impact_type="interest_risk", direction="neutral", severity="medium",
                description=f"Købsmoms ({input_vat:.2f}) er {ratio:.1f}x salgsmoms ({output_vat:.2f}). "
                            f"Et vedvarende højt forhold giver store negative momsangivelser.",
                fix_suggestion="Store, vedvarende momsrefusioner tiltrækker kontrol. Bekræft at alle "
                               "salg er medtaget, og at fradragene er erhvervsmæssige.",
                transactions=[{"input_vat": input_vat, "output_vat": output_vat,
                               "ratio": round(ratio, 2), "highlighted_field": "tax_amount"}],
            ))
    elif input_vat > 0:
        findings.append(make_finding(
            test_id=76, test_name="Købsmoms uden salgsmoms",
            impact_type="interest_risk", direction="neutral", severity="medium",
            description=f"Der er købsmoms ({input_vat:.2f}) men ingen salgsmoms i perioden.",
            fix_suggestion="Manglende salgsmoms kan være korrekt (opstart/investering) men bør bekræftes.",
            transactions=[{"input_vat": input_vat, "output_vat": output_vat,
                           "highlighted_field": "tax_amount"}],
        ))
    return findings


# === TEST 77: Afstemning mod momskonto ===

def test_77_vat_account_reconciliation(data):
    """Sammenlign beregnet nettomoms med saldoen på en momskonto, hvis en
    sådan kan identificeres (kontonavn indeholder 'moms' ELLER 'vat',
    case-insensitivt).

    Udvidet 2026-09-17 (Del B, Bal-godkendt): kontonavns-heuristikken
    matchede tidligere KUN det danske 'moms' og kunne derfor aldrig ramme en
    engelsksproget kontoplan (fx BC/NAV-standardopsætning, hvor momskonti
    hedder "VAT payable"/"Output VAT" osv.). 'vat' tilføjes som et
    sideordnet, case-insensitivt mønster — uændret adfærd på danske
    kontoplaner (de matcher fortsat kun via 'moms', medmindre navnet
    tilfældigvis også indeholder 'vat')."""
    findings = []
    input_vat, output_vat = _input_output_vat(data)
    net_vat = round(output_vat - input_vat, 2)

    vat_accounts = [
        a for a in data.get("accounts", [])
        if vr.text_matches_any(a.get("description", ""), ("moms", "vat"))
    ]
    if not vat_accounts:
        return findings
    account_balance = sum((a.get("closing_balance", 0) or 0) for a in vat_accounts)
    if account_balance == 0:
        return findings  # Ingen saldoinformation (typisk ved Excel-import)

    diff = round(abs(account_balance) - abs(net_vat), 2)
    if abs(diff) > 1.0:
        findings.append(make_finding(
            test_id=77, test_name="Momskonto afstemmer ikke",
            impact_type="economic", direction="neutral", severity="high",
            description=f"Beregnet nettomoms ({net_vat:.2f}) afviger fra momskontoens saldo "
                        f"({account_balance:.2f}) med {diff:.2f}.",
            fix_suggestion="Afstem momskontoen mod de bogførte moms-posteringer og find differencen.",
            estimated_amount=abs(diff),
            transactions=[{"net_vat": net_vat, "account_balance": account_balance,
                           "difference": diff, "highlighted_field": "tax_amount"}],
        ))
    return findings


# === TEST 78: Negativt momstilsvar (refusion) ===

def test_78_negative_vat_liability(data):
    findings = []
    input_vat, output_vat = _input_output_vat(data)
    net = round(output_vat - input_vat, 2)
    if net < 0:
        findings.append(make_finding(
            test_id=78, test_name="Negativt momstilsvar",
            impact_type="interest_risk", direction="positive", severity="low",
            description=f"Perioden giver et negativt momstilsvar (refusion) på {abs(net):.2f} "
                        f"(salgsmoms {output_vat:.2f} − købsmoms {input_vat:.2f}).",
            fix_suggestion="Negativt tilsvar udløser udbetaling og kan medføre kontrol. Sikr at "
                           "dokumentationen er på plads.",
            estimated_amount=abs(net),
            transactions=[{"input_vat": input_vat, "output_vat": output_vat, "net": net,
                           "highlighted_field": "tax_amount"}],
        ))
    return findings


# === TEST 79: Købsmoms uden tilhørende køb ===

def test_79_input_vat_no_purchase(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            base = line["tax_base"] or 0
            debit = line.get("debit_amount", 0) or 0
            if vat > 0 and debit > 0 and base <= 0:
                findings.append(make_finding(
                    test_id=79, test_name="Købsmoms uden grundlag",
                    impact_type="economic", direction="negative", severity="high",
                    description=f"Købsmoms {vat:.2f} fratrukket på transaktion {txn['transaction_id']} "
                                f"uden et tilhørende momsgrundlag.",
                    fix_suggestion="Momsfradrag kræver et underliggende køb. Kontrollér grundlaget eller "
                                   "tilbagefør fradraget.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, tax_amount=vat, tax_base=base,
                                       highlighted_field="tax_base")],
                ))
    return findings


# === TEST 80: Salgsmoms mangler på indtægt ===
#
# AGGREGERET PR. KONTO (byggetrin ~9, Del B, Bal-godkendt 2026-09-17), IKKE
# pr. postering. Diagnose (verificeret manuelt): 24.152 per-posterings-fund
# fordelte sig på blot 77 konti; top-10 konti udgjorde 97 % af fundene
# (typisk interne allokeringskonti som 319160/381360, hvor "uden momskode"
# er en systematisk kontobrug, ikke N individuelle fejl). Den faglige
# beslutning ("er denne konto håndteret korrekt momsmæssigt?") træffes pr.
# KONTO, ikke pr. postering — så kontrollen udsteder nu ÉT fund pr. konto:
# kontonummer(+navn hvis kendt), antal kvalificerende posteringer, deres
# samlede grundlag, og andelen af KONTOENS posteringer (alle, ikke kun de
# kvalificerende) der mangler en momskode. Et lille udsnit af transaktions-
# referencer (materiality.CONTROL_80_MAX_REFS, default 10) følger med til
# drill-down; resten opsummeres i beskrivelsen.
#
# Dette er en BEVIDST granularitetsændring på tværs af ALLE input-veje
# (Bal-godkendt) — ikke en ny betingelse for hvornår kontrollen fyrer.
# Severity gradueres efter kontoens samlede beløb (materiality.
# CONTROL_80_HIGH_THRESHOLD/CONTROL_80_MEDIUM_THRESHOLD), så et fund på en
# konto med et beskedent beløb ikke vejer lige så tungt som ét på en konto
# med millionbeløb.

def test_80_revenue_without_output_vat(data):
    findings = []

    # Kontoens "andel uden momskode": nævner er ALLE linjer bogført på
    # kontoen (ikke kun dem der udløser kontrollen) — et generelt
    # datakvalitetssignal for, om manglende momskode er systematisk for
    # kontoen eller isoleret til få posteringer.
    account_line_totals = defaultdict(lambda: {"lines": 0, "no_code": 0})
    account_names = {}
    for acc in data.get("accounts", []):
        if acc.get("description"):
            account_names[acc.get("account_id")] = acc["description"]

    per_account = defaultdict(lambda: {"count": 0, "base_sum": 0.0, "refs": []})

    for txn in data["transactions"]:
        for line in txn["lines"]:
            acc_id = line.get("account_id")
            if acc_id:
                totals = account_line_totals[acc_id]
                totals["lines"] += 1
                if not line.get("tax_code"):
                    totals["no_code"] += 1

            credit = line.get("credit_amount", 0) or 0
            if credit <= 0:
                continue
            # Momsrelevans-scope: spring balanceposter over (betalinger, mellemregninger
            # m.v. er ikke momspligtig omsætning). Kun aktivt når kontotypen kendes (SAF-T).
            if vr.is_non_vat_account(line):
                continue
            vat = line["tax_amount"] or 0
            code = line["tax_code"]
            country = vr.normalize_country(line.get("country", ""))
            # Indtægt uden moms OG uden momskode OG uden udenlandsk forklaring
            if vat == 0 and not code and not vr.is_foreign(country) and credit >= 5000:
                bucket = per_account[acc_id]
                bucket["count"] += 1
                bucket["base_sum"] += credit
                if len(bucket["refs"]) < materiality.CONTROL_80_MAX_REFS:
                    bucket["refs"].append(_ref(txn, line, highlighted_field="tax_code"))

    for acc_id, bucket in per_account.items():
        totals = account_line_totals.get(acc_id, {"lines": 0, "no_code": 0})
        total_lines = totals["lines"] or bucket["count"]
        no_code_share = totals["no_code"] / total_lines if total_lines else 0.0
        base_sum = round(bucket["base_sum"], 2)
        name = account_names.get(acc_id, "")
        acc_label = f"{acc_id} ({name})" if name else (acc_id or "(ukendt konto)")

        if base_sum >= materiality.CONTROL_80_HIGH_THRESHOLD:
            severity = "high"
        elif base_sum >= materiality.CONTROL_80_MEDIUM_THRESHOLD:
            severity = "medium"
        else:
            severity = "low"

        more = bucket["count"] - len(bucket["refs"])
        more_note = f" Viser {len(bucket['refs'])} eksempler — og {more} flere posteringer på kontoen." \
            if more > 0 else ""

        findings.append(make_finding(
            test_id=80, test_name="Indtægt uden momsbehandling (pr. konto)",
            impact_type="economic", direction="positive", severity=severity,
            description=(
                f"Konto {acc_label}: {bucket['count']} posteringer for i alt {base_sum:.2f} bogført "
                f"uden moms og uden momskode ({no_code_share * 100:.0f}% af kontoens posteringer "
                f"mangler momskode)." + more_note
            ),
            fix_suggestion="Bekræft på KONTONIVEAU om posteringerne er momspligtige (25%), momsfrie "
                           "eller udenlandske. Vurdér kontoens generelle brug frem for hver enkelt "
                           "postering — ofte er dette en systematisk kontobrug (fx interne "
                           "allokeringskonti), ikke N individuelle fejl.",
            estimated_amount=round(base_sum * vr.STANDARD_RATE / 100, 2),
            transactions=bucket["refs"],
        ))
    return findings


# === TEST 81: Andel af momsfri omsætning ===

def test_81_zero_rated_share(data):
    findings = []
    taxable_turnover = 0.0
    zero_turnover = 0.0
    for txn in data["transactions"]:
        for line in txn["lines"]:
            credit = line.get("credit_amount", 0) or 0
            if credit <= 0:
                continue
            rate = line["tax_percentage"] or 0
            if rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE:
                taxable_turnover += credit
            else:
                zero_turnover += credit
    total = taxable_turnover + zero_turnover
    if total < 10000:
        return findings
    share = zero_turnover / total
    if share > 0.5:
        findings.append(make_finding(
            test_id=81, test_name="Stor andel momsfri omsætning",
            impact_type="compliance", direction="neutral", severity="low",
            description=f"{share*100:.0f}% af omsætningen er nulsat/momsfri ({zero_turnover:.0f} af "
                        f"{total:.0f}). Høj andel kan udløse delvis fradragsret.",
            fix_suggestion="Ved blandet momspligtig og momsfri omsætning skal fradragsretten for "
                           "fællesomkostninger opgøres forholdsmæssigt (delvis fradragsret).",
            transactions=[{"taxable": round(taxable_turnover, 2), "zero_rated": round(zero_turnover, 2),
                           "share": round(share, 3), "highlighted_field": "tax_percentage"}],
        ))
    return findings


# === TEST 82: Periodetotaler/rubrikker vs. deklaration ===
#
# Aktiveret byggetrin 8, Del B (Bal-godkendt 2026-09-17) — kræver den
# indberettede momsangivelse (analytics.vat_declarations.load_declarations)
# som eksternt input. Findes intet, springer testen fortsat pænt over,
# PRÆCIS som hidtil (ingen adfærdsændring for eksisterende input-veje uden
# en angivelsesfil — se readiness.EXTERNAL_DATA[82] og engine.run_all_tests).
#
# RUBRIK-LOGIK (ikke retnings-logik), EMPIRISK VALIDERET mod den rigtige
# BC/NAV-fil (byggetrin 8, Del D, 2026-09-17, Bal-godkendt):
#   - Angivet "Output VAT" (udgående moms) = salgsmoms (sale-retning) +
#     INDENLANDSK omvendt betalingspligt (købs-rækker med en DKRC-kode, fx
#     "DOMESTIC|REDUCED_PRIVATE_DKRC"). RC-ydelser fra udlandet
#     (*SERVICE_VAT_EU*/*SERVICE_VAT_NOT_EU* på købssiden) har sin EGEN
#     rubrik og tælles IKKE med i udgående moms.
#   - "Sale" vs. "køb" afgøres af den kanoniske CSV's EGET ``supply_direction``
#     -felt ("sale"/"purchase") -- IKKE af debet-/kredit-siden. Dette blev
#     rettet under E2E-verifikationen: debet/kredit så plausibelt ud i teorien
#     (og er stadig fallback, se ``_line_direction``), men gav en kraftigt
#     oppustet udgående-rubrik på den rigtige fil (BC/NAV kopierer tilsyneladende
#     invoice-niveau momsmetadata ud på flere GL-linjer af samme bilag,
#     inklusive modpost-/betalingslinjer, som IKKE er selve sale-/
#     købstransaktionen). Med ``supply_direction`` som kilde afstemmer
#     udgående moms og RC-ydelser til < 1 kr. for ALLE 12 måneder 2025.
#   - Hver rubrik summeres MED FORTEGN over linjerne FØR abs()/negering
#     anvendes ÉN GANG på summen (ikke pr. linje) -- se
#     ``_compute_period_rubrics`` for hvorfor (kreditnota-nettoeffekt).
#   - vat_period (transactions[].period/period_year) er korrekt periode-
#     basis for BÅDE udgående moms og RC-ydelser (bekræftet perfekt match pr.
#     periode). Købsmoms (input_vat) angives derimod på SETTLEMENT-basis, så
#     en per-periode-difference på købssiden kan være ren TIMING, ikke en
#     fejl. V1-håndtering (Bal-godkendt): sammenlign pr. periode OG
#     årstotal — hvis årstotalen stemmer (inden for tolerance) men enkelte
#     perioder afviger, klassificeres differencen som "timing" (severity
#     low), ikke et reelt fund (severity high). Årsvurderingen bruger BÅDE
#     den absolutte tolerance og en relativ grænse
#     (VAT_DECLARATION_ANNUAL_TIMING_PCT, default 1 % af angivet årstotal),
#     fordi settlement-basis giver spillover hen over årsgrænsen, som en
#     etårig sammenligning aldrig kan se nulstillet.
#   - KØBSMOMS-RUBRIKKEN OMFATTER RC-FRADRAGSSIDEN: omvendt betalingspligt
#     (DKRC + RC-ydelser) angives i sin egen rubrik OG som fradrag i
#     købsmomsen (netter til nul i totalen). En tidligere version udelod
#     RC-siden af input_vat og producerede en KUNSTIG "reel" årsdifference
#     på præcis årets RC-sum (4,0 mio. på den rigtige fil) — rettet
#     2026-09-17 efter manuel verifikation mod kundens egen 3-vejs-
#     afstemning (residual herefter: -326 t.kr. = 0,56 % af årstotalen,
#     konsistent med settlement-timing; kundens egen afstemning lukker på
#     1 kr. på settlement-basis).
#
# DKRC-/SERVICE_VAT-kodegenkendelse er BEVIDST konfigurerbar (materiality.
# VAT_DECLARATION_DKRC_PATTERNS/VAT_DECLARATION_SERVICE_VAT_PATTERNS) —
# IKKE hårdkodet til én kundes momskode-navngivning. Se materiality.py for
# den dokumenterede begrænsning (kalibreret til den observerede BC/NAV-
# taksonomi; en anden klients koder kræver en engagement-specifik override).
#
# HÆRDNING (byggetrin 9, Del A, Bal-godkendt 2026-09-17): er
# balai_extensions-feltet ``vat_calculation_type`` til stede (kun på den
# kanoniske vej, når vat_setup.csv er indlæst), bruger _purchase_rubric det
# FØR navnemønstrene -- deterministisk "reverse charge"-detektion plus
# Bus.-gruppen (vat_codes-strengens første led: DOMESTIC vs. EU/OUTSIDE
# DK/EU) til at skelne indenlandsk RC fra RC fra udlandet. Beregningstypen
# ALENE kan ikke skelne RC-ydelser fra RC-varekøb fra udlandet (samme
# beregningstype for begge) -- den sidste skelnen falder fortsat tilbage
# til SERVICE_VAT-navnemønstret. Se _purchase_rubric's docstring for hele
# beslutningstræet. Regressionskriterium: kontrol 82's E2E-resultat på
# byggetrin 8/9's v4-datasæt er UÆNDRET efter denne hærdning (verificeret).

_DECLARATION_RUBRICS = ("output_vat", "rc_services", "input_vat")

_RUBRIC_LABELS = {
    "output_vat": "Udgående moms (salgsmoms + indenlandsk omvendt betalingspligt)",
    "rc_services": "RC-ydelser fra udlandet (omvendt betalingspligt, ydelser)",
    "input_vat": "Indgående moms (købsmoms)",
}


def _purchase_rubric(tax_code: str, vat_calculation_type: str = "") -> str:
    """Klassificér en KØBSLINJES momskode til rubrik: 'dkrc' (indenlandsk
    omvendt betalingspligt -> tælles med i udgående moms), 'rc_services'
    (RC-ydelser udland -> egen rubrik) eller 'input' (almindelig købsmoms).

    ``vat_calculation_type`` (balai_extensions, kontrakt v0.4.0, tax_table[]/
    lines[].vat_calculation_type -- kun til stede på den kanoniske vej NÅR
    vat_setup.csv er indlæst, jf. canonical_masterdata.enrich_canonical):
    deterministisk ERP-mekanisme-flag pr. kode (Bal-godkendt 2026-09-17,
    aktiveret her). Bruges FØR navnemønstrene, når det er til stede:

    - Feltet siger IKKE "reverse charge" (fx "Normal VAT"/"Full VAT") ->
      'input', deterministisk -- ingen grund til at konsultere mønstrene.
    - Feltet siger "reverse charge", MEN kan IKKE alene skelne indenlandsk
      RC fra RC (ydelser ELLER varer) fra udlandet -- begge bruger samme
      beregningstype i BC/NAV. Bus.-gruppen (vat_codes-strengens FØRSTE led,
      adskilt med "|", fx "DOMESTIC"/"EU"/"OUTSIDE DK/EU") løser
      indenlandsk-vs-udenlandsk-skellet:
        * Bus.-gruppe "DOMESTIC" -> 'dkrc' (udgående rubrik), UDEN at kræve
          "dkrc" i selve kodenavnet -- hærdning af den tidligere rene
          navnemønster-afhængighed.
        * Enhver anden Bus.-gruppe (EU/OUTSIDE DK/EU) er udenlandsk RC, men
          hverken beregningstype eller Bus.-gruppe skelner "ydelse" fra
          "vare" (fx "EU|SERVICE_VAT_EU" vs. "EU|GOODS_VAT_EU" bruger begge
          "Reverse Charge VAT" og Bus.-gruppe "EU") -- den sidste skelnen
          kræver stadig SERVICE_VAT-navnemønstret (samme mønster som
          fallback-vejen, ikke en dublet-implementering). RC-varekøb fra
          udlandet lander dermed i 'input' (almindelig købsmoms-rubrik),
          PRÆCIS som før denne hærdning -- ingen adfærdsændring, empirisk
          bekræftet uændret på den rigtige BC/NAV-fil (kontrol 82's
          regressionskriterium, byggetrin 9/Del A, 2026-09-17).

    Er ``vat_calculation_type`` fraværende/tomt (Excel-/SAF-T-oprindelse,
    eller kanonisk vej uden vat_setup.csv), falder funktionen tilbage til
    den REN navnemønster-klassifikation (uændret hidtidig adfærd) -- se
    modulets Del B-dokumentation ovenfor for den dokumenterede begrænsning
    i mønster-genkendelsen."""
    calc_type = (vat_calculation_type or "").strip().lower()
    if calc_type:
        if "reverse charge" not in calc_type:
            return "input"
        bus_group = (tax_code or "").split("|", 1)[0].strip().upper()
        if bus_group == "DOMESTIC":
            return "dkrc"
        if vr.text_matches_any(tax_code, materiality.VAT_DECLARATION_SERVICE_VAT_PATTERNS):
            return "rc_services"
        return "input"
    if vr.text_matches_any(tax_code, materiality.VAT_DECLARATION_DKRC_PATTERNS):
        return "dkrc"
    if vr.text_matches_any(tax_code, materiality.VAT_DECLARATION_SERVICE_VAT_PATTERNS):
        return "rc_services"
    return "input"


def classify_purchase_rubric(tax_code: str, vat_calculation_type: str = "") -> str:
    """Offentlig alias for ``_purchase_rubric`` (byggetrin ~10, Bal-godkendt
    2026-09-18): ``tools/generate_report.py``s 'Momsmotoren'-sektion skal
    vise KUNDEN, hvilken angivelsesrubrik en given momskode klassificeres
    til — samme rubrik-logik som kontrol 82, ikke en gendannet kopi af den
    (kilde-af-sandhed-disciplinen). Ren gennemstilling; se ``_purchase_rubric``
    for hele beslutningstræet."""
    return _purchase_rubric(tax_code, vat_calculation_type)


def _line_direction(line: dict):
    """Sale- vs. købs-retning for én linje. Foretrækker det ÆGTE
    ``supply_direction``-signal ("sale"/"purchase") — den kanoniske CSV's
    egen felt, og PRÆCIS det signal Bal manuelt validerede rubrik-logikken
    imod (byggetrin 8, Del B/D, empirisk bekræftet på den rigtige BC/NAV-fil,
    2026-09-17: udgående moms + RC-ydelser afstemmer til < 1 kr. for alle 12
    måneder 2025 med denne retningskilde). En anden, EKSPLICIT værdi (fx
    "settlement" -- VAT-afregningsposteringer, der ikke selv er en sale-/
    købstransaktion) udelukkes bevidst (returnerer None).

    Er ``supply_direction`` fraværende (Excel-/SAF-T-oprindelse har ikke
    feltet i dag, eller en ældre kanonisk fil mangler kolonnen), falder
    funktionen tilbage til debet-/kredit-retningen (samme konvention som
    ``_input_output_vat`` ovenfor: kredit = salg, debet = køb) — en svagere,
    men ikke-blokerende proxy, så kontrol 82 ikke er strukturelt blind uden
    for den kanoniske vej."""
    direction = (line.get("supply_direction") or "").strip().lower()
    if direction in ("sale", "purchase"):
        return direction
    if direction:
        return None  # eksplicit andet (fx "settlement") -- ikke en sale-/købslinje

    credit = line.get("credit_amount", 0) or 0
    debit = line.get("debit_amount", 0) or 0
    if credit > 0:
        return "sale"
    if debit > 0:
        return "purchase"
    return None


def _compute_period_rubrics(data: dict) -> dict:
    """Beregn de tre afstemmelige rubrikker pr. periode (nøgle "YYYY-MM") fra
    transaktionerne, på bogførings-/vat_period-basis
    (transactions[].period_year/period). Se _line_direction for sale-/
    købs-klassifikationen.

    VIGTIGT (empirisk bekræftet, byggetrin 8/Del D, 2026-09-17): hvert
    rubrik-udtryk summeres FØRST MED FORTEGN over linjerne, og abs()/
    negeringen anvendes ÉN GANG på summen bagefter -- IKKE abs() pr. linje.
    En kreditnota/reversering inde i én rubrik skal kunne NETTE mod de øvrige
    linjer i samme rubrik, før fortegnet låses; abs() pr. linje ville i
    stedet SUMMERE en reversering oveni i stedet for at trække den fra, og gav
    i praksis en falsk, kraftigt oppustet udgående-/RC-rubrik på den rigtige
    fil, indtil dette blev rettet."""
    raw: dict = {}
    for txn in data.get("transactions", []):
        year = (txn.get("period_year") or "").strip()
        month = (txn.get("period") or "").strip()
        if not year or not month:
            continue
        key = f"{year}-{month.zfill(2)}"
        bucket = raw.setdefault(key, {"sale": 0.0, "dkrc": 0.0, "service": 0.0, "input": 0.0})
        for line in txn.get("lines", []):
            vat = line.get("tax_amount") or 0
            if vat == 0:
                continue
            direction = _line_direction(line)
            if direction == "sale":
                bucket["sale"] += vat
            elif direction == "purchase":
                rubric = _purchase_rubric(line.get("tax_code", ""),
                                           line.get("vat_calculation_type", ""))
                if rubric == "dkrc":
                    bucket["dkrc"] += vat
                elif rubric == "rc_services":
                    bucket["service"] += vat
                else:
                    bucket["input"] += vat
            # direction is None (fx "settlement", eller hverken debet/kredit
            # udfyldt): hverken sale eller køb i moms-forstand -- ignoreres.

    return {
        key: {
            "output_vat": round(-v["sale"] + abs(v["dkrc"]), 2),
            "rc_services": round(abs(v["service"]), 2),
            # Købsmoms-rubrikken omfatter OGSAA fradragssiden af omvendt
            # betalingspligt (DKRC + RC-ydelser): RC-moms angives i baade sin
            # egen rubrik OG som fradrag i koebsmomsen, saa den netter til nul
            # i angivelsens total. Uden RC-siden opstaar en kunstig difference
            # paa praecis aarets RC-sum (verificeret paa den rigtige fil
            # 2026-09-17: 4,0 mio. kunstig -> -326 t.kr. reelt timing-residual).
            "input_vat": round(v["input"] + v["dkrc"] + v["service"], 2),
        }
        for key, v in raw.items()
    }


def build_declaration_reconciliation_table(data, declarations=None):
    """Byg den FULDE periode-/rubrik-afstemningstabel (kontrol 82) til
    "tillidsanker"-tabellen i kundedialog-rapporten (byggetrin ~9, Del C,
    Bal-godkendt 2026-09-17, ``backend/tools/generate_report.py``).

    Modsat ``test_82_period_declaration`` (som kun udsteder ET FUND pr.
    afvigende periode/rubrik), viser denne funktion ALLE perioder x rubrikker
    — inkl. dem der matcher ("grønne") — så rapporten kan vise den fulde
    12-måneders afstemning, ikke kun undtagelserne. Samme beregningsgrundlag
    og samme timing-klassifikation som test_82 (se dens docstring/kommentarer
    ovenfor for baggrunden); holdt som en selvstændig funktion (ikke en
    delt hjælper) for ikke at risikere at ændre test_82's allerede
    validerede/testede adfærd ved en delt refaktorering.

    Returnerer None hvis ingen deklaration/ingen perioder er givet (samme
    "spring pænt over"-filosofi som resten af kontrol 82). Ellers:
        {
          "perioder": [{"periode": "2025-01",
                        "rubrikker": {"output_vat": {"beregnet", "angivet",
                                                      "difference", "status"},
                                      "rc_services": {...}, "input_vat": {...}}}, ...],
          "aarstotaler": {rubrik: {"beregnet", "angivet", "difference"}},
          "rubrik_labels": {...},
        }
    ``status`` pr. rubrik/periode: "match" (grøn), "timing" (kun input_vat —
    nulstilles over årstotalen), "afvigelse" (reel), eller "ingen_angivelse"
    (perioden findes i de bogførte data, men er ikke angivet).
    """
    if not declarations or not declarations.get("periods"):
        return None

    computed = _compute_period_rubrics(data)
    declared_by_period = {
        p["period"]: p for p in declarations["periods"] if p.get("period")
    }
    all_periods = sorted(set(computed) | set(declared_by_period))
    if not all_periods:
        return None

    tolerance = materiality.VAT_DECLARATION_TOLERANCE

    annual_computed = {r: 0.0 for r in _DECLARATION_RUBRICS}
    annual_declared = {r: 0.0 for r in _DECLARATION_RUBRICS}
    for key in all_periods:
        d = declared_by_period.get(key)
        if d is None:
            continue
        c = computed.get(key, {})
        for r in _DECLARATION_RUBRICS:
            annual_computed[r] += c.get(r, 0.0)
            annual_declared[r] += float(d.get(r) or 0.0)
    annual_diff = {r: round(annual_computed[r] - annual_declared[r], 2) for r in _DECLARATION_RUBRICS}

    periods_out = []
    for key in all_periods:
        d = declared_by_period.get(key)
        c = computed.get(key, {"output_vat": 0.0, "rc_services": 0.0, "input_vat": 0.0})
        rubrics_out = {}
        for r in _DECLARATION_RUBRICS:
            computed_amt = round(c.get(r, 0.0), 2)
            if d is None:
                rubrics_out[r] = {"beregnet": computed_amt, "angivet": None,
                                   "difference": None, "status": "ingen_angivelse"}
                continue
            declared_amt = round(float(d.get(r) or 0.0), 2)
            diff = round(computed_amt - declared_amt, 2)
            if abs(diff) <= tolerance:
                status = "match"
            else:
                annual_timing_cap = max(
                    tolerance,
                    materiality.VAT_DECLARATION_ANNUAL_TIMING_PCT / 100.0 * abs(annual_declared[r]),
                )
                status = "timing" if abs(annual_diff[r]) <= annual_timing_cap else "afvigelse"
            rubrics_out[r] = {"beregnet": computed_amt, "angivet": declared_amt,
                               "difference": diff, "status": status}
        periods_out.append({"periode": key, "rubrikker": rubrics_out})

    return {
        "perioder": periods_out,
        "aarstotaler": {
            r: {"beregnet": round(annual_computed[r], 2), "angivet": round(annual_declared[r], 2),
                "difference": annual_diff[r]}
            for r in _DECLARATION_RUBRICS
        },
        "rubrik_labels": dict(_RUBRIC_LABELS),
    }


def test_82_period_declaration(data, declarations=None):
    """Afstem periodens rubrikker (udgående moms, RC-ydelser udland, indgående
    moms — se _compute_period_rubrics) mod den indberettede momsangivelse.

    ``declarations``: parset vat_declarations.json (analytics.vat_declarations),
    eller None. Er den None/uden perioder, springes testen pænt over —
    uændret adfærd for alle eksisterende input-veje uden en angivelsesfil.
    """
    findings = []
    if not declarations or not declarations.get("periods"):
        return findings

    computed = _compute_period_rubrics(data)
    declared_by_period = {
        p["period"]: p for p in declarations["periods"] if p.get("period")
    }
    all_periods = sorted(set(computed) | set(declared_by_period))
    if not all_periods:
        return findings

    tolerance = materiality.VAT_DECLARATION_TOLERANCE

    # Årstotaler (til timing-klassifikation, jf. V1-reglen ovenfor: en
    # difference der nulstilles over året, er timing, ikke et reelt fund).
    # Kun perioder, der HAR en deklareret modpost, tæller med — en periode
    # uden en deklareret værdi er der intet at afstemme mod for.
    annual_computed = {r: 0.0 for r in _DECLARATION_RUBRICS}
    annual_declared = {r: 0.0 for r in _DECLARATION_RUBRICS}
    for key in all_periods:
        d = declared_by_period.get(key)
        if d is None:
            continue
        c = computed.get(key, {})
        for r in _DECLARATION_RUBRICS:
            annual_computed[r] += c.get(r, 0.0)
            annual_declared[r] += float(d.get(r) or 0.0)
    annual_diff = {r: round(annual_computed[r] - annual_declared[r], 2) for r in _DECLARATION_RUBRICS}

    for key in all_periods:
        d = declared_by_period.get(key)
        if d is None:
            continue  # ingen deklareret værdi for perioden -- intet at afstemme mod
        c = computed.get(key, {"output_vat": 0.0, "rc_services": 0.0, "input_vat": 0.0})
        for r in _DECLARATION_RUBRICS:
            computed_amt = round(c.get(r, 0.0), 2)
            declared_amt = round(float(d.get(r) or 0.0), 2)
            diff = round(computed_amt - declared_amt, 2)
            if abs(diff) <= tolerance:
                continue  # match -- intet fund (grønt)

            # Timing-vurdering: koebsmoms angives paa settlement-basis, mens
            # rubrikken her beregnes paa vat_period-basis — spillover hen over
            # aarsgraensen er derfor forventeligt og IKKE en fejl. Aarstotalen
            # vurderes med baade den absolutte tolerance og en relativ
            # aarsgraense (default 1 % af angivet aarstotal, konfigurerbar).
            annual_timing_cap = max(
                tolerance,
                materiality.VAT_DECLARATION_ANNUAL_TIMING_PCT / 100.0
                * abs(annual_declared[r]),
            )
            is_timing = abs(annual_diff[r]) <= annual_timing_cap
            severity = "low" if is_timing else "high"
            timing_note = (
                " Årstotalen stemmer (inden for tolerance) — differencen vurderes at "
                "være TIMING (fx købsmoms angivet på settlement- frem for "
                "vat_period-basis), ikke en reel fejl."
                if is_timing else
                " Differencen nulstilles IKKE over årstotalen — vurderes reel."
            )
            findings.append(make_finding(
                test_id=82, test_name="Periode-/rubrikafstemning mod momsangivelse",
                impact_type="economic", direction="neutral", severity=severity,
                description=(
                    f"{key}: {_RUBRIC_LABELS[r]} beregnet til {computed_amt:.2f}, men "
                    f"angivet {declared_amt:.2f} (difference {diff:+.2f})." + timing_note
                ),
                fix_suggestion=(
                    "Timing-differencer på købsmoms bør stadig dokumenteres (hvilken "
                    "periode beløbet reelt hører til), men kræver ikke korrektion, når "
                    "årstotalen stemmer."
                    if is_timing else
                    "Undersøg differencen — den forsvinder ikke over årstotalen og kan "
                    "indikere en fejl i angivelsen eller i bogføringen."
                ),
                estimated_amount=abs(diff),
                transactions=[{
                    "period": key, "rubrik": r, "beregnet": computed_amt,
                    "angivet": declared_amt, "difference": diff, "timing": is_timing,
                    "highlighted_field": "tax_amount",
                }],
            ))
    return findings


# === TEST 83: Delvis fradragsret ===

def test_83_partial_deduction(data):
    """Korrekt opgørelse af delvis fradragsret kræver en fordelingsnøgle
    (omsætningsfordeling pr. omkostning) der ikke fremgår af posteringerne.
    Springer pænt over; den overordnede indikator gives af test 81."""
    return []
