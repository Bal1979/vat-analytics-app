"""
Kategori 13: Krydsdimensionelle kontroller (Tests 104-109)

Nye deterministiske kontroller fra gap-analysen (BALAI-motoren vs.
ekspertleverancen, Bal-godkendt 2026-09-18). Fælles for dem alle:
de krydser TO dimensioner, motoren i forvejen har hver for sig (valuta,
momskode/Bus.-gruppe, bilagstype/source_code) — ingen af dem kræver ny
kildedata ud over det, der allerede findes i kontrakten (plus source_code,
kontrol 107-108, se nedenfor). Se GAP-ANALYSE-motor-vs-ekspert.md (kundedata,
IKKE i dette repo — nævnes her kun ved kontrolnummer/mekanik, aldrig med
kunde-/leverandørnavne).

Fælles BC/NAV-semantik: en momskode er opaque "Bus.-gruppe|Produktkode" (fx
"EU|SERVICE_VAT_EU", "OUTSIDE DK/EU|GOODS_VAT_NOT_EU") — se
vat_rules.vat_bus_group/vat_product_code. Hvor kundens vat_setup er indlæst,
foretrækkes det deterministiske balai_extensions-felt vat_calculation_type
("Reverse Charge VAT"/"Normal VAT"/"Full VAT") frem for navnemønster-
fallback — samme prioritering som kontrol 22 (vat_rules.
is_reverse_charge_sale_code, Bal-godkendt 2026-09-18).

Kontrol 107-108 kræver ``source_code`` (bilagstype/BC-"Source Code",
byggetrin ~11) — et felt der endnu ikke er udbredt på nogen input-vej. Uden
feltet "ikke målbar"-gates de via analytics/readiness.py, præcis som motorens
øvrige felt-afhængige kontroller (fx kontrol 25 uden landekolonne).

INGEN af kontrollerne her hardkoder kunde-/leverandørnavne eller -numre —
alle grænser er strukturelle (Bus.-gruppe, produktkode-mønster, valuta,
calc type, bilagstype), jf. opgavens eksplicitte krav.

Kontrol 109 (gap-analyse-runde 2, Bal-godkendt 2026-09-20): Fradragsprocent-
afvigelse — ekspertens KRITISKE systemfejl-fund gjort deterministisk efter
F3's bilagsniveau-momsmodel (se analytics.vat_form). Formuafhængig (virker
på både linje- og kontobaseret form).
"""

from collections import defaultdict

from analytics.models import make_finding
from analytics import materiality
from analytics import vat_rules as vr
from analytics import vat_form as vf


def run_cross_dimension_tests(data: dict) -> list:
    findings = []
    findings.extend(test_104_foreign_currency_domestic_vat(data))
    findings.extend(test_105_cross_border_purchase_no_rc(data))
    findings.extend(test_106_import_indicator(data))
    findings.extend(test_107_atypical_vat_by_source_code(data))
    findings.extend(test_108_source_code_spread(data))
    findings.extend(test_109_deduction_rate_deviation(data))
    return findings


def _ref(txn, line, **extra):
    ref = {
        "transaction_id": txn["transaction_id"],
        "journal_id": txn.get("journal_id", ""),
        "date": txn.get("date", ""),
        "account_id": line.get("account_id", ""),
        "description": txn.get("description", ""),
        "amount": (line.get("debit_amount") or 0) + (line.get("credit_amount") or 0),
    }
    ref.update(extra)
    return ref


# === TEST 104: Udenlandsk valuta med indenlandsk standardmoms ===

def test_104_foreign_currency_domestic_vat(data: dict) -> list:
    """Købslinje i udenlandsk valuta (Source Currency Code != tom/DKK)
    bogført med den danske STANDARD-momskode (Bus.-gruppe DOMESTIC,
    produktkode STANDARD_VAT) og en calc type, der ikke er reverse charge.

    Gap-analysens F29-fund (2026-09-18, side 39, en konkret leverandør-
    familie i ekspertens katalog — ikke navngivet her): valuta alene beviser
    INTET — en dansk leverandør kan lovligt fakturere i EUR med dansk moms.
    Fundet stilles derfor altid som SPØRGSMÅL (severity medium, aldrig en
    konklusion): burde dette være omvendt betalingspligt, eller er det en
    dansk leverandørs udenlandsk-valuta-faktura?"""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line.get("tax_code") or ""
            if vr.vat_bus_group(code) != "DOMESTIC":
                continue
            if "STANDARD_VAT" not in vr.vat_product_code(code):
                continue
            if vr.is_rc_calc_type(line.get("vat_calculation_type", "")):
                continue  # denne kode er (for denne linje) markeret RC -- kontrol 105's territorium
            currency = (line.get("currency") or "DKK").strip().upper()
            if not currency or currency == "DKK":
                continue
            amount = vr.line_amount(line)
            if amount <= 0:
                continue
            findings.append(make_finding(
                test_id=104, test_name="Udenlandsk valuta med indenlandsk standardmoms",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Transaktion {txn['transaction_id']} er i {currency} men bogført med "
                            f"den danske standardmomskode '{code}'. Udenlandsk leverandørvaluta med "
                            f"dansk moms — burde det være omvendt betalingspligt, eller fakturerer "
                            f"leverandøren (evt. dansk) blot i {currency}?",
                fix_suggestion="Bekræft leverandørens hjemsted og momsbehandling: er leverandøren "
                               "dansk (korrekt dansk moms, blot faktureret i udenlandsk valuta), "
                               "eller udenlandsk (bør være omvendt betalingspligt)?",
                transactions=[_ref(txn, line, currency=currency, highlighted_field="currency")],
            ))
    return findings


# === TEST 105: EU-/udlandskøb uden reverse charge-beregning ===

def test_105_cross_border_purchase_no_rc(data: dict) -> list:
    """Købslinje med EU-/OUTSIDE DK/EU-Bus.-gruppe og INTET bogført momsbeløb.

    To sub-populationer med forskellig sikkerhed (gap-analysens punkt 1.2:
    "57 EU-køb uden momsberegning — delvist pension/forsikring, resten bør
    gennemgås"):
      - Koden ER (eller ligner navnemæssigt) en reverse charge-kode: HØJERE
        sikkerhed — erhvervelsesmoms burde være selvangivet. Severity high.
      - Koden er en 'NO_VAT'-familiekode (vat_rules.is_no_vat_product):
        LAVERE sikkerhed — kan være en ægte fritaget ydelse (fx pension/
        forsikring, jf. eksperten), men bør bekræftes, ikke antages.
        Severity medium, spørgsmålsform. Kontrollen tager IKKE selv stilling
        til om 0 kr. er korrekt.
    Prioritering: vat_calculation_type (deterministisk, når vat_setup er
    indlæst) FØR navnemønster — samme princip som vat_rules.
    is_reverse_charge_sale_code. Er hverken calc type kendt eller koden en
    NO_VAT-kode, antages RC (BC/NAV-konventionen: enhver ikke-indenlandsk
    handelskode uden andet signal er en RC-/eksportkode)."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line.get("tax_code") or ""
            bus = vr.vat_bus_group(code)
            if bus not in ("EU", "OUTSIDE DK/EU"):
                continue
            if (line.get("debit_amount") or 0) <= 0:
                continue  # kun købslinjer (debet)
            if (line.get("tax_amount") or 0) != 0:
                continue
            base = line.get("tax_base") or line.get("debit_amount") or 0
            calc_type = line.get("vat_calculation_type", "")
            no_vat_family = vr.is_no_vat_product(code)

            if no_vat_family and not vr.is_rc_calc_type(calc_type):
                findings.append(make_finding(
                    test_id=105, test_name="Nulkode på udenlandsk køb",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"Købslinje i transaktion {txn['transaction_id']} bruger nulkoden "
                                f"'{code}' (Bus.-gruppe {bus}) uden bogført moms. Kan være en ægte "
                                f"fritaget ydelse (fx pension/forsikring) — bør bekræftes, ikke "
                                f"antages.",
                    fix_suggestion="Bekræft at ydelsen reelt er momsfritaget. Er den ikke, mangler "
                                   "der en reverse charge-beregning.",
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
                ))
            else:
                findings.append(make_finding(
                    test_id=105, test_name="Reverse charge-beregning mangler",
                    impact_type="interest_risk", direction="neutral", severity="high",
                    description=f"Købslinje i transaktion {txn['transaction_id']} bruger koden "
                                f"'{code}' (Bus.-gruppe {bus}) men har intet bogført momsbeløb. "
                                f"Erhvervelsesmoms ser ud til ikke at være selvangivet.",
                    fix_suggestion="Beregn og bogfør reverse charge-moms (både købs- og "
                                   "salgsmoms/erhvervelsesmoms) for denne postering.",
                    estimated_amount=round(base * vr.STANDARD_RATE / 100, 2),
                    transactions=[_ref(txn, line, highlighted_field="tax_amount")],
                ))
    return findings


# === TEST 106: Import fra 3.-land — bekræft importørregistrering ===

def test_106_import_indicator(data: dict) -> list:
    """Varekøb fra et land uden for EU (Bus.-gruppe OUTSIDE DK/EU, produktkode
    der matcher BÅDE 'GOODS_VAT' og 'NOT_EU' — importvarer, ikke ydelser).

    Informativt fund (gap-analysens punkt 1.1: "import uden importør-
    registrering", ~40 t.kr., PDF s. 11) — beder om at bekræfte importør-
    registrering/toldbehandling. IKKE en fejlpåstand: momsen kan sagtens
    være korrekt beregnet (GOODS_VAT_NOT_EU er typisk selv en RC-varekode).
    Kontrollen flager derfor UANSET om der er beregnet moms — spørgsmålet om
    gyldig importørregistrering er uafhængigt af momsberegningen."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line.get("tax_code") or ""
            if vr.vat_bus_group(code) != "OUTSIDE DK/EU":
                continue
            product = vr.vat_product_code(code)
            if "GOODS_VAT" not in product or "NOT_EU" not in product:
                continue
            if (line.get("debit_amount") or 0) <= 0:
                continue
            vat = line.get("tax_amount") or 0
            findings.append(make_finding(
                test_id=106, test_name="Varekøb fra 3.-land — bekræft importørregistrering",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Transaktion {txn['transaction_id']} er et varekøb fra et land uden for "
                            f"EU (momskode '{code}'). Bekræft at importøren er registreret korrekt "
                            f"hos Skattestyrelsen/Toldstyrelsen, og at toldbehandlingen (angivelse, "
                            f"evt. importmoms) er på plads.",
                fix_suggestion="Bekræft importørregistrering og toldbehandling for varekøb fra "
                               "3.-lande. Informativ kontrol — ikke en fejlpåstand.",
                estimated_amount=vat,
                transactions=[_ref(txn, line, highlighted_field="tax_code")],
            ))
    return findings


# === TEST 107: Atypisk moms på bilagstype (kræver source_code) ===

def test_107_atypical_vat_by_source_code(data: dict) -> list:
    """Lær filens EGEN fordeling: for hver bilagstype (source_code), hvor
    stor en andel af dens linjer bærer et momsbeløb? En bilagstype hvor kun
    en lille minoritet har moms (typisk rene betalings-/interne journal-
    typer) er "typisk momsfri" — de FÅ linjer der alligevel har moms på
    netop DEN bilagstype er derfor en afvigelse værd at bekræfte
    (gap-analysens punkt 1.6, side 22-tabellen).

    Ingen hardkodede bilagstype-navne — tærsklerne
    (materiality.CONTROL_107_MIN_LINES_PER_SOURCE_CODE/
    CONTROL_107_MAX_TYPICAL_VAT_SHARE) er filuafhængige; "typisk" udledes af
    selve datasættet. Kræver ``source_code`` — helt fraværende felt (0% på
    hele datasættet) "ikke målbar"-gates via analytics/readiness.py, som
    resten af motorens felt-afhængige kontroller."""
    by_code = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = (line.get("source_code") or "").strip()
            if not code:
                continue
            by_code[code].append((txn, line))

    findings = []
    for code, items in by_code.items():
        if len(items) < materiality.CONTROL_107_MIN_LINES_PER_SOURCE_CODE:
            continue  # for lille population til at udlede et "typisk" mønster
        with_vat = [(t, l) for t, l in items if (l.get("tax_amount") or 0) != 0]
        if not with_vat:
            continue  # slet ingen momslinjer på denne bilagstype -- intet at flage
        share = len(with_vat) / len(items)
        if share > materiality.CONTROL_107_MAX_TYPICAL_VAT_SHARE:
            continue  # bilagstypen er ikke "typisk momsfri" -- moms er normalt her
        for t, l in with_vat:
            vat = l.get("tax_amount") or 0
            findings.append(make_finding(
                test_id=107, test_name="Atypisk moms på bilagstype",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Bilagstype '{code}' har moms på kun {len(with_vat)} af {len(items)} "
                            f"linjer ({share:.1%}) — transaktion {t['transaction_id']} er en af "
                            f"undtagelserne. Bekræft at momsbehandlingen på denne linje er tilsigtet.",
                fix_suggestion="Gennemgå de fremhævede posteringer — er bilagstypen fejlbrugt til "
                               "denne postering, eller er momsen her en bevidst undtagelse?",
                estimated_amount=vat,
                transactions=[_ref(t, l, source_code=code, highlighted_field="tax_amount")],
            ))
    return findings


# === TEST 108: Salg/køb spredt over mange bilagstyper (kræver source_code) ===

def test_108_source_code_spread(data: dict) -> list:
    """Procesobservation (gap-analysens punkt 1.6, side 32/36): bruges der et
    usædvanligt stort antal FORSKELLIGE bilagstyper til salg hhv. køb? Kan
    indikere posteringer uden for det normale debitor-/kreditorsystem
    (manuelle journaler, alternative registreringsveje) — ikke en fejl i sig
    selv, men en proces værd at kortlægge sammen med kunden. ÉT aggregeret
    fund PR. RETNING (ikke pr. linje), med bilagstype-fordelingen som
    evidens. Kræver ``source_code`` — gates "ikke målbar" ved fravær, som
    kontrol 107."""
    by_direction = defaultdict(lambda: defaultdict(int))
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = (line.get("source_code") or "").strip()
            if not code:
                continue
            if (line.get("debit_amount") or 0) > 0:
                by_direction["køb"][code] += 1
            elif (line.get("credit_amount") or 0) > 0:
                by_direction["salg"][code] += 1

    findings = []
    for direction, counts in by_direction.items():
        total_lines = sum(counts.values())
        if total_lines < materiality.CONTROL_108_MIN_LINES:
            continue
        if len(counts) < materiality.CONTROL_108_MIN_SOURCE_CODES:
            continue
        top = sorted(counts.items(), key=lambda kv: -kv[1])
        fordeling = ", ".join(f"{code} ({n})" for code, n in top[:10])
        mere = " ..." if len(top) > 10 else ""
        # test_name er en FAST literal (ikke en f-string) med vilje: det
        # statiske regelkatalog (tools/build_rules_catalog.py, AST-baseret)
        # udtrækker kun streng-literaler -- retningen (salg/køb) fremgår i
        # stedet af beskrivelsesteksten nedenfor.
        findings.append(make_finding(
            test_id=108, test_name="Salg/køb spredt over mange bilagstyper",
            impact_type="compliance", direction="neutral", severity="low",
            description=f"{direction.capitalize()} er bogført over {len(counts)} forskellige "
                        f"bilagstyper ({total_lines} linjer i alt): {fordeling}{mere}. Ikke "
                        f"nødvendigvis en fejl, men værd at kortlægge processen for sammen med "
                        f"kunden.",
            fix_suggestion="Bekræft om spredningen over bilagstyper afspejler en bevidst proces "
                           "(fx flere systemer/afdelinger), eller om posteringer uden for det "
                           "normale debitor-/kreditorsystem bør samles/ryddes op.",
            transactions=[],
        ))
    return findings


# === TEST 109: Fradragsprocent-afvigelse ===
#
# Gap-analyse-runde 2 (Bal-godkendt 2026-09-20): ekspertens KRITISKE
# systemfejl-fund (X-Ray s. 39-42, momskoder med delvis fradragsret som
# ET50/R/EI) — moms bogført med FULDT fradrag i stedet for den reducerede
# Deductible%, som kundens egen vat_setup faktisk registrerer. Kun mulig
# som en DETERMINISTISK kontrol efter F3's bilagsniveau-koblingen (se
# analytics.vat_form): på kontobaseret form (fx IFS) ligger grundlag og
# moms på forskellige linjer i samme bilag, så en pr.-linje-sammenligning
# aldrig ville kunne se afvigelsen.

def test_109_deduction_rate_deviation(data: dict) -> list:
    """Pr. bilag+momskode med delvis fradragsret (vat_setup's
    ``non_deductible_vat_pct`` > 0): forventet BOGFØRT (fradragsberettiget)
    moms = grundlag × sats × Deductible% (Deductible% = 100% -
    non_deductible_vat_pct). En afvigelse ud over materialitets-tolerancen
    er et fund — typisk fordi den FULDE moms er bogført som fradragsberettiget
    indgående moms i stedet for kun den fradragsberettigede andel.

    Kræver kundens vat_setup indlæst (``header.vat_setup_loaded``) OG at
    non_deductible_vat_pct-signalet rent faktisk er sat for koden (balai_
    extensions, kontrakt v0.4.0) — uden det er der intet Deductible% at måle
    imod, og kontrollen springer pænt over (samme mønster som kontrol
    19/24's vat_setup-afhængige gren, ``cat03_vat_rate_validation.py``).

    Formuafhængig (F3): virker på BÅDE linjebaseret (BC) og kontobaseret
    (IFS) form via ``vat_form.voucher_code_aggregates`` — på linjebaseret
    form falder bilag+kode-aggregatet naturligt sammen med linjens egne tal
    (grundlag og moms bor på samme linje), så kontrollen er ligeså
    meningsfuld på et BC-udtræk med en delvis-fradragsret-opsætning."""
    header = data.get("header") or {}
    if not header.get("vat_setup_loaded"):
        return []

    setup_by_code = vf.code_rate_lookup(data)
    account_based = vf.is_account_based(data)

    findings = []
    for txn in data["transactions"]:
        agg = vf.voucher_code_aggregates(txn, account_based)
        for code, entry in agg.items():
            setup = setup_by_code.get(code)
            if not setup or not setup.get("setup_matched"):
                continue
            nd_pct = setup.get("non_deductible_vat_pct")
            if not nd_pct:  # None eller 0 -- fuld fradragsret, intet at måle
                continue
            rate = setup.get("tax_percentage") or 0
            # Fortegns-bemærkning (samme som cat01/cat03's account-based
            # grene): moms og grundlag sammenlignes som MAGNITUDER, fordi
            # IFS' vat_amount bærer samme fortegn som grundlagets netto
            # debet-kredit (negativt på salgssiden).
            base = abs(entry["base"])
            actual_vat = abs(entry["vat"])
            if not rate or base == 0 or actual_vat == 0:
                continue
            deductible_pct = max(0.0, 100.0 - nd_pct)
            expected_vat = round(base * rate / 100 * deductible_pct / 100, 2)
            diff = round(actual_vat - expected_vat, 2)
            if abs(diff) <= materiality.CONTROL_109_TOLERANCE:
                continue
            direction = "positive" if diff > 0 else "negative"  # positive = for meget fratrukket
            ref_line = (entry["vat_lines"] or entry["base_lines"])[0]
            findings.append(make_finding(
                test_id=109,
                test_name="Fradragsprocent-afvigelse",
                impact_type="economic",
                direction=direction,
                severity="high" if abs(diff) > materiality.CONTROL_109_HIGH_THRESHOLD else "medium",
                description=(
                    f"Bilag {txn['transaction_id']}, momskode '{code}' (Deductible% "
                    f"{deductible_pct:g}%): bogført moms {actual_vat:.2f}, forventet "
                    f"{expected_vat:.2f} (grundlag {base:.2f} × {rate:g}% × "
                    f"{deductible_pct:g}%) — difference {abs(diff):.2f} DKK."
                ),
                fix_suggestion=(
                    f"Tjek fradragsbegrænsningen for momskode '{code}'. Kun "
                    f"{deductible_pct:g}% af momsen er fradragsberettiget (§42-lignende "
                    f"begrænsning) — de resterende {nd_pct:g}% skal bogføres som en "
                    f"ikke-fradragsberettiget omkostning, ikke som fratrukket indgående moms."
                ),
                estimated_amount=abs(diff),
                transactions=[{
                    "transaction_id": txn["transaction_id"],
                    "journal_id": txn.get("journal_id", ""),
                    "date": txn.get("date", ""),
                    "account_id": ref_line.get("account_id", ""),
                    "description": txn.get("description", ""),
                    "amount": base,
                    "tax_code": code,
                    "vat_recorded": actual_vat,
                    "vat_expected": expected_vat,
                    "deductible_pct": deductible_pct,
                    "non_deductible_pct": nd_pct,
                    "difference": diff,
                    "highlighted_field": "tax_amount",
                }],
            ))
    return findings
