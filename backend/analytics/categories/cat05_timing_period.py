"""
Kategori 5: Timing & Periodetest (Tests 39-46)

Kontrollerer at posteringer ligger i den rigtige periode, ikke er
fremtidsdaterede, og afdækker mistænkelige bogføringsmønstre omkring
periodeskift (window dressing / periodeforskydning).
"""

import calendar
from datetime import datetime, date
from collections import defaultdict
from analytics.models import make_finding
from analytics import materiality


def run_timing_tests(data: dict) -> list:
    findings = []
    findings.extend(test_39_after_period_end(data))
    findings.extend(test_40_before_period_start(data))
    findings.extend(test_41_weekend_postings(data))
    findings.extend(test_42_period_end_clustering(data))
    findings.extend(test_43_future_dates(data))
    findings.extend(test_44_period_boundary(data))
    findings.extend(test_45_sequence_vs_date(data))
    findings.extend(test_46_invoice_posting_lag(data))
    return findings


def _parse(d):
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _ref(txn, **extra):
    ref = {
        "transaction_id": txn["transaction_id"],
        "journal_id": txn["journal_id"],
        "date": txn["date"],
        "description": txn["description"],
        "amount": txn["total_debit"] + txn["total_credit"],
    }
    ref.update(extra)
    return ref


def _period_bounds(data):
    """Returnér (start_date, end_date) fra header.period_start/period_end, ellers (None, None)."""
    header = data.get("header", {})
    summary = data.get("summary", {})
    start = _parse(header.get("period_start") or summary.get("period_start"))
    end = _parse(header.get("period_end") or summary.get("period_end"))
    return start, end


# === TEST 39: Bogføring efter periodeslut ===

def test_39_after_period_end(data):
    findings = []
    _, period_end = _period_bounds(data)
    if not period_end:
        return findings
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if d and d > period_end:
            findings.append(make_finding(
                test_id=39,
                test_name="Bogføring efter periodeslut",
                impact_type="interest_risk",
                direction="neutral",
                severity="medium",
                description=f"Transaktion {txn['transaction_id']} har dato {txn['date']}, som ligger "
                            f"efter periodeslut ({period_end}).",
                fix_suggestion="Kontrollér om transaktionen hører til den næste momsperiode. "
                               "Forkert periodisering kan udløse renter.",
                transactions=[_ref(txn, period_end=str(period_end), highlighted_field="date")],
            ))
    return findings


# === TEST 40: Bogføring før periodestart ===

def test_40_before_period_start(data):
    findings = []
    period_start, _ = _period_bounds(data)
    if not period_start:
        return findings
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if d and d < period_start:
            findings.append(make_finding(
                test_id=40,
                test_name="Bogføring før periodestart",
                impact_type="interest_risk",
                direction="neutral",
                severity="low",
                description=f"Transaktion {txn['transaction_id']} har dato {txn['date']}, som ligger "
                            f"før periodestart ({period_start}).",
                fix_suggestion="Kontrollér om transaktionen hører til en tidligere momsperiode.",
                transactions=[_ref(txn, period_start=str(period_start), highlighted_field="date")],
            ))
    return findings


# === TEST 41: Weekend-bogføring ===

def test_41_weekend_postings(data):
    findings = []
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if d and d.weekday() >= 5:  # 5=lørdag, 6=søndag
            day = "lørdag" if d.weekday() == 5 else "søndag"
            findings.append(make_finding(
                test_id=41,
                test_name="Weekend-bogføring",
                impact_type="compliance",
                direction="neutral",
                severity="low",
                description=f"Transaktion {txn['transaction_id']} er dateret {txn['date']} ({day}).",
                fix_suggestion="Weekend-datoer kan være korrekte, men store eller usædvanlige "
                               "weekend-posteringer bør stikprøvekontrolleres.",
                transactions=[_ref(txn, weekday=day, highlighted_field="date")],
            ))
    return findings


# === TEST 42: Ophobning ved periodeslut ===

def test_42_period_end_clustering(data):
    """Mange posteringer på den sidste dag i en måned kan indikere
    periodeforskydning eller efterposteringer."""
    findings = []
    by_day = defaultdict(list)
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if not d:
            continue
        last_day = calendar.monthrange(d.year, d.month)[1]
        if d.day == last_day:
            by_day[(d.year, d.month)].append(txn)

    total = len([t for t in data["transactions"] if _parse(t["date"])])
    for (year, month), txns in by_day.items():
        if len(txns) >= 5 and total and len(txns) / total > 0.15:
            findings.append(make_finding(
                test_id=42,
                test_name="Ophobning ved periodeslut",
                impact_type="interest_risk",
                direction="neutral",
                severity="low",
                description=f"{len(txns)} posteringer er dateret den sidste dag i {month:02d}/{year}. "
                            f"Ophobning ved periodeslut kan indikere periodeforskydning.",
                fix_suggestion="Stikprøvekontrollér om leveringstidspunktet reelt er månedens sidste dag, "
                               "eller om posteringer er skubbet for at ramme en bestemt periode.",
                transactions=[_ref(t, highlighted_field="date") for t in txns[:10]],
            ))
    return findings


# === TEST 43: Fremtidige datoer ===

def test_43_future_dates(data):
    findings = []
    today = date.today()
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if d and d > today:
            findings.append(make_finding(
                test_id=43,
                test_name="Fremtidig dato",
                impact_type="compliance",
                direction="neutral",
                severity="high",
                description=f"Transaktion {txn['transaction_id']} er dateret {txn['date']} — en dato "
                            f"i fremtiden (efter {today}).",
                fix_suggestion="Fremtidsdaterede posteringer er normalt fejl. Ret datoen til den "
                               "faktiske transaktionsdato.",
                transactions=[_ref(txn, today=str(today), highlighted_field="date")],
            ))
    return findings


# === TEST 44: Posteringer på momsperiode-grænse ===

def test_44_period_boundary(data):
    """Salg dateret de sidste dage i et kvartal kan være skubbet over
    periodegrænsen for at udskyde salgsmoms."""
    findings = []
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if not d:
            continue
        if txn["total_credit"] <= 0:  # Kun salg/indtægter
            continue
        # Kvartalsafslutninger: marts, juni, september, december
        if d.month in (3, 6, 9, 12):
            last_day = calendar.monthrange(d.year, d.month)[1]
            if d.day >= last_day - 1:
                findings.append(make_finding(
                    test_id=44,
                    test_name="Salg på kvartalsgrænse",
                    impact_type="interest_risk",
                    direction="neutral",
                    severity="low",
                    description=f"Salg på transaktion {txn['transaction_id']} er dateret {txn['date']}, "
                                f"helt på kvartalsgrænsen.",
                    fix_suggestion="Bekræft at leveringstidspunktet er korrekt — salg nær periodeskift "
                                   "bør falde i den periode hvor leveringen faktisk sker.",
                    transactions=[_ref(txn, highlighted_field="date")],
                ))
    return findings


# === TEST 45: Sekvens vs. dato-konsistens ===

def test_45_sequence_vs_date(data):
    """Et højere transaktionsnummer bør have en senere eller samme dato.
    Flag tilfælde hvor nummerrækkefølge og dato modsiger hinanden."""
    import re
    findings = []
    seq = []
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        nums = re.findall(r"\d+", txn["transaction_id"] or "")
        if d and nums:
            seq.append((int(nums[-1]), d, txn))

    if len(seq) < 3:
        return findings

    seq.sort(key=lambda x: x[0])  # sortér efter nummer
    inversions = []
    for i in range(1, len(seq)):
        if seq[i][1] < seq[i - 1][1]:
            inversions.append((seq[i - 1][2], seq[i][2]))

    # Kun flag hvis der er flere inversioner (enkelte kan være legitime)
    if len(inversions) >= 2:
        examples = []
        for prev, cur in inversions[:5]:
            examples.append(_ref(cur, prev_id=prev["transaction_id"], prev_date=prev["date"],
                                 highlighted_field="date"))
        findings.append(make_finding(
            test_id=45,
            test_name="Sekvens/dato-inkonsistens",
            impact_type="compliance",
            direction="neutral",
            severity="low",
            description=f"{len(inversions)} tilfælde hvor et højere transaktionsnummer har en tidligere "
                        f"dato end et lavere nummer. Kan indikere efterposteringer eller manipulation.",
            fix_suggestion="Undersøg om transaktionsnumre tildeles i datorækkefølge. Brud kan skyldes "
                           "bagudrettede posteringer.",
            transactions=examples,
        ))
    return findings


# === TEST 46: Forsinkelse faktura → bogføring ===

def test_46_invoice_posting_lag(data):
    """Stort lag mellem fakturadato (document_date) og bogføringsdato (date)
    risikerer at momsen medtages i en forkert periode. Kører kun når BEGGE datoer
    findes på transaktionen; ellers springes pænt over (flade udtræk med kun én dato)."""
    findings = []
    lag_threshold_days = materiality.INVOICE_POSTING_LAG_DAYS
    for txn in data["transactions"]:
        posting = _parse(txn.get("date"))
        invoice = _parse(txn.get("document_date"))
        if posting is None or invoice is None:
            continue
        lag = (posting - invoice).days
        if lag > lag_threshold_days:
            findings.append(make_finding(
                test_id=46,
                test_name="Stort lag mellem faktura- og bogføringsdato",
                impact_type="interest_risk",
                direction="neutral",
                severity="medium",
                description=f"Fakturadato {invoice.isoformat()} og bogføringsdato {posting.isoformat()} "
                            f"på transaktion {txn['transaction_id']} ligger {lag} dage fra hinanden. "
                            f"Et stort lag kan betyde, at momsen er medtaget i en forkert periode.",
                fix_suggestion="Kontrollér at momsen af fakturaen er angivet i den korrekte periode "
                               "(typisk leverings-/fakturadatoens periode), og ret periodiseringen om nødvendigt.",
                transactions=[_ref(txn, document_date=invoice.isoformat(),
                                   posting_date=posting.isoformat(), lag_days=lag,
                                   highlighted_field="document_date")],
            ))
    return findings
