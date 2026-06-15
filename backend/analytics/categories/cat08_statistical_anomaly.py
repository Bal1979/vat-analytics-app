"""
Kategori 8: Statistisk Anomalidetektion (Tests 63-69)

Anvender statistiske metoder (Benford, fordelinger, frekvensanalyse) til
at finde mønstre der afviger fra det forventede i ægte regnskabsdata.
Disse tests kræver et minimum af datapunkter og springer pænt over på
små datasæt.
"""

from collections import Counter, defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


def run_statistical_tests(data: dict) -> list:
    findings = []
    findings.extend(test_63_benford_first_digit(data))
    findings.extend(test_64_round_number_bias(data))
    findings.extend(test_65_rare_tax_code(data))
    findings.extend(test_66_duplicate_descriptions(data))
    findings.extend(test_67_timeseries_spike(data))
    findings.extend(test_68_rare_account(data))
    findings.extend(test_69_last_digit_uniformity(data))
    return findings


def _all_amounts(data):
    return [abs(line["debit_amount"] + line["credit_amount"])
            for txn in data["transactions"] for line in txn["lines"]
            if (line["debit_amount"] or line["credit_amount"])]


# === TEST 63: Benford's Law (første ciffer) ===

def test_63_benford_first_digit(data):
    findings = []
    amounts = [a for a in _all_amounts(data) if a >= 10]
    if len(amounts) < 50:
        return findings  # For lille til meningsfuld Benford-analyse
    chi, n, observed = vr.benford_chi_square(amounts)
    if chi > vr.BENFORD_CHI_CRITICAL:
        dist = {str(d): observed.get(d, 0) for d in range(1, 10)}
        findings.append(make_finding(
            test_id=63, test_name="Benford-afvigelse (første ciffer)",
            impact_type="compliance", direction="neutral", severity="medium",
            description=f"Fordelingen af første ciffer i {n} beløb afviger signifikant fra Benfords lov "
                        f"(chi² = {chi:.1f} > {vr.BENFORD_CHI_CRITICAL}). Kan indikere konstruerede tal.",
            fix_suggestion="En unaturlig cifferfordeling kan skyldes manuelt opfundne beløb. "
                           "Gennemgå posteringer med over-repræsenterede førstecifre.",
            transactions=[{"observed_distribution": dist, "n": n, "chi_square": round(chi, 1),
                           "highlighted_field": "amount"}],
        ))
    return findings


# === TEST 64: Overrepræsentation af runde tal ===

def test_64_round_number_bias(data):
    findings = []
    amounts = _all_amounts(data)
    if len(amounts) < 30:
        return findings
    round_count = sum(1 for a in amounts if vr.is_round_amount(a, base=1000))
    ratio = round_count / len(amounts)
    # I ægte data er andelen af hele tusinder typisk lav (<5%).
    if ratio > 0.20:
        findings.append(make_finding(
            test_id=64, test_name="Mange runde beløb",
            impact_type="compliance", direction="neutral", severity="low",
            description=f"{round_count} af {len(amounts)} beløb ({ratio*100:.0f}%) er hele tusinder. "
                        f"En så høj andel er usædvanlig i ægte transaktionsdata.",
            fix_suggestion="En høj andel runde beløb kan indikere estimater, a conto-posteringer eller "
                           "fiktive tal. Stikprøvekontrollér.",
            transactions=[{"round_count": round_count, "total": len(amounts),
                           "ratio": round(ratio, 3), "highlighted_field": "amount"}],
        ))
    return findings


# === TEST 65: Sjælden momskode ===

def test_65_rare_tax_code(data):
    findings = []
    code_counts = Counter()
    code_example = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line["tax_code"]
            if code:
                code_counts[code] += 1
                code_example.setdefault(code, (txn, line))
    total = sum(code_counts.values())
    if total < 50:
        return findings
    for code, count in code_counts.items():
        if count / total < 0.01:  # Brugt i under 1% af posteringerne
            txn, line = code_example[code]
            findings.append(make_finding(
                test_id=65, test_name="Sjælden momskode",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Momskode '{code}' bruges kun {count} gange ud af {total} posteringer "
                            f"({count/total*100:.1f}%). Sjældent brugte koder er fejludsatte.",
                fix_suggestion="Bekræft at den sjældne momskode er anvendt korrekt og ikke er en "
                               "fejlindtastning.",
                transactions=[{"tax_code": code, "count": count, "total": total,
                               "transaction_id": txn["transaction_id"],
                               "highlighted_field": "tax_code"}],
            ))
    return findings


# === TEST 66: Identiske beskrivelser ===

def test_66_duplicate_descriptions(data):
    findings = []
    desc_groups = defaultdict(list)
    for txn in data["transactions"]:
        desc = (txn["description"] or "").strip().lower()
        if len(desc) >= 5:
            desc_groups[desc].append(txn)

    total = len(data["transactions"])
    if total < 20:
        return findings
    for desc, txns in desc_groups.items():
        if len(txns) >= 10 and len(txns) / total > 0.10:
            findings.append(make_finding(
                test_id=66, test_name="Gentaget beskrivelse",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Beskrivelsen '{txns[0]['description']}' bruges på {len(txns)} posteringer "
                            f"({len(txns)/total*100:.0f}%). Generiske gentagne tekster slører revisionssporet.",
                fix_suggestion="Brug specifikke posteringstekster, så hver transaktion kan identificeres.",
                transactions=[{"transaction_id": t["transaction_id"], "date": t["date"],
                               "amount": t["total_debit"] + t["total_credit"],
                               "highlighted_field": "description"} for t in txns[:10]],
            ))
    return findings


# === TEST 67: Tidsserie-spike ===

def test_67_timeseries_spike(data):
    """Dage hvor antallet af posteringer afviger kraftigt fra det normale."""
    findings = []
    by_day = defaultdict(int)
    for txn in data["transactions"]:
        if txn["date"]:
            by_day[txn["date"]] += 1
    if len(by_day) < 10:
        return findings
    counts = list(by_day.values())
    m = vr.mean(counts)
    sd = vr.stdev(counts)
    if sd == 0:
        return findings
    for day, count in sorted(by_day.items()):
        z = (count - m) / sd
        if z >= 3:
            findings.append(make_finding(
                test_id=67, test_name="Posteringsspike",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"{count} posteringer den {day} — {z:.1f} std.afv. over det daglige "
                            f"gennemsnit ({m:.1f}).",
                fix_suggestion="Et pludseligt spring i posteringsvolumen kan skyldes batch-import eller "
                               "efterposteringer. Bekræft at posteringerne er reelle.",
                transactions=[{"date": day, "count": count, "daily_mean": round(m, 1),
                               "z_score": round(z, 1), "highlighted_field": "date"}],
            ))
    return findings


# === TEST 68: Sjældent brugt konto ===

def test_68_rare_account(data):
    findings = []
    acct_counts = Counter()
    acct_total = defaultdict(float)
    acct_example = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            acct = line["account_id"]
            if acct:
                acct_counts[acct] += 1
                acct_total[acct] += abs(line["debit_amount"] + line["credit_amount"])
                acct_example.setdefault(acct, (txn, line))
    total = sum(acct_counts.values())
    if total < 50:
        return findings
    for acct, count in acct_counts.items():
        # Sjælden konto (1 brug) med et væsentligt beløb
        if count == 1 and acct_total[acct] >= 25000:
            txn, line = acct_example[acct]
            findings.append(make_finding(
                test_id=68, test_name="Sjældent brugt konto",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Konto {acct} bruges kun én gang, med et beløb på {acct_total[acct]:.2f}.",
                fix_suggestion="Engangskonti med store beløb kan indikere fejlkontering eller "
                               "skjult postering. Kontrollér.",
                estimated_amount=acct_total[acct],
                transactions=[{"transaction_id": txn["transaction_id"], "account_id": acct,
                               "amount": acct_total[acct], "highlighted_field": "account_id"}],
            ))
    return findings


# === TEST 69: Sidste-ciffer-uniformitet ===

def test_69_last_digit_uniformity(data):
    """I ægte beløb er sidste ciffer (øre) nogenlunde jævnt fordelt.
    En kraftig overvægt af fx '00' indikerer afrundede/konstruerede tal."""
    findings = []
    last_digits = []
    for a in _all_amounts(data):
        cents = int(round(a * 100)) % 100
        last_digits.append(cents)
    if len(last_digits) < 50:
        return findings
    zero_count = sum(1 for c in last_digits if c == 0)
    ratio = zero_count / len(last_digits)
    # Forventet ~1% hvis jævnt; flag hvis >40% (kraftig afrunding til hele kroner)
    if ratio > 0.40:
        findings.append(make_finding(
            test_id=69, test_name="Skæv øre-fordeling",
            impact_type="compliance", direction="neutral", severity="low",
            description=f"{ratio*100:.0f}% af beløbene ender på ',00'. En så stor overvægt af hele "
                        f"kroner-beløb er usædvanlig og kan indikere konstruerede tal.",
            fix_suggestion="Mange beløb uden ører kan være estimater eller fiktive posteringer. "
                           "Stikprøvekontrollér mod bilag.",
            transactions=[{"zero_cents_count": zero_count, "total": len(last_digits),
                           "ratio": round(ratio, 3), "highlighted_field": "amount"}],
        ))
    return findings
