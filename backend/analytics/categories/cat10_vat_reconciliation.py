"""
Kategori 10: Indgående/Udgående Moms Afstemning (Tests 76-83)

Afstemmer købsmoms (indgående) og salgsmoms (udgående) på tværs af
regnskabet og mod momskonti, og afdækker usædvanlige forhold mellem dem.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


def run_reconciliation_tests(data: dict) -> list:
    findings = []
    findings.extend(test_76_input_output_ratio(data))
    findings.extend(test_77_vat_account_reconciliation(data))
    findings.extend(test_78_negative_vat_liability(data))
    findings.extend(test_79_input_vat_no_purchase(data))
    findings.extend(test_80_revenue_without_output_vat(data))
    findings.extend(test_81_zero_rated_share(data))
    findings.extend(test_82_period_declaration(data))
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
        # Købsmoms mere end 3x salgsmoms = vedvarende negativ position
        if ratio > 3.0:
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
    sådan kan identificeres (kontonavn indeholder 'moms')."""
    findings = []
    input_vat, output_vat = _input_output_vat(data)
    net_vat = round(output_vat - input_vat, 2)

    vat_accounts = [a for a in data.get("accounts", [])
                    if "moms" in (a.get("description", "") or "").lower()]
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

def test_80_revenue_without_output_vat(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            credit = line.get("credit_amount", 0) or 0
            if credit <= 0:
                continue
            vat = line["tax_amount"] or 0
            code = line["tax_code"]
            country = vr.normalize_country(line.get("country", ""))
            # Indtægt uden moms OG uden momskode OG uden udenlandsk forklaring
            if vat == 0 and not code and not vr.is_foreign(country) and credit >= 5000:
                findings.append(make_finding(
                    test_id=80, test_name="Indtægt uden momsbehandling",
                    impact_type="economic", direction="positive", severity="medium",
                    description=f"Indtægt på {credit:.2f} (transaktion {txn['transaction_id']}) er bogført "
                                f"uden moms og uden momskode.",
                    fix_suggestion="Bekræft om salget er momspligtigt (25%), momsfrit eller udenlandsk. "
                                   "Manglende salgsmoms på indenlandsk salg er en fejl.",
                    estimated_amount=round(credit * vr.STANDARD_RATE / 100, 2),
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
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


# === TEST 82: Periodetotaler vs. deklaration ===

def test_82_period_declaration(data):
    """Kræver den indberettede momsangivelse at sammenligne mod. Findes ikke i
    en ren bogføringseksport, så testen springer pænt over."""
    return []


# === TEST 83: Delvis fradragsret ===

def test_83_partial_deduction(data):
    """Korrekt opgørelse af delvis fradragsret kræver en fordelingsnøgle
    (omsætningsfordeling pr. omkostning) der ikke fremgår af posteringerne.
    Springer pænt over; den overordnede indikator gives af test 81."""
    return []
