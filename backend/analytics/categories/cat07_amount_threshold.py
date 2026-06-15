"""
Kategori 7: Beløbs- & Tærskeltest (Tests 55-62)

Afdækker mistænkelige beløbsmønstre: runde tal, beløb lige under
godkendelsesgrænser, kontantgrænser, statistiske outliers, manglende
bilag på store momsbeløb og strukturering (splitting).
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


# Typiske interne godkendelses-/indberetningsgrænser (DKK).
APPROVAL_THRESHOLDS = [10000, 25000, 50000, 100000, 250000, 500000]
# Kontantgrænse: erhvervsdrivende må ikke modtage kontant betaling >= 20.000 DKK.
CASH_LIMIT = 20000.0


def run_amount_tests(data: dict) -> list:
    findings = []
    findings.extend(test_55_round_amounts(data))
    findings.extend(test_56_just_below_threshold(data))
    findings.extend(test_57_cash_limit(data))
    findings.extend(test_58_unusually_large(data))
    findings.extend(test_59_large_vat_no_document(data))
    findings.extend(test_60_negative_vat(data))
    findings.extend(test_61_account_outlier(data))
    findings.extend(test_62_structuring(data))
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


# === TEST 55: Runde beløb ===

def test_55_round_amounts(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amount = abs(line["debit_amount"] + line["credit_amount"])
            if vr.is_round_amount(amount, base=1000) and amount >= 10000:
                findings.append(make_finding(
                    test_id=55, test_name="Rundt beløb",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Transaktion {txn['transaction_id']} har et helt rundt beløb "
                                f"({amount:.0f}). Faktiske handler giver sjældent runde tal.",
                    fix_suggestion="Runde beløb kan være estimater, a conto eller fiktive posteringer. "
                                   "Stikprøvekontrollér mod underliggende bilag.",
                    transactions=[_ref(txn, line, highlighted_field="amount")],
                ))
    return findings


# === TEST 56: Beløb lige under godkendelsesgrænse ===

def test_56_just_below_threshold(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amount = abs(line["debit_amount"] + line["credit_amount"])
            for thr in APPROVAL_THRESHOLDS:
                # Inden for 3% under en grænse (men ikke over)
                if thr * 0.97 <= amount < thr:
                    findings.append(make_finding(
                        test_id=56, test_name="Beløb lige under grænse",
                        impact_type="compliance", direction="neutral", severity="medium",
                        description=f"Transaktion {txn['transaction_id']} har beløb {amount:.2f}, lige "
                                    f"under grænsen {thr}. Kan være splittet for at undgå godkendelse.",
                        fix_suggestion="Tjek om beløbet bevidst er holdt under en godkendelses- eller "
                                       "indberetningsgrænse (struktureret).",
                        transactions=[_ref(txn, line, threshold=thr, highlighted_field="amount")],
                    ))
                    break
    return findings


# === TEST 57: Kontantbetalingsgrænse ===

def test_57_cash_limit(data):
    findings = []
    for txn in data["transactions"]:
        is_cash = vr.text_matches_any(txn["description"], vr.CASH_KEYWORDS)
        for line in txn["lines"]:
            if not (is_cash or vr.text_matches_any(line.get("description", ""), vr.CASH_KEYWORDS)):
                continue
            amount = abs(line["debit_amount"] + line["credit_amount"])
            if amount >= CASH_LIMIT:
                findings.append(make_finding(
                    test_id=57, test_name="Kontant over grænsen",
                    impact_type="compliance", direction="neutral", severity="high",
                    description=f"Mulig kontant betaling på {amount:.2f} (transaktion {txn['transaction_id']}) "
                                f"— på eller over kontantgrænsen ({CASH_LIMIT:.0f} DKK).",
                    fix_suggestion="Erhvervsdrivende må ikke modtage kontant betaling på 20.000 DKK eller "
                                   "mere. Momsfradrag kan nægtes for kontante køb over grænsen.",
                    estimated_amount=amount,
                    transactions=[_ref(txn, line, highlighted_field="amount")],
                ))
    return findings


# === TEST 58: Usædvanligt store beløb (outlier) ===

def test_58_unusually_large(data):
    findings = []
    amounts = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amounts.append(abs(line["debit_amount"] + line["credit_amount"]))
    if len(amounts) < 5:
        return findings
    m = vr.mean(amounts)
    sd = vr.stdev(amounts)
    if sd == 0:
        return findings
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amount = abs(line["debit_amount"] + line["credit_amount"])
            z = (amount - m) / sd
            if z >= 3:
                findings.append(make_finding(
                    test_id=58, test_name="Usædvanligt stort beløb",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"Transaktion {txn['transaction_id']} har beløb {amount:.2f}, "
                                f"{z:.1f} standardafvigelser over gennemsnittet ({m:.0f}).",
                    fix_suggestion="Outlier-beløb bør kontrolleres mod bilag og godkendelse.",
                    estimated_amount=amount,
                    transactions=[_ref(txn, line, z_score=round(z, 1), highlighted_field="amount")],
                ))
    return findings


# === TEST 59: Stort momsbeløb uden bilag ===

def test_59_large_vat_no_document(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = abs(line["tax_amount"] or 0)
            if vat >= 5000 and not line.get("source_document_id"):
                findings.append(make_finding(
                    test_id=59, test_name="Stort momsbeløb uden bilag",
                    impact_type="economic", direction="negative", severity="high",
                    description=f"Momsbeløb {vat:.2f} på transaktion {txn['transaction_id']} har intet "
                                f"bilags-/fakturanummer.",
                    fix_suggestion="Momsfradrag kræver en forskriftsmæssig faktura. Knyt bilaget til "
                                   "posteringen eller tilbagefør fradraget.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, tax_amount=vat, highlighted_field="source_document_id")],
                ))
    return findings


# === TEST 60: Negativt momsbeløb ===

def test_60_negative_vat(data):
    findings = []
    for txn in data["transactions"]:
        desc = (txn["description"] or "").lower()
        is_credit_note = "kreditnota" in desc or "credit note" in desc
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            if vat < 0 and not is_credit_note:
                findings.append(make_finding(
                    test_id=60, test_name="Negativt momsbeløb",
                    impact_type="economic", direction="neutral", severity="medium",
                    description=f"Negativt momsbeløb ({vat:.2f}) på transaktion {txn['transaction_id']} "
                                f"uden at posteringen er markeret som kreditnota.",
                    fix_suggestion="Negativ moms uden for kreditnotaer indikerer en fejlpostering. "
                                   "Kontrollér fortegn og modpostering.",
                    estimated_amount=abs(vat),
                    transactions=[_ref(txn, line, tax_amount=vat, highlighted_field="tax_amount")],
                ))
    return findings


# === TEST 61: Beløb over normalt for konto ===

def test_61_account_outlier(data):
    findings = []
    by_account = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            acct = line["account_id"]
            if acct:
                by_account[acct].append((txn, line, abs(line["debit_amount"] + line["credit_amount"])))

    for acct, entries in by_account.items():
        if len(entries) < 5:
            continue
        amounts = [a for _, _, a in entries]
        m = vr.mean(amounts)
        sd = vr.stdev(amounts)
        if sd == 0:
            continue
        for txn, line, amount in entries:
            z = (amount - m) / sd
            if z >= 3.5:
                findings.append(make_finding(
                    test_id=61, test_name="Konto-beløb outlier",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Beløb {amount:.2f} på konto {acct} (transaktion {txn['transaction_id']}) "
                                f"er {z:.1f} std.afv. over kontoens gennemsnit ({m:.0f}).",
                    fix_suggestion="Usædvanligt store posteringer på en ellers stabil konto bør "
                                   "kontrolleres for fejlkontering.",
                    estimated_amount=amount,
                    transactions=[_ref(txn, line, account_mean=round(m, 2), z_score=round(z, 1),
                                       highlighted_field="amount")],
                ))
    return findings


# === TEST 62: Strukturering (splitting) ===

def test_62_structuring(data):
    """Flere posteringer til samme leverandør samme dag, der tilsammen
    overstiger en grænse, men hver især ligger under."""
    findings = []
    by_vendor_day = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            if not sid:
                continue
            amount = abs(line["debit_amount"] + line["credit_amount"])
            by_vendor_day[(sid, txn["date"])].append((txn, line, amount))

    for (sid, day), entries in by_vendor_day.items():
        if len(entries) < 2:
            continue
        total = sum(a for _, _, a in entries)
        max_single = max(a for _, _, a in entries)
        for thr in APPROVAL_THRESHOLDS:
            if max_single < thr <= total:
                findings.append(make_finding(
                    test_id=62, test_name="Mulig strukturering",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"{len(entries)} posteringer til leverandør '{sid}' den {day} udgør "
                                f"tilsammen {total:.2f} (over grænsen {thr}), men hver enkelt er under. "
                                f"Mulig splitting.",
                    fix_suggestion="Undersøg om en større faktura er splittet i flere posteringer for at "
                                   "holde sig under en godkendelses-/kontrolgrænse.",
                    estimated_amount=total,
                    transactions=[_ref(t, l, threshold=thr, highlighted_field="amount")
                                  for t, l, _ in entries[:10]],
                ))
                break
    return findings
