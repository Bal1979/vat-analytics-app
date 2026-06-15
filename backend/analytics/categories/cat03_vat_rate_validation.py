"""
Kategori 3: Momssats-validering (Tests 19-26)

Kontrollerer at de anvendte momssatser er gyldige efter dansk momsret
(kun 25% standardsats og 0% nulsats), at de matcher momstabellen, og at
satsen er konsistent på tværs af samme momskode.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


def run_vat_rate_tests(data: dict) -> list:
    findings = []
    findings.extend(test_19_invalid_rate(data))
    findings.extend(test_20_rate_vs_taxtable(data))
    findings.extend(test_21_reduced_rate(data))
    findings.extend(test_22_missing_output_vat(data))
    findings.extend(test_23_rate_consistency_per_code(data))
    findings.extend(test_24_implied_rate(data))
    findings.extend(test_25_zero_rate_domestic(data))
    findings.extend(test_26_missing_tax_code(data))
    return findings


def _txn_ref(txn, line, **extra):
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


# === TEST 19: Ugyldig momssats ===

def test_19_invalid_rate(data: dict) -> list:
    """Flag momssatser der ikke er gyldige i Danmark (≠ 0% og ≠ 25%)."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            rate = line["tax_percentage"]
            if not line["tax_code"] or rate is None:
                continue
            if rate in vr.VALID_DK_RATES:
                continue
            findings.append(make_finding(
                test_id=19,
                test_name="Ugyldig momssats",
                impact_type="compliance",
                direction="neutral",
                severity="high",
                description=f"Momssats {rate}% på linje {line['record_id']} i transaktion "
                            f"{txn['transaction_id']} er ikke en gyldig dansk sats (0% eller 25%).",
                fix_suggestion="Ret momssatsen til 25% (standard) eller 0% (nulsats). "
                               "Danmark har ingen reducerede momssatser.",
                transactions=[_txn_ref(txn, line, tax_rate=rate, highlighted_field="tax_percentage")],
            ))
    return findings


# === TEST 20: Sats matcher ikke momstabel ===

def test_20_rate_vs_taxtable(data: dict) -> list:
    """Sammenlign linjens sats med satsen i TaxTable for samme momskode."""
    findings = []
    table = {t["tax_code"]: t["tax_percentage"] for t in data["tax_table"]}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line["tax_code"]
            if not code or code not in table:
                continue
            table_rate = table[code]
            line_rate = line["tax_percentage"]
            if line_rate is None:
                continue
            if abs(line_rate - table_rate) > vr.RATE_TOLERANCE:
                findings.append(make_finding(
                    test_id=20,
                    test_name="Sats afviger fra momstabel",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Momskode '{code}' bruges med {line_rate}% på linje {line['record_id']} "
                                f"(transaktion {txn['transaction_id']}), men TaxTable angiver {table_rate}%.",
                    fix_suggestion="Bring linjens momssats i overensstemmelse med momstabellen "
                                   "eller ret momstabellen.",
                    transactions=[_txn_ref(txn, line, line_rate=line_rate, table_rate=table_rate,
                                           highlighted_field="tax_percentage")],
                ))
    return findings


# === TEST 21: Udenlandsk reduceret sats ===

def test_21_reduced_rate(data: dict) -> list:
    """Flag typiske udenlandske reducerede satser anvendt i et dansk regnskab."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            rate = line["tax_percentage"]
            if rate is None or not line["tax_code"]:
                continue
            if rate in vr.COMMON_FOREIGN_RATES:
                findings.append(make_finding(
                    test_id=21,
                    test_name="Reduceret/udenlandsk momssats",
                    impact_type="economic",
                    direction="negative",
                    severity="high",
                    description=f"Reduceret momssats {rate}% anvendt på linje {line['record_id']} "
                                f"(transaktion {txn['transaction_id']}). Ligner en udenlandsk sats.",
                    fix_suggestion="Dansk handel skal anvende 25%. Hvis dette er udenlandsk moms, "
                                   "skal den ikke fratrækkes i den danske momsangivelse.",
                    estimated_amount=line["tax_amount"] or 0,
                    transactions=[_txn_ref(txn, line, tax_rate=rate, highlighted_field="tax_percentage")],
                ))
    return findings


# === TEST 22: Manglende salgsmoms (output) ===

def test_22_missing_output_vat(data: dict) -> list:
    """Indtægt (kredit) på en momspligtig konto med momskode men uden momsbeløb."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            credit = line["credit_amount"] or 0
            if credit <= 0 or not line["tax_code"]:
                continue
            rate = line["tax_percentage"] or 0
            vat = line["tax_amount"] or 0
            # Forventet standardsats men intet momsbeløb registreret
            if rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE and vat == 0:
                expected = round(credit * vr.STANDARD_RATE / 100, 2)
                findings.append(make_finding(
                    test_id=22,
                    test_name="Manglende salgsmoms",
                    impact_type="economic",
                    direction="positive",
                    severity="high",
                    description=f"Salg på {credit:.2f} (transaktion {txn['transaction_id']}) med momskode "
                                f"'{line['tax_code']}' ({rate}%) har intet registreret momsbeløb.",
                    fix_suggestion=f"Beregn og afregn salgsmoms. Forventet: {expected:.2f} DKK.",
                    estimated_amount=expected,
                    transactions=[_txn_ref(txn, line, tax_amount=vat, vat_expected=expected,
                                           highlighted_field="tax_amount")],
                ))
    return findings


# === TEST 23: Sats-konsistens pr. momskode ===

def test_23_rate_consistency_per_code(data: dict) -> list:
    """Samme momskode bør altid bruge samme sats. Flag koder med flere satser."""
    findings = []
    code_rates = defaultdict(set)
    code_examples = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            code = line["tax_code"]
            rate = line["tax_percentage"]
            if not code or rate is None:
                continue
            code_rates[code].add(rate)
            if len(code_examples[code]) < 5:
                code_examples[code].append(_txn_ref(txn, line, tax_rate=rate,
                                                    highlighted_field="tax_percentage"))

    for code, rates in code_rates.items():
        if len(rates) > 1:
            findings.append(make_finding(
                test_id=23,
                test_name="Inkonsistent sats pr. momskode",
                impact_type="compliance",
                direction="neutral",
                severity="medium",
                description=f"Momskode '{code}' bruges med flere forskellige satser: "
                            f"{', '.join(f'{r}%' for r in sorted(rates))}.",
                fix_suggestion="En momskode bør altid svare til én sats. Ret de afvigende posteringer.",
                transactions=code_examples[code],
            ))
    return findings


# === TEST 24: Implicit sats ugyldig ===

def test_24_implied_rate(data: dict) -> list:
    """Beregn implicit sats (moms/grundlag) og flag når den ikke er en gyldig DK-sats."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            base = line["tax_base"] or 0
            if vat <= 0 or base <= 0:
                continue
            implied = vr.implied_rate(base, vat)
            if implied is None:
                continue
            # Er den implicitte sats tæt på en gyldig sats?
            if any(abs(implied - r) <= vr.RATE_TOLERANCE for r in vr.VALID_DK_RATES):
                continue
            findings.append(make_finding(
                test_id=24,
                test_name="Implicit sats ugyldig",
                impact_type="economic",
                direction="negative" if implied > vr.STANDARD_RATE else "positive",
                severity="medium",
                description=f"Implicit momssats {implied}% (moms {vat:.2f} af grundlag {base:.2f}) "
                            f"på transaktion {txn['transaction_id']} matcher ingen gyldig dansk sats.",
                fix_suggestion="Tjek om momsgrundlag og momsbeløb hører sammen. "
                               "Den faktiske sats bør være 25% eller 0%.",
                estimated_amount=abs(round(vat - base * vr.STANDARD_RATE / 100, 2)),
                transactions=[_txn_ref(txn, line, implied_rate=implied,
                                       highlighted_field="tax_amount")],
            ))
    return findings


# === TEST 25: Nulsats på indenlandsk handel ===

def test_25_zero_rate_domestic(data: dict) -> list:
    """0% anvendt hvor modparten er dansk/ukendt (nulsats kræver eksport/EU/fritagelse)."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not line["tax_code"]:
                continue
            rate = line["tax_percentage"]
            base = line["tax_base"] or 0
            if rate != 0 or base <= 0:
                continue
            country = vr.normalize_country(line.get("country", ""))
            # Nulsats er kun OK for udenlandsk modpart. Tom landekode = indenlandsk antagelse.
            if vr.is_foreign(country):
                continue
            findings.append(make_finding(
                test_id=25,
                test_name="Nulsats på indenlandsk handel",
                impact_type="economic",
                direction="positive",
                severity="medium",
                description=f"Nulsats (0%) anvendt på transaktion {txn['transaction_id']} med "
                            f"grundlag {base:.2f}, men ingen udenlandsk modpart er angivet.",
                fix_suggestion="Nulsats kræver dokumentation (eksport, EU-leverance eller fritagelse). "
                               "Indenlandsk handel skal som udgangspunkt have 25% moms.",
                estimated_amount=round(base * vr.STANDARD_RATE / 100, 2),
                transactions=[_txn_ref(txn, line, country=country or "(ingen)",
                                       highlighted_field="tax_percentage")],
            ))
    return findings


# === TEST 26: Manglende momskode på momspligtigt beløb ===

def test_26_missing_tax_code(data: dict) -> list:
    """Linje med momsbeløb registreret, men uden momskode."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            if vat != 0 and not line["tax_code"]:
                findings.append(make_finding(
                    test_id=26,
                    test_name="Momsbeløb uden momskode",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Linje {line['record_id']} i transaktion {txn['transaction_id']} har "
                                f"momsbeløb {vat:.2f} men ingen momskode.",
                    fix_suggestion="Tilføj den korrekte momskode, så momsen kan henføres korrekt "
                                   "i momsangivelsen.",
                    transactions=[_txn_ref(txn, line, tax_amount=vat, highlighted_field="tax_code")],
                ))
    return findings
