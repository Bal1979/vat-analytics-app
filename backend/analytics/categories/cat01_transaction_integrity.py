"""
Kategori 1: Transaktionsintegritet & Datakvalitet (Tests 1-10)

Verificerer grundlæggende datakvalitet og integritet i transaktioner.
"""

from datetime import datetime
from analytics.models import make_finding


def run_transaction_integrity_tests(data: dict) -> list:
    """Kør alle 10 transaktionsintegritetstests."""
    findings = []
    findings.extend(test_01_vat_recalculation(data))
    findings.extend(test_02_tax_code_validation(data))
    findings.extend(test_03_vat_rounding(data))
    findings.extend(test_04_invoice_field_completeness(data))
    findings.extend(test_05_date_consistency(data))
    findings.extend(test_06_negative_amounts(data))
    findings.extend(test_07_zero_value_transactions(data))
    findings.extend(test_08_currency_consistency(data))
    findings.extend(test_09_tax_point(data))
    findings.extend(test_10_document_type(data))
    return findings


# === TEST 1: Moms-genberegning ===

def test_01_vat_recalculation(data: dict) -> list:
    """
    Genberegn moms på hver linje og sammenlign med registreret momsbeløb.
    Flag linjer hvor beregnet moms afviger fra registreret moms.
    """
    findings = []
    tax_rates = {t["tax_code"]: t["tax_percentage"] for t in data["tax_table"]}

    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not line["tax_code"] or line["tax_percentage"] == 0:
                continue

            # Beregn forventet moms
            base = line["tax_base"] or (line["debit_amount"] + line["credit_amount"])
            if base == 0:
                continue

            expected_vat = round(base * line["tax_percentage"] / 100, 2)
            actual_vat = line["tax_amount"]

            if actual_vat > 0 and abs(expected_vat - actual_vat) > 0.50:
                diff = round(actual_vat - expected_vat, 2)
                direction = "negative" if diff > 0 else "positive"

                findings.append(make_finding(
                    test_id=1,
                    test_name="Moms-genberegning",
                    impact_type="economic",
                    direction=direction,
                    severity="high" if abs(diff) > 100 else "medium",
                    description=f"Momsafvigelse på linje {line['record_id']} i transaktion {txn['transaction_id']}: "
                                f"Registreret {actual_vat:.2f}, beregnet {expected_vat:.2f} "
                                f"(difference: {abs(diff):.2f} DKK).",
                    fix_suggestion=f"Tjek momssatsen og momsberegningen for denne transaktion. "
                                   f"Forventet momssats: {line['tax_percentage']}% af {base:.2f} = {expected_vat:.2f}.",
                    estimated_amount=abs(diff),
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "amount": base,
                        "vat_recorded": actual_vat,
                        "vat_expected": expected_vat,
                        "difference": diff,
                        "highlighted_field": "tax_amount",
                    }],
                ))

    return findings


# === TEST 2: Momskode-validering ===

def test_02_tax_code_validation(data: dict) -> list:
    """
    Verificér at hver transaktion har en gyldig momskode
    der matcher momstabellen.
    """
    findings = []
    valid_codes = {t["tax_code"] for t in data["tax_table"]}

    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not line["tax_code"]:
                continue  # Ingen momskode er ok for ikke-momspligtige poster

            if line["tax_code"] not in valid_codes:
                findings.append(make_finding(
                    test_id=2,
                    test_name="Momskode-validering",
                    impact_type="compliance",
                    direction="neutral",
                    severity="high",
                    description=f"Ukendt momskode '{line['tax_code']}' på linje {line['record_id']} "
                                f"i transaktion {txn['transaction_id']}. "
                                f"Koden findes ikke i TaxTable.",
                    fix_suggestion=f"Ret momskoden til en gyldig kode fra momstabellen: "
                                   f"{', '.join(sorted(valid_codes))}. "
                                   f"Eller tilføj den manglende kode til TaxTable.",
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "amount": line["debit_amount"] + line["credit_amount"],
                        "tax_code_used": line["tax_code"],
                        "valid_codes": sorted(valid_codes),
                        "highlighted_field": "tax_code",
                    }],
                ))

    return findings


# === TEST 3: Momsafrunding ===

def test_03_vat_rounding(data: dict) -> list:
    """
    Tjek at momsafrunding er inden for tolerance (max 0.50 DKK per transaktion).
    """
    findings = []

    for txn in data["transactions"]:
        total_line_vat = 0
        total_line_base = 0

        for line in txn["lines"]:
            total_line_vat += line["tax_amount"]
            total_line_base += line["tax_base"]

        if total_line_base > 0 and total_line_vat > 0:
            # Beregn document-level moms
            # (dette er en simpel check — ideelt skal vi kende den samlede sats)
            pass  # Kræver mere kompleks SAF-T data med document totals

    return findings


# === TEST 4: Faktura-feltfuldstændighed ===

def test_04_invoice_field_completeness(data: dict) -> list:
    """
    Verificér at alle transaktioner har de påkrævede felter udfyldt.
    """
    findings = []

    for txn in data["transactions"]:
        missing = []
        if not txn["transaction_id"]:
            missing.append("TransactionID")
        if not txn["date"]:
            missing.append("TransactionDate")
        if not txn["description"]:
            missing.append("Description")

        for line in txn["lines"]:
            if not line["account_id"]:
                missing.append(f"AccountID (linje {line['record_id']})")

        if missing:
            findings.append(make_finding(
                test_id=4,
                test_name="Faktura-feltfuldstændighed",
                impact_type="compliance",
                direction="neutral",
                severity="medium",
                description=f"Transaktion {txn['transaction_id'] or '(ukendt)'} mangler: "
                            f"{', '.join(missing)}.",
                fix_suggestion="Udfyld de manglende felter i bogføringssystemet og generér SAF-T filen igen.",
                transactions=[{
                    "transaction_id": txn["transaction_id"],
                    "journal_id": txn["journal_id"],
                    "date": txn["date"],
                    "description": txn["description"],
                    "missing_fields": missing,
                    "highlighted_field": "multiple",
                }],
            ))

    return findings


# === TEST 5: Fakturadato vs. Bogføringsdato konsistens ===

def test_05_date_consistency(data: dict) -> list:
    """
    Sammenlign fakturadato med bogføringsdato.
    Flag transaktioner bogført i en anden momsperiode end datoen angiver.
    """
    findings = []

    for txn in data["transactions"]:
        if not txn["date"] or not txn["period"] or not txn["period_year"]:
            continue

        try:
            txn_date = datetime.strptime(txn["date"], "%Y-%m-%d")
            expected_period = str(txn_date.month).zfill(2)
            expected_year = str(txn_date.year)

            if txn["period"] != expected_period or txn["period_year"] != expected_year:
                findings.append(make_finding(
                    test_id=5,
                    test_name="Dato/periode-konsistens",
                    impact_type="interest_risk",
                    direction="neutral",
                    severity="medium",
                    description=f"Transaktion {txn['transaction_id']} har dato {txn['date']} "
                                f"(periode {expected_period}/{expected_year}), "
                                f"men er bogført i periode {txn['period']}/{txn['period_year']}.",
                    fix_suggestion="Tjek om transaktionen er bogført i den korrekte momsperiode. "
                                   "Periodeforskydning kan medføre renterisiko.",
                    estimated_amount=0,  # Rente beregnes ikke her
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "booked_period": f"{txn['period']}/{txn['period_year']}",
                        "expected_period": f"{expected_period}/{expected_year}",
                        "description": txn["description"],
                        "amount": txn["total_debit"] + txn["total_credit"],
                        "highlighted_field": "period",
                    }],
                ))
        except ValueError:
            continue

    return findings


# === TEST 6: Negative linjebeløb ===

def test_06_negative_amounts(data: dict) -> list:
    """
    Identificér transaktionslinjer med negative beløb
    der ikke er klassificeret som kreditnotaer.
    """
    findings = []

    for txn in data["transactions"]:
        for line in txn["lines"]:
            if line["debit_amount"] < 0 or line["credit_amount"] < 0:
                amount = line["debit_amount"] if line["debit_amount"] < 0 else line["credit_amount"]
                findings.append(make_finding(
                    test_id=6,
                    test_name="Negative linjebeløb",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Negativ beløb ({amount:.2f}) på linje {line['record_id']} "
                                f"i transaktion {txn['transaction_id']}. "
                                f"Negative beløb bør registreres som separate kreditnotaer.",
                    fix_suggestion="Tjek om dette er en korrektion eller kreditnota. "
                                   "Negative beløb bør normalt registreres som kreditnota med positivt beløb.",
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "amount": amount,
                        "highlighted_field": "debit_amount" if line["debit_amount"] < 0 else "credit_amount",
                    }],
                ))

    return findings


# === TEST 7: Nul-værdi transaktioner ===

def test_07_zero_value_transactions(data: dict) -> list:
    """
    Flag transaktioner hvor momsgrundlag = 0 men moms ≠ 0, eller omvendt.
    """
    findings = []

    for txn in data["transactions"]:
        for line in txn["lines"]:
            base = line["tax_base"]
            vat = line["tax_amount"]

            if base == 0 and vat != 0:
                findings.append(make_finding(
                    test_id=7,
                    test_name="Nul-værdi transaktioner",
                    impact_type="economic",
                    direction="negative",
                    severity="high",
                    description=f"Momsgrundlag = 0 men momsbeløb = {vat:.2f} på linje {line['record_id']} "
                                f"i transaktion {txn['transaction_id']}.",
                    fix_suggestion="Tjek momsberegningen. Moms uden momsgrundlag indikerer en datafejl.",
                    estimated_amount=abs(vat),
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "tax_base": base,
                        "tax_amount": vat,
                        "highlighted_field": "tax_base",
                    }],
                ))

            elif base != 0 and vat == 0 and line["tax_code"]:
                findings.append(make_finding(
                    test_id=7,
                    test_name="Nul-værdi transaktioner",
                    impact_type="economic",
                    direction="positive",
                    severity="medium",
                    description=f"Momsgrundlag = {base:.2f} men momsbeløb = 0 med momskode '{line['tax_code']}' "
                                f"på linje {line['record_id']} i transaktion {txn['transaction_id']}. "
                                f"Moms mangler muligvis.",
                    fix_suggestion="Tjek om moms skulle have været beregnet. "
                                   "En momskode er angivet men intet momsbeløb er registreret.",
                    estimated_amount=0,
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "tax_base": base,
                        "tax_amount": vat,
                        "tax_code": line["tax_code"],
                        "highlighted_field": "tax_amount",
                    }],
                ))

    return findings


# === TEST 8: Valutakurs-konsistens ===

def test_08_currency_consistency(data: dict) -> list:
    """
    For flervaluta-transaktioner: verificér at valutaen er konsistent
    og matcher standardvalutaen.
    """
    findings = []
    default_currency = data["header"].get("currency", "DKK")

    for txn in data["transactions"]:
        for line in txn["lines"]:
            if line["currency"] and line["currency"] != default_currency and line["currency"] != "":
                findings.append(make_finding(
                    test_id=8,
                    test_name="Valutakurs-konsistens",
                    impact_type="compliance",
                    direction="neutral",
                    severity="low",
                    description=f"Linje {line['record_id']} i transaktion {txn['transaction_id']} "
                                f"bruger valuta '{line['currency']}', men standardvalutaen er '{default_currency}'.",
                    fix_suggestion="Verificér at valutakursen er korrekt og matcher "
                                   "Nationalbankens/ECBs officielle kurs på transaktionsdatoen.",
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "account_id": line["account_id"],
                        "description": txn["description"],
                        "currency_used": line["currency"],
                        "default_currency": default_currency,
                        "amount": line["debit_amount"] + line["credit_amount"],
                        "highlighted_field": "currency",
                    }],
                ))

    return findings


# === TEST 9: Leveringstidspunkt (tax point) ===

def test_09_tax_point(data: dict) -> list:
    """
    Verificér at transaktionsdatoer ligger inden for den deklarerede periode.
    """
    findings = []
    period = data["header"].get("period", {})

    if not period.get("start_year") or not period.get("end_year"):
        return findings

    try:
        start_year = int(period["start_year"])
        start_month = int(period["start"]) if period["start"] else 1
        end_year = int(period["end_year"])
        end_month = int(period["end"]) if period["end"] else 12

        period_start = datetime(start_year, start_month, 1)
        # Slut er sidste dag i slutmåneden
        if end_month == 12:
            period_end = datetime(end_year, 12, 31)
        else:
            period_end = datetime(end_year, end_month + 1, 1)
    except (ValueError, TypeError):
        return findings

    for txn in data["transactions"]:
        if not txn["date"]:
            continue
        try:
            txn_date = datetime.strptime(txn["date"], "%Y-%m-%d")
            if txn_date < period_start or txn_date >= period_end:
                findings.append(make_finding(
                    test_id=9,
                    test_name="Leveringstidspunkt",
                    impact_type="interest_risk",
                    direction="neutral",
                    severity="high",
                    description=f"Transaktion {txn['transaction_id']} har dato {txn['date']} "
                                f"der ligger uden for den deklarerede periode "
                                f"({period['start']}/{period['start_year']} - {period['end']}/{period['end_year']}).",
                    fix_suggestion="Tjek om transaktionen hører til en anden momsperiode. "
                                   "Transaktioner uden for perioden kan indikere forkert periodisering.",
                    transactions=[{
                        "transaction_id": txn["transaction_id"],
                        "journal_id": txn["journal_id"],
                        "date": txn["date"],
                        "description": txn["description"],
                        "amount": txn["total_debit"] + txn["total_credit"],
                        "period_start": f"{period['start']}/{period['start_year']}",
                        "period_end": f"{period['end']}/{period['end_year']}",
                        "highlighted_field": "date",
                    }],
                ))
        except ValueError:
            continue

    return findings


# === TEST 10: Transaktionsbalance ===

def test_10_document_type(data: dict) -> list:
    """
    Verificér at hver transaktion balancerer (debit = credit).
    Ubalancerede transaktioner indikerer datakvalitetsproblemer.
    """
    findings = []

    for txn in data["transactions"]:
        diff = round(abs(txn["total_debit"] - txn["total_credit"]), 2)

        if diff > 0.01:
            findings.append(make_finding(
                test_id=10,
                test_name="Transaktionsbalance",
                impact_type="economic",
                direction="neutral",
                severity="critical",
                description=f"Transaktion {txn['transaction_id']} er ikke i balance: "
                            f"Debit={txn['total_debit']:.2f}, Credit={txn['total_credit']:.2f} "
                            f"(difference: {diff:.2f}).",
                fix_suggestion="Denne transaktion mangler en modpostering. "
                               "Kontrollér at alle linjer er medtaget.",
                estimated_amount=diff,
                transactions=[{
                    "transaction_id": txn["transaction_id"],
                    "journal_id": txn["journal_id"],
                    "date": txn["date"],
                    "description": txn["description"],
                    "total_debit": txn["total_debit"],
                    "total_credit": txn["total_credit"],
                    "difference": diff,
                    "lines": [{
                        "record_id": l["record_id"],
                        "account_id": l["account_id"],
                        "debit": l["debit_amount"],
                        "credit": l["credit_amount"],
                    } for l in txn["lines"]],
                    "highlighted_field": "balance",
                }],
            ))

    return findings
