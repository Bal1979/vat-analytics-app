"""
Kategori 2: Dubletdetektion (Tests 11-18)

Finder duplikerede fakturaer, betalinger og kreditnotaer.
"""

import re
from collections import defaultdict
from analytics.models import make_finding


def run_duplicate_detection_tests(data: dict) -> list:
    """Kør alle 8 dubletdetektionstests."""
    findings = []
    findings.extend(test_11_exact_duplicate(data))
    findings.extend(test_12_fuzzy_duplicate(data))
    findings.extend(test_13_same_amount_same_vendor(data))
    findings.extend(test_14_normalized_invoice_number(data))
    findings.extend(test_15_duplicate_payment(data))
    findings.extend(test_16_credit_note_duplicate(data))
    findings.extend(test_17_cross_entity_duplicate(data))
    findings.extend(test_18_sequential_gaps(data))
    return findings


def _normalize_id(s: str) -> str:
    """Normalisér et ID: fjern special-tegn, leading zeros, konvertér til uppercase."""
    s = s.upper().strip()
    s = re.sub(r"[^A-Z0-9]", "", s)
    s = s.lstrip("0") or "0"
    return s


# === TEST 11: Eksakt dubletfaktura ===

def test_11_exact_duplicate(data: dict) -> list:
    """
    Find transaktioner hvor alle nøglefelter er identiske:
    leverandør/kunde, beløb, dato og kilde-dokument.
    """
    findings = []

    # Gruppér transaktioner efter en nøgle
    seen = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            key = (
                line["supplier_id"] or line["customer_id"] or "",
                line["account_id"],
                round(line["debit_amount"] + line["credit_amount"], 2),
                txn["date"],
                line["source_document_id"],
            )
            if key[0] and key[4]:  # Kun hvis vi har leverandør/kunde OG dokumentref
                seen[key].append({
                    "transaction_id": txn["transaction_id"],
                    "journal_id": txn["journal_id"],
                    "date": txn["date"],
                    "account_id": line["account_id"],
                    "description": txn["description"],
                    "amount": line["debit_amount"] + line["credit_amount"],
                    "supplier_id": line["supplier_id"],
                    "customer_id": line["customer_id"],
                    "source_document_id": line["source_document_id"],
                    "highlighted_field": "source_document_id",
                })

    for key, txns in seen.items():
        if len(txns) > 1:
            amount = key[2]
            findings.append(make_finding(
                test_id=11,
                test_name="Eksakt dubletfaktura",
                impact_type="economic",
                direction="negative",
                severity="critical",
                description=f"{len(txns)} identiske posteringer fundet: "
                            f"Leverandør/Kunde '{key[0]}', beløb {amount:.2f}, "
                            f"dato {key[3]}, dokument '{key[4]}'. "
                            f"Mulig dobbeltbogføring.",
                fix_suggestion="Undersøg om fakturaen er bogført mere end én gang. "
                               "Fjern den duplikerede postering og ret momsangivelsen.",
                estimated_amount=amount * (len(txns) - 1),
                transactions=txns,
            ))

    return findings


# === TEST 12: Fuzzy dubletfaktura ===

def test_12_fuzzy_duplicate(data: dict) -> list:
    """
    Find transaktioner hvor leverandør og beløb matcher men
    dokumentnumre afviger minimalt (normalisering).
    """
    findings = []

    # Gruppér per leverandør + beløb + dato
    groups = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            party = line["supplier_id"] or line["customer_id"]
            if not party or not line["source_document_id"]:
                continue
            key = (party, round(line["debit_amount"] + line["credit_amount"], 2), txn["date"])
            groups[key].append({
                "transaction_id": txn["transaction_id"],
                "original_doc_id": line["source_document_id"],
                "normalized_doc_id": _normalize_id(line["source_document_id"]),
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "account_id": line["account_id"],
                "description": txn["description"],
                "amount": line["debit_amount"] + line["credit_amount"],
                "highlighted_field": "source_document_id",
            })

    for key, entries in groups.items():
        if len(entries) < 2:
            continue

        # Tjek om normaliserede doc-IDs matcher mens originale er forskellige
        norm_groups = defaultdict(list)
        for e in entries:
            norm_groups[e["normalized_doc_id"]].append(e)

        for norm_id, norm_entries in norm_groups.items():
            original_ids = {e["original_doc_id"] for e in norm_entries}
            if len(norm_entries) > 1 and len(original_ids) > 1:
                amount = key[1]
                findings.append(make_finding(
                    test_id=12,
                    test_name="Fuzzy dubletfaktura",
                    impact_type="economic",
                    direction="negative",
                    severity="high",
                    description=f"Mulig dublet: {len(norm_entries)} posteringer fra "
                                f"'{key[0]}' med beløb {amount:.2f} og lignende dokumentnumre: "
                                f"{', '.join(sorted(original_ids))}.",
                    fix_suggestion="Dokumentnumrene ligner hinanden efter normalisering. "
                                   "Undersøg om det er samme faktura bogført med forskellige formater.",
                    estimated_amount=amount * (len(norm_entries) - 1),
                    transactions=norm_entries,
                ))

    return findings


# === TEST 13: Samme beløb, samme leverandør ===

def test_13_same_amount_same_vendor(data: dict) -> list:
    """
    Find flere transaktioner fra samme leverandør med identisk beløb
    inden for 30 dage.
    """
    findings = []
    from datetime import datetime, timedelta

    # Gruppér per leverandør + beløb
    vendor_amounts = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not line["supplier_id"]:
                continue
            amount = round(line["debit_amount"] + line["credit_amount"], 2)
            if amount <= 0:
                continue
            vendor_amounts[(line["supplier_id"], amount)].append({
                "transaction_id": txn["transaction_id"],
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "account_id": line["account_id"],
                "description": txn["description"],
                "amount": amount,
                "supplier_id": line["supplier_id"],
                "highlighted_field": "amount",
            })

    for (vendor, amount), entries in vendor_amounts.items():
        if len(entries) < 2:
            continue

        # Tjek om de er inden for 30 dage af hinanden
        dated_entries = []
        for e in entries:
            try:
                d = datetime.strptime(e["date"], "%Y-%m-%d")
                dated_entries.append((d, e))
            except (ValueError, TypeError):
                continue

        dated_entries.sort(key=lambda x: x[0])

        clusters = []
        current_cluster = [dated_entries[0]] if dated_entries else []

        for i in range(1, len(dated_entries)):
            if (dated_entries[i][0] - dated_entries[i-1][0]).days <= 30:
                current_cluster.append(dated_entries[i])
            else:
                if len(current_cluster) > 1:
                    clusters.append(current_cluster)
                current_cluster = [dated_entries[i]]
        if len(current_cluster) > 1:
            clusters.append(current_cluster)

        for cluster in clusters:
            txn_list = [e for _, e in cluster]
            findings.append(make_finding(
                test_id=13,
                test_name="Samme beløb, samme leverandør",
                impact_type="economic",
                direction="negative",
                severity="medium",
                description=f"{len(txn_list)} posteringer fra leverandør '{vendor}' "
                            f"med identisk beløb {amount:.2f} inden for 30 dage.",
                fix_suggestion="Undersøg om dette er separate fakturaer eller en dobbeltbogføring. "
                               "Sammenlign med leverandørens kontoudtog.",
                estimated_amount=amount * (len(txn_list) - 1),
                transactions=txn_list,
            ))

    return findings


# === TEST 14: Normaliseret fakturanummer ===

def test_14_normalized_invoice_number(data: dict) -> list:
    """
    Normalisér alle fakturanumre og find genbrug af samme dokumentnummer
    — uafhængigt af leverandør. Samme normaliserede fakturanummer brugt på
    flere posteringer kan indikere dublet eller forkert nummerering.
    """
    findings = []

    norm_groups = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            doc = line["source_document_id"]
            if not doc:
                continue
            norm = _normalize_id(doc)
            if not norm or norm == "0":
                continue
            party = line["supplier_id"] or line["customer_id"] or ""
            norm_groups[norm].append({
                "transaction_id": txn["transaction_id"],
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "account_id": line["account_id"],
                "description": txn["description"],
                "amount": line["debit_amount"] + line["credit_amount"],
                "original_doc_id": doc,
                "party": party,
                "highlighted_field": "source_document_id",
            })

    for norm, entries in norm_groups.items():
        # Kun interessant hvis dokumentnummeret optræder på mere end én postering
        if len(entries) < 2:
            continue
        # Spring over hvis det er præcis samme part+beløb+dato (fanges af test 11/12)
        signatures = {(e["party"], round(e["amount"], 2), e["date"]) for e in entries}
        if len(signatures) < 2:
            continue
        findings.append(make_finding(
            test_id=14,
            test_name="Genbrugt fakturanummer",
            impact_type="compliance",
            direction="neutral",
            severity="medium",
            description=f"Fakturanummer '{norm}' (normaliseret) er brugt på {len(entries)} "
                        f"posteringer med forskellige beløb/parter/datoer.",
            fix_suggestion="Undersøg om samme fakturanummer er genbrugt fejlagtigt, "
                           "eller om to forskellige bilag har fået samme nummer.",
            transactions=entries,
        ))

    return findings


# === TEST 15: Dobbelbetalingsdetektion ===

def test_15_duplicate_payment(data: dict) -> list:
    """
    Identificér potentielle dobbeltbetalinger baseret på
    identisk beløb + konto + kort tidsinterval.
    """
    findings = []
    from datetime import datetime

    # Gruppér betalingslignende transaktioner (kredit på bankkonti)
    # Vi antager at konti med type "Asset" og credit-posteringer er betalinger
    asset_accounts = {a["account_id"] for a in data["accounts"] if a["account_type"] == "Asset"}

    payments = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if line["account_id"] in asset_accounts and line["credit_amount"] > 0:
                key = (line["account_id"], round(line["credit_amount"], 2))
                payments[key].append({
                    "transaction_id": txn["transaction_id"],
                    "journal_id": txn["journal_id"],
                    "date": txn["date"],
                    "account_id": line["account_id"],
                    "description": txn["description"],
                    "amount": line["credit_amount"],
                    "highlighted_field": "credit_amount",
                })

    for (account, amount), entries in payments.items():
        if len(entries) < 2 or amount < 100:  # Ignorer små beløb
            continue

        # Tjek om de er inden for 7 dage
        dated = []
        for e in entries:
            try:
                dated.append((datetime.strptime(e["date"], "%Y-%m-%d"), e))
            except (ValueError, TypeError):
                continue

        dated.sort(key=lambda x: x[0])
        for i in range(1, len(dated)):
            if (dated[i][0] - dated[i-1][0]).days <= 7:
                findings.append(make_finding(
                    test_id=15,
                    test_name="Dobbeltbetalingsdetektion",
                    impact_type="economic",
                    direction="negative",
                    severity="high",
                    description=f"Mulig dobbeltbetaling: {amount:.2f} fra konto {account} "
                                f"den {dated[i-1][1]['date']} og {dated[i][1]['date']} (inden for 7 dage).",
                    fix_suggestion="Undersøg om dette er to separate betalinger eller en dobbeltbetaling. "
                                   "Afstem med bankkontoudtog.",
                    estimated_amount=amount,
                    transactions=[dated[i-1][1], dated[i][1]],
                ))

    return findings


# === TEST 16: Kreditnota-dubletter ===

def test_16_credit_note_duplicate(data: dict) -> list:
    """
    Find duplikerede kreditnotaer: posteringer der ligner kreditnotaer
    (negativt beløb eller beskrivelse med "kreditnota") og optræder flere
    gange med samme part, beløb og dokument.
    """
    findings = []

    seen = defaultdict(list)
    for txn in data["transactions"]:
        desc = (txn["description"] or "").lower()
        is_credit_note_text = "kreditnota" in desc or "credit note" in desc or "kreditnotaer" in desc
        for line in txn["lines"]:
            amount = line["debit_amount"] + line["credit_amount"]
            is_negative = line["debit_amount"] < 0 or line["credit_amount"] < 0
            if not (is_credit_note_text or is_negative):
                continue
            party = line["supplier_id"] or line["customer_id"] or ""
            if not party:
                continue
            key = (party, round(abs(amount), 2), line["source_document_id"], txn["date"])
            seen[key].append({
                "transaction_id": txn["transaction_id"],
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "account_id": line["account_id"],
                "description": txn["description"],
                "amount": amount,
                "party": party,
                "source_document_id": line["source_document_id"],
                "highlighted_field": "amount",
            })

    for key, entries in seen.items():
        if len(entries) > 1:
            findings.append(make_finding(
                test_id=16,
                test_name="Kreditnota-dubletter",
                impact_type="economic",
                direction="negative",
                severity="high",
                description=f"{len(entries)} ens kreditnota-posteringer for part '{key[0]}' "
                            f"med beløb {key[1]:.2f}, dokument '{key[2]}', dato {key[3]}. "
                            f"Mulig dobbeltkreditering.",
                fix_suggestion="Undersøg om samme kreditnota er bogført mere end én gang. "
                               "Dobbelte kreditnotaer reducerer momstilsvaret fejlagtigt.",
                estimated_amount=key[1] * (len(entries) - 1),
                transactions=entries,
            ))

    return findings


# === TEST 17: Tværgående enheds-dubletter ===

def test_17_cross_entity_duplicate(data: dict) -> list:
    """
    Find samme faktura bogført mod forskellige leverandører/kunder:
    identisk (normaliseret) dokumentnummer + beløb, men forskellige parts-IDer.
    Indikerer at ét bilag er konteret på flere kreditor-/debitorkonti.
    """
    findings = []

    groups = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            doc = line["source_document_id"]
            if not doc:
                continue
            norm = _normalize_id(doc)
            if not norm or norm == "0":
                continue
            amount = round(line["debit_amount"] + line["credit_amount"], 2)
            party = line["supplier_id"] or line["customer_id"] or ""
            if not party:
                continue
            groups[(norm, amount)].append({
                "transaction_id": txn["transaction_id"],
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "account_id": line["account_id"],
                "description": txn["description"],
                "amount": amount,
                "party": party,
                "original_doc_id": doc,
                "highlighted_field": "party",
            })

    for (norm, amount), entries in groups.items():
        parties = {e["party"] for e in entries}
        if len(entries) > 1 and len(parties) > 1:
            findings.append(make_finding(
                test_id=17,
                test_name="Tværgående enheds-dubletter",
                impact_type="economic",
                direction="negative",
                severity="high",
                description=f"Fakturanummer '{norm}' med beløb {amount:.2f} er bogført mod "
                            f"{len(parties)} forskellige parter: {', '.join(sorted(parties))}. "
                            f"Mulig dobbeltbogføring på tværs af konti.",
                fix_suggestion="Kontrollér om samme bilag er konteret på flere kreditor-/"
                               "debitorkonti. Fjern den overflødige postering.",
                estimated_amount=amount * (len(entries) - 1),
                transactions=entries,
            ))

    return findings


# === TEST 18: Sekventielle fakturanummer — huller og dubletter ===

def test_18_sequential_gaps(data: dict) -> list:
    """
    Analysér transaktions-ID sekvenser for huller (slettede transaktioner?)
    og dubletter (samme nummer brugt flere gange).
    """
    findings = []

    # Tjek for duplikerede TransactionIDs
    txn_ids = defaultdict(list)
    for txn in data["transactions"]:
        if txn["transaction_id"]:
            txn_ids[txn["transaction_id"]].append({
                "transaction_id": txn["transaction_id"],
                "journal_id": txn["journal_id"],
                "date": txn["date"],
                "description": txn["description"],
                "amount": txn["total_debit"] + txn["total_credit"],
                "highlighted_field": "transaction_id",
            })

    for txn_id, entries in txn_ids.items():
        if len(entries) > 1:
            findings.append(make_finding(
                test_id=18,
                test_name="Sekventielle transaktionsnumre",
                impact_type="compliance",
                direction="neutral",
                severity="high",
                description=f"TransactionID '{txn_id}' bruges {len(entries)} gange. "
                            f"Transaktions-IDer skal være unikke.",
                fix_suggestion="Undersøg hvorfor samme transaktionsnummer er brugt flere gange. "
                               "Det kan indikere en systemfejl eller manipulation.",
                transactions=entries,
            ))

    # Tjek for numeriske huller i TransactionIDs
    numeric_ids = []
    for txn_id in txn_ids.keys():
        # Prøv at ekstrahere tal fra ID
        numbers = re.findall(r"\d+", txn_id)
        if numbers:
            numeric_ids.append((int(numbers[-1]), txn_id))

    if len(numeric_ids) >= 3:
        numeric_ids.sort()
        gaps = []
        for i in range(1, len(numeric_ids)):
            diff = numeric_ids[i][0] - numeric_ids[i-1][0]
            if diff > 1:
                gaps.append((numeric_ids[i-1][1], numeric_ids[i][1], diff - 1))

        if gaps:
            total_missing = sum(g[2] for g in gaps)
            gap_descriptions = [f"mellem {g[0]} og {g[1]} ({g[2]} manglende)" for g in gaps[:5]]

            findings.append(make_finding(
                test_id=18,
                test_name="Sekventielle transaktionsnumre",
                impact_type="compliance",
                direction="neutral",
                severity="medium",
                description=f"{total_missing} huller fundet i transaktionsnummer-sekvensen: "
                            f"{'; '.join(gap_descriptions)}"
                            f"{'...' if len(gaps) > 5 else ''}.",
                fix_suggestion="Huller i nummersekvensen kan indikere slettede transaktioner. "
                               "Undersøg om de manglende numre er annullerede eller fejlagtigt fjernet.",
                transactions=[{
                    "gap_from": g[0],
                    "gap_to": g[1],
                    "missing_count": g[2],
                    "highlighted_field": "transaction_id",
                } for g in gaps],
            ))

    return findings
