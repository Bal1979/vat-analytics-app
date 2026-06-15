"""
Kategori 6: Leverandør- & Kundevalidering (Tests 47-54)

Kontrollerer stamdata for handelspartnere: manglende navne, dublerede
parter, manglende eller ugyldige CVR/momsnumre, engangsleverandører og
parter der optræder i flere roller.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


def run_party_tests(data: dict) -> list:
    findings = []
    findings.extend(test_47_missing_party_name(data))
    findings.extend(test_48_duplicate_suppliers(data))
    findings.extend(test_49_missing_cvr_dk_supplier(data))
    findings.extend(test_50_invalid_cvr_format(data))
    findings.extend(test_51_one_off_supplier(data))
    findings.extend(test_52_customer_without_vat(data))
    findings.extend(test_53_shared_identity(data))
    findings.extend(test_54_party_in_both_roles(data))
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


def _line_country(line, supplier_lookup):
    c = vr.normalize_country(line.get("country", ""))
    if c:
        return c
    sup = supplier_lookup.get(line.get("supplier_id", ""))
    if sup:
        return vr.normalize_country(sup.get("country", ""))
    return ""


def _line_vat(line, supplier_lookup):
    v = vr.clean_vat_number(line.get("vat_number", ""))
    if v:
        return v
    sup = supplier_lookup.get(line.get("supplier_id", ""))
    if sup:
        return vr.clean_vat_number(sup.get("vat_number", ""))
    return ""


# === TEST 47: Manglende parts-navn ===

def test_47_missing_party_name(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            cid = line.get("customer_id", "")
            if sid and not (line.get("supplier_name") or "").strip():
                findings.append(make_finding(
                    test_id=47, test_name="Manglende leverandørnavn",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"Leverandør-ID '{sid}' på transaktion {txn['transaction_id']} har "
                                f"intet navn registreret.",
                    fix_suggestion="Udfyld leverandørens navn i kreditorkartoteket.",
                    transactions=[_ref(txn, line, supplier_id=sid, highlighted_field="supplier_name")],
                ))
            if cid and not (line.get("customer_name") or "").strip():
                findings.append(make_finding(
                    test_id=47, test_name="Manglende kundenavn",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"Kunde-ID '{cid}' på transaktion {txn['transaction_id']} har "
                                f"intet navn registreret.",
                    fix_suggestion="Udfyld kundens navn i debitorkartoteket.",
                    transactions=[_ref(txn, line, customer_id=cid, highlighted_field="customer_name")],
                ))
    return findings


# === TEST 48: Dublerede leverandører ===

def test_48_duplicate_suppliers(data):
    """Samme normaliserede navn på forskellige leverandør-IDer."""
    findings = []
    by_name = defaultdict(set)
    name_examples = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            name = line.get("supplier_name", "")
            if not sid or not name:
                continue
            norm = vr.normalize_name(name)
            if not norm:
                continue
            by_name[norm].add(sid)
            name_examples.setdefault(norm, []).append((sid, name))

    for norm, ids in by_name.items():
        if len(ids) > 1:
            shown = {}
            for sid, name in name_examples[norm]:
                shown.setdefault(sid, name)
            findings.append(make_finding(
                test_id=48, test_name="Dublerede leverandører",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Leverandørnavn '{name_examples[norm][0][1]}' optræder under flere "
                            f"leverandør-IDer: {', '.join(sorted(ids))}.",
                fix_suggestion="Konsolidér dublerede kreditorer. Flere IDer for samme leverandør kan "
                               "skjule dobbeltbetalinger og sløre fakturahistorikken.",
                transactions=[{"supplier_id": sid, "supplier_name": nm,
                               "highlighted_field": "supplier_id"} for sid, nm in shown.items()],
            ))
    return findings


# === TEST 49: Manglende CVR/momsnr på dansk leverandør ===

def test_49_missing_cvr_dk_supplier(data):
    findings = []
    supplier_lookup = {s["supplier_id"]: s for s in data.get("suppliers", [])}
    seen = set()
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            if not sid or sid in seen:
                continue
            country = _line_country(line, supplier_lookup)
            # Tom landekode antages dansk
            if country and country != "DK":
                continue
            if _line_vat(line, supplier_lookup):
                continue
            seen.add(sid)
            findings.append(make_finding(
                test_id=49, test_name="Manglende CVR på dansk leverandør",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Dansk leverandør '{line.get('supplier_name') or sid}' har intet "
                            f"CVR/momsnummer registreret.",
                fix_suggestion="Registrér leverandørens CVR-nummer. Det er nødvendigt for at "
                               "dokumentere momsfradraget.",
                transactions=[_ref(txn, line, supplier_id=sid, highlighted_field="vat_number")],
            ))
    return findings


# === TEST 50: Ugyldigt CVR-format ===

def test_50_invalid_cvr_format(data):
    findings = []
    supplier_lookup = {s["supplier_id"]: s for s in data.get("suppliers", [])}
    seen = set()
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            country = _line_country(line, supplier_lookup)
            if country and country != "DK":
                continue
            vat = _line_vat(line, supplier_lookup)
            if not vat:
                continue
            # Træk evt. DK-præfiks fra og validér CVR-cifrene
            digits = vat[2:] if vat[:2] == "DK" else vat
            if sid in seen:
                continue
            if not vr.validate_cvr(digits):
                seen.add(sid)
                findings.append(make_finding(
                    test_id=50, test_name="Ugyldigt CVR-nummer",
                    impact_type="compliance", direction="neutral", severity="high",
                    description=f"CVR/momsnummer '{vat}' for leverandør '{line.get('supplier_name') or sid}' "
                                f"består ikke modulus-11-kontrollen (ugyldigt CVR).",
                    fix_suggestion="Ret CVR-nummeret. Et ugyldigt CVR kan betyde en fiktiv leverandør "
                                   "eller en tastefejl der underkender fradraget.",
                    transactions=[_ref(txn, line, supplier_id=sid, vat_number=vat,
                                       highlighted_field="vat_number")],
                ))
    return findings


# === TEST 51: Engangsleverandør med højt beløb ===

def test_51_one_off_supplier(data):
    findings = []
    supplier_txns = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            if sid:
                supplier_txns[sid].append((txn, line))

    if not supplier_txns:
        return findings
    amounts = [abs(l["debit_amount"] + l["credit_amount"]) for entries in supplier_txns.values()
               for _, l in entries]
    threshold = max(50000.0, vr.mean(amounts) + 2 * vr.stdev(amounts))

    for sid, entries in supplier_txns.items():
        if len(entries) != 1:
            continue
        txn, line = entries[0]
        amount = abs(line["debit_amount"] + line["credit_amount"])
        if amount >= threshold:
            findings.append(make_finding(
                test_id=51, test_name="Engangsleverandør, højt beløb",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Leverandør '{line.get('supplier_name') or sid}' optræder kun én gang, "
                            f"med et stort beløb ({amount:.2f}).",
                fix_suggestion="Engangsleverandører med store beløb bør stikprøvekontrolleres for "
                               "ægthed (fiktive fakturaer / svindel).",
                estimated_amount=amount,
                transactions=[_ref(txn, line, supplier_id=sid, highlighted_field="amount")],
            ))
    return findings


# === TEST 52: Kunde uden momsnr ved store beløb ===

def test_52_customer_without_vat(data):
    findings = []
    customer_totals = defaultdict(float)
    customer_example = {}
    customer_vat = defaultdict(str)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            cid = line.get("customer_id", "")
            if not cid:
                continue
            customer_totals[cid] += abs(line["credit_amount"] + line["debit_amount"])
            customer_vat[cid] = customer_vat[cid] or vr.clean_vat_number(line.get("vat_number", ""))
            customer_example.setdefault(cid, (txn, line))

    for cid, total in customer_totals.items():
        if total >= 100000.0 and not customer_vat[cid]:
            txn, line = customer_example[cid]
            findings.append(make_finding(
                test_id=52, test_name="Stor kunde uden momsnummer",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Kunde '{line.get('customer_name') or cid}' har en samlet omsætning på "
                            f"{total:.2f} men intet momsnummer. Ved B2B-handel bør momsnr foreligge.",
                fix_suggestion="Afklar om kunden er erhvervsdrivende (B2B → indhent momsnr) eller "
                               "privat (B2C → korrekt).",
                estimated_amount=total,
                transactions=[_ref(txn, line, customer_id=cid, highlighted_field="vat_number")],
            ))
    return findings


# === TEST 53: Samme identitet på flere parter ===

def test_53_shared_identity(data):
    """Samme momsnummer knyttet til flere forskellige parts-IDer."""
    findings = []
    vat_to_ids = defaultdict(set)
    vat_example = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = vr.clean_vat_number(line.get("vat_number", ""))
            if not vat:
                continue
            pid = line.get("supplier_id") or line.get("customer_id") or ""
            if not pid:
                continue
            vat_to_ids[vat].add(pid)
            vat_example.setdefault(vat, (txn, line))

    for vat, ids in vat_to_ids.items():
        if len(ids) > 1:
            txn, line = vat_example[vat]
            findings.append(make_finding(
                test_id=53, test_name="Samme momsnr på flere parter",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Momsnummer '{vat}' er knyttet til flere parts-IDer: "
                            f"{', '.join(sorted(ids))}.",
                fix_suggestion="Samme momsnummer på flere kreditorer/debitorer kan være en dublet "
                               "eller en forsøg på at sløre samhandel. Konsolidér eller undersøg.",
                transactions=[_ref(txn, line, vat_number=vat, party_ids=sorted(ids),
                                   highlighted_field="vat_number")],
            ))
    return findings


# === TEST 54: Part i både leverandør- og kunderolle ===

def test_54_party_in_both_roles(data):
    findings = []
    supplier_ids = set()
    customer_ids = set()
    example = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if line.get("supplier_id"):
                supplier_ids.add(line["supplier_id"])
                example.setdefault(line["supplier_id"], (txn, line))
            if line.get("customer_id"):
                customer_ids.add(line["customer_id"])
                example.setdefault(line["customer_id"], (txn, line))

    for pid in supplier_ids & customer_ids:
        txn, line = example[pid]
        findings.append(make_finding(
            test_id=54, test_name="Part i begge roller",
            impact_type="compliance", direction="neutral", severity="medium",
            description=f"Part '{pid}' optræder både som leverandør og kunde. "
                        f"Modregning/round-tripping bør kontrolleres.",
            fix_suggestion="Bekræft at samhandlen begge veje er reel. Parter i dobbeltrolle kan "
                           "bruges til at oppuste omsætning eller udligne mellemværender uden for moms.",
            transactions=[_ref(txn, line, party_id=pid, highlighted_field="supplier_id")],
        ))
    return findings
