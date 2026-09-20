"""
Kategori 11: Svindel & Karrusel/MTIC (Tests 84-93)

Heuristikker for momssvindel: missing trader-indikatorer, hurtig
ind-/udgående gennemstrømning, højrisikovarer, gennemstrømningskonti og
mistænkelige faktura- og beløbsmønstre.

Flere klassiske MTIC-tests kræver vare-flow-data (forsendelse, ejerskab)
der ikke findes i en flad regnskabseksport. De springer pænt over.
"""

from datetime import datetime
from collections import defaultdict
from analytics.models import make_finding
from analytics import materiality
from analytics import vat_rules as vr


def run_fraud_tests(data: dict) -> list:
    supplier_lookup = {s["supplier_id"]: s for s in data.get("suppliers", [])}
    findings = []
    findings.extend(test_84_missing_trader(data, supplier_lookup))
    findings.extend(test_85_carousel_pattern(data))
    findings.extend(test_86_rapid_throughput(data))
    findings.extend(test_87_high_risk_goods(data))
    findings.extend(test_88_zero_margin(data))
    findings.extend(test_89_new_high_volume_party(data))
    findings.extend(test_90_payment_pattern(data))
    findings.extend(test_91_clearing_account_abuse(data))
    findings.extend(test_92_false_invoice(data, supplier_lookup))
    findings.extend(test_93_identical_amounts_across_parties(data))
    return findings


def _parse(d):
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


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


def _country(line, supplier_lookup):
    c = vr.normalize_country(line.get("country", ""))
    if c:
        return c
    sup = supplier_lookup.get(line.get("supplier_id", ""))
    return vr.normalize_country(sup.get("country", "")) if sup else ""


def _vat(line, supplier_lookup):
    v = vr.clean_vat_number(line.get("vat_number", ""))
    if v:
        return v
    sup = supplier_lookup.get(line.get("supplier_id", ""))
    return vr.clean_vat_number(sup.get("vat_number", "")) if sup else ""


# === TEST 84: Missing trader-indikator ===

def _is_intercompany(line):
    """Koncernintern strøm — generisk, ALDRIG kunde-/selskabsnavne-specifikt
    (K4, gap-analyse-runde 2/kunde 2, Bal-godkendt 2026-09-20).

    To uafhængige, begge generiske signaler:
      1. ERP'ens EGEN intercompany-markering på linjen (``line["intercompany"]``
         -- canonical-only felt, se ``parsers/canonical_parser.py``). Det mest
         pålidelige signal, når kilden leverer det.
      2. Modpartens "momsnummer" har formen af en intern koncern-partskode
         (landepræfiks + 1-3 cifre, fx "US10" -- se
         ``vat_rules.looks_like_internal_party_code``) i stedet for et ægte
         formateret Tax ID. Sekundært signal, dækker input-veje uden
         intercompany-kolonnen.

    Empirisk baggrund: kontrol 84 gav 309 KRITISKE fund på kunde 2s
    IFS-datasæt. ~20% (62/309) var bekræftet koncerninterne (ERP'ens egen
    flag=true) og udløste udelukkende "manglende momsnr" (koncern-
    mellemregninger kræver ikke et eksternt momsnummer) + højt beløb +
    udenlandsk. En koncernintern strøm har en KENDT modpart (selskabets eget
    datterselskab) og kan derfor strukturelt ikke være en missing trader
    (ukendt/forsvundet handelspartner) -- konservativt undtaget, ikke bare
    nedgraderet."""
    if line.get("intercompany"):
        return True
    return vr.looks_like_internal_party_code(line.get("vat_number", ""))


def _norm_desc(text):
    """Normaliseret beskrivelsestekst til kollektivnummer-optællingen:
    whitespace kollapset, casefoldet. Ingen semantisk tolkning."""
    return " ".join((text or "").split()).casefold()


def _shared_vat_desc_counts(data, supplier_lookup, cap):
    """Antal DISTINKTE normaliserede beskrivelsestekster pr. momsnummer-værdi
    i hele datasættet (kollektivnummer-signalet, se ``test_84_missing_trader``).
    Optællingen er bounded: pr. nummer gemmes højst ``cap`` distinkte tekster
    (vi skal kun vide, om antallet NÅR tærsklen, ikke det præcise antal)."""
    seen = {}
    for txn in data["transactions"]:
        t_desc = txn.get("description") or ""
        for line in txn["lines"]:
            vat = _vat(line, supplier_lookup)
            if not vat:
                continue
            bucket = seen.setdefault(vat, set())
            if len(bucket) < cap:
                bucket.add(_norm_desc(t_desc or line.get("description") or ""))
    return {v: len(s) for v, s in seen.items()}


def test_84_missing_trader(data, supplier_lookup):
    """Kombinerer flere risikofaktorer: EU-leverandør, manglende/ugyldigt
    momsnummer, højt beløb og højrisikovare.

    Kalibrering "kontrol 84-efterforskningen"/K5-b (kunde 2/IFS-empiri,
    2026-09-20 — generisk, INTET kundenavn/-nummer i koden):

    1. **Momskode-værn** (K5-b, samme princip som K2/kontrol 32): en linje
       HELT uden momskode er strukturelt uden for momsscope (bank-/
       afregnings-/valutakurs-/koncernmellemregningslinjer) — der er ikke
       angivet noget momsfradrag på linjen, så der findes ingen missing
       trader-eksponering at flage. Empirisk: 235/330 kritiske fund på
       kunde 2s IFS-datasæt lå på momskode-løse linjer (IC-tilgodehavender,
       valutakursdifferencer, AR/AP-afregning); BC-v5's 2 fund (ved direkte kald --
       i fuld pipeline er kontrollen 'ikke målbar'-gated på BC-vejen,
       country mangler) har begge momskode og er uberørte.
    2. **Kollektivnummer-undtagelsen**: et udfyldt nummer, der fejler
       EU-formatvalideringen, tæller ikke som "ugyldigt momsnr"-faktor, når
       samme værdi optræder med >= ``materiality.CONTROL_84_SHARED_VAT_MIN_
       DESCS`` distinkte beskrivelsestekster i datasættet — så er værdien en
       samle-/kollektivkonto-konvention i leverandørkartoteket (mange reelt
       forskellige, navngivne modparter deler ét generisk/afkortet nummer,
       fx "DE" eller et 8-cifret DE-nummer), ikke én skjult handelspartner.
       En ægte missing trader har ét navn (1-2 tekstvarianter) og rammes
       ikke af undtagelsen. Formatfejlen dækkes fortsat af kontrol 28.
       Empirisk: 8 delte, ugyldige numre bar 184/330 af fundene; 0 fund
       havde et ugyldigt nummer med < 3 beskrivelsestekster.
    3. **Severity pr. fund**: "critical" kun når højrisikovare-faktoren
       indgår (den klassiske MTIC-profil, fx mobiltelefoner — BC-v5's
       2 fund); ellers "high" (residualpopulationen er empirisk
       leverandørkartotek-datakvalitet: reelle, navngivne leverandører
       uden registreret momsnummer — et afklaringsspørgsmål, ikke en
       kritisk svindelalarm)."""
    findings = []
    shared_descs = _shared_vat_desc_counts(
        data, supplier_lookup, cap=materiality.CONTROL_84_SHARED_VAT_MIN_DESCS)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if (line.get("debit_amount", 0) or 0) <= 0:
                continue
            if not line.get("tax_code"):
                continue  # momskode-værn (K5-b) -- se docstring pkt. 1
            if _is_intercompany(line):
                continue  # koncernintern strøm -- se _is_intercompany
            country = _country(line, supplier_lookup)
            amount = abs(line["debit_amount"] + line["credit_amount"])
            vat = _vat(line, supplier_lookup)
            flags = []
            if vr.is_foreign(country):
                flags.append("udenlandsk leverandør")
            if not vat:
                flags.append("manglende momsnr")
            elif vr.is_eu_country(country):
                valid, _ = vr.validate_eu_vat_format(vat, country)
                if not valid and shared_descs.get(vat, 0) < materiality.CONTROL_84_SHARED_VAT_MIN_DESCS:
                    # Delte numre er kollektivnummer-artefakter -- se docstring pkt. 2
                    flags.append("ugyldigt momsnr")
            if amount >= 50000:
                flags.append("højt beløb")
            if vr.text_matches_any(f"{txn['description']} {line.get('description','')}",
                                   vr.MTIC_HIGH_RISK_KEYWORDS):
                flags.append("højrisikovare")

            if len(flags) >= 3:
                severity = "critical" if "højrisikovare" in flags else "high"
                findings.append(make_finding(
                    test_id=84, test_name="Missing trader-indikator",
                    impact_type="economic", direction="negative", severity=severity,
                    description=f"Transaktion {txn['transaction_id']} har flere svindel-risikofaktorer: "
                                f"{', '.join(flags)}.",
                    fix_suggestion="Kombinationen ligner en missing trader (MTIC). Verificér leverandøren "
                                   "grundigt (VIES, eksistens, levering) før momsfradrag.",
                    estimated_amount=line["tax_amount"] or round(amount * 0.2, 2),
                    transactions=[_ref(txn, line, risk_flags=flags, country=country,
                                       highlighted_field="amount")],
                ))
    return findings


# === TEST 85: Karruselhandel-mønster ===

def test_85_carousel_pattern(data):
    """Ægte karruseldetektion kræver at følge de samme varer gennem flere
    handelsled på tværs af virksomheder — data der ikke findes her."""
    return []


# === TEST 86: Hurtig ind-/udgående gennemstrømning ===

def test_86_rapid_throughput(data):
    """Samme beløb købt og solgt igen inden for få dage (gennemstrømning)."""
    findings = []
    purchases = []
    sales = []
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        if not d:
            continue
        for line in txn["lines"]:
            amount = round(abs(line["debit_amount"] + line["credit_amount"]), 2)
            if amount < 10000:
                continue
            if (line.get("debit_amount", 0) or 0) > 0:
                purchases.append((amount, d, txn, line))
            elif (line.get("credit_amount", 0) or 0) > 0:
                sales.append((amount, d, txn, line))

    sales_by_amount = defaultdict(list)
    for s in sales:
        sales_by_amount[s[0]].append(s)

    seen = set()
    for amount, pd, ptxn, pline in purchases:
        for samount, sd, stxn, sline in sales_by_amount.get(amount, []):
            if 0 <= (sd - pd).days <= 7:
                key = (ptxn["transaction_id"], stxn["transaction_id"])
                if key in seen:
                    continue
                seen.add(key)
                findings.append(make_finding(
                    test_id=86, test_name="Hurtig gennemstrømning",
                    impact_type="economic", direction="neutral", severity="medium",
                    description=f"Beløb {amount:.2f} købt {pd} (transaktion {ptxn['transaction_id']}) og "
                                f"solgt igen {sd} (transaktion {stxn['transaction_id']}) inden for "
                                f"{(sd-pd).days} dage.",
                    fix_suggestion="Hurtig køb-og-videresalg af samme beløb kan indikere "
                                   "gennemstrømningshandel. Bekræft at varerne reelt er handlet.",
                    estimated_amount=amount,
                    transactions=[_ref(ptxn, pline, role="køb", highlighted_field="amount"),
                                  _ref(stxn, sline, role="salg", highlighted_field="amount")],
                ))
                break
    return findings


# === TEST 87: Højrisikovarer ===

def test_87_high_risk_goods(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amount = abs(line["debit_amount"] + line["credit_amount"])
            text = f"{txn['description']} {line.get('description','')}"
            if amount >= 25000 and vr.text_matches_any(text, vr.MTIC_HIGH_RISK_KEYWORDS):
                findings.append(make_finding(
                    test_id=87, test_name="Højrisikovare",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Transaktion {txn['transaction_id']} ({amount:.2f}) vedrører en typisk "
                                f"svindel-udsat vare (mobiler/CPU/ædelmetal m.v.).",
                    fix_suggestion="Højrisikovarer bør have skærpet leverandør- og leveringskontrol.",
                    estimated_amount=amount,
                    transactions=[_ref(txn, line, highlighted_field="description")],
                ))
    return findings


# === TEST 88: Nul-margin gennemhandel ===

def test_88_zero_margin(data):
    """Højrisikovarer købt og solgt til næsten samme pris (ingen avance) er
    et klassisk karrusel-tegn."""
    findings = []
    buys = defaultdict(list)   # vare-signatur -> beløb
    sells = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.MTIC_HIGH_RISK_KEYWORDS):
                continue
            amount = abs(line["debit_amount"] + line["credit_amount"])
            if amount < 10000:
                continue
            if (line.get("debit_amount", 0) or 0) > 0:
                buys[None].append((amount, txn, line))
            elif (line.get("credit_amount", 0) or 0) > 0:
                sells[None].append((amount, txn, line))

    for b_amount, btxn, bline in buys[None]:
        for s_amount, stxn, sline in sells[None]:
            if b_amount <= 0:
                continue
            margin = (s_amount - b_amount) / b_amount
            if -0.02 <= margin <= 0.02:
                findings.append(make_finding(
                    test_id=88, test_name="Nul-margin på højrisikovare",
                    impact_type="economic", direction="neutral", severity="medium",
                    description=f"Højrisikovare købt for {b_amount:.2f} og solgt for {s_amount:.2f} "
                                f"(margin {margin*100:.1f}%). Manglende avance er et karrusel-tegn.",
                    fix_suggestion="Reel handel har normalt en avance. Undersøg om der er tale om "
                                   "gennemstrømning udelukkende for momsens skyld.",
                    estimated_amount=b_amount,
                    transactions=[_ref(btxn, bline, role="køb", highlighted_field="amount"),
                                  _ref(stxn, sline, role="salg", highlighted_field="amount")],
                ))
                break
    return findings


# === TEST 89: Nystartet høj-volumen part ===

def test_89_new_high_volume_party(data):
    """Leverandør med kort aktiv periode men højt samlet volumen."""
    findings = []
    sup_dates = defaultdict(list)
    sup_total = defaultdict(float)
    sup_example = {}
    for txn in data["transactions"]:
        d = _parse(txn["date"])
        for line in txn["lines"]:
            sid = line.get("supplier_id", "")
            if not sid or not d:
                continue
            sup_dates[sid].append(d)
            sup_total[sid] += abs(line["debit_amount"] + line["credit_amount"])
            sup_example.setdefault(sid, (txn, line))

    for sid, dates in sup_dates.items():
        if len(dates) < 3:
            continue
        span = (max(dates) - min(dates)).days
        if span <= 30 and sup_total[sid] >= 200000:
            txn, line = sup_example[sid]
            findings.append(make_finding(
                test_id=89, test_name="Nystartet høj-volumen leverandør",
                impact_type="economic", direction="negative", severity="high",
                description=f"Leverandør '{line.get('supplier_name') or sid}' har {len(dates)} handler for "
                            f"i alt {sup_total[sid]:.2f} inden for blot {span} dage.",
                fix_suggestion="Højt volumen i et kort vindue fra én leverandør kan være en missing "
                               "trader. Verificér leverandørens realitet og leveringsevne.",
                estimated_amount=sup_total[sid],
                transactions=[_ref(txn, line, supplier_id=sid, active_days=span,
                                   highlighted_field="amount")],
            ))
    return findings


# === TEST 90: Atypisk betalingsmønster ===

def test_90_payment_pattern(data):
    """Kræver betalingsdata (modtagerkonto, betalingsdato, tredjepart) der ikke
    findes i bogføringseksporten. Springer pænt over."""
    return []


# === TEST 91: Misbrug af gennemstrømnings-/mellemregningskonti ===

def test_91_clearing_account_abuse(data):
    findings = []
    keywords = {"mellemregning", "clearing", "suspense", "afventer", "uafklaret", "diverse"}
    acct_total = defaultdict(float)
    acct_count = defaultdict(int)
    acct_example = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            desc = (line.get("description", "") or "")
            acct = line["account_id"]
            if vr.text_matches_any(desc, keywords) or vr.text_matches_any(txn["description"], keywords):
                acct_total[acct] += abs(line["debit_amount"] + line["credit_amount"])
                acct_count[acct] += 1
                acct_example.setdefault(acct, (txn, line))

    for acct, total in acct_total.items():
        if acct_count[acct] >= 3 and total >= 50000:
            txn, line = acct_example[acct]
            findings.append(make_finding(
                test_id=91, test_name="Mellemregnings-/gennemstrømningskonto",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Konto {acct} bruges {acct_count[acct]} gange til mellemregning/uafklarede "
                            f"posteringer for i alt {total:.2f}.",
                fix_suggestion="Store, hyppige mellemregninger kan sløre den reelle transaktion. "
                               "Afklar og omposter til de rigtige konti.",
                estimated_amount=total,
                transactions=[_ref(txn, line, account_id=acct, count=acct_count[acct],
                                   highlighted_field="account_id")],
            ))
    return findings


# === TEST 92: Falsk faktura-indikator ===

def test_92_false_invoice(data, supplier_lookup):
    """Stort momsfradrag fra udenlandsk/uidentificeret leverandør med manglende
    eller intetsigende fakturanummer."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = line["tax_amount"] or 0
            if vat < 2500 or (line.get("debit_amount", 0) or 0) <= 0:
                continue
            doc = (line.get("source_document_id", "") or "").strip()
            sid = line.get("supplier_id", "")
            sname = (line.get("supplier_name", "") or "").strip()
            weak_invoice = not doc or doc.lower() in ("0", "na", "n/a", "-", "diverse")
            weak_party = not sid and not sname
            if weak_invoice or weak_party:
                problem = []
                if weak_invoice:
                    problem.append("intet/intetsigende fakturanummer")
                if weak_party:
                    problem.append("ingen leverandøridentifikation")
                findings.append(make_finding(
                    test_id=92, test_name="Falsk faktura-indikator",
                    impact_type="economic", direction="negative", severity="high",
                    description=f"Momsfradrag {vat:.2f} på transaktion {txn['transaction_id']} med "
                                f"{' og '.join(problem)}.",
                    fix_suggestion="Momsfradrag kræver en forskriftsmæssig faktura med identificerbar "
                                   "leverandør. Manglende dokumentation kan betyde fiktiv faktura.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, tax_amount=vat,
                                       highlighted_field="source_document_id")],
                ))
    return findings


# === TEST 93: Identiske beløb på tværs af parter ===

def test_93_identical_amounts_across_parties(data):
    findings = []
    amount_parties = defaultdict(set)
    amount_example = defaultdict(list)
    for txn in data["transactions"]:
        for line in txn["lines"]:
            amount = round(abs(line["debit_amount"] + line["credit_amount"]), 2)
            if amount < 10000:
                continue
            party = line.get("supplier_id") or line.get("customer_id") or ""
            if not party:
                continue
            amount_parties[amount].add(party)
            if len(amount_example[amount]) < 8:
                amount_example[amount].append(_ref(txn, line, party=party, highlighted_field="amount"))

    for amount, parties in amount_parties.items():
        if len(parties) >= 4:
            findings.append(make_finding(
                test_id=93, test_name="Identisk beløb hos mange parter",
                impact_type="compliance", direction="neutral", severity="low",
                description=f"Beløbet {amount:.2f} optræder hos {len(parties)} forskellige parter. "
                            f"Identiske store beløb på tværs af parter kan indikere koordinerede "
                            f"fiktive handler.",
                fix_suggestion="Undersøg om de identiske beløb dækker reelle, uafhængige handler.",
                estimated_amount=amount,
                transactions=amount_example[amount],
            ))
    return findings
