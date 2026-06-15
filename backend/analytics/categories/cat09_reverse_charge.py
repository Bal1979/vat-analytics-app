"""
Kategori 9: Reverse Charge & Selvangivelse (Tests 70-75)

Kontrollerer omvendt betalingspligt: EU-ydelser, indenlandske RC-varer
(mobiler, metalskrot m.v.), byggeydelser og at RC-posteringer er korrekt
markeret og dokumenteret.
"""

from analytics.models import make_finding
from analytics import vat_rules as vr


# Signaler for at en postering er markeret som omvendt betalingspligt.
_RC_CODE_HINTS = ("RC", "OMV", "REV", "OB")
_RC_TEXT_HINTS = ("omvendt betalingspligt", "reverse charge", "reverse-charge",
                  "selvangivet moms", "erhvervelsesmoms")


def _is_reverse_charge_marked(line, txn):
    code = (line.get("tax_code") or "").upper()
    if any(h in code for h in _RC_CODE_HINTS):
        return True
    text = f"{txn.get('description', '')} {line.get('description', '')}".lower()
    return any(h in text for h in _RC_TEXT_HINTS)


def run_reverse_charge_tests(data: dict) -> list:
    supplier_lookup = {s["supplier_id"]: s for s in data.get("suppliers", [])}
    findings = []
    findings.extend(test_70_eu_service_no_rc(data, supplier_lookup))
    findings.extend(test_71_rc_on_domestic(data, supplier_lookup))
    findings.extend(test_72_domestic_rc_goods(data, supplier_lookup))
    findings.extend(test_73_asymmetric_rc(data))
    findings.extend(test_74_construction_rc(data, supplier_lookup))
    findings.extend(test_75_rc_missing_documentation(data, supplier_lookup))
    return findings


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


# === TEST 70: Manglende reverse charge på EU-ydelser ===

def test_70_eu_service_no_rc(data, supplier_lookup):
    """EU-køb uden moms og uden RC-markering — erhvervelsesmoms mangler muligvis."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if (line.get("debit_amount", 0) or 0) <= 0:
                continue
            country = _country(line, supplier_lookup)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            vat = line["tax_amount"] or 0
            if vat != 0:
                continue  # Moms pålagt håndteres af test 29
            if _is_reverse_charge_marked(line, txn):
                continue
            base = line["tax_base"] or abs(line["debit_amount"])
            findings.append(make_finding(
                test_id=70, test_name="EU-køb uden reverse charge-markering",
                impact_type="interest_risk", direction="neutral", severity="high",
                description=f"Køb fra EU ({country}) på transaktion {txn['transaction_id']} har hverken "
                            f"moms eller RC-markering. Erhvervelsesmoms skal selvangives.",
                fix_suggestion="Beregn erhvervelsesmoms (25% af købet) som både købs- og salgsmoms, og "
                               "markér posteringen som omvendt betalingspligt.",
                estimated_amount=round(base * vr.STANDARD_RATE / 100, 2),
                transactions=[_ref(txn, line, country=country, highlighted_field="tax_code")],
            ))
    return findings


# === TEST 71: Reverse charge på indenlandsk handel ===

def test_71_rc_on_domestic(data, supplier_lookup):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_reverse_charge_marked(line, txn):
                continue
            country = _country(line, supplier_lookup)
            # Indenlandsk RC findes (varer), men ikke generelt — flag ren indenlandsk
            # RC uden at det er en kendt RC-vare (det fanges af test 72).
            if vr.is_foreign(country):
                continue
            if vr.text_matches_any(f"{txn['description']} {line.get('description','')}",
                                   vr.DOMESTIC_REVERSE_CHARGE_KEYWORDS |
                                   vr.CONSTRUCTION_REVERSE_CHARGE_KEYWORDS):
                continue  # Legitim indenlandsk RC-vare/-ydelse
            findings.append(make_finding(
                test_id=71, test_name="Reverse charge på indenlandsk handel",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"Transaktion {txn['transaction_id']} er markeret omvendt betalingspligt, "
                            f"men modparten er dansk/ukendt og varen er ikke en kendt RC-vare.",
                fix_suggestion="Almindelig indenlandsk handel skal have 25% moms. Omvendt betalingspligt "
                               "gælder kun specifikke varer/ydelser (eller udenlandsk handel).",
                transactions=[_ref(txn, line, country=country or "(ingen)", highlighted_field="tax_code")],
            ))
    return findings


# === TEST 72: Indenlandsk RC-vare uden omvendt betalingspligt ===

def test_72_domestic_rc_goods(data, supplier_lookup):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if (line.get("debit_amount", 0) or 0) <= 0:
                continue
            country = _country(line, supplier_lookup)
            if vr.is_foreign(country):
                continue  # Indenlandsk
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.DOMESTIC_REVERSE_CHARGE_KEYWORDS):
                continue
            vat = line["tax_amount"] or 0
            if vat > 0 and not _is_reverse_charge_marked(line, txn):
                findings.append(make_finding(
                    test_id=72, test_name="RC-vare med moms i stedet for omvendt betalingspligt",
                    impact_type="interest_risk", direction="neutral", severity="medium",
                    description=f"Transaktion {txn['transaction_id']} ser ud til at vedrøre en RC-vare "
                                f"(fx mobiltelefoner/metalskrot) men er pålagt moms {vat:.2f} i stedet "
                                f"for omvendt betalingspligt.",
                    fix_suggestion="Indenlandsk handel med mobiltelefoner, bærbare, chips, metalskrot m.v. "
                                   "over tærsklen skal afregnes med omvendt betalingspligt.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
                ))
    return findings


# === TEST 73: Asymmetrisk reverse charge ===

def test_73_asymmetric_rc(data):
    """RC-køb skal modposteres med både købs- og salgsmoms. I en flad eksport
    kan modposten ikke spores pr. transaktion; testen rapporterer aggregeret
    hvis der findes RC-køb men ingen tilsvarende RC-salgsmomspostering."""
    findings = []
    rc_purchases = []
    rc_output_seen = False
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_reverse_charge_marked(line, txn):
                continue
            if (line.get("debit_amount", 0) or 0) > 0:
                rc_purchases.append(_ref(txn, line, highlighted_field="tax_code"))
            if (line.get("credit_amount", 0) or 0) > 0 and (line["tax_amount"] or 0) > 0:
                rc_output_seen = True

    if rc_purchases and not rc_output_seen:
        findings.append(make_finding(
            test_id=73, test_name="Asymmetrisk reverse charge",
            impact_type="interest_risk", direction="neutral", severity="medium",
            description=f"{len(rc_purchases)} RC-markerede køb fundet, men ingen tilsvarende selvangivet "
                        f"salgsmoms. Erhvervelsesmoms ser ud til kun at være fratrukket, ikke afregnet.",
            fix_suggestion="Omvendt betalingspligt skal bogføres som både købsmoms og salgsmoms. "
                           "Kontrollér at salgsmomssiden er medtaget.",
            transactions=rc_purchases[:10],
        ))
    return findings


# === TEST 74: Byggeydelser omvendt betalingspligt ===

def test_74_construction_rc(data, supplier_lookup):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if (line.get("debit_amount", 0) or 0) <= 0:
                continue
            country = _country(line, supplier_lookup)
            if vr.is_foreign(country):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.CONSTRUCTION_REVERSE_CHARGE_KEYWORDS):
                continue
            vat = line["tax_amount"] or 0
            if vat > 0 and not _is_reverse_charge_marked(line, txn):
                findings.append(make_finding(
                    test_id=74, test_name="Byggeydelse uden omvendt betalingspligt",
                    impact_type="interest_risk", direction="neutral", severity="medium",
                    description=f"Transaktion {txn['transaction_id']} vedrører muligvis byggeydelser/"
                                f"arbejdsudleje men er pålagt moms {vat:.2f}.",
                    fix_suggestion="Byggeydelser mellem virksomheder og arbejdsudleje er omfattet af "
                                   "omvendt betalingspligt — kontrollér momsbehandlingen.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
                ))
    return findings


# === TEST 75: Manglende dokumentation for reverse charge ===

def test_75_rc_missing_documentation(data, supplier_lookup):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_reverse_charge_marked(line, txn):
                continue
            country = _country(line, supplier_lookup)
            # RC ved udenlandsk handel kræver modpartens momsnummer
            if not vr.is_foreign(country):
                continue
            if _vat(line, supplier_lookup):
                continue
            findings.append(make_finding(
                test_id=75, test_name="Reverse charge uden modparts-momsnr",
                impact_type="compliance", direction="neutral", severity="medium",
                description=f"RC-postering (transaktion {txn['transaction_id']}, modpart i {country}) "
                            f"mangler modpartens momsnummer.",
                fix_suggestion="Omvendt betalingspligt ved EU-handel forudsætter modpartens gyldige "
                               "momsnummer. Indhent og registrér det.",
                transactions=[_ref(txn, line, country=country, highlighted_field="vat_number")],
            ))
    return findings
