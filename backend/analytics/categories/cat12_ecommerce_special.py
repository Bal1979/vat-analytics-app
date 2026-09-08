"""
Kategori 12: E-handel, Digitale Ydelser & Særordninger (Tests 94-103)

Dækker fjernsalg til EU-forbrugere (OSS/MOSS), elektronisk leverede
ydelser, telekommunikation, samt margin-/særordninger (rejsebureau,
brugtmoms). Mange af disse forhold kræver kunde-/leverandørland og
B2C/B2B-skel, som kun delvist fremgår af en bogføringseksport; de
relevante tests springer pænt over, når grundlaget mangler.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr
from analytics import materiality


# Fjernsalgstærskel for EU (samlet B2C-salg til andre EU-lande): 10.000 EUR.
# Konverteret groft til DKK; engagement-kalibrerbar via materiality.
DISTANCE_SELLING_THRESHOLD_DKK = materiality.DISTANCE_SELLING_THRESHOLD_DKK
# OSS-relevante tekst-signaler.
_ECOMMERCE_KEYWORDS = {"webshop", "e-handel", "ehandel", "online salg", "netbutik",
                       "fjernsalg", "distance selling"}


def run_ecommerce_tests(data: dict) -> list:
    customer_lookup = {c.get("customer_id", ""): c for c in data.get("customers", [])}
    findings = []
    findings.extend(test_94_eu_b2c_oss(data, customer_lookup))
    findings.extend(test_95_distance_selling_threshold(data, customer_lookup))
    findings.extend(test_96_digital_service_eu_consumer(data, customer_lookup))
    findings.extend(test_97_dk_vat_on_eu_consumer(data, customer_lookup))
    findings.extend(test_98_ioss_low_value(data))
    findings.extend(test_99_platform_liability(data))
    findings.extend(test_100_electronic_services(data))
    findings.extend(test_101_telecom_broadcasting(data))
    findings.extend(test_102_travel_margin(data))
    findings.extend(test_103_used_goods_margin(data))
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


def _cust_country(line, txn, customer_lookup):
    c = vr.normalize_country(line.get("country", ""))
    if c:
        return c
    cust = customer_lookup.get(line.get("customer_id", "") or txn.get("customer_id", ""))
    return vr.normalize_country(cust.get("country", "")) if cust else ""


def _is_sale(line):
    return (line.get("credit_amount", 0) or 0) > 0


# === TEST 94: EU B2C-salg uden OSS-håndtering ===

def test_94_eu_b2c_oss(data, customer_lookup):
    """Salg til forbruger i et andet EU-land bør afregnes via One Stop Shop
    med forbrugslandets sats — ikke dansk 25% og ikke uden moms."""
    findings = []
    eu_b2c = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            country = _cust_country(line, txn, customer_lookup)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            # B2C antages når der ikke er et modparts-momsnummer (ellers RC/B2B).
            if vr.clean_vat_number(line.get("vat_number", "")):
                continue
            credit = line.get("credit_amount", 0) or 0
            eu_b2c.append((txn, line, country, credit))

    if not eu_b2c:
        return findings
    total = sum(c for *_, c in eu_b2c)
    findings.append(make_finding(
        test_id=94, test_name="EU-forbrugersalg (OSS)",
        impact_type="compliance", direction="neutral", severity="medium",
        description=f"{len(eu_b2c)} salg til forbrugere i andre EU-lande (i alt {total:.0f}) uden "
                    f"modparts-momsnummer. Sådanne fjernsalg skal som udgangspunkt afregnes via "
                    f"One Stop Shop (OSS) med forbrugslandets momssats.",
        fix_suggestion="Registrér virksomheden i OSS-ordningen og afregn EU-forbrugersalg med "
                       "bestemmelseslandets sats, hvis fjernsalgstærsklen er overskredet.",
        estimated_amount=round(total, 2),
        transactions=[_ref(t, l, country=co, highlighted_field="country")
                      for t, l, co, _ in eu_b2c[:10]],
    ))
    return findings


# === TEST 95: Fjernsalgstærskel (10.000 EUR) ===

def test_95_distance_selling_threshold(data, customer_lookup):
    findings = []
    eu_b2c_total = 0.0
    refs = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            country = _cust_country(line, txn, customer_lookup)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            if vr.clean_vat_number(line.get("vat_number", "")):
                continue
            credit = line.get("credit_amount", 0) or 0
            eu_b2c_total += credit
            if len(refs) < 10:
                refs.append(_ref(txn, line, country=country, highlighted_field="country"))

    if eu_b2c_total >= DISTANCE_SELLING_THRESHOLD_DKK:
        findings.append(make_finding(
            test_id=95, test_name="Fjernsalgstærskel overskredet",
            impact_type="compliance", direction="neutral", severity="high",
            description=f"Samlet B2C-fjernsalg til EU udgør {eu_b2c_total:.0f} DKK og overstiger "
                        f"fjernsalgstærsklen (~{DISTANCE_SELLING_THRESHOLD_DKK:.0f} DKK / 10.000 EUR). "
                        f"Over tærsklen skal der afregnes moms i forbrugslandet (OSS).",
            fix_suggestion="Ved fjernsalg over 10.000 EUR samlet til EU-forbrugere skal salget "
                           "momses i bestemmelseslandet via OSS, ikke i Danmark.",
            estimated_amount=round(eu_b2c_total, 2),
            transactions=refs,
        ))
    return findings


# === TEST 96: Digital ydelse til EU-forbruger ===

def test_96_digital_service_eu_consumer(data, customer_lookup):
    """Elektronisk leverede ydelser til EU-forbrugere momses altid i
    forbrugslandet (ingen tærskel ud over de samlede 10.000 EUR)."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            country = _cust_country(line, txn, customer_lookup)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            if vr.clean_vat_number(line.get("vat_number", "")):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.DIGITAL_SERVICE_KEYWORDS):
                continue
            rate = line["tax_percentage"] or 0
            if rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE:
                credit = line.get("credit_amount", 0) or 0
                findings.append(make_finding(
                    test_id=96, test_name="Digital ydelse med dansk moms til EU-forbruger",
                    impact_type="compliance", direction="neutral", severity="medium",
                    description=f"Digital ydelse solgt til forbruger i {country} (transaktion "
                                f"{txn['transaction_id']}) er pålagt dansk sats {rate:.0f}%. "
                                f"Elektroniske ydelser til EU-forbrugere momses i forbrugslandet.",
                    fix_suggestion="Afregn elektronisk leverede ydelser til EU-forbrugere med "
                                   "forbrugslandets sats via OSS — ikke dansk 25%.",
                    estimated_amount=round(credit, 2),
                    transactions=[_ref(txn, line, country=country, highlighted_field="tax_percentage")],
                ))
    return findings


# === TEST 97: Dansk moms på EU-forbruger generelt ===

def test_97_dk_vat_on_eu_consumer(data, customer_lookup):
    """Bredere variant: et hvilket som helst varesalg til EU-forbruger med
    dansk 25% kan være forkert, hvis fjernsalgstærsklen er passeret. Lav
    sværhedsgrad — vejledende, da B2C/B2B-skellet er usikkert."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            country = _cust_country(line, txn, customer_lookup)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            if vr.clean_vat_number(line.get("vat_number", "")):
                continue
            rate = line["tax_percentage"] or 0
            vat = line["tax_amount"] or 0
            if vat > 0 and rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE:
                findings.append(make_finding(
                    test_id=97, test_name="Dansk moms på EU-forbrugersalg",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Salg til forbruger i {country} (transaktion {txn['transaction_id']}) "
                                f"er pålagt dansk moms {vat:.2f}. Over fjernsalgstærsklen skal "
                                f"forbrugslandets sats anvendes.",
                    fix_suggestion="Kontrollér om fjernsalgstærsklen er overskredet; i så fald skal "
                                   "salget momses i forbrugslandet via OSS.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, country=country, highlighted_field="tax_percentage")],
                ))
    return findings


# === TEST 98: IOSS / lavværdiimport (< 150 EUR) ===

def test_98_ioss_low_value(data):
    """Import af varer under 150 EUR fra tredjeland kan afregnes via Import
    One Stop Shop. Uden told-/oprindelsesdata kan vi kun flagge køb fra
    ikke-EU med eksplicitte import-signaler. Springer pænt over uden data."""
    findings = []
    LOW_VALUE_DKK = 1120.0  # ~150 EUR
    import_kw = {"import", "told", "indførsel", "3. land", "tredjeland"}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if (line.get("debit_amount", 0) or 0) <= 0:
                continue
            country = vr.normalize_country(line.get("country", ""))
            if country and (vr.is_eu_country(country) or country == "DK"):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, import_kw):
                continue
            amount = abs(line["debit_amount"] + line["credit_amount"])
            vat = line["tax_amount"] or 0
            if amount <= LOW_VALUE_DKK and vat == 0:
                findings.append(make_finding(
                    test_id=98, test_name="Lavværdiimport uden importmoms (IOSS)",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Muligt lavværdikøb fra tredjeland ({amount:.0f} DKK, transaktion "
                                f"{txn['transaction_id']}) uden importmoms. Varer under 150 EUR kan "
                                f"afregnes via IOSS, men moms skal stadig opkræves.",
                    fix_suggestion="Kontrollér at importmoms (eller IOSS-afregning) er håndteret for "
                                   "lavværdiimport fra lande uden for EU.",
                    transactions=[_ref(txn, line, country=country or "(ukendt)",
                                       highlighted_field="country")],
                ))
    return findings


# === TEST 99: Platform-/markedspladsansvar ===

def test_99_platform_liability(data):
    """Hvorvidt en markedsplads hæfter for momsen kræver viden om
    salgskanal og underliggende sælger, som ikke fremgår af bogføringen.
    Springer pænt over."""
    return []


# === TEST 100: Elektronisk leverede ydelser (klassifikation) ===

def test_100_electronic_services(data):
    """Salg med digitale tekst-signaler men uden momskode — leveringssted
    og dermed momsbehandling for elektroniske ydelser er let at fejle."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.DIGITAL_SERVICE_KEYWORDS):
                continue
            code = line["tax_code"]
            credit = line.get("credit_amount", 0) or 0
            if not code and credit >= 5000:
                findings.append(make_finding(
                    test_id=100, test_name="Elektronisk ydelse uden momskode",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Salg af elektronisk ydelse (transaktion {txn['transaction_id']}, "
                                f"{credit:.0f}) er bogført uden momskode. Leveringsstedet afgør "
                                f"momsbehandlingen og bør fastlægges eksplicit.",
                    fix_suggestion="Fastlæg leveringssted for elektroniske ydelser (B2B vs. B2C, "
                                   "DK/EU/3.-land) og påfør korrekt momskode.",
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
                ))
    return findings


# === TEST 101: Teleydelser / broadcasting ===

def test_101_telecom_broadcasting(data):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.TELECOM_KEYWORDS):
                continue
            code = line["tax_code"]
            credit = line.get("credit_amount", 0) or 0
            if not code and credit >= 5000:
                findings.append(make_finding(
                    test_id=101, test_name="Teleydelse uden momskode",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Salg af tele-/broadcastingydelse (transaktion {txn['transaction_id']}, "
                                f"{credit:.0f}) er bogført uden momskode. Disse ydelser momses i "
                                f"forbrugslandet ligesom elektroniske ydelser.",
                    fix_suggestion="Behandl tele-, radio- og tv-ydelser efter forbrugslandsprincippet "
                                   "(OSS for EU-forbrugere) og påfør momskode.",
                    transactions=[_ref(txn, line, highlighted_field="tax_code")],
                ))
    return findings


# === TEST 102: Rejsebureau-margenordning ===

def test_102_travel_margin(data):
    """Rejseydelser under margenordningen må ikke have almindelig 25% moms
    af hele salgsprisen — kun af avancen."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.TRAVEL_AGENCY_KEYWORDS):
                continue
            rate = line["tax_percentage"] or 0
            vat = line["tax_amount"] or 0
            if vat > 0 and rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE:
                findings.append(make_finding(
                    test_id=102, test_name="Rejseydelse med fuld moms",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Mulig rejseydelse (transaktion {txn['transaction_id']}) er pålagt "
                                f"fuld moms {vat:.2f} ({rate:.0f}%). Under rejsebureauernes "
                                f"margenordning beregnes moms kun af avancen.",
                    fix_suggestion="Hvis salget er omfattet af rejsebureauernes særordning, beregnes "
                                   "momsen alene af fortjenstmargenen — ikke hele salgsprisen.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, highlighted_field="tax_percentage")],
                ))
    return findings


# === TEST 103: Brugtmoms / margenordning ===

def test_103_used_goods_margin(data):
    """Brugte varer, kunst og samlerobjekter under brugtmomsordningen
    momses af avancen, ikke hele salgsprisen."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            text = f"{txn['description']} {line.get('description','')}"
            if not vr.text_matches_any(text, vr.USED_GOODS_MARGIN_KEYWORDS):
                continue
            rate = line["tax_percentage"] or 0
            vat = line["tax_amount"] or 0
            if vat > 0 and rate >= vr.STANDARD_RATE - vr.RATE_TOLERANCE:
                findings.append(make_finding(
                    test_id=103, test_name="Brugtmoms-vare med fuld moms",
                    impact_type="compliance", direction="neutral", severity="low",
                    description=f"Mulig brugtmoms-vare (transaktion {txn['transaction_id']}) er pålagt "
                                f"fuld moms {vat:.2f} af hele prisen. Under brugtmomsordningen beregnes "
                                f"moms kun af avancen.",
                    fix_suggestion="Ved salg af brugte varer/kunst/samlerobjekter under brugtmoms­"
                                   "ordningen beregnes momsen af avancen — bekræft behandlingen.",
                    estimated_amount=vat,
                    transactions=[_ref(txn, line, highlighted_field="tax_percentage")],
                ))
    return findings
