"""
Kategori 4: Grænseoverskridende & EU-compliance (Tests 27-38)

Kontrollerer EU-handel, eksport til tredjelande, reverse charge ved
EU-erhvervelser, momsnummer-format og land/valuta-konsistens.

Bruger modpartens land og momsnummer. Disse hentes fra linjen (udfyldt af
data_adapter) og falder tilbage til leverandør-/kunde-masterlisterne. Når
hverken land eller momsnummer findes, springer testene pænt over.
"""

from collections import defaultdict
from analytics.models import make_finding
from analytics import vat_rules as vr


def run_cross_border_tests(data: dict) -> list:
    supplier_lookup = {s["supplier_id"]: s for s in data.get("suppliers", [])}
    customer_lookup = {c["customer_id"]: c for c in data.get("customers", [])}
    ctx = {"suppliers": supplier_lookup, "customers": customer_lookup}

    findings = []
    findings.extend(test_27_eu_trade_no_vat_number(data, ctx))
    findings.extend(test_28_invalid_eu_vat_format(data, ctx))
    findings.extend(test_29_eu_acquisition_no_reverse_charge(data, ctx))
    findings.extend(test_30_export_with_vat(data, ctx))
    findings.extend(test_31_eu_sale_with_dk_vat(data, ctx))
    findings.extend(test_32_missing_country_on_foreign(data, ctx))
    findings.extend(test_33_currency_country_mismatch(data, ctx))
    findings.extend(test_34_dk_vat_on_foreign_party(data, ctx))
    findings.extend(test_35_vat_prefix_vs_country(data, ctx))
    findings.extend(test_36_triangulation(data, ctx))
    findings.extend(test_37_vies_verification(data, ctx))
    findings.extend(test_38_import_without_docs(data, ctx))
    return findings


def _country_of(line, ctx):
    """Modpartens landekode: linje først, derefter leverandør-master."""
    c = vr.normalize_country(line.get("country", ""))
    if c:
        return c
    sup = ctx["suppliers"].get(line.get("supplier_id", ""))
    if sup:
        return vr.normalize_country(sup.get("country", ""))
    return ""


def _vat_of(line, ctx):
    """Modpartens momsnummer: linje først, derefter leverandør-master."""
    v = vr.clean_vat_number(line.get("vat_number", ""))
    if v:
        return v
    sup = ctx["suppliers"].get(line.get("supplier_id", ""))
    if sup:
        return vr.clean_vat_number(sup.get("vat_number", ""))
    return ""


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


def _is_purchase(line):
    return (line.get("debit_amount", 0) or 0) > 0


def _is_sale(line):
    return (line.get("credit_amount", 0) or 0) > 0


# === TEST 27: EU-handel uden momsnummer ===

def test_27_eu_trade_no_vat_number(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            if _vat_of(line, ctx):
                continue
            findings.append(make_finding(
                test_id=27,
                test_name="EU-handel uden momsnummer",
                impact_type="compliance",
                direction="neutral",
                severity="high",
                description=f"EU-handel med {country} på transaktion {txn['transaction_id']}, men "
                            f"modpartens momsnummer mangler.",
                fix_suggestion="Indhent og registrér modpartens EU-momsnummer. Det er en betingelse "
                               "for momsfri EU-handel og for korrekt indberetning til EU-salgsangivelsen.",
                transactions=[_ref(txn, line, country=country, highlighted_field="vat_number")],
            ))
    return findings


# === TEST 28: Ugyldigt EU-momsnummer-format ===

def test_28_invalid_eu_vat_format(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            vat = _vat_of(line, ctx)
            if not vat:
                continue
            country = _country_of(line, ctx)
            valid, reason = vr.validate_eu_vat_format(vat, country)
            if valid:
                continue
            findings.append(make_finding(
                test_id=28,
                test_name="Ugyldigt momsnummer-format",
                impact_type="compliance",
                direction="neutral",
                severity="high",
                description=f"Momsnummer '{vat}' på transaktion {txn['transaction_id']} har ugyldigt "
                            f"format ({reason}).",
                fix_suggestion="Verificér momsnummeret i VIES og ret det. Et ugyldigt momsnummer kan "
                               "underkende momsfritagelsen ved EU-handel.",
                transactions=[_ref(txn, line, vat_number=vat, reason=reason,
                                   highlighted_field="vat_number")],
            ))
    return findings


# === TEST 29: EU-erhvervelse uden reverse charge ===

def test_29_eu_acquisition_no_reverse_charge(data, ctx):
    """Køb fra EU-leverandør med dansk moms pålagt (skulle være 0 + reverse charge)."""
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_purchase(line):
                continue
            country = _country_of(line, ctx)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            vat = line["tax_amount"] or 0
            if vat <= 0:
                continue
            findings.append(make_finding(
                test_id=29,
                test_name="EU-erhvervelse uden reverse charge",
                impact_type="interest_risk",
                direction="neutral",
                severity="high",
                description=f"Køb fra EU-leverandør ({country}) på transaktion {txn['transaction_id']} "
                            f"har moms {vat:.2f} pålagt. EU-erhvervelser skal håndteres med "
                            f"omvendt betalingspligt (0% på fakturaen, selvangivet moms).",
                fix_suggestion="Bogfør EU-køb uden fakturamoms og beregn erhvervelsesmoms (køb + salg) "
                               "i momsangivelsen. Fejlagtig fakturamoms kan ikke fratrækkes.",
                estimated_amount=vat,
                transactions=[_ref(txn, line, country=country, tax_amount=vat,
                                   highlighted_field="tax_amount")],
            ))
    return findings


# === TEST 30: Eksport til tredjeland med moms pålagt ===

def test_30_export_with_vat(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            if not country or vr.is_eu_country(country):
                continue  # Tredjeland = kendt, ikke-EU
            vat = line["tax_amount"] or 0
            if vat <= 0:
                continue
            findings.append(make_finding(
                test_id=30,
                test_name="Eksport med moms pålagt",
                impact_type="economic",
                direction="negative",
                severity="high",
                description=f"Handel med tredjeland ({country}) på transaktion {txn['transaction_id']} "
                            f"har moms {vat:.2f} pålagt. Eksport til lande uden for EU er nulsat.",
                fix_suggestion="Eksport (og import) til tredjelande skal være momsfri (0%). "
                               "Fjern den fejlagtige moms.",
                estimated_amount=vat,
                transactions=[_ref(txn, line, country=country, tax_amount=vat,
                                   highlighted_field="tax_amount")],
            ))
    return findings


# === TEST 31: EU B2B-salg med dansk moms ===

def test_31_eu_sale_with_dk_vat(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_sale(line):
                continue
            country = _country_of(line, ctx)
            if not vr.is_eu_country(country) or country == "DK":
                continue
            if not _vat_of(line, ctx):
                continue  # Uden momsnr er det måske B2C — håndteres af cat12
            vat = line["tax_amount"] or 0
            if vat <= 0:
                continue
            findings.append(make_finding(
                test_id=31,
                test_name="EU-salg med dansk moms",
                impact_type="economic",
                direction="negative",
                severity="high",
                description=f"B2B-salg til EU-kunde ({country}, momsnr oplyst) på transaktion "
                            f"{txn['transaction_id']} er pålagt dansk moms {vat:.2f}. "
                            f"Momsfrit EU-salg skulle have været 0%.",
                fix_suggestion="Fakturér momsfrit (0%) til momsregistrerede EU-kunder og indberet "
                               "til EU-salg uden moms (listesystemet).",
                estimated_amount=vat,
                transactions=[_ref(txn, line, country=country, tax_amount=vat,
                                   highlighted_field="tax_amount")],
            ))
    return findings


# === TEST 32: Manglende landekode på udenlandsk part ===

def test_32_missing_country_on_foreign(data, ctx):
    """Momsnr-præfiks eller valuta tyder på udland, men landekode mangler."""
    findings = []
    default_currency = data["header"].get("currency", "DKK")
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            if country:
                continue
            vat = _vat_of(line, ctx)
            prefix = vr.vat_prefix(vat)
            currency = line.get("currency", "") or ""
            foreign_currency = currency and currency != default_currency
            if (prefix and prefix not in ("DK", "")) or foreign_currency:
                signal = f"momsnr-præfiks '{prefix}'" if prefix and prefix != "DK" else f"valuta '{currency}'"
                findings.append(make_finding(
                    test_id=32,
                    test_name="Manglende landekode på udenlandsk part",
                    impact_type="compliance",
                    direction="neutral",
                    severity="low",
                    description=f"Transaktion {txn['transaction_id']} ser udenlandsk ud ({signal}), "
                                f"men ingen landekode er registreret.",
                    fix_suggestion="Registrér modpartens land, så EU-/eksportreglerne kan anvendes korrekt.",
                    transactions=[_ref(txn, line, vat_number=vat, currency=currency,
                                       highlighted_field="country")],
                ))
    return findings


# === TEST 33: Valuta/land-uoverensstemmelse ===

def test_33_currency_country_mismatch(data, ctx):
    findings = []
    default_currency = data["header"].get("currency", "DKK")
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            currency = line.get("currency", "") or ""
            if not country or not currency:
                continue
            # Dansk modpart men udenlandsk valuta
            if country == "DK" and currency != default_currency:
                findings.append(make_finding(
                    test_id=33,
                    test_name="Valuta/land-uoverensstemmelse",
                    impact_type="compliance",
                    direction="neutral",
                    severity="low",
                    description=f"Dansk modpart men valuta '{currency}' på transaktion "
                                f"{txn['transaction_id']}.",
                    fix_suggestion="Kontrollér om land eller valuta er fejlregistreret, og at "
                                   "valutakursen er korrekt omregnet.",
                    transactions=[_ref(txn, line, country=country, currency=currency,
                                       highlighted_field="currency")],
                ))
    return findings


# === TEST 34: Dansk momsnummer på udenlandsk part ===

def test_34_dk_vat_on_foreign_party(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            if not vr.is_foreign(country):
                continue
            prefix = vr.vat_prefix(_vat_of(line, ctx))
            if prefix == "DK":
                findings.append(make_finding(
                    test_id=34,
                    test_name="Dansk momsnummer på udenlandsk part",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Modpart i {country} på transaktion {txn['transaction_id']} har et "
                                f"dansk momsnummer (DK-præfiks). Land og momsnummer er uforenelige.",
                    fix_suggestion="Afklar om modparten er dansk- eller udenlandsk-registreret. "
                                   "Et DK-momsnummer betyder dansk momspligt (25%).",
                    transactions=[_ref(txn, line, country=country, highlighted_field="vat_number")],
                ))
    return findings


# === TEST 35: Momsnr-præfiks matcher ikke landekode ===

def test_35_vat_prefix_vs_country(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = vr.normalize_country(_country_of(line, ctx))
            vat = _vat_of(line, ctx)
            prefix = vr.vat_prefix(vat)
            if not country or not prefix:
                continue
            expected_prefix = vr.VAT_PREFIX_FOR_COUNTRY.get(country, country)
            if prefix != expected_prefix:
                findings.append(make_finding(
                    test_id=35,
                    test_name="Momsnr-præfiks matcher ikke land",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Transaktion {txn['transaction_id']}: landekode '{country}' men "
                                f"momsnummer-præfiks '{prefix}' (forventet '{expected_prefix}').",
                    fix_suggestion="Ret enten landekoden eller momsnummeret, så de stemmer overens.",
                    transactions=[_ref(txn, line, country=country, vat_prefix=prefix,
                                       highlighted_field="vat_number")],
                ))
    return findings


# === TEST 36: Trekantshandel-indikator ===

def test_36_triangulation(data, ctx):
    """Place-of-supply / trekantshandel ud fra vareflow (ship_from/ship_to).

    Kører kun når forsendelsesland findes på linjen; ellers springes pænt over
    (flade udtræk uden vareflow giver ingen falske alarmer). Fanger to klassiske,
    dyre fejl:
      A) Varen forlader ikke DK (ship_from=DK, ship_to=DK), men sælges til udland
         uden dansk moms -> uberettiget nulsats; leverancen er indenlandsk (25%).
      B) Varen leveres til et andet land end modparten -> mulig trekantshandel
         eller forkert place-of-supply (afgøres af hvor varen fysisk leveres).
    """
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            sf = vr.normalize_country(line.get("ship_from_country", ""))
            st = vr.normalize_country(line.get("ship_to_country", ""))
            if not sf and not st:
                continue  # intet vareflow -> uændret adfærd for flade udtræk
            country = _country_of(line, ctx)
            vat = line.get("tax_amount") or 0

            # A) Uberettiget nulsats: varen forlader ikke DK, men sælges til udland.
            if _is_sale(line) and sf == "DK" and st == "DK" and vr.is_foreign(country) and vat <= 0:
                base = line.get("tax_base") or (
                    (line.get("debit_amount", 0) or 0) + (line.get("credit_amount", 0) or 0)
                )
                findings.append(make_finding(
                    test_id=36,
                    test_name="Indenlandsk leverance nulsat som udlandssalg",
                    impact_type="economic",
                    direction="negative",
                    severity="high",
                    description=f"Varen afsendes fra og til Danmark (ship_from=DK, ship_to=DK) på "
                                f"transaktion {txn['transaction_id']}, men sælges til {country} uden "
                                f"dansk moms. Når varen ikke forlader DK, er leverancen indenlandsk "
                                f"og momspligtig (25%).",
                    fix_suggestion="Pålæg 25% dansk moms. Nulsats ved EU-salg/eksport kræver, at varen "
                                   "fysisk forlader Danmark — kundens land er ikke nok.",
                    estimated_amount=round(base * vr.STANDARD_RATE / 100, 2),
                    transactions=[_ref(txn, line, country=country, ship_from=sf, ship_to=st,
                                       highlighted_field="ship_to_country")],
                ))
                continue

            # B) Leveringsland != modpartsland -> mulig trekantshandel / forkert place-of-supply.
            if st and country and st != country:
                findings.append(make_finding(
                    test_id=36,
                    test_name="Leveringsland afviger fra modpartens land",
                    impact_type="compliance",
                    direction="neutral",
                    severity="medium",
                    description=f"Varen leveres til {st}, men modparten er registreret i {country} "
                                f"(transaktion {txn['transaction_id']}). Det kan være trekantshandel "
                                f"eller en forkert place-of-supply-behandling.",
                    fix_suggestion="Afklar leveringskæden: ved trekantshandel gælder forenklingsreglen "
                                   "med særlige fakturakrav, og place-of-supply afgøres af hvor varen "
                                   "fysisk leveres — ikke kun modpartens land.",
                    transactions=[_ref(txn, line, country=country, ship_to=st,
                                       highlighted_field="ship_to_country")],
                ))
    return findings


# === TEST 37: Manglende VIES-verifikation (rådgivende) ===

def test_37_vies_verification(data, ctx):
    """Saml distinkte EU-momsnumre der bør VIES-verificeres."""
    findings = []
    seen = {}
    for txn in data["transactions"]:
        for line in txn["lines"]:
            country = _country_of(line, ctx)
            vat = _vat_of(line, ctx)
            if not vat or not vr.is_eu_country(country) or country == "DK":
                continue
            valid, _ = vr.validate_eu_vat_format(vat, country)
            if not valid:
                continue  # Format-fejl fanges af test 28
            if vat not in seen:
                seen[vat] = {
                    "vat_number": vat,
                    "country": country,
                    "highlighted_field": "vat_number",
                }
    if seen:
        findings.append(make_finding(
            test_id=37,
            test_name="VIES-verifikation anbefales",
            impact_type="compliance",
            direction="neutral",
            severity="low",
            description=f"{len(seen)} distinkte EU-momsnumre indgår i EU-handel og bør verificeres "
                        f"i VIES for at sikre gyldig momsfritagelse.",
            fix_suggestion="Kør numrene gennem VIES (ec.europa.eu/taxation_customs/vies) og gem "
                           "verifikationsbevis pr. handelsdato.",
            transactions=list(seen.values()),
        ))
    return findings


# === TEST 38: Import fra tredjeland uden dokumentation ===

def test_38_import_without_docs(data, ctx):
    findings = []
    for txn in data["transactions"]:
        for line in txn["lines"]:
            if not _is_purchase(line):
                continue
            country = _country_of(line, ctx)
            if not country or vr.is_eu_country(country):
                continue  # Tredjeland
            vat = line["tax_amount"] or 0
            if vat <= 0:
                continue
            findings.append(make_finding(
                test_id=38,
                test_name="Import uden dokumentation",
                impact_type="compliance",
                direction="neutral",
                severity="medium",
                description=f"Import fra tredjeland ({country}) på transaktion {txn['transaction_id']} "
                            f"med moms {vat:.2f}. Importmoms forudsætter tolddokumentation.",
                fix_suggestion="Importmoms beregnes via tolddeklarationen, ikke leverandørfaktura. "
                               "Sikr at fortoldningsbilag understøtter momsfradraget.",
                estimated_amount=vat,
                transactions=[_ref(txn, line, country=country, tax_amount=vat,
                                   highlighted_field="tax_amount")],
            ))
    return findings
