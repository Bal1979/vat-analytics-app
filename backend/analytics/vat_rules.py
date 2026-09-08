"""
Danske momsregler og hjælpefunktioner — delt af alle test-kategorier.

Indeholder konstanter og helpers der afspejler dansk momsret (som den
anvendes af Skattestyrelsens kontroller): standardsats, gyldige satser,
EU-medlemslande, EU-momsnummer-formater, CVR mod-11 validering samt
statistiske hjælpere (gennemsnit, standardafvigelse, Benford).

Helpers er bevidst defensive: de antager ikke at felter findes, og
returnerer neutrale værdier i stedet for at kaste, så tests kan
"springe pænt over" når data mangler.
"""

import re
import math
from collections import Counter

from analytics import standard_accounts

# === SATSER ===

# Dansk standard-momssats. Danmark har ingen reducerede satser —
# kun 25% (standard) og 0% (nulsats: eksport, EU-leverancer, aviser m.v.).
STANDARD_RATE = 25.0
ZERO_RATE = 0.0
VALID_DK_RATES = {0.0, 25.0}

# Satser der typisk indikerer en udenlandsk (ikke-dansk) momssats anvendt
# ved en fejl. Bruges til at flagge "reduceret sats" i et dansk regnskab.
COMMON_FOREIGN_RATES = {5.0, 5.5, 6.0, 7.0, 8.0, 9.0, 10.0, 12.0, 13.0,
                        15.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0}

RATE_TOLERANCE = 0.5  # procentpoint


# === EU ===

# ISO 3166-1 alpha-2 koder for EU-medlemslande (inkl. Danmark).
EU_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE",
}

# Momsnummer-præfiks afviger fra landekoden for Grækenland (EL).
VAT_PREFIX_FOR_COUNTRY = {c: c for c in EU_COUNTRY_CODES}
VAT_PREFIX_FOR_COUNTRY["GR"] = "EL"

# Regex for EU-momsnummer-formater (uden præfiks-bogstaverne).
# Kilde: EU's VIES-format-specifikation. Forenklet, men dækker de
# almindelige formater til format-validering (ikke check-cifre).
_EU_VAT_BODY = {
    "AT": r"U\d{8}",
    "BE": r"\d{10}",
    "BG": r"\d{9,10}",
    "HR": r"\d{11}",
    "CY": r"\d{8}[A-Z]",
    "CZ": r"\d{8,10}",
    "DK": r"\d{8}",
    "EE": r"\d{9}",
    "FI": r"\d{8}",
    "FR": r"[A-Z0-9]{2}\d{9}",
    "DE": r"\d{9}",
    "EL": r"\d{9}",
    "HU": r"\d{8}",
    "IE": r"\d{7}[A-Z]{1,2}|\d[A-Z]\d{5}[A-Z]",
    "IT": r"\d{11}",
    "LV": r"\d{11}",
    "LT": r"\d{9}|\d{12}",
    "LU": r"\d{8}",
    "MT": r"\d{8}",
    "NL": r"\d{9}B\d{2}",
    "PL": r"\d{10}",
    "PT": r"\d{9}",
    "RO": r"\d{2,10}",
    "SK": r"\d{10}",
    "SI": r"\d{8}",
    "ES": r"[A-Z0-9]\d{7}[A-Z0-9]",
    "SE": r"\d{12}",
}
_EU_VAT_REGEX = {p: re.compile(r"^" + body + r"$") for p, body in _EU_VAT_BODY.items()}

# Almindelige landenavne -> ISO-kode (bruges når kildedata har navn frem for kode).
_COUNTRY_NAME_TO_CODE = {
    "DANMARK": "DK", "DENMARK": "DK",
    "TYSKLAND": "DE", "GERMANY": "DE", "DEUTSCHLAND": "DE",
    "SVERIGE": "SE", "SWEDEN": "SE",
    "NORGE": "NO", "NORWAY": "NO",
    "HOLLAND": "NL", "NEDERLANDENE": "NL", "NETHERLANDS": "NL",
    "FRANKRIG": "FR", "FRANCE": "FR",
    "ITALIEN": "IT", "ITALY": "IT",
    "SPANIEN": "ES", "SPAIN": "ES",
    "POLEN": "PL", "POLAND": "PL",
    "STORBRITANNIEN": "GB", "UK": "GB", "ENGLAND": "GB", "UNITED KINGDOM": "GB",
    "USA": "US", "UNITED STATES": "US",
    "FINLAND": "FI", "BELGIEN": "BE", "BELGIUM": "BE",
    "ØSTRIG": "AT", "AUSTRIA": "AT", "IRLAND": "IE", "IRELAND": "IE",
    "PORTUGAL": "PT", "GRÆKENLAND": "GR", "GREECE": "GR",
}


def normalize_country(value):
    """Normalisér en landeangivelse til en ISO alpha-2 kode (uppercase).

    Accepterer både koder ("dk") og navne ("Danmark"). Returnerer "" når
    værdien er tom eller ukendt-formet.
    """
    if not value:
        return ""
    s = str(value).strip().upper()
    if len(s) == 2 and s.isalpha():
        return s
    return _COUNTRY_NAME_TO_CODE.get(s, "")


def is_eu_country(code):
    """True hvis landekoden er et EU-medlemsland (Danmark inkluderet)."""
    return normalize_country(code) in EU_COUNTRY_CODES


def is_foreign(code):
    """True hvis koden er et udland (kendt og != DK). Tom kode = ukendt = False."""
    c = normalize_country(code)
    return bool(c) and c != "DK"


# === MOMSNUMRE / CVR ===

_NON_ALNUM = re.compile(r"[^A-Z0-9]")


def clean_vat_number(vat):
    """Fjern mellemrum/punktum og uppercase et momsnummer."""
    if not vat:
        return ""
    return _NON_ALNUM.sub("", str(vat).upper())


def vat_prefix(vat):
    """Returnér de to indledende bogstaver i et momsnummer (landepræfiks), ellers ""."""
    v = clean_vat_number(vat)
    if len(v) >= 2 and v[:2].isalpha():
        return v[:2]
    return ""


def validate_cvr(number):
    """Validér et dansk CVR-nummer (8 cifre) med modulus-11 kontrol.

    Vægte 2,7,6,5,4,3,2,1; summen skal være delelig med 11.
    """
    if not number:
        return False
    digits = re.sub(r"\D", "", str(number))
    if len(digits) != 8:
        return False
    weights = [2, 7, 6, 5, 4, 3, 2, 1]
    total = sum(int(d) * w for d, w in zip(digits, weights))
    return total % 11 == 0


def validate_eu_vat_format(vat, country=None):
    """Format-validér et EU-momsnummer.

    Tjekker at præfikset er et EU-præfiks og at kroppen matcher landets
    format-regex. Validerer IKKE mod VIES (kun syntaks). Returnerer
    (is_valid, reason).
    """
    v = clean_vat_number(vat)
    if not v:
        return False, "tomt momsnummer"

    prefix = v[:2] if v[:2].isalpha() else ""
    body = v[2:] if prefix else v

    if not prefix:
        # Intet præfiks — prøv mod det angivne lands format
        c = normalize_country(country)
        prefix = VAT_PREFIX_FOR_COUNTRY.get(c, "")
        if not prefix:
            return False, "manglende landepræfiks"
        body = v

    regex = _EU_VAT_REGEX.get(prefix)
    if regex is None:
        return False, f"ukendt EU-præfiks '{prefix}'"
    if not regex.match(body):
        return False, f"forkert format for {prefix}"
    return True, "ok"


# === NAVN-NORMALISERING (fuzzy parts-match) ===

_NAME_NOISE = re.compile(r"\b(A/S|APS|IVS|P/S|K/S|I/S|LTD|GMBH|AB|AG|BV|SA|SARL|INC|CORP|CO)\b")


def normalize_name(name):
    """Normalisér et parts-navn til fuzzy sammenligning: uppercase, fjern
    selskabsformer og special-tegn."""
    if not name:
        return ""
    s = str(name).upper().strip()
    s = _NAME_NOISE.sub("", s)
    s = re.sub(r"[^A-Z0-9ÆØÅ]", "", s)
    return s


# === REVERSE CHARGE / HØJRISIKO-VARER ===

# Indenlandsk omvendt betalingspligt (DK): metalskrot, mobiltelefoner,
# integrerede kredsløb, spillekonsoller, tablets/bærbare, gas/el, CO2-kvoter.
DOMESTIC_REVERSE_CHARGE_KEYWORDS = {
    "metalskrot", "skrot", "mobiltelefon", "mobil", "tablet", "bærbar",
    "laptop", "spillekonsol", "konsol", "integreret kredsløb", "chip",
    "co2", "co2-kvote", "kvote", "gas", "elektricitet",
}

# Byggeydelser / arbejdsudleje (omvendt betalingspligt i byggebranchen).
CONSTRUCTION_REVERSE_CHARGE_KEYWORDS = {
    "byggeydelse", "byggearbejde", "entreprise", "arbejdsudleje",
    "håndværker", "montage", "installation",
}

# Typiske MTIC/karrusel-højrisikovarer.
MTIC_HIGH_RISK_KEYWORDS = {
    "mobil", "telefon", "smartphone", "cpu", "processor", "grafikkort",
    "gpu", "konsol", "ædelmetal", "guld", "sølv", "platin", "chip",
}

# Tekst-signaler for kontantbetaling.
CASH_KEYWORDS = {"kontant", "cash", "kontanter"}

# Tekst-signaler for margin/særordninger.
TRAVEL_AGENCY_KEYWORDS = {"rejsebureau", "rejse", "pakkerejse", "travel"}
USED_GOODS_MARGIN_KEYWORDS = {"brugt", "brugtmoms", "antik", "kunst", "samlerobjekt", "second-hand"}
DIGITAL_SERVICE_KEYWORDS = {"software", "licens", "download", "streaming", "app",
                            "e-bog", "ebook", "online", "saas", "cloud", "abonnement"}
TELECOM_KEYWORDS = {"telefoni", "teleydelse", "broadcasting", "tv-ydelse", "roaming"}


def text_matches_any(text, keywords):
    """True hvis nogen af nøgleordene optræder i teksten (case-insensitivt)."""
    if not text:
        return False
    low = str(text).lower()
    return any(kw in low for kw in keywords)


# === STATISTIK ===

def mean(values):
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def stdev(values):
    vals = [v for v in values if v is not None]
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    var = sum((v - m) ** 2 for v in vals) / (n - 1)
    return math.sqrt(var)


# Forventet Benford-fordeling for første ciffer (1-9).
BENFORD_EXPECTED = {d: math.log10(1 + 1 / d) for d in range(1, 10)}


def first_digit(value):
    """Første betydende ciffer (1-9) af et tals absolutte værdi, ellers None."""
    try:
        v = abs(float(value))
    except (ValueError, TypeError):
        return None
    if v < 1:
        # Skift kommaet indtil vi har et ciffer >= 1
        if v == 0:
            return None
        while v < 1:
            v *= 10
    s = f"{v:.10f}".lstrip("0").lstrip(".")
    for ch in s:
        if ch.isdigit() and ch != "0":
            return int(ch)
    return None


def benford_chi_square(values):
    """Beregn chi-i-anden mod Benford for første-ciffer-fordelingen.

    Returnerer (chi_square, n, observed_counts) hvor observed_counts er en
    dict ciffer->antal. Tom/lille input giver (0.0, n, {}).
    """
    digits = [first_digit(v) for v in values]
    digits = [d for d in digits if d is not None]
    n = len(digits)
    if n == 0:
        return 0.0, 0, {}
    observed = Counter(digits)
    chi = 0.0
    for d in range(1, 10):
        exp = BENFORD_EXPECTED[d] * n
        obs = observed.get(d, 0)
        if exp > 0:
            chi += (obs - exp) ** 2 / exp
    return chi, n, dict(observed)


# Kritisk chi-i-anden værdi, 8 frihedsgrader, p=0.05.
BENFORD_CHI_CRITICAL = 15.51


def is_round_amount(value, base=1000):
    """True hvis beløbet er et 'rundt' tal (deleligt med base, fx hele tusinder)."""
    try:
        v = abs(float(value))
    except (ValueError, TypeError):
        return False
    return v >= base and v % base == 0


# === LINJE-HELPERS ===

# Kontotyper (SAF-T AccountType) der er balanceposter — IKKE momsbærende.
# Momsrelevans-scope: en momskontrol må kun undertrykke et fund, når vi POSITIVT
# kan se, at linjen er en balancekonto (asset/liability/equity). Er kontotypen
# ukendt (fx Excel-import uden kontoplan), undertrykkes intet — adfærden er uændret.
_NON_VAT_ACCOUNT_TYPES = {"asset", "liability", "equity"}


def is_non_vat_account(line):
    """True hvis linjen positivt kan identificeres som en ikke-moms (balance-)konto.

    To uafhængige signaler (enten er nok):
      1. SAF-T ``AccountType`` er en balancetype (asset/liability/equity).
      2. ``StandardAccountID`` er en balancekonto (≥ 5000 i standardkontoplanen).
         Dette er det ROBUSTE signal på rigtige filer, hvor AccountType ofte er
         fejlmærket "Other" — se analytics/standard_accounts.py.
    Begge ukendte (fx fladt Excel-udtræk uden kontoplan) → False, så adfærden er
    uændret.
    """
    at = (line.get("account_type") or "").strip().lower()
    if at in _NON_VAT_ACCOUNT_TYPES:
        return True
    return standard_accounts.is_balance_account(line.get("standard_account_id"))


def line_amount(line):
    """Samlet beløb på en linje (debet + kredit)."""
    return (line.get("debit_amount", 0) or 0) + (line.get("credit_amount", 0) or 0)


def counterparty(line):
    """Returnér (id, navn, rolle) for modparten — leverandør eller kunde."""
    if line.get("supplier_id"):
        return line["supplier_id"], line.get("supplier_name", ""), "supplier"
    if line.get("customer_id"):
        return line["customer_id"], line.get("customer_name", ""), "customer"
    return "", "", ""


def implied_rate(base, vat):
    """Implicit momssats ud fra grundlag og momsbeløb, ellers None."""
    try:
        base = float(base)
        vat = float(vat)
    except (ValueError, TypeError):
        return None
    if base == 0:
        return None
    return round(vat / base * 100, 2)
