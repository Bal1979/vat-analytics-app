"""
Landetabel-runden (2026-09-20): `vat_rules._COUNTRY_NAME_TO_CODE` udvidet fra
~25 navne til en fuld, GENERISK ISO 3166-1-navnetabel (officielle engelske
kortnavne inkl. ISO's kommaformer, almindelige engelske varianter og danske
navne) — root cause bag kontrol 25's resterende kalibreringsstøj og en del af
kontrol 28/32's oprindelige omfang på ERP-udtræk, der bærer fulde engelske
landenavne (fx IFS). Se docs/CHANGELOG.md for den empiriske før/efter.

Ingen kundedata her: alle navne er standard ISO-/dansksprogede landenavne.
"""

from analytics import vat_rules as vr


# === Opslags-normalisering ====================================================

def test_two_letter_codes_pass_through_unchanged():
    """Eksisterende adfærd: en to-bogstavskode returneres som-is (uppercase),
    uden tabelopslag."""
    assert vr.normalize_country("dk") == "DK"
    assert vr.normalize_country("Ch") == "CH"
    # Også en "kode" der ikke er et ISO-land — uændret defensiv adfærd.
    assert vr.normalize_country("ZZ") == "ZZ"


def test_leading_the_prefix_is_stripped():
    assert vr.normalize_country("THE NETHERLANDS") == "NL"
    assert vr.normalize_country("the netherlands") == "NL"
    assert vr.normalize_country("The Philippines") == "PH"


def test_whitespace_is_collapsed_and_stripped():
    assert vr.normalize_country("  ESTONIA  ") == "EE"
    assert vr.normalize_country("UNITED  KINGDOM") == "GB"
    assert vr.normalize_country("CZECH\tREPUBLIC") == "CZ"


def test_unknown_or_empty_stays_unknown():
    """Konservativt: udfyldt-men-ukendt tekst er 'ukendt land' ("") — aldrig
    et gæt. Tom værdi ligeså."""
    assert vr.normalize_country("") == ""
    assert vr.normalize_country(None) == ""
    assert vr.normalize_country("XYZLAND") == ""
    assert vr.normalize_country("THE MOON") == ""


# === ISO-kommaformer (officielle engelske kortnavne) ==========================

def test_iso_comma_forms_resolve():
    assert vr.normalize_country("TAIWAN, PROVINCE OF CHINA") == "TW"
    assert vr.normalize_country("KOREA, REPUBLIC OF") == "KR"
    assert vr.normalize_country("MOLDOVA, REPUBLIC OF") == "MD"
    assert vr.normalize_country("TANZANIA, UNITED REPUBLIC OF") == "TZ"
    assert vr.normalize_country("IRAN, ISLAMIC REPUBLIC OF") == "IR"
    assert vr.normalize_country("VENEZUELA, BOLIVARIAN REPUBLIC OF") == "VE"


# === EU-dækning (kritisk for is_eu_country-kontrollerne) ======================

_EU_ENGLISH = {
    "AUSTRIA": "AT", "BELGIUM": "BE", "BULGARIA": "BG", "CROATIA": "HR",
    "CYPRUS": "CY", "CZECHIA": "CZ", "DENMARK": "DK", "ESTONIA": "EE",
    "FINLAND": "FI", "FRANCE": "FR", "GERMANY": "DE", "GREECE": "GR",
    "HUNGARY": "HU", "IRELAND": "IE", "ITALY": "IT", "LATVIA": "LV",
    "LITHUANIA": "LT", "LUXEMBOURG": "LU", "MALTA": "MT", "NETHERLANDS": "NL",
    "POLAND": "PL", "PORTUGAL": "PT", "ROMANIA": "RO", "SLOVAKIA": "SK",
    "SLOVENIA": "SI", "SPAIN": "ES", "SWEDEN": "SE",
}

_EU_DANISH = {
    "ØSTRIG": "AT", "BELGIEN": "BE", "BULGARIEN": "BG", "KROATIEN": "HR",
    "CYPERN": "CY", "TJEKKIET": "CZ", "DANMARK": "DK", "ESTLAND": "EE",
    "FINLAND": "FI", "FRANKRIG": "FR", "TYSKLAND": "DE", "GRÆKENLAND": "GR",
    "UNGARN": "HU", "IRLAND": "IE", "ITALIEN": "IT", "LETLAND": "LV",
    "LITAUEN": "LT", "LUXEMBOURG": "LU", "MALTA": "MT", "NEDERLANDENE": "NL",
    "POLEN": "PL", "PORTUGAL": "PT", "RUMÆNIEN": "RO", "SLOVAKIET": "SK",
    "SLOVENIEN": "SI", "SPANIEN": "ES", "SVERIGE": "SE",
}


def test_all_27_eu_countries_english_names_resolve():
    for name, code in _EU_ENGLISH.items():
        assert vr.normalize_country(name) == code, name
        assert vr.is_eu_country(name), name


def test_all_27_eu_countries_danish_names_resolve():
    for name, code in _EU_DANISH.items():
        assert vr.normalize_country(name) == code, name
        assert vr.is_eu_country(name), name


def test_gr_greece_maps_to_gr_with_el_vat_prefix():
    """Grækenland: landekode GR, men momsnummer-præfiks EL — tabellen må
    ikke blande de to sammen."""
    assert vr.normalize_country("GREECE") == "GR"
    assert vr.VAT_PREFIX_FOR_COUNTRY["GR"] == "EL"


# === Ikke-EU (afgørende for is_foreign / tredjelandskontroller) ===============

def test_common_third_countries_resolve_as_foreign_non_eu():
    for name, code in {
        "SWITZERLAND": "CH", "NORWAY": "NO", "CHINA": "CN", "JAPAN": "JP",
        "UNITED KINGDOM": "GB", "UNITED STATES": "US", "INDIA": "IN",
        "SOUTH AFRICA": "ZA", "HONG KONG": "HK", "SINGAPORE": "SG",
        "UNITED ARAB EMIRATES": "AE", "SAUDI ARABIA": "SA", "SERBIA": "RS",
        "UKRAINE": "UA", "ICELAND": "IS", "TURKEY": "TR",
    }.items():
        assert vr.normalize_country(name) == code, name
        assert vr.is_foreign(name), name
        assert not vr.is_eu_country(name), name


def test_danish_realm_is_not_eu():
    """Grønland og Færøerne er uden for EU's momsområde — de skal genkendes
    (ikke-tom kode), men ikke som EU-lande."""
    for name, code in {"GREENLAND": "GL", "GRØNLAND": "GL",
                       "FAROE ISLANDS": "FO", "FÆRØERNE": "FO"}.items():
        assert vr.normalize_country(name) == code, name
        assert not vr.is_eu_country(name), name


# === Tabelhygiejne ============================================================

def test_all_mapped_codes_are_two_letter_uppercase():
    for name, code in vr._COUNTRY_NAME_TO_CODE.items():
        assert len(code) == 2 and code.isalpha() and code.isupper(), (name, code)


def test_all_table_keys_are_normalized_uppercase_single_spaced():
    """Nøglerne SKAL være i opslagsform (upper, single-space), ellers er de
    strukturelt uopnåelige for normalize_country."""
    for name in vr._COUNTRY_NAME_TO_CODE:
        assert name == " ".join(name.upper().split()), name


def test_table_covers_every_eu_code():
    assert vr.EU_COUNTRY_CODES <= set(vr._COUNTRY_NAME_TO_CODE.values())


def test_previous_small_table_entries_still_resolve_identically():
    """Bagudkompatibilitet: alle navne fra den tidligere ~25-navne-tabel
    slår fortsat op til de samme koder."""
    legacy = {
        "DANMARK": "DK", "DENMARK": "DK", "TYSKLAND": "DE", "GERMANY": "DE",
        "DEUTSCHLAND": "DE", "SVERIGE": "SE", "SWEDEN": "SE", "NORGE": "NO",
        "NORWAY": "NO", "HOLLAND": "NL", "NEDERLANDENE": "NL",
        "NETHERLANDS": "NL", "FRANKRIG": "FR", "FRANCE": "FR",
        "ITALIEN": "IT", "ITALY": "IT", "SPANIEN": "ES", "SPAIN": "ES",
        "POLEN": "PL", "POLAND": "PL", "STORBRITANNIEN": "GB", "UK": "GB",
        "ENGLAND": "GB", "UNITED KINGDOM": "GB", "USA": "US",
        "UNITED STATES": "US", "FINLAND": "FI", "BELGIEN": "BE",
        "BELGIUM": "BE", "ØSTRIG": "AT", "AUSTRIA": "AT", "IRLAND": "IE",
        "IRELAND": "IE", "PORTUGAL": "PT", "GRÆKENLAND": "GR", "GREECE": "GR",
    }
    for name, code in legacy.items():
        assert vr._COUNTRY_NAME_TO_CODE.get(name) == code, name
