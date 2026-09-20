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

# Landenavne -> ISO 3166-1 alpha-2 kode (bruges når kildedata har navn frem
# for kode). GENERISK standardtabel — fuld ISO 3166-1-dækning med (1) det
# officielle engelske kortnavn (inkl. ISO's kommaformer som "KOREA, REPUBLIC
# OF"), (2) almindelige engelske varianter og (3) danske navne. IKKE
# kunde-/ERP-specifik: udvidet 2026-09-20 (landetabel-runden) fordi ERP-udtræk
# (fx IFS) typisk bærer fulde engelske landenavne, som den tidligere ~25-navne-
# tabel ikke dækkede. Opslag sker på .strip().upper() med kollapset whitespace;
# et ledende "THE " strippes i ``normalize_country`` (så "THE NETHERLANDS"
# rammer "NETHERLANDS" uden en variant pr. land).
_COUNTRY_NAME_TO_CODE = {
    # --- EU-medlemslande ---
    "AUSTRIA": "AT", "ØSTRIG": "AT",
    "BELGIUM": "BE", "BELGIEN": "BE",
    "BULGARIA": "BG", "BULGARIEN": "BG",
    "CROATIA": "HR", "KROATIEN": "HR",
    "CYPRUS": "CY", "CYPERN": "CY",
    "CZECHIA": "CZ", "CZECH REPUBLIC": "CZ", "TJEKKIET": "CZ",
    "DENMARK": "DK", "DANMARK": "DK",
    "ESTONIA": "EE", "ESTLAND": "EE",
    "FINLAND": "FI",
    "FRANCE": "FR", "FRANKRIG": "FR",
    "GERMANY": "DE", "TYSKLAND": "DE", "DEUTSCHLAND": "DE",
    "GREECE": "GR", "GRÆKENLAND": "GR",
    "HUNGARY": "HU", "UNGARN": "HU",
    "IRELAND": "IE", "IRLAND": "IE",
    "ITALY": "IT", "ITALIEN": "IT",
    "LATVIA": "LV", "LETLAND": "LV",
    "LITHUANIA": "LT", "LITAUEN": "LT",
    "LUXEMBOURG": "LU", "LUXEMBURG": "LU",
    "MALTA": "MT",
    "NETHERLANDS": "NL", "NEDERLANDENE": "NL", "HOLLAND": "NL",
    "NETHERLANDS (KINGDOM OF THE)": "NL",
    "POLAND": "PL", "POLEN": "PL",
    "PORTUGAL": "PT",
    "ROMANIA": "RO", "RUMÆNIEN": "RO",
    "SLOVAKIA": "SK", "SLOVAKIET": "SK",
    "SLOVENIA": "SI", "SLOVENIEN": "SI",
    "SPAIN": "ES", "SPANIEN": "ES",
    "SWEDEN": "SE", "SVERIGE": "SE",
    # --- Øvrige Europa / EØS / mikrostater ---
    "ALBANIA": "AL", "ALBANIEN": "AL",
    "ANDORRA": "AD",
    "BELARUS": "BY", "HVIDERUSLAND": "BY",
    "BOSNIA AND HERZEGOVINA": "BA", "BOSNIEN-HERCEGOVINA": "BA",
    "BOSNIEN OG HERCEGOVINA": "BA",
    "FAROE ISLANDS": "FO", "FAEROE ISLANDS": "FO", "FÆRØERNE": "FO",
    "GIBRALTAR": "GI",
    "GREENLAND": "GL", "GRØNLAND": "GL",
    "GUERNSEY": "GG",
    "HOLY SEE": "VA", "VATICAN": "VA", "VATICAN CITY": "VA",
    "VATIKANSTATEN": "VA",
    "ICELAND": "IS", "ISLAND": "IS",
    "ISLE OF MAN": "IM",
    "JERSEY": "JE",
    "KOSOVO": "XK",
    "LIECHTENSTEIN": "LI",
    "MOLDOVA": "MD", "MOLDOVA, REPUBLIC OF": "MD",
    "MONACO": "MC",
    "MONTENEGRO": "ME",
    "NORTH MACEDONIA": "MK", "MACEDONIA": "MK", "NORDMAKEDONIEN": "MK",
    "NORWAY": "NO", "NORGE": "NO",
    "RUSSIA": "RU", "RUSSIAN FEDERATION": "RU", "RUSLAND": "RU",
    "SAN MARINO": "SM",
    "SERBIA": "RS", "SERBIEN": "RS",
    "SVALBARD AND JAN MAYEN": "SJ", "SVALBARD OG JAN MAYEN": "SJ",
    "SWITZERLAND": "CH", "SCHWEIZ": "CH",
    "UKRAINE": "UA",
    "UNITED KINGDOM": "GB", "UK": "GB", "ENGLAND": "GB",
    "GREAT BRITAIN": "GB", "STORBRITANNIEN": "GB",
    "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND": "GB",
    "ÅLAND ISLANDS": "AX", "ALAND ISLANDS": "AX", "ÅLAND": "AX",
    # --- Mellemøsten / Centralasien ---
    "AFGHANISTAN": "AF",
    "ARMENIA": "AM", "ARMENIEN": "AM",
    "AZERBAIJAN": "AZ", "ASERBAJDSJAN": "AZ",
    "BAHRAIN": "BH",
    "GEORGIA": "GE", "GEORGIEN": "GE",
    "IRAN": "IR", "IRAN, ISLAMIC REPUBLIC OF": "IR",
    "IRAQ": "IQ", "IRAK": "IQ",
    "ISRAEL": "IL",
    "JORDAN": "JO",
    "KAZAKHSTAN": "KZ", "KASAKHSTAN": "KZ",
    "KUWAIT": "KW",
    "KYRGYZSTAN": "KG", "KIRGISISTAN": "KG",
    "LEBANON": "LB", "LIBANON": "LB",
    "OMAN": "OM",
    "PALESTINE": "PS", "PALESTINE, STATE OF": "PS", "PALÆSTINA": "PS",
    "QATAR": "QA",
    "SAUDI ARABIA": "SA", "SAUDI-ARABIEN": "SA", "SAUDI ARABIEN": "SA",
    "SYRIA": "SY", "SYRIAN ARAB REPUBLIC": "SY", "SYRIEN": "SY",
    "TAJIKISTAN": "TJ", "TADSJIKISTAN": "TJ",
    "TURKEY": "TR", "TÜRKIYE": "TR", "TURKIYE": "TR", "TYRKIET": "TR",
    "TURKMENISTAN": "TM",
    "UNITED ARAB EMIRATES": "AE", "DE FORENEDE ARABISKE EMIRATER": "AE",
    "FORENEDE ARABISKE EMIRATER": "AE",
    "UZBEKISTAN": "UZ", "USBEKISTAN": "UZ",
    "YEMEN": "YE",
    # --- Asien / Oceanien ---
    "AMERICAN SAMOA": "AS", "AMERIKANSK SAMOA": "AS",
    "AUSTRALIA": "AU", "AUSTRALIEN": "AU",
    "BANGLADESH": "BD",
    "BHUTAN": "BT",
    "BRITISH INDIAN OCEAN TERRITORY": "IO",
    "BRUNEI": "BN", "BRUNEI DARUSSALAM": "BN",
    "CAMBODIA": "KH", "CAMBODJA": "KH",
    "CHINA": "CN", "KINA": "CN",
    "CHRISTMAS ISLAND": "CX", "JULEØEN": "CX",
    "COCOS (KEELING) ISLANDS": "CC", "COCOSØERNE": "CC",
    "COOK ISLANDS": "CK", "COOKØERNE": "CK",
    "FIJI": "FJ",
    "FRENCH POLYNESIA": "PF", "FRANSK POLYNESIEN": "PF",
    "FRENCH SOUTHERN TERRITORIES": "TF",
    "GUAM": "GU",
    "HONG KONG": "HK", "HONGKONG": "HK",
    "INDIA": "IN", "INDIEN": "IN",
    "INDONESIA": "ID", "INDONESIEN": "ID",
    "JAPAN": "JP",
    "KIRIBATI": "KI",
    "KOREA, REPUBLIC OF": "KR", "SOUTH KOREA": "KR", "SYDKOREA": "KR",
    "KOREA": "KR",
    "KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF": "KP", "NORTH KOREA": "KP",
    "NORDKOREA": "KP",
    "LAOS": "LA", "LAO PEOPLE'S DEMOCRATIC REPUBLIC": "LA",
    "MACAO": "MO", "MACAU": "MO",
    "MALAYSIA": "MY",
    "MALDIVES": "MV", "MALDIVERNE": "MV",
    "MARSHALL ISLANDS": "MH", "MARSHALLØERNE": "MH",
    "MICRONESIA": "FM", "MICRONESIA, FEDERATED STATES OF": "FM",
    "MIKRONESIEN": "FM",
    "MONGOLIA": "MN", "MONGOLIET": "MN",
    "MYANMAR": "MM", "BURMA": "MM",
    "NAURU": "NR",
    "NEPAL": "NP",
    "NEW CALEDONIA": "NC", "NY KALEDONIEN": "NC",
    "NEW ZEALAND": "NZ",
    "NIUE": "NU",
    "NORFOLK ISLAND": "NF", "NORFOLKØEN": "NF",
    "NORTHERN MARIANA ISLANDS": "MP",
    "PAKISTAN": "PK",
    "PALAU": "PW",
    "PAPUA NEW GUINEA": "PG", "PAPUA NY GUINEA": "PG",
    "PHILIPPINES": "PH", "FILIPPINERNE": "PH",
    "PITCAIRN": "PN",
    "SAMOA": "WS",
    "SINGAPORE": "SG",
    "SOLOMON ISLANDS": "SB", "SALOMONØERNE": "SB",
    "SRI LANKA": "LK",
    "TAIWAN": "TW", "TAIWAN, PROVINCE OF CHINA": "TW",
    "THAILAND": "TH",
    "TIMOR-LESTE": "TL", "EAST TIMOR": "TL", "ØSTTIMOR": "TL",
    "TOKELAU": "TK",
    "TONGA": "TO",
    "TUVALU": "TV",
    "UNITED STATES MINOR OUTLYING ISLANDS": "UM",
    "VANUATU": "VU",
    "VIETNAM": "VN", "VIET NAM": "VN",
    "WALLIS AND FUTUNA": "WF",
    # --- Afrika ---
    "ALGERIA": "DZ", "ALGERIET": "DZ",
    "ANGOLA": "AO",
    "BENIN": "BJ",
    "BOTSWANA": "BW",
    "BURKINA FASO": "BF",
    "BURUNDI": "BI",
    "CABO VERDE": "CV", "CAPE VERDE": "CV", "KAP VERDE": "CV",
    "CAMEROON": "CM", "CAMEROUN": "CM",
    "CENTRAL AFRICAN REPUBLIC": "CF", "DEN CENTRALAFRIKANSKE REPUBLIK": "CF",
    "CENTRALAFRIKANSKE REPUBLIK": "CF",
    "CHAD": "TD", "TCHAD": "TD",
    "COMOROS": "KM", "COMORERNE": "KM",
    "CONGO": "CG", "CONGO-BRAZZAVILLE": "CG", "REPUBLIC OF THE CONGO": "CG",
    "CONGO, DEMOCRATIC REPUBLIC OF THE": "CD",
    "DEMOCRATIC REPUBLIC OF THE CONGO": "CD", "DR CONGO": "CD",
    "CONGO-KINSHASA": "CD", "DEN DEMOKRATISKE REPUBLIK CONGO": "CD",
    "COTE D'IVOIRE": "CI", "CÔTE D'IVOIRE": "CI", "IVORY COAST": "CI",
    "ELFENBENSKYSTEN": "CI",
    "DJIBOUTI": "DJ",
    "EGYPT": "EG", "EGYPTEN": "EG", "ÆGYPTEN": "EG",
    "EQUATORIAL GUINEA": "GQ", "ÆKVATORIALGUINEA": "GQ",
    "ERITREA": "ER",
    "ESWATINI": "SZ", "SWAZILAND": "SZ",
    "ETHIOPIA": "ET", "ETIOPIEN": "ET",
    "GABON": "GA",
    "GAMBIA": "GM",
    "GHANA": "GH",
    "GUINEA": "GN",
    "GUINEA-BISSAU": "GW",
    "KENYA": "KE",
    "LESOTHO": "LS",
    "LIBERIA": "LR",
    "LIBYA": "LY", "LIBYEN": "LY",
    "MADAGASCAR": "MG", "MADAGASKAR": "MG",
    "MALAWI": "MW",
    "MALI": "ML",
    "MAURITANIA": "MR", "MAURETANIEN": "MR",
    "MAURITIUS": "MU",
    "MAYOTTE": "YT",
    "MOROCCO": "MA", "MAROKKO": "MA",
    "MOZAMBIQUE": "MZ",
    "NAMIBIA": "NA",
    "NIGER": "NE",
    "NIGERIA": "NG",
    "RWANDA": "RW",
    "RÉUNION": "RE", "REUNION": "RE",
    "SAO TOME AND PRINCIPE": "ST", "SÃO TOMÉ OG PRÍNCIPE": "ST",
    "SENEGAL": "SN",
    "SEYCHELLES": "SC", "SEYCHELLERNE": "SC",
    "SIERRA LEONE": "SL",
    "SOMALIA": "SO",
    "SOUTH AFRICA": "ZA", "SYDAFRIKA": "ZA",
    "SOUTH SUDAN": "SS", "SYDSUDAN": "SS",
    "SUDAN": "SD",
    "TANZANIA": "TZ", "TANZANIA, UNITED REPUBLIC OF": "TZ",
    "TOGO": "TG",
    "TUNISIA": "TN", "TUNESIEN": "TN",
    "UGANDA": "UG",
    "WESTERN SAHARA": "EH", "VESTSAHARA": "EH",
    "ZAMBIA": "ZM",
    "ZIMBABWE": "ZW",
    # --- Amerika / Caribien ---
    "ANGUILLA": "AI",
    "ANTIGUA AND BARBUDA": "AG", "ANTIGUA OG BARBUDA": "AG",
    "ARGENTINA": "AR",
    "ARUBA": "AW",
    "BAHAMAS": "BS",
    "BARBADOS": "BB",
    "BELIZE": "BZ",
    "BERMUDA": "BM",
    "BOLIVIA": "BO", "BOLIVIA, PLURINATIONAL STATE OF": "BO",
    "BONAIRE, SINT EUSTATIUS AND SABA": "BQ",
    "BRAZIL": "BR", "BRASILIEN": "BR",
    "CANADA": "CA",
    "CAYMAN ISLANDS": "KY", "CAYMANØERNE": "KY",
    "CHILE": "CL",
    "COLOMBIA": "CO",
    "COSTA RICA": "CR",
    "CUBA": "CU",
    "CURAÇAO": "CW", "CURACAO": "CW",
    "DOMINICA": "DM",
    "DOMINICAN REPUBLIC": "DO", "DEN DOMINIKANSKE REPUBLIK": "DO",
    "DOMINIKANSKE REPUBLIK": "DO",
    "ECUADOR": "EC",
    "EL SALVADOR": "SV",
    "FALKLAND ISLANDS": "FK", "FALKLAND ISLANDS (MALVINAS)": "FK",
    "FALKLANDSØERNE": "FK",
    "FRENCH GUIANA": "GF", "FRANSK GUYANA": "GF",
    "GRENADA": "GD",
    "GUADELOUPE": "GP",
    "GUATEMALA": "GT",
    "GUYANA": "GY",
    "HAITI": "HT",
    "HONDURAS": "HN",
    "JAMAICA": "JM",
    "MARTINIQUE": "MQ",
    "MEXICO": "MX",
    "MONTSERRAT": "MS",
    "NICARAGUA": "NI",
    "PANAMA": "PA",
    "PARAGUAY": "PY",
    "PERU": "PE",
    "PUERTO RICO": "PR",
    "SAINT BARTHÉLEMY": "BL", "SAINT BARTHELEMY": "BL",
    "SAINT KITTS AND NEVIS": "KN",
    "SAINT LUCIA": "LC",
    "SAINT MARTIN": "MF", "SAINT MARTIN (FRENCH PART)": "MF",
    "SAINT PIERRE AND MIQUELON": "PM",
    "SAINT VINCENT AND THE GRENADINES": "VC",
    "SINT MAARTEN": "SX", "SINT MAARTEN (DUTCH PART)": "SX",
    "SURINAME": "SR", "SURINAM": "SR",
    "TRINIDAD AND TOBAGO": "TT", "TRINIDAD OG TOBAGO": "TT",
    "TURKS AND CAICOS ISLANDS": "TC", "TURKS- OG CAICOSØERNE": "TC",
    "UNITED STATES": "US", "USA": "US", "UNITED STATES OF AMERICA": "US",
    "AMERIKAS FORENEDE STATER": "US",
    "URUGUAY": "UY",
    "VENEZUELA": "VE", "VENEZUELA, BOLIVARIAN REPUBLIC OF": "VE",
    "VIRGIN ISLANDS, BRITISH": "VG", "BRITISH VIRGIN ISLANDS": "VG",
    "DE BRITISKE JOMFRUØER": "VG", "BRITISKE JOMFRUØER": "VG",
    "VIRGIN ISLANDS, U.S.": "VI", "U.S. VIRGIN ISLANDS": "VI",
    "US VIRGIN ISLANDS": "VI", "DE AMERIKANSKE JOMFRUØER": "VI",
    "AMERIKANSKE JOMFRUØER": "VI",
    # --- Antarktis m.v. ---
    "ANTARCTICA": "AQ", "ANTARKTIS": "AQ",
    "BOUVET ISLAND": "BV", "BOUVETØEN": "BV",
    "HEARD ISLAND AND MCDONALD ISLANDS": "HM",
    "SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS": "GS",
    "SAINT HELENA": "SH",
    "SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA": "SH",
}

_WHITESPACE = re.compile(r"\s+")


def normalize_country(value):
    """Normalisér en landeangivelse til en ISO alpha-2 kode (uppercase).

    Accepterer både koder ("dk") og navne ("Danmark", "THE NETHERLANDS",
    "KOREA, REPUBLIC OF"). Opslaget kollapser whitespace og stripper et
    ledende "THE " (engelsk artikel — ERP-udtræk skriver ofte
    "THE NETHERLANDS"). Returnerer "" når værdien er tom eller ukendt-formet
    — en udfyldt-men-ukendt landetekst forbliver altså "ukendt land", aldrig
    et gæt.
    """
    if not value:
        return ""
    s = str(value).strip().upper()
    if len(s) == 2 and s.isalpha():
        return s
    s = _WHITESPACE.sub(" ", s)
    code = _COUNTRY_NAME_TO_CODE.get(s, "")
    if code:
        return code
    if s.startswith("THE "):
        return _COUNTRY_NAME_TO_CODE.get(s[4:], "")
    return ""


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


# K4 (gap-analyse-runde 2/kunde 2, Bal-godkendt 2026-09-20): et generisk,
# IKKE kunde-/navne-specifikt mønster for et internt koncern-partskodenummer
# i et kunde-/leverandørkartotek -- et to-bogstavs landepræfiks efterfulgt af
# 1-3 cifre (fx "US10"/"CA10"/"CN10" -- empirisk fundet i kunde 2s eget
# kundekartotek, "DK10/FI10-mønstret" i gap-analysen). ET RIGTIGT momsnummer
# (EU eller globalt Tax ID) har altid et længere, landeformats-specifikt
# ciffersæt efter præfikset (8+ cifre) -- et 1-3-cifret "nummer" er derfor et
# stærkt signal om en INTERN opslagskode for koncernens eget selskab i det
# pågældende land, ikke en ægte ekstern modparts momsnummer. Bruges KUN som
# et sekundært, generisk koncern-signal (se cat11_fraud_mtic._is_intercompany)
# -- ALDRIG et hardkodet kunde-/selskabsnavn.
_INTERNAL_PARTY_CODE = re.compile(r"^[A-Z]{2}\d{1,3}$")


def looks_like_internal_party_code(vat):
    """True hvis et (rengjort) momsnummer har formen af en intern koncern-
    partskode (landepræfiks + 1-3 cifre) frem for et ægte formateret
    momsnummer/Tax ID. Se modulkommentaren ved ``_INTERNAL_PARTY_CODE``."""
    return bool(_INTERNAL_PARTY_CODE.match(clean_vat_number(vat)))


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
    """Format-validér et EU-momsnummer — LANDEFORM-BEVIDST (K1, gap-analyse-
    runde 2/kunde 2, Bal-godkendt 2026-09-20).

    Tjekker at præfikset er et EU-præfiks og at kroppen matcher landets
    format-regex. Validerer IKKE mod VIES (kun syntaks). Returnerer
    (is_valid, reason).

    Baggrund/empiri: motoren har hidtil tvunget ETHVERT momsnummer (også
    globale Tax ID'er — amerikansk EIN, kinesisk USCC, schweizisk
    UID/CHE-nummer, m.fl.) gennem EU-formatkataloget. På kunde 2s
    IFS-datasæt, hvor kartoteket bærer ægte globale Tax ID'er med et
    pålideligt ISO-landepræfiks (fra D-parter-joinet: fx "CH…", "CN…",
    "US…", "GB…", "HK…", "TW…", "ZA…", "CA…"), gav det 120.826 falske
    høj-fund (kontrol 28) — EU's momsnummerformater er IKKE designet til at
    validere et ikke-EU Tax ID, og "forkert format" er derfor ikke en
    retvisende konklusion for de tal. Empirisk fordeling (kunde 2): af
    ca. 1,16 mio. linjer med momsnummer er ~1,03 mio. EU-præfikserede
    (heraf kun ~3.900 REELT ugyldige mod deres eget lands format), ~107.000
    har et alfabetisk, men ikke-EU præfiks (CH/NO/CN/GB/US/HK/IS/CA/TW/ZA/…),
    og ~15.000 mangler helt et alfabetisk præfiks.

    Fix: valider KUN mod EU-mønstre når partens land — bestemt af
    momsnummerets EGET præfiks (det mest pålidelige signal, uafhængigt af en
    evt. fejlbehæftet landenavne-kolonne), eller subsidiært det oplyste
    ``country``-parameter, når nummeret ikke selv bærer noget præfiks — er
    ET KENDT EU-LAND. Er landet udenlandsk-men-ikke-EU, eller slet ikke til
    at bestemme, findes der intet EU-formatkatalog at holde det op imod —
    kontrollen konkluderer da "ikke valideret" (intet fund), i stedet for at
    gætte og fejlagtigt flage et lovligt globalt Tax ID. Et EU-præfikseret
    nummer, der rent faktisk fejler sit eget lands regex, forbliver et
    fund (uændret sværhedsgrad) — det er netop den type der IKKE må
    forsvinde.
    """
    v = clean_vat_number(vat)
    if not v:
        return False, "tomt momsnummer"

    prefix = v[:2] if v[:2].isalpha() else ""
    body = v[2:] if prefix else v

    if not prefix:
        # Intet præfiks i selve nummeret — prøv mod det oplyste lands format.
        c = normalize_country(country)
        prefix = VAT_PREFIX_FOR_COUNTRY.get(c, "")
        if not prefix:
            # Hverken nummeret selv eller det oplyste land peger på et EU-land
            # — vi kan ikke validere formatet for et ukendt/ikke-EU land.
            # "Ellers intet fund" (K1-princippet): ikke det samme som gyldigt,
            # men IKKE et EU-formatfund.
            return True, "intet landepræfiks og ukendt/ikke-EU land — ikke valideret mod EU-format"
        body = v

    regex = _EU_VAT_REGEX.get(prefix)
    if regex is None:
        # Alfabetisk præfiks, men ikke et EU-præfiks (fx CH/NO/CN/GB/US/HK/
        # IS/CA/TW/ZA/…) — et globalt Tax ID fra et udenlandsk-men-ikke-EU
        # land. Vi har intet katalog over andre landes momsnummerformater,
        # så vi validerer IKKE mod EU-mønstre (landeform-bevidst, K1).
        return True, f"ikke-EU landepræfiks '{prefix}' — ikke valideret mod EU-format"
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


# === BC/NAV VAT POSTING SETUP-SEMANTIK ===
#
# Generel platform-egenskab (2026-09-18, Bal-godkendt gap-analyse-fix A) --
# IKKE en kunde-specifik regel. I Microsoft Dynamics 365 Business Centrals
# VAT Posting Setup er satsen registreret på en reverse charge-kode
# BEREGNINGSSATSEN FOR KØBSSIDEN (omvendt betalingspligt: kunden
# selvangiver moms med denne sats). På SALGSSIDEN dækker PRÆCIS samme kode
# typisk nulsats-eksport/EU-ydelsessalg -- sælgers salg er momsfrit i
# Danmark, og 0 kr. udgående moms er derfor KORREKT, ikke et fund. Uden
# dette retningsskel bliver en kontrol, der bruger linjens (setup-arvede)
# sats retningsløst, vildledt til at flage lovligt eksportsalg som
# "manglende salgsmoms" (fx et salg med koden "OUTSIDE DK/EU|
# SERVICE_VAT_NOT_EU" (25,0% -- købssidens RC-sats) uden bogført momsbeløb).


def is_reverse_charge_sale_code(tax_code, vat_calculation_type=""):
    """True hvis momskoden er en reverse charge-kode, hvis sats KUN gælder
    købssiden (se modulets BC/NAV-kommentar ovenfor) -- dvs. hvis en
    SALGSLINJE med denne kode og 0 kr. moms er korrekt eksport, ikke et fund.

    To valideringsveje, i prioriteret rækkefølge:

      1. ``vat_calculation_type`` (balai_extensions, kontrakt v0.4.0,
         tax_table[]/lines[].vat_calculation_type -- kun til stede på den
         kanoniske vej når vat_setup.csv er indlæst): DETERMINISTISK.
         Værdien "Reverse Charge VAT" (fra BC) betyder pr. definition, at
         opsætningens sats er købssidens RC-sats.
      2. Fravær af feltet (Excel-/SAF-T-oprindelse, eller kanonisk vej uden
         vat_setup.csv): fallback på Bus.-gruppen -- ``tax_code``s FØRSTE
         led, adskilt med "|" (BC's egen "gruppe|kode"-konvention, fx
         "EU|SERVICE_VAT_EU", "OUTSIDE DK/EU|SERVICE_VAT_NOT_EU"). Er
         gruppen IKKE "DOMESTIC", er koden pr. definition udenlandsk handel
         -- og enhver udenlandsk salgskode er nulsats/RC på salgssiden.
      3. F2 (gap-analyse-runde 2, Bal-godkendt 2026-09-20): følger
         ``tax_code`` IKKE gruppe|kode-konventionen (intet "|") -- fx en
         ERP uden BC's Bus.-gruppe-taksonomi som IFS, hvis koder er opake
         mnemonics ("RC", "RC50") -- er der intet Bus.-gruppe-signal, men
         koden kan stadig matche et KONFIGURERET RC-kode-mønster
         (``materiality.RC_CODE_PREFIXES``, default kun "RC", prefiks-match
         case-insensitivt, samme princip som NO_VAT_PRODUCT_PATTERNS).
         Matcher intet af de konfigurerede mønstre, returneres False
         (uændret, konservativ adfærd -- ingen gættet RC-status uden
         evidens).
    """
    calc_type = (vat_calculation_type or "").strip().lower()
    if calc_type:
        return "reverse charge" in calc_type
    code = (tax_code or "").strip()
    if "|" in code:
        bus_group = code.split("|", 1)[0].strip().upper()
        return bus_group != "DOMESTIC"
    if not code:
        return False
    from analytics import materiality  # lokal import: undgår cirkularitet ved modulindlæsning
    upper = code.upper()
    return any(upper.startswith(p) for p in materiality.RC_CODE_PREFIXES)


# --- BC/NAV "Bus.-gruppe|Produktkode"-konvention: generelle helpers ---------
#
# Gap-analyse-kontrollerne 104-108 (2026-09-18, Bal-godkendt, cat13_cross_
# dimension.py) genbruger den samme opaque "gruppe|kode"-streng som
# is_reverse_charge_sale_code ovenfor, men har brug for BEGGE led hver for
# sig (ikke kun "er det udenlandsk"). Disse to helpers er bevidst dumme
# streng-splits — ingen fortolkning af selve koden — samme "opaque streng"-
# disciplin som canonical_parser.py's modul-docstring beskriver for
# ``vat_codes``.

def vat_bus_group(tax_code):
    """Bus.-gruppen (FØRSTE led før '|'), uppercase/trimmet. '' hvis koden
    ikke følger 'gruppe|kode'-konventionen (intet '|')."""
    code = tax_code or ""
    if "|" not in code:
        return ""
    return code.split("|", 1)[0].strip().upper()


def vat_product_code(tax_code):
    """Produktkoden (efter '|'), uppercase/trimmet. Følger koden ikke
    konventionen (intet '|'), returneres HELE koden uppercase/trimmet —
    så et opslag som ``"NO_VAT" in vat_product_code(code)`` også virker på en
    kode uden Bus.-gruppe-præfiks."""
    code = tax_code or ""
    if "|" not in code:
        return code.strip().upper()
    return code.split("|", 1)[1].strip().upper()


def is_rc_calc_type(vat_calculation_type):
    """True hvis ``vat_calculation_type`` (balai_extensions, fra vat_setup.csv)
    eksplicit siger reverse charge. Tomt/ukendt -> False (intet signal, ikke
    en gættet RC-status)."""
    return "reverse charge" in (vat_calculation_type or "").strip().lower()


def is_no_vat_product(tax_code):
    """True hvis produktkoden matcher et af de konfigurerede 'ægte nulsats/
    fritaget'-mønstre (materiality.NO_VAT_PRODUCT_PATTERNS, default kun
    'NO_VAT') -- BC/NAV's egen navnekonvention for eksplicit nulsats-koder
    (modsat en reverse charge-kode, der bruger GOODS_VAT_*/SERVICE_VAT_*-
    navne). Delstrengs-match, case-insensitivt, samme princip som
    text_matches_any/kontrol 82's mønstre."""
    from analytics import materiality  # lokal import: undgår cirkularitet ved modulindlæsning
    product = vat_product_code(tax_code).lower()
    if not product:
        return False
    return any(p in product for p in materiality.NO_VAT_PRODUCT_PATTERNS)


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

# Kontotyper der er balanceposter — IKKE momsbærende. Momsrelevans-scope: en
# momskontrol må kun undertrykke et fund, når vi POSITIVT kan se, at linjen er
# en balancekonto. Er kontotypen ukendt (fx Excel-import uden kontoplan),
# undertrykkes intet — adfærden er uændret. To ERP-konventioner er kendte og
# begge understøttet: SAF-T's AccountType-enum (ental: "asset"/"liability"/
# "equity") OG BC/NAV's egen kontoplan-eksport (flertal: "assets"/
# "liabilities" — bekræftet på den kanoniske vejs chart_of_accounts.csv,
# byggetrin 8/Del C, Bal-godkendt 2026-09-17). "income"/"expense" er IKKE
# balanceposter og indgår bevidst ikke her.
_NON_VAT_ACCOUNT_TYPES = {"asset", "liability", "equity", "assets", "liabilities"}


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
