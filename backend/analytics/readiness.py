"""
Datagrundlag / kørbarhed: hvilke kontroller KAN køre på det uploadede datasæt,
og hvad mangler der af data for dem, der ikke kan.

Baggrund
--------
En kontrol "springer i dag pænt over", når data mangler — den returnerer bare
ingen fund. Derfor ligner "grøn / 0 fund" tre helt forskellige ting:
  1. kørte og fandt intet (reelt rent)
  2. kunne ikke køre — et nødvendigt felt mangler i udtrækket
  3. kørte aldrig — kontrollens analyse-modul er slået fra
Dette modul gør forskellen EKSPLICIT: det profilerer datasættet (hvilke felter er
udfyldt) og afgør pr. kontrol en effektiv status, uden at køre kontrollerne.

Modellen er deklarativ og bevidst konservativ: et felt tæller som "til stede", så
snart det er udfyldt på mindst én linje (så en lav dækning — fx land kun på de
udenlandske linjer — ikke fejlagtigt markeres som "mangler"). Dækningsgraden vises
som kontekst. Kravene kan forfines af den fagansvarlige (kategori-default +
per-kontrol-override).
"""

from __future__ import annotations

from analytics import modules

# --- Datakrav ---------------------------------------------------------------
# "Signal-felter": de felter hvis fravær gør kategoriens kontroller ude af stand
# til at give et meningsfuldt momsfagligt resultat. Ubikvitære felter (beløb,
# konto) er ikke krav — de er altid til stede i den kanoniske struktur.
# Udledt af det faktiske feltforbrug i analytics/categories/cat*.py.
CATEGORY_REQUIREMENTS = {
    1: [],                          # transaktionsintegritet: basisfelter
    2: ["source_document_id"],      # dubletter: bilags-/fakturareference
    3: ["tax_code"],                # momssats-validering: momskode/sats
    4: ["country"],                 # grænseoverskridende: landeinfo
    5: [],                          # timing: dato/periode (txn-niveau, næsten altid)
    6: ["vat_number"],              # parts-validering: momsnr
    7: [],                          # beløb/tærskel: basisbeløb
    8: [],                          # statistik: basisbeløb (volumen — se note)
    9: ["country", "tax_code"],     # reverse charge: land + momskode
    10: ["tax_code"],               # afstemning: momskode/sats
    11: ["country"],                # MTIC: land (+ tværvirksomhed = eksternt)
    12: ["country"],                # e-handel: land/salgskanal
}

# Per-kontrol-override (mere præcise krav end kategoriens default).
CONTROL_REQUIREMENTS = {
    36: ["ship_from_country", "ship_to_country"],  # place-of-supply: vareflow
    49: ["vat_number"],                            # manglende CVR på dansk leverandør
}

# Kontroller der kræver EKSTERNE data, som en enkelt-virksomheds-eksport ikke
# bærer. Svarer til de strukturelt inaktive kontroller (returnerer altid []).
EXTERNAL_DATA = {
    82: "En indberettet momsangivelse at afstemme imod",
    83: "En fordelingsnøgle for delvis fradragsret (omsætningsfordeling)",
    85: "Vareflow på tværs af virksomheder (karrusel/MTIC)",
    90: "Fuld betalingsdata (faktiske overførsler)",
    99: "Salgskanal-/markedspladsdata",
}

# Forretningssprog + kilde for hvert signal-felt (til "hvad mangler"-beskeden).
FIELD_INFO = {
    "country": ("Modpartens land", "SAF-T stamdata (Country) eller en kolonne 'Land'"),
    "vat_number": ("Momsnummer/CVR", "SAF-T Customers/Suppliers eller en kolonne 'Momsnr/CVR'"),
    "tax_code": ("Momskode", "SAF-T TaxCode/TaxInformation eller en kolonne 'Momskode'"),
    "tax_percentage": ("Momssats", "SAF-T TaxInformation/TaxTable eller en kolonne 'Momssats'"),
    "source_document_id": ("Bilags-/fakturanummer", "SAF-T Transaction eller en kolonne 'Bilagsnr/Fakturanr'"),
    "ship_from_country": ("Afsenderland", "en kolonne 'Afsenderland' (findes ikke i SAF-T Financial)"),
    "ship_to_country": ("Modtagerland", "en kolonne 'Modtagerland' (findes ikke i SAF-T Financial)"),
}

# Effektive statusser (prioriteret rækkefølge afgøres i assess).
STATUS_KOERT = "koert"                         # kørte (fund eller rent)
STATUS_SPRUNGET_DATA = "sprunget_over_data"    # kunne ikke køre — felt mangler
STATUS_MODUL_FRA = "modul_fra"                 # modulet er slået fra
STATUS_EKSTERNE_DATA = "kraever_eksterne_data" # kræver data uden for udtrækket

_MIN_TX_FOR_STATISTIK = 30  # under dette er statistik-/anomalikontroller svage


def _control_requirements(test_id: int, category_id: int) -> list:
    if test_id in CONTROL_REQUIREMENTS:
        return CONTROL_REQUIREMENTS[test_id]
    return CATEGORY_REQUIREMENTS.get(category_id, [])


def _populated(field: str, value) -> bool:
    """Er feltet udfyldt på en linje? Strenge: ikke-tom; tal: forskellig fra 0."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (int, float)):
        return value != 0
    return bool(value)


def profile_dataset(data: dict) -> dict:
    """Feltdækning: for hvert relevant signal-felt, hvor mange linjer det er udfyldt på."""
    lines = [l for t in data.get("transactions", []) for l in t.get("lines", [])]
    total = len(lines)
    coverage = {}
    for field in FIELD_INFO:
        n = sum(1 for l in lines if _populated(field, l.get(field)))
        coverage[field] = {
            "udfyldte_linjer": n,
            "andel": round(n / total, 3) if total else 0.0,
            "til_stede": n > 0,
        }
    return {"linjer": total, "transaktioner": len(data.get("transactions", [])),
            "felter": coverage}


def _category_of(test_id: int, categories: list):
    for c in categories:
        lo, hi = c["test_range"]
        if lo <= test_id <= hi:
            return c["id"], c["name"]
    return None, None


def assess(data: dict, active_modules: set, categories: list) -> dict:
    """Afgør pr. kontrol den effektive datagrundlags-status + kategori-rollup.

    ``categories`` er engine.CATEGORIES (id/name/test_range). ``active_modules`` er
    det sæt analyse-moduler, motoren rent faktisk kørte med.
    """
    prof = profile_dataset(data)
    cov = prof["felter"]
    few_tx = prof["transaktioner"] < _MIN_TX_FOR_STATISTIK

    controls = []
    for tid in range(1, 104):
        cat_id, cat_name = _category_of(tid, categories)
        modul = modules.module_of(tid)
        modul_aktiv = modul in active_modules

        req = _control_requirements(tid, cat_id)
        mangler = [f for f in req if not cov.get(f, {}).get("til_stede", False)]

        if tid in EXTERNAL_DATA:
            status = STATUS_EKSTERNE_DATA
            aarsag = EXTERNAL_DATA[tid]
        elif not modul_aktiv:
            status = STATUS_MODUL_FRA
            aarsag = f"Modulet “{modules.MODULES[modul]['navn']}” er slået fra"
        elif mangler:
            status = STATUS_SPRUNGET_DATA
            aarsag = "Mangler: " + ", ".join(FIELD_INFO[f][0] for f in mangler)
        else:
            status = STATUS_KOERT
            aarsag = ""

        controls.append({
            "test_id": tid,
            "kategori_id": cat_id,
            "analyse_modul": modul,
            "modul_aktiv": modul_aktiv,
            "status": status,
            "aarsag": aarsag,
            "manglende_felter": mangler,
        })

    # Kategori-rollup: tæl statusser pr. kategori.
    cat_rollup = []
    for c in categories:
        in_cat = [x for x in controls if x["kategori_id"] == c["id"]]
        cat_rollup.append({
            "id": c["id"], "navn": c["name"],
            "antal": len(in_cat),
            "koert": sum(1 for x in in_cat if x["status"] == STATUS_KOERT),
            "sprunget_over_data": sum(1 for x in in_cat if x["status"] == STATUS_SPRUNGET_DATA),
            "modul_fra": sum(1 for x in in_cat if x["status"] == STATUS_MODUL_FRA),
            "kraever_eksterne_data": sum(1 for x in in_cat if x["status"] == STATUS_EKSTERNE_DATA),
        })

    # "Hvad mangler" — pr. manglende felt: hvor mange kontroller det blokerer.
    blocked_by = {}
    for x in controls:
        if x["status"] != STATUS_SPRUNGET_DATA:
            continue
        for f in x["manglende_felter"]:
            blocked_by.setdefault(f, []).append(x["test_id"])
    mangelliste = [{
        "felt": f, "navn": FIELD_INFO[f][0], "kilde": FIELD_INFO[f][1],
        "blokerer_kontroller": sorted(ids), "antal": len(ids),
    } for f, ids in sorted(blocked_by.items(), key=lambda kv: -len(kv[1]))]

    total_koert = sum(1 for x in controls if x["status"] == STATUS_KOERT)
    return {
        "profil": prof,
        "faa_transaktioner": few_tx,
        "opsummering": {
            "koert": total_koert,
            "sprunget_over_data": sum(1 for x in controls if x["status"] == STATUS_SPRUNGET_DATA),
            "modul_fra": sum(1 for x in controls if x["status"] == STATUS_MODUL_FRA),
            "kraever_eksterne_data": sum(1 for x in controls if x["status"] == STATUS_EKSTERNE_DATA),
            "i_alt": len(controls),
        },
        "kategorier": cat_rollup,
        "kontroller": controls,
        "manglende_data": mangelliste,
    }
