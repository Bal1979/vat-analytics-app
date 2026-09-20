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

Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): "sprunget over" ovenfor
var oprindeligt kun INFORMATIV — kontrollerne blev stadig kørt af engine.py og
kunne stadig generere per-transaktions-støj på et felt, der reelt er 0%
fraværende i HELE datasættet (fx kontrol 4 på Description, kontrol 25 på
country). Modulet har derfor nu en FJERDE, HÅNDHÆVET tilstand,
``STATUS_IKKE_MAALBART``: samme 0%-betingelse som "sprunget over", men kun når
populationen er stor nok til at udelukke en tilfældig/enkeltstående tomhed
(``field_is_gated``/``MIN_TX_FOR_GATING``). Kun DENNE status får
``analytics/engine.py`` til faktisk at fjerne fund — "sprunget over data" er
fortsat rent informativ, uændret adfærd.
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
    13: [],                         # krydsdimensionelle kontroller: se CONTROL_REQUIREMENTS (107/108)
}

# Per-kontrol-override (mere præcise krav end kategoriens default).
CONTROL_REQUIREMENTS = {
    36: ["ship_from_country", "ship_to_country"],  # place-of-supply: vareflow
    49: ["vat_number"],                            # manglende CVR på dansk leverandør
    # Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): kontrol 25 tjekker
    # line["country"] direkte (nulsats kun OK for udenlandsk modpart) — kategori
    # 3's default (kun tax_code) fanger ikke dette. Uden override var kontrol 25
    # strukturelt blind for "intet landesignal i hele filen" og gav 10.671
    # falske "ingen udenlandsk modpart"-fund på et GL-udtræk uden landekolonne.
    25: ["country"],
    # Kontrol 107-108 (gap-analysen, Bal-godkendt 2026-09-18, cat13_cross_
    # dimension.py): bilagstype-krydskontroller kræver ``source_code``
    # (BC/NAV "Source Code") — et felt der endnu ikke er udbredt på nogen
    # input-vej. Kategori 13's øvrige kontroller (104-106) har intet
    # kategori-krav (se CATEGORY_REQUIREMENTS), så override er nødvendig
    # her, samme mønster som kontrol 25/36/49 ovenfor.
    107: ["source_code"],
    108: ["source_code"],
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
    # Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): kun brugt af
    # SUBCHECK_FIELDS (kontrol 4's Description-delcheck) i dag — se dér.
    "description": ("Bilagstekst/beskrivelse", "SAF-T Description (transaktion/linje) eller en kolonne 'Beskrivelse/Tekst'"),
    "source_code": ("Bilagstype/Source Code", "Den kanoniske vejs 'source_code'-kolonne (BC/NAV Source Code) eller en kolonne 'Bilagstype'"),
}

# Effektive statusser (prioriteret rækkefølge afgøres i assess).
STATUS_KOERT = "koert"                         # kørte (fund eller rent)
STATUS_SPRUNGET_DATA = "sprunget_over_data"    # kunne ikke køre — felt mangler
STATUS_MODUL_FRA = "modul_fra"                 # modulet er slået fra
STATUS_EKSTERNE_DATA = "kraever_eksterne_data" # kræver data uden for udtrækket
# Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): en SKÆRPET variant af
# STATUS_SPRUNGET_DATA. Begge betyder "et påkrævet felt er 0% udfyldt" — men
# STATUS_IKKE_MAALBART udløses KUN når populationen samtidig er stor nok til at
# udelukke, at det bare er én lille test-/scenarie-transaktion, der tilfældigvis
# mangler feltet (se MIN_TX_FOR_GATING/field_is_gated nedenfor). KUN denne status
# udløser faktisk fund-undertrykkelse i analytics/engine.py — STATUS_SPRUNGET_DATA
# forbliver rent informativ, præcis som hidtil (uændret adfærd/tests).
STATUS_IKKE_MAALBART = "ikke_maalbar"

_MIN_TX_FOR_STATISTIK = 30  # under dette er statistik-/anomalikontroller svage

# Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): størrelses-guard for
# HÅNDHÆVET gating (se field_is_gated). IKKE en fuzzy udfyldningstærskel —
# tærsklen forbliver 0% (v1-kravet) — men en minimumspopulation, under hvilken
# "0% udfyldt" intet siger om HELE datasættet (fx valideringssuitens
# et-transaktions-scenarier, der bevidst tømmer ét felt for at plante en ægte,
# enkeltstående defekt). Samme værdi/begrundelse som _MIN_TX_FOR_STATISTIK.
MIN_TX_FOR_GATING = 30


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


# --- Del B: reel gating af "ikke målbare" felter (medium-fund-analysen, ------
# Bal-godkendt 2026-09-17) ----------------------------------------------------
#
# "Ingen falske alarmer"-filosofien (jf. CLAUDE.md) udvidet til medium-laget:
# når et felt, en kontrol (eller ÉN delcheck i en multi-felt-kontrol) hårdt
# afhænger af, er 0% udfyldt i HELE datasættet, skal kontrollen (eller
# delchecket) rapportere "ikke målbar" ÉN gang — ikke generere støj pr.
# transaktion. field_is_gated() er den fælles primitiv: den bruges BÅDE af
# assess() nedenfor (til at skærpe STATUS_SPRUNGET_DATA til
# STATUS_IKKE_MAALBART for hele kontroller styret af CATEGORY_REQUIREMENTS/
# CONTROL_REQUIREMENTS) OG direkte af kontrolkoden for multi-felt-kontroller,
# der IKKE kan gates som helhed (se SUBCHECK_FIELDS/test_04 nedenfor).

def field_is_gated(data: dict, field: str, level: str = "line") -> bool:
    """Er FELT reelt umuligt at måle på hele datasættet? Kræver BEGGE:
    (1) 0% udfyldt — v1-tærsklen, ingen fuzzy-mellemtrin: et felt der er
        udfyldt på blot ÉN linje/transaktion tæller som "til stede" (samme
        semantik som _populated/til_stede ovenfor) og gates ALDRIG, uanset
        hvor lav dækningen ellers er.
    (2) populationen er mindst MIN_TX_FOR_GATING stor — under den grænse kan
        "0% udfyldt" ikke skelnes fra "denne ene test-transaktion mangler
        tilfældigvis feltet" (fx valideringssuitens et-transaktions-scenarier,
        der bevidst tømmer ét felt for at plante en ægte, enkeltstående
        defekt — dem skal denne funktion ALDRIG gate).

    ``level``: "line" tæller transactions[].lines[] (fx country, vat_number —
    de fleste CATEGORY_REQUIREMENTS/CONTROL_REQUIREMENTS-felter); "transaction"
    tæller transactions[] selv (fx description, som kontrol 4 tjekker på
    transaktionsniveau, jf. cat01_transaction_integrity.test_04)."""
    if level == "transaction":
        items = data.get("transactions", [])
    else:
        items = [l for t in data.get("transactions", []) for l in t.get("lines", [])]
    if len(items) < MIN_TX_FOR_GATING:
        return False
    return not any(_populated(field, it.get(field)) for it in items)


# Delcheck-niveau gating: kontroller hvor ÉT felt kun styrer ÉN delmængde af
# kontrollens tjek — kontrol 4 er den kendte "multi-felt"-kontrol (den tjekker
# TransactionID/TransactionDate/AccountID OGSÅ, som ALDRIG må gates, selvom
# Description er strukturelt fraværende). Disse kontroller kan derfor IKKE
# gates som helhed via CATEGORY_REQUIREMENTS/CONTROL_REQUIREMENTS (det ville
# fejlagtigt undertrykke de andre, stadig-kørbare delcheck) — selve
# kontrolkoden kalder field_is_gated() direkte. Registreringen her bruges KUN
# til at overføre samme "ikke målbar"-besked til rapporten (delkontrol_gates).
SUBCHECK_FIELDS = {
    4: [("description", "transaction")],  # kontrol 4: kun Description-delchecket
}


def subcheck_gates(data: dict) -> list:
    """Delcheck-niveau 'ikke målbar'-noter (se SUBCHECK_FIELDS) — vises i
    rapporten ved siden af de fulde per-kontrol-statusser i assess()["kontroller"]."""
    gates = []
    for tid, fields in SUBCHECK_FIELDS.items():
        for field, level in fields:
            if field_is_gated(data, field, level=level):
                navn, _kilde = FIELD_INFO[field]
                gates.append({
                    "test_id": tid,
                    "felt": field,
                    "status": STATUS_IKKE_MAALBART,
                    "besked": f"Kan ikke måles: {navn} findes ikke i datagrundlaget",
                })
    return gates


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


def assess(data: dict, active_modules: set, categories: list,
           external_data_provided: dict = None) -> dict:
    """Afgør pr. kontrol den effektive datagrundlags-status + kategori-rollup.

    ``categories`` er engine.CATEGORIES (id/name/test_range). ``active_modules`` er
    det sæt analyse-moduler, motoren rent faktisk kørte med.

    ``external_data_provided``: {test_id: bool} — for en kontrol i
    EXTERNAL_DATA, der RENT FAKTISK har fået sit eksterne input i denne
    kørsel (fx kontrol 82 med en angivelsesfil, byggetrin 8/Del B,
    Bal-godkendt 2026-09-17), springes STATUS_EKSTERNE_DATA-branchen over —
    kontrollen vurderes i stedet efter de normale felt-/modul-krav. Ukendt/
    ikke angivet test_id = uændret adfærd (samme som i dag).
    """
    external_data_provided = external_data_provided or {}
    prof = profile_dataset(data)
    cov = prof["felter"]
    few_tx = prof["transaktioner"] < _MIN_TX_FOR_STATISTIK

    controls = []
    for tid in range(1, 110):
        cat_id, cat_name = _category_of(tid, categories)
        modul = modules.module_of(tid)
        modul_aktiv = modul in active_modules

        req = _control_requirements(tid, cat_id)
        mangler = [f for f in req if not cov.get(f, {}).get("til_stede", False)]
        # Del B: hvilke af de manglende felter er REELT 0%-fraværende på en
        # population, der er stor nok til at håndhæve (field_is_gated)? Kun
        # DEM skærper status til STATUS_IKKE_MAALBART (og udløser den faktiske
        # fund-undertrykkelse i engine.run_all_tests). Et lille datasæt
        # (under MIN_TX_FOR_GATING) beholder den hidtidige, rent informative
        # STATUS_SPRUNGET_DATA — uændret adfærd for valideringssuitens
        # et-transaktions-scenarier og øvrige eksisterende tests.
        gated_fields = [f for f in mangler if field_is_gated(data, f)]

        if tid in EXTERNAL_DATA and not external_data_provided.get(tid, False):
            status = STATUS_EKSTERNE_DATA
            aarsag = EXTERNAL_DATA[tid]
        elif not modul_aktiv:
            status = STATUS_MODUL_FRA
            aarsag = f"Modulet “{modules.MODULES[modul]['navn']}” er slået fra"
        elif gated_fields:
            status = STATUS_IKKE_MAALBART
            aarsag = ("Kan ikke måles: " + " og ".join(FIELD_INFO[f][0] for f in gated_fields)
                      + " findes ikke i datagrundlaget")
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
            "ikke_maalbar": sum(1 for x in in_cat if x["status"] == STATUS_IKKE_MAALBART),
            "modul_fra": sum(1 for x in in_cat if x["status"] == STATUS_MODUL_FRA),
            "kraever_eksterne_data": sum(1 for x in in_cat if x["status"] == STATUS_EKSTERNE_DATA),
        })

    # "Hvad mangler" — pr. manglende felt: hvor mange kontroller det blokerer
    # (både den rent informative STATUS_SPRUNGET_DATA og den håndhævede
    # STATUS_IKKE_MAALBART — begge betyder "feltet mangler", forskellen er kun
    # om populationen var stor nok til at håndhæve det, se field_is_gated).
    blocked_by = {}
    for x in controls:
        if x["status"] not in (STATUS_SPRUNGET_DATA, STATUS_IKKE_MAALBART):
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
            # Del B (medium-fund-analysen, Bal-godkendt 2026-09-17): additiv
            # nøgle — tælles IKKE med i "sprunget_over_data" (adskilt bucket),
            # så eksisterende summerings-tjek (koert+sprunget+modul+ekstern==103)
            # forbliver korrekt uændret på små datasæt (hvor denne altid er 0).
            "ikke_maalbar": sum(1 for x in controls if x["status"] == STATUS_IKKE_MAALBART),
            "modul_fra": sum(1 for x in controls if x["status"] == STATUS_MODUL_FRA),
            "kraever_eksterne_data": sum(1 for x in controls if x["status"] == STATUS_EKSTERNE_DATA),
            "i_alt": len(controls),
        },
        "kategorier": cat_rollup,
        "kontroller": controls,
        "manglende_data": mangelliste,
        # Del B: delcheck-niveau "ikke målbar"-noter for multi-felt-kontroller
        # (kontrol 4's Description-delcheck) — se SUBCHECK_FIELDS/subcheck_gates.
        # Disse kontroller optræder som STATUS_KOERT i "kontroller" ovenfor
        # (de øvrige delcheck kører jo fint), så noten her er den ENESTE plads
        # i rapporten, der viser at ét bestemt delcheck ikke kunne måles.
        "delkontrol_gates": subcheck_gates(data),
    }
