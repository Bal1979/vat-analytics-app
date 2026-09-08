"""
Analyse-moduler: momsrelevans-slankning af de 103 kontroller.

Baggrund
--------
Værktøjet rummer 103 kontroller, men langt fra alle er momsanalyser. Efter
gennemgangen med Fabian (berigelsesnotatet, del A) skelner vi mellem:

- **Momskernen** (default TIL): de kontroller, der har en direkte momsfaglig
  konsekvens (fradragsret, angivelse, sats, RC, place-of-supply, afstemning).
- **Udvidede moduler** (default FRA): forensic/statistik/MTIC, ren
  stamdatakvalitet, bredere dublet-/recovery-scanninger og e-handels-særordninger.
  De er fagligt korrekte, men hører til et andet mandat (JE/besvigelse,
  datakvalitet) eller kun til bestemte klientprofiler (B2C-fjernsalg/OSS).

Intet slettes: en deaktiveret kontrol kører bare ikke og emitterer ingen fund.
Alt kan tændes igen — pr. kørsel eller via miljøvariablen ``ANALYTICS_MODULES``.
Kataloget (catalog/rules.json) bærer ``analyse_modul`` + ``default_aktiv`` pr.
kontrol, så det er fuldt sporbart, hvad der er slået fra og hvorfor.

Konfiguration
-------------
Aktive moduler bestemmes (i prioriteret rækkefølge):
  1. Et eksplicit ``override`` (iterabel af modulnøgler) givet pr. kørsel.
  2. Miljøvariablen ``ANALYTICS_MODULES`` (komma-separeret liste af modulnøgler).
     Særtilfælde: ``ANALYTICS_MODULES=alle`` tænder ALT.
  3. Ellers: modulerne med ``default_active=True`` (dvs. kun momskernen).

Modulnøgler der ikke kendes, ignoreres (robusthed mod stavefejl i env).
"""

from __future__ import annotations

import os
from typing import Iterable, Optional

# --- Modul-metadata --------------------------------------------------------
# Rækkefølgen er visnings-rækkefølgen (kernen først).
MODULES = {
    "moms_kerne": {
        "navn": "Momskerne",
        "beskrivelse": "Kontroller med direkte momsfaglig konsekvens: sats, "
                       "fradragsret, angivelse, reverse charge, place-of-supply "
                       "og momsafstemning. Fundamentet i en momsgennemgang.",
        "default_active": True,
    },
    "forensic_statistik": {
        "navn": "Forensic & statistik",
        "beskrivelse": "Statistisk anomalidetektion, beløbs-outliers, timing-"
                       "anomalier og karrusel/MTIC-indikatorer. Hører til et "
                       "JE-/besvigelsesmandat — ikke en momsgennemgang.",
        "default_active": False,
    },
    "datakvalitet": {
        "navn": "Stamdata- & datakvalitet",
        "beskrivelse": "Parts-/stamdatavalidering uden direkte fradrags-"
                       "konsekvens (fx dubletnavne, formatfejl). Datakvalitet, "
                       "ikke momsfund.",
        "default_active": False,
    },
    "dublet_recovery": {
        "navn": "Dublet-recovery (bredt)",
        "beskrivelse": "Bredere dublet-/recovery-scanninger (nær-dubletter, "
                       "beløbsmatch på tværs). Den stærke eksakt-dublet med "
                       "moms-dobbeltfradrag ligger i momskernen.",
        "default_active": False,
    },
    "ehandel_saerordninger": {
        "navn": "E-handel & særordninger",
        "beskrivelse": "OSS/fjernsalg, digitale ydelser, margin-/brugtmoms. "
                       "Momsfagligt korrekte, men kun relevante ved B2C-"
                       "fjernsalg/særordninger — klientbetinget, default fra.",
        "default_active": False,
    },
}

# --- Kontrol -> modul ------------------------------------------------------
# Alt er momskerne som udgangspunkt; kun de kontroller, berigelsesnotatet (del A)
# eksplicit henfører til et andet mandat, flyttes. Uspecificerede kontroller i
# delvist skårne kategorier forbliver bevidst i kernen (konservativt: vi skærer
# kun det, vi udtrykkeligt har besluttet at skære).
_OVERRIDES = {
    # Forensic & statistik (default fra)
    **{tid: "forensic_statistik" for tid in (
        41, 42, 43, 45,                       # kat 5: timing-anomalier
        55, 56, 58, 61, 62,                   # kat 7: beløbs-outliers
        63, 64, 65, 66, 67, 68, 69,           # kat 8: statistik (hele)
        84, 85, 86, 87, 88, 89, 90, 91, 92, 93,  # kat 11: MTIC/karrusel (hele)
    )},
    # Stamdata- & datakvalitet (default fra)
    **{tid: "datakvalitet" for tid in (47, 48, 51, 54)},  # kat 6: partsvalidering
    # Dublet-recovery bredt (default fra) — 11/16/17 bliver i kernen (moms-dobbeltfradrag)
    **{tid: "dublet_recovery" for tid in (13, 15, 18)},
    # E-handel & særordninger (default fra, klientbetinget)
    **{tid: "ehandel_saerordninger" for tid in range(94, 104)},  # kat 12 (hele)
}

# Fuldt opslag for alle 103 kontroller.
CONTROL_MODULE = {tid: _OVERRIDES.get(tid, "moms_kerne") for tid in range(1, 104)}


# --- Opslag ----------------------------------------------------------------

def module_of(test_id: int) -> str:
    """Modulnøgle for en kontrol (moms_kerne hvis ukendt id)."""
    return CONTROL_MODULE.get(test_id, "moms_kerne")


def default_active_keys() -> list:
    """Modulnøgler der er tændt som standard (kun momskernen)."""
    return [k for k, m in MODULES.items() if m["default_active"]]


def all_module_keys() -> set:
    """Alle modulnøgler — bruges når ALT skal køre (fx valideringssuiten, der
    validerer hver kontrol uafhængigt af produktions-default)."""
    return set(MODULES)


def resolve_active_modules(override: Optional[Iterable[str]] = None) -> set:
    """Bestem det aktive modul-sæt ud fra override -> env -> default.

    - ``override`` (hvis givet): eksplicit iterabel af modulnøgler.
    - ellers ``ANALYTICS_MODULES`` env (komma-separeret; ``alle`` = alt).
    - ellers default-aktive moduler (momskernen).
    Ukendte nøgler ignoreres. Momskernen kan fravælges eksplicit, men er med i
    default.
    """
    known = set(MODULES)
    if override is not None:
        return {k for k in override if k in known}

    raw = os.environ.get("ANALYTICS_MODULES", "").strip()
    if raw:
        if raw.lower() in ("alle", "all", "*"):
            return set(known)
        wanted = {part.strip() for part in raw.split(",") if part.strip()}
        return {k for k in wanted if k in known}

    return set(default_active_keys())


def is_control_active(test_id: int, active_modules: set) -> bool:
    """Er kontrollen i et af de aktive moduler?"""
    return module_of(test_id) in active_modules


def module_summary(active_modules: set) -> list:
    """Struktureret oversigt til rapport/UI: hvert modul med aktiv-flag,
    antal kontroller og default-tilstand."""
    counts = {}
    for tid in range(1, 104):
        counts[module_of(tid)] = counts.get(module_of(tid), 0) + 1
    out = []
    for key, meta in MODULES.items():
        out.append({
            "noegle": key,
            "navn": meta["navn"],
            "beskrivelse": meta["beskrivelse"],
            "default_aktiv": meta["default_active"],
            "aktiv": key in active_modules,
            "antal_kontroller": counts.get(key, 0),
        })
    return out
