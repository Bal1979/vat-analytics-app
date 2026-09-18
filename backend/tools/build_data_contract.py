#!/usr/bin/env python3
"""
build_data_contract.py — udled catalog/data_contract.json fra den håndholdte
single source ``tools/data_contract_data.py``, plus (for run_config) direkte
introspektion af ``analytics/modules.py`` — samme mønster som
``build_rules_catalog.py`` bruger for ``analytics/modules`` i regelkataloget.

Kør:
    python -m tools.build_data_contract
    # skriver catalog/data_contract.json

Dette er en DESKRIPTIV kontrakt (jf. BALAI-dataflow-arkitektur.md §7, byggetrin
2) — ingen runtime-validering/pydantic indføres her. Kataloget er drift-gated
af tests/test_data_contract_fresh.py (committet == genereret), akkurat som
catalog/rules.json.
"""

import json
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
sys.path.insert(0, _BACKEND)  # så analytics.* og tools.* kan importeres uanset cwd

from tools import data_contract_data as dcd  # noqa: E402
from analytics import modules as _modules  # noqa: E402
from analytics import materiality as _materiality  # noqa: E402

_OUT = os.path.join(_BACKEND, "catalog", "data_contract.json")
_MATERIALITY_SRC = os.path.join(_BACKEND, "analytics", "materiality.py")


def _count_fields(objects: dict) -> int:
    n = 0
    for obj in objects.values():
        n += len(obj["felter"])
        if "sub_objekt" in obj:
            n += len(obj["sub_objekt"]["felter"])
    return n


def _analytics_modules_run_config() -> dict:
    """Modul-registeret hentet direkte fra analytics/modules.py — aldrig
    hånd-duplikeret, så det ikke kan drifte fra koden (samme princip som
    build_rules_catalog.py's brug af analytics.modules)."""
    moduler = [
        {
            "noegle": key,
            "navn": meta["navn"],
            "beskrivelse": meta["beskrivelse"],
            "default_aktiv": meta["default_active"],
            "antal_kontroller": sum(
                1 for tid in range(1, 109) if _modules.module_of(tid) == key
            ),
        }
        for key, meta in _modules.MODULES.items()
    ]
    return {
        "env": "ANALYTICS_MODULES",
        "beskrivelse": "Komma-separeret liste af aktive modulnøgler. "
                       "'alle'/'all'/'*' tænder alt. Prioritet: eksplicit "
                       "override pr. kørsel -> ANALYTICS_MODULES env -> "
                       "default_aktiv-moduler (kun momskerne).",
        "kilde": "analytics/modules.py (MODULES, CONTROL_MODULE)",
        "moduler": moduler,
    }


def _check_materiality_env_names(problems: list) -> None:
    """Drift-fanger: hvert env-navn i data_contract_data.MATERIALITY_RUN_CONFIG
    skal rent faktisk forekomme i analytics/materiality.py's kildekode."""
    with open(_MATERIALITY_SRC, encoding="utf-8") as f:
        src = f.read()
    for entry in dcd.MATERIALITY_RUN_CONFIG:
        env = entry["env"]
        if not re.search(r'["\']' + re.escape(env) + r'["\']', src):
            problems.append(env)


def build_contract():
    """Byg kontrakt-dict'en i hukommelsen. Returnerer (contract, problems)."""
    problems = {"materiality_env_mismatch": []}
    _check_materiality_env_names(problems["materiality_env_mismatch"])

    objects_out = {}
    for name, obj in dcd.OBJECTS.items():
        entry = {"beskrivelse": obj["beskrivelse"], "felter": obj["felter"]}
        if "sub_objekt" in obj:
            entry["sub_objekt"] = obj["sub_objekt"]
        objects_out[name] = entry

    contract = {
        "contract_version": dcd.CONTRACT_VERSION,
        "beskrivelse": "Maskinlæsbar datakontrakt for VAT Analytics-motorens "
                       "input — den kanoniske struktur, adapt_excel_to_saft() "
                       "og saft_parser.parse_saft() BEGGE producerer. "
                       "DESKRIPTIV, ikke håndhævende (ingen runtime-validering "
                       "indført af dette katalog). Rediger ikke i hånden; kør "
                       "tools/build_data_contract.py.",
        "genereret_fra": "tools/data_contract_data.py (håndholdt single source) "
                         "+ analytics/modules.py (introspektion af analyse-moduler)",
        "ankerdokument": "balai-platform/BALAI-dataflow-arkitektur.md §2a + §7",
        "antal_objekter": len(objects_out),
        "antal_felter": _count_fields(objects_out),
        "objekter": objects_out,
        "balai_extensions": {
            "beskrivelse": "Felter der IKKE er native SAF-T Financial-elementer, "
                           "men som motoren kræver — jf. §2a-tabellen. Hvert felt "
                           "her optræder også i sit naturlige objekt ovenfor "
                           "(ekstension=true), undtagen version-triplens to "
                           "endnu-ikke-implementerede felter.",
            "felter": dcd.BALAI_EXTENSIONS,
        },
        "run_config": {
            "beskrivelse": "Inputkontrakten er datashape + konfiguration: hvilke "
                           "analyse-moduler der kører, og hvilke materialitets-/"
                           "tærskelvalg der er i spil for kørslen.",
            "analytics_modules": _analytics_modules_run_config(),
            "materiality": {
                "beskrivelse": "Engagement-kalibrerbare tærskler/vægte. "
                               "Kilde: analytics/materiality.py.",
                "knapper": dcd.MATERIALITY_RUN_CONFIG,
            },
        },
        "known_gaps": dcd.KNOWN_GAPS,
        "parse_info_note": dcd.PARSE_INFO_NOTE,
    }
    return contract, problems


def serialize(contract) -> str:
    """Kanonisk JSON-serialisering — præcis som filen skrives til disk."""
    return json.dumps(contract, ensure_ascii=False, indent=2) + "\n"


def build():
    """Byg + skriv kontrakten til disk. Returnerer exit-kode (0 = ok)."""
    contract, problems = build_contract()
    os.makedirs(os.path.dirname(_OUT), exist_ok=True)
    with open(_OUT, "w", encoding="utf-8") as f:
        f.write(serialize(contract))

    print(f"Skrev {_OUT}")
    print(f"  contract_version: {contract['contract_version']}")
    print(f"  objekter:         {contract['antal_objekter']}")
    print(f"  felter:           {contract['antal_felter']}")
    print(f"  balai_extensions: {len(contract['balai_extensions']['felter'])}")
    print(f"  known_gaps:       {len(contract['known_gaps'])}")
    if problems["materiality_env_mismatch"]:
        print(f"  ADVARSEL materiality-env ikke fundet i materiality.py: "
              f"{problems['materiality_env_mismatch']}")
    ok = not problems["materiality_env_mismatch"]
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(build())
