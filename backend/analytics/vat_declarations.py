"""
vat_declarations.py — indlæsning af den indberettede momsangivelse (ekstern
til enhver bogføringseksport), til kontrol 82's periode-/rubrikafstemning
(byggetrin 8, Del A, BALAI-dataflow-arkitektur.md §7, Bal-godkendt
2026-09-17).

Snitfladen er AFTALT med vat-extract (som bygger udtrækkeren parallelt) og
MÅ IKKE afviges:

    {
      "declarations_version": "1.0.0",
      "source": "...",
      "generated": "...",
      "periods": [
        {"period": "2025-01", "output_vat": 0, "input_vat": 0,
         "rc_services": 0, "rc_goods": 0, "energy_taxes": 0, "total": 0}
      ]
    }

``period`` er "YYYY-MM" — samme format som cat10_vat_reconciliation's
periode-nøgle (afledt af transactions[].period_year/period).

Samme fejlfilosofi som ``analytics/reconciliation_gate.py``:
``load_declarations()`` fejler ALDRIG med en exception — en teknisk fejl
(manglende/ugyldig/uparsebar fil) må aldrig fremstå som et fagligt afslag.
Kontrol 82 springer bare over (uændret adfærd), hvis intet/ugyldigt er
angivet — se ``analytics/categories/cat10_vat_reconciliation.py``.

``rc_goods``/``energy_taxes`` accepteres og valideres i formatet, men
kontrol 82's rubrik-logik (v1, Bal-godkendt) sammenligner kun de tre
rubrikker, den selv kan udlede deterministisk af transaktionerne:
``output_vat``, ``rc_services``, ``input_vat``. Se cat10-modulets docstring
for baggrund — en fremtidig udvidelse til rc_goods/energy_taxes kræver en
tilsvarende udledningsregel fra transaktionerne, som ikke er specificeret i
denne opgave.
"""

from __future__ import annotations

import json

DECLARATIONS_VERSION_SUPPORTED = "1.0.0"

# Alle felter en periode-post SKAL have, jf. den aftalte snitflade. Kun
# 'period' + de tre rubrikker kontrol 82 rent faktisk bruger er obligatoriske
# for AT AFSTEMME MOD — men snitfladen er aftalt fast, så vi validerer hele
# formen for tidligt (og tydeligt) at fange en afvigende leverance.
_REQUIRED_PERIOD_FIELDS = {
    "period", "output_vat", "input_vat", "rc_services", "rc_goods",
    "energy_taxes", "total",
}


def load_declarations(path: str) -> tuple:
    """Læs + valider en vat_declarations.json. Returnerer (dict|None,
    error|None). Fejler ALDRIG med exception."""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, UnicodeDecodeError) as e:
        return None, f"Angivelsesfilen kunne ikke læses: {e}"
    except json.JSONDecodeError as e:
        return None, f"Angivelsesfilen er ikke gyldig JSON: {e}"

    if not isinstance(doc, dict):
        return None, "Angivelsesfilen skal være et JSON-objekt."
    if doc.get("declarations_version") != DECLARATIONS_VERSION_SUPPORTED:
        return None, (
            "Ukendt/manglende declarations_version (forventede "
            f"{DECLARATIONS_VERSION_SUPPORTED!r}, fik {doc.get('declarations_version')!r})."
        )

    periods = doc.get("periods")
    if not isinstance(periods, list):
        return None, "Angivelsesfilen mangler en 'periods'-liste."
    for entry in periods:
        if not isinstance(entry, dict):
            return None, "Hver post i 'periods' skal være et JSON-objekt."
        missing = sorted(_REQUIRED_PERIOD_FIELDS - set(entry))
        if missing:
            return None, f"Post i 'periods' mangler felter: {missing}."
        if not isinstance(entry.get("period"), str) or not entry["period"]:
            return None, "Hver post i 'periods' skal have et ikke-tomt 'period' (\"YYYY-MM\")."

    return doc, None
