#!/usr/bin/env python3
"""
build_rules_catalog.py — udled det versionerede regelkatalog for VAT Analytics
DIREKTE fra koden (de 103 test_NN-funktioner), så kataloget aldrig kan drifte
fra de faktiske kontroller.

Statisk AST-analyse (kører ingen kode, intet netværk):
  * Finder hver funktion navngivet test_<id>_* i analytics/categories/cat*.py
  * Udtrækker test_id, test_name, impact_type og severity fra make_finding(...)
  * Markerer en kontrol som "inaktiv" hvis funktionen ikke kalder make_finding
    (dvs. den returnerer altid [] — typisk fordi den kræver kildedata, der ikke
    findes i en flad regnskabseksport, fx vareflow/forsendelsesland)

Felterne `kilde` (autoritativ retskilde) og `test` (dækkende valideringstest)
er bevidst tomme i v1 — de udfyldes i Fase B/C. Strukturen matcher
sporbarhedsmatricen: kontrol -> kilde -> modul -> test.

Kør:
    python -m tools.build_rules_catalog
    # skriver catalog/rules.json
"""

import ast
import json
import os
import re
import sys

CATALOG_VERSION = "1.0.0"

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
sys.path.insert(0, _BACKEND)  # så validation.scenarios kan importeres uanset cwd
_CATEGORIES_DIR = os.path.join(_BACKEND, "analytics", "categories")
_OUT = os.path.join(_BACKEND, "catalog", "rules.json")
_NOTES = os.path.join(_BACKEND, "catalog", "rule_notes.json")

# Kategori-definitioner (skal matche analytics/engine.py::CATEGORIES).
CATEGORIES = [
    (1, "Transaktionsintegritet & Datakvalitet", 1, 10),
    (2, "Dubletdetektion", 11, 18),
    (3, "Momssats-validering", 19, 26),
    (4, "Grænseoverskridende & EU-compliance", 27, 38),
    (5, "Timing & Periodetest", 39, 46),
    (6, "Leverandør- & Kundevalidering", 47, 54),
    (7, "Beløbs- & Tærskeltest", 55, 62),
    (8, "Statistisk Anomalidetektion", 63, 69),
    (9, "Reverse Charge & Selvangivelse", 70, 75),
    (10, "Indgående/Udgående Moms Afstemning", 76, 83),
    (11, "Svindeldetektion & Karrusel/MTIC", 84, 93),
    (12, "E-handel, Digitale Ydelser & Særordninger", 94, 103),
]

_TEST_FN_RE = re.compile(r"^test_(\d+)_")


def _category_for(test_id):
    for cid, name, lo, hi in CATEGORIES:
        if lo <= test_id <= hi:
            return cid, name
    return None, None


def _literal(node):
    """Returnér en streng-literal hvis noden er en konstant streng, ellers None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_make_findings(func_node):
    """Saml (test_name, impact_type, severity) fra alle make_finding-kald i funktionen."""
    names, impacts, severities = [], set(), set()
    has_dynamic_severity = False
    found = False
    for node in ast.walk(func_node):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == "make_finding":
            found = True
            kw = {k.arg: k.value for k in node.keywords if k.arg}
            tn = _literal(kw.get("test_name"))
            if tn and tn not in names:
                names.append(tn)
            it = _literal(kw.get("impact_type"))
            if it:
                impacts.add(it)
            sev = _literal(kw.get("severity"))
            if sev:
                severities.add(sev)
            elif "severity" in kw:
                has_dynamic_severity = True
    return found, names, impacts, severities, has_dynamic_severity


def _load_notes():
    """Indlæs håndvedligeholdte annotationer (kilde/test/afhaenger_af/scope_beslutning)."""
    if not os.path.exists(_NOTES):
        return {}
    with open(_NOTES, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("noter", {})


def _scenario_ids():
    """test_id'er der har et valideringsscenarie (ren/defekt) i valideringssuiten."""
    try:
        from validation.scenarios import SCENARIOS
        return {s["test_id"] for s in SCENARIOS}
    except Exception:  # noqa: BLE001 — generatoren må ikke vælte hvis suiten ikke kan importeres
        return set()


def build_catalog():
    """Byg katalog-dict'en i hukommelsen (skriver ikke til disk).
    Returnerer (catalog, problems-dict)."""
    rules = []
    notes = _load_notes()
    covered = _scenario_ids()
    _VALIDATION_TEST = "tests/test_validation_suite.py + validation/scenarios.py"
    for fname in sorted(os.listdir(_CATEGORIES_DIR)):
        if not fname.startswith("cat") or not fname.endswith(".py"):
            continue
        path = os.path.join(_CATEGORIES_DIR, fname)
        with open(path, encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=fname)
        module = f"analytics/categories/{fname}"
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            m = _TEST_FN_RE.match(node.name)
            if not m:
                continue
            tid = int(m.group(1))
            cid, cname = _category_for(tid)
            found, names, impacts, severities, dyn_sev = _extract_make_findings(node)
            if not found:
                status = "inaktiv_kraever_kildedata"
            else:
                status = "aktiv"
            sev_out = sorted(severities)
            if dyn_sev:
                sev_out = sev_out + ["dynamisk"]
            note = notes.get(str(tid), {})
            rules.append({
                "id": f"VATA-{tid:03d}",
                "test_id": tid,
                "navn": names[0] if names else node.name,
                "kategori_id": cid,
                "kategori": cname,
                "impact_type": sorted(impacts),
                "severity": sev_out,
                "status": status,
                "modul": module,
                "funktion": node.name,
                "kilde": note.get("kilde", ""),          # autoritativ retskilde — udfyldes i Fase B
                "test": note.get("test") or (_VALIDATION_TEST if tid in covered else ""),
                "afhaenger_af": note.get("afhaenger_af", ""),
                "scope_beslutning": note.get("scope_beslutning", ""),
            })

    rules.sort(key=lambda r: r["test_id"])

    # Konsistenstjek: noter må kun pege på reelle test_id'er.
    valid_ids = {r["test_id"] for r in rules}
    unknown_notes = sorted(int(k) for k in notes if k.isdigit() and int(k) not in valid_ids)

    # Konsistenstjek: præcis 103 kontroller, ingen huller i 1..103.
    ids = [r["test_id"] for r in rules]
    missing = [i for i in range(1, 104) if i not in ids]
    dupes = sorted({i for i in ids if ids.count(i) > 1})

    catalog = {
        "catalog_version": CATALOG_VERSION,
        "beskrivelse": "Regelkatalog for VAT Analytics — auto-genereret fra de 103 "
                       "test_NN-funktioner i analytics/categories/. Rediger ikke i hånden; "
                       "kør tools/build_rules_catalog.py.",
        "genereret_fra": "analytics/categories/cat*.py (statisk AST)",
        "antal_kontroller": len(rules),
        "kategorier": [{"id": c[0], "navn": c[1], "test_range": [c[2], c[3]]} for c in CATEGORIES],
        "regler": rules,
    }

    return catalog, {"missing": missing, "dupes": dupes, "unknown_notes": unknown_notes}


def serialize(catalog) -> str:
    """Kanonisk JSON-serialisering — præcis som filen skrives til disk."""
    return json.dumps(catalog, ensure_ascii=False, indent=2) + "\n"


def build():
    """Byg + skriv kataloget til disk. Returnerer exit-kode (0 = ok)."""
    catalog, problems = build_catalog()
    os.makedirs(os.path.dirname(_OUT), exist_ok=True)
    with open(_OUT, "w", encoding="utf-8") as f:
        f.write(serialize(catalog))

    rules = catalog["regler"]
    inaktive = [r["test_id"] for r in rules if r["status"] != "aktiv"]
    print(f"Skrev {_OUT}")
    print(f"  catalog_version: {catalog['catalog_version']}")
    print(f"  kontroller:      {len(rules)}")
    print(f"  inaktive:        {len(inaktive)} -> {inaktive}")
    if problems["missing"]:
        print(f"  ADVARSEL manglende test_id: {problems['missing']}")
    if problems["dupes"]:
        print(f"  ADVARSEL dublerede test_id: {problems['dupes']}")
    if problems["unknown_notes"]:
        print(f"  ADVARSEL rule_notes peger på ukendte test_id: {problems['unknown_notes']}")
    ok = (len(rules) == 103 and not problems["missing"]
          and not problems["dupes"] and not problems["unknown_notes"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(build())
