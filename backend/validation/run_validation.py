#!/usr/bin/env python3
"""
run_validation.py — kør den uafhængige valideringssuite og skriv rapporten.

For hvert scenarie køres analysemotoren mod (1) en ren baseline, hvor kontrollen
IKKE må fyre, og (2) en defekt variant, hvor kontrollen SKAL fyre. Et scenarie
består kun hvis begge holder.

    python -m validation.run_validation        # rapport til stdout + docs/, exit≠0 ved fejl

Gated i CI sammen med pytest.
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics.engine import run_all_tests
from analytics import modules
from validation.scenarios import SCENARIOS

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DOCS = os.path.join(_REPO, "docs")


def _fired_ids(data):
    # Valideringssuiten validerer HVER kontrol uafhængigt af produktions-default,
    # så alle analyse-moduler tændes her (ellers ville fund fra default-fra
    # moduler blive filtreret bort og få scenarier til at fejle fejlagtigt).
    report = run_all_tests(data, active_modules=modules.all_module_keys())
    return {f["test_id"] for f in report["all_findings"]}


def evaluate():
    results = []
    for s in SCENARIOS:
        tid = s["test_id"]
        clean_fired = tid in _fired_ids(s["clean"])
        defect_fired = tid in _fired_ids(s["defect"])
        results.append({
            "test_id": tid,
            "navn": s["navn"],
            "fanger_defekt": defect_fired,
            "ren_er_tavs": not clean_fired,
            "ok": defect_fired and not clean_fired,
        })
    return results


def write_report(results):
    passed = sum(1 for r in results if r["ok"])
    total = len(results)
    L = []
    L.append("# VAT Analytics — Valideringsrapport")
    L.append("")
    L.append(f"> Auto-genereret af `python -m validation.run_validation` · {date.today().isoformat()}")
    L.append("")
    status = "✅ **BESTÅET**" if passed == total else f"❌ **{total - passed} FEJL**"
    L.append(f"{status} — {passed}/{total} scenarier bestået "
             f"(plantet defekt fanges + ren baseline er tavs).")
    L.append("")
    L.append("| test_id | Kontrol | Fanger defekt | Ren er tavs | Resultat |")
    L.append("|---------|---------|---------------|-------------|----------|")
    for r in sorted(results, key=lambda x: x["test_id"]):
        L.append(f"| {r['test_id']} | {r['navn']} | "
                 f"{'ja' if r['fanger_defekt'] else 'NEJ'} | "
                 f"{'ja' if r['ren_er_tavs'] else 'NEJ'} | "
                 f"{'✅' if r['ok'] else '❌'} |")
    L.append("")
    L.append(f"Dækning: **{total}** scenarier (repræsentativt startsæt; udvides til alle "
             f"98 aktive kontroller).")
    L.append("")
    os.makedirs(_DOCS, exist_ok=True)
    out = os.path.join(_DOCS, "VAT-Analytics_Valideringsrapport.md")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L) + "\n")
    return out, passed, total


def main():
    results = evaluate()
    out, passed, total = write_report(results)
    print(f"Valideringssuite: {passed}/{total} scenarier bestået")
    for r in sorted(results, key=lambda x: x["test_id"]):
        if not r["ok"]:
            print(f"  FEJL test_id={r['test_id']} ({r['navn']}): "
                  f"fanger_defekt={r['fanger_defekt']} ren_er_tavs={r['ren_er_tavs']}")
    print("Skrev", os.path.relpath(out, _REPO))
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
