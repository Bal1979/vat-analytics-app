#!/usr/bin/env python3
"""
analyze_canonical.py — offline analysekørsel på den kanoniske gl_entries-CSV
(byggetrin 8, Del 3, BALAI-dataflow-arkitektur.md §7, Bal-godkendt 2026-09-17).

Kører HELE motoren (``analytics.engine.run_all_tests``, alle analyse-moduler
tændt som standard — jf. "hele motoren", ikke kun produktions-default-
momskernen) på output fra ``parsers.canonical_parser.parse_canonical``, uden
webserver/FastAPI. Stempler rapporten med reproducerbarheds-metadata
(catalog_version, data_contract-version, mapping_version, schema_fingerprint)
og kører den valgfrie afstemningsgate (``analytics.reconciliation_gate``) FØR
analysen, hvis en kontroltotal-fil er angivet.

Kør (fra backend/):
    python tools/analyze_canonical.py <gl_entries.csv> \\
        [--summary <transform_summary.json>] \\
        [--reconciliation <reconciliation.json>] [--tolerance 0.01] \\
        [--modules alle|momskerne|<komma-liste>] \\
        --out <rapport.json>

    # eller, som modul:
    python -m tools.analyze_canonical <gl_entries.csv> --out <rapport.json>

INGEN kundedata skrives til stdout eller til logfiler her — kun nøgletal
(antal, summer, køretider). Rapport-JSON'en (som KAN indeholde kundedata i
``all_findings``) skal gemmes uden for repoet (fx scratchpad), aldrig
committes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
sys.path.insert(0, _BACKEND)  # så analytics.*/parsers.*/tools.* kan importeres uanset cwd

from analytics.engine import run_all_tests  # noqa: E402
from analytics import modules as _modules  # noqa: E402
from analytics import reconciliation_gate  # noqa: E402
from parsers import canonical_parser  # noqa: E402
from tools import data_contract_data as _dcd  # noqa: E402

_RULES_CATALOG_PATH = os.path.join(_BACKEND, "catalog", "rules.json")


def _catalog_version() -> str:
    try:
        with open(_RULES_CATALOG_PATH, encoding="utf-8") as f:
            return json.load(f).get("catalog_version", "")
    except (OSError, json.JSONDecodeError):
        return ""


def _resolve_modules(spec: str | None):
    if not spec or spec == "default":
        return None  # lad run_all_tests bruge normal prioritet (override->env->default)
    if spec in ("alle", "all", "*"):
        return _modules.all_module_keys()
    return {m.strip() for m in spec.split(",") if m.strip()}


def build_report(csv_path: str, summary_path: str | None, reconciliation_path: str | None,
                  tolerance: float, modules_spec: str | None) -> dict:
    """Byg den fulde, lineage-stemplede analyserapport. Kaster ikke — tekniske
    fejl (parse-fejl, manglende fil) rapporteres i ``rapport["fejl"]``."""
    t0 = time.monotonic()
    canonical, parse_info = canonical_parser.parse_canonical(csv_path, summary_path=summary_path)
    if canonical is None:
        return {
            "kanonisk_vej": True,
            "fejl": parse_info.get("errors", ["Ukendt parse-fejl."]),
        }

    active_modules = _resolve_modules(modules_spec)
    gate_result = reconciliation_gate.reconcile(canonical, reconciliation_path, tolerance=tolerance)

    t_parse_done = time.monotonic()
    analytics_report = run_all_tests(canonical, active_modules=active_modules)
    t_done = time.monotonic()

    lineage = parse_info.get("lineage", {})
    report = {
        "kanonisk_vej": True,
        "lineage": {
            "kilde": "canonical",
            "catalog_version": _catalog_version(),
            "data_contract_version": _dcd.CONTRACT_VERSION,
            "mapping_version": lineage.get("mapping_version", ""),
            "schema_fingerprint": lineage.get("schema_fingerprint", ""),
            "source_erp": lineage.get("source_erp", ""),
            "profile_version": lineage.get("profile_version", ""),
        },
        "afstemning": gate_result,
        "parse_info": {
            "warnings": parse_info.get("warnings", []),
            "sections": parse_info.get("sections", {}),
        },
        "analytics": analytics_report,
        "koeretid_sekunder": {
            "parsing": round(t_parse_done - t0, 3),
            "analyse": round(t_done - t_parse_done, 3),
            "total": round(t_done - t0, 3),
        },
    }
    return report


def _print_summary(report: dict) -> None:
    """Nøgletal UDEN kundedata til stdout (antal, ikke værdier/navne)."""
    if report.get("fejl"):
        print("FEJL:", "; ".join(report["fejl"]))
        return
    lineage = report["lineage"]
    print(f"catalog_version:        {lineage['catalog_version']}")
    print(f"data_contract_version:  {lineage['data_contract_version']}")
    print(f"mapping_version:        {lineage['mapping_version'] or '(ukendt)'}")
    print(f"schema_fingerprint:     {lineage['schema_fingerprint'] or '(ukendt)'}")
    print()
    print(f"Afstemningsgate:        {report['afstemning']['gate_status']} — {report['afstemning']['message']}")
    print()
    analytics = report["analytics"]
    print(f"Transaktioner analyseret: {analytics['summary'].get('total_transactions', '?')}")
    print(f"Fund i alt:               {analytics['total_findings']}")
    sev = analytics["severity_summary"]
    print(f"  kritisk={sev['critical']} høj={sev['high']} medium={sev['medium']} lav={sev['low']}")
    n_active = sum(1 for m in analytics["moduler"] if m.get("aktiv"))
    print(f"Analyse-moduler aktive:   {n_active}/{len(analytics['moduler'])}")
    print(f"Fund filtreret fra (inaktive moduler): {analytics['filtrerede_fund']}")
    print()
    kt = report["koeretid_sekunder"]
    print(f"Køretid: parsing={kt['parsing']}s analyse={kt['analyse']}s total={kt['total']}s")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_path", help="Sti til den kanoniske gl_entries-CSV.")
    parser.add_argument("--summary", default=None,
                         help="Sti til transform_summary.json (default: samme mappe som csv_path).")
    parser.add_argument("--reconciliation", default=None,
                         help="Sti til kontroltotal-fil (valgfri — udelades = 'afstemning ikke udført').")
    parser.add_argument("--tolerance", type=float, default=reconciliation_gate.DEFAULT_TOLERANCE,
                         help=f"Afstemningstolerance i DKK (default {reconciliation_gate.DEFAULT_TOLERANCE}).")
    parser.add_argument("--modules", default=None,
                         help="'alle' (hele motoren, default for denne CLI), 'default' (kun momskernen, "
                              "produktionsadfærd), eller en komma-liste af modulnøgler.")
    parser.add_argument("--out", required=True, help="Sti til output-rapport (JSON). "
                                                        "Gem UDEN FOR repoet (kan indeholde kundedata).")
    args = parser.parse_args(argv)

    # Default for denne CLI er "hele motoren" (jf. opgaven), modsat webappens
    # produktions-default (kun momskernen) — men kan overstyres med --modules default.
    modules_spec = args.modules or "alle"

    report = build_report(
        args.csv_path, args.summary, args.reconciliation, args.tolerance, modules_spec,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    _print_summary(report)
    print(f"\nFuld rapport skrevet til: {args.out}")
    return 1 if report.get("fejl") else 0


if __name__ == "__main__":
    raise SystemExit(main())
