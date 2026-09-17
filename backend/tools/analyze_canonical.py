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
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_BACKEND = os.path.dirname(_HERE)
sys.path.insert(0, _BACKEND)  # så analytics.*/parsers.*/tools.* kan importeres uanset cwd

from analytics.engine import run_all_tests  # noqa: E402
from analytics import modules as _modules  # noqa: E402
from analytics import reconciliation_gate  # noqa: E402
from analytics import vat_declarations  # noqa: E402
from parsers import canonical_parser  # noqa: E402
from tools import data_contract_data as _dcd  # noqa: E402

_RULES_CATALOG_PATH = os.path.join(_BACKEND, "catalog", "rules.json")


def _catalog_version() -> str:
    try:
        with open(_RULES_CATALOG_PATH, encoding="utf-8") as f:
            return json.load(f).get("catalog_version", "")
    except (OSError, json.JSONDecodeError):
        return ""


def _default_declarations_path(csv_path: str) -> str:
    """Konvention: vat_declarations.json liggende ved siden af CSV'en (samme
    mønster som transform_summary.json/de tre stamdata-sidecar-filer)."""
    return os.path.join(os.path.dirname(os.path.abspath(csv_path)), "vat_declarations.json")


def _load_declarations(csv_path: str, declarations_path: str | None) -> tuple:
    """Byggetrin 8, Del A/D: den indberettede momsangivelse (valgfri).
    Returnerer (declarations_dict|None, warning|None) — fejler ALDRIG,
    samme filosofi som resten af den kanoniske vej (teknisk fejl != fagligt
    afslag). Ikke angivet eksplicit -> forsøges ved siden af csv_path;
    findes den ikke, køres analysen som hidtil (kontrol 82 springer over)."""
    path = declarations_path or _default_declarations_path(csv_path)
    if not os.path.exists(path):
        return None, None
    declarations, error = vat_declarations.load_declarations(path)
    if error:
        return None, f"Angivelsesfil fundet ({path}) men ikke brugt: {error}"
    return declarations, None


def _resolve_modules(spec: str | None):
    if not spec or spec == "default":
        return None  # lad run_all_tests bruge normal prioritet (override->env->default)
    if spec in ("alle", "all", "*"):
        return _modules.all_module_keys()
    return {m.strip() for m in spec.split(",") if m.strip()}


def build_report(csv_path: str, summary_path: str | None, reconciliation_path: str | None,
                  tolerance: float, modules_spec: str | None,
                  declarations_path: str | None = None) -> dict:
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
    declarations, decl_warning = _load_declarations(csv_path, declarations_path)

    t_parse_done = time.monotonic()
    analytics_report = run_all_tests(canonical, active_modules=active_modules, declarations=declarations)
    t_done = time.monotonic()

    lineage = parse_info.get("lineage", {})
    parse_warnings = list(parse_info.get("warnings", []))
    if decl_warning:
        parse_warnings.append(decl_warning)
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
            # Byggetrin ~9, Del C (Bal-godkendt 2026-09-17): kørselstidspunkt
            # til rapportens lineage-footer (tools/generate_report.py) — ISO
            # 8601 UTC, sat NÅR analysen faktisk kører (ikke når HTML'en
            # senere genereres fra rapport-JSON'en).
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "afstemning": gate_result,
        "angivelse": {
            "brugt": declarations is not None,
            "antal_perioder": len(declarations["periods"]) if declarations else 0,
        },
        "parse_info": {
            "warnings": parse_warnings,
            "sections": parse_info.get("sections", {}),
            "stamdata": parse_info.get("stamdata", {}),
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
    angivelse = report.get("angivelse", {})
    print(f"Momsangivelse (kontrol 82): "
          f"{'brugt, ' + str(angivelse.get('antal_perioder', 0)) + ' perioder' if angivelse.get('brugt') else 'ikke angivet (kontrol 82 springer over)'}")
    stamdata = report.get("parse_info", {}).get("stamdata", {})
    if stamdata:
        print(f"Stamdata: vat_setup={stamdata.get('vat_setup_koder', 0)} koder, "
              f"chart_of_accounts={stamdata.get('chart_of_accounts_konti', 0)} konti, "
              f"customers={stamdata.get('customers', 0)}")
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
    parser.add_argument("--declarations", default=None,
                         help="Sti til vat_declarations.json (valgfri — udelades: forsøges ved siden "
                              "af csv_path; findes den ikke der heller, springer kontrol 82 over).")
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
        declarations_path=args.declarations,
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    _print_summary(report)
    print(f"\nFuld rapport skrevet til: {args.out}")
    return 1 if report.get("fejl") else 0


if __name__ == "__main__":
    raise SystemExit(main())
