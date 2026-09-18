#!/usr/bin/env python3
"""Semantik-PoC byggetrin 4: scoring mod facit (arbejdspapirets §13.4-stil).

Sammenligner run_poc.py's predictions.json mod facit.json pr. linje og
rapporterer: recall pr. fund-id, præcision (falske positiver rapporteres
separat — ikke automatisk dømt som forkerte, kan være reelle nye fund),
OK-nøjagtighed, hallucinerede fund-id'er, skema-validitet og tid pr. batch.

Fund der kræver tværlinje-kontekst (jf. katalogets
kraever_tvaerlinje_kontekst-flag, sat af build_katalog.py) kan en
pr.-linje-klassifikation ikke fange fair og holdes UDENFOR recall/præcision
— de rapporteres separat som "uden for pr.-linje-scope".

Scorer kun linjer, der faktisk har en prediction (dvs. virker både på en
røgtest med én batch og en fuld kørsel).

Brug:
  python3 score_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \\
      --run-dir <scratch>/run_qwen27b --out <scratch>/run_qwen27b/scoring.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path


def load(run_dir: Path, name: str) -> dict:
    p = run_dir / name
    return json.loads(p.read_text()) if p.exists() else {}


def score(facit_lines: list[dict], predictions: dict[str, dict], excluded_funds: set[str],
          valid_fund_ids: set[str]) -> dict:
    facit_by_id = {l["line_id"]: l for l in facit_lines}
    scored_ids = [lid for lid in predictions if lid in facit_by_id]

    per_fund_recall: dict[str, dict] = defaultdict(lambda: {"facit_antal": 0, "fundet": 0})
    ok_total = ok_correct = 0
    correct_total = wrong_total = 0
    false_positives = []       # facit=OK, prediction=fund (muligt nyt fund eller falsk alarm)
    wrong_fund = []            # facit=fund X, prediction=fund Y != X
    missed = []                # facit=fund X, prediction=OK
    hallucinated = []          # prediction er hverken "OK" eller et kendt fund-id
    out_of_scope_lines = []    # facit-fund kræver tværlinje-kontekst — ikke scoret

    for lid in scored_ids:
        facit_line = facit_by_id[lid]
        pred = predictions[lid]
        facit_fund = facit_line["fund_id"]
        pred_fund = pred.get("fund_id")

        if pred_fund != "OK" and pred_fund not in valid_fund_ids:
            hallucinated.append({"line_id": lid, "predicted": pred_fund})

        if facit_fund in excluded_funds:
            out_of_scope_lines.append({
                "line_id": lid, "facit_fund": facit_fund, "predicted": pred_fund,
            })
            continue

        if facit_fund == "OK":
            ok_total += 1
            if pred_fund == "OK":
                ok_correct += 1
                correct_total += 1
            else:
                wrong_total += 1
                false_positives.append({
                    "line_id": lid, "predicted": pred_fund,
                    "begrundelse": pred.get("begrundelse_kort"),
                    "konto": facit_line.get("account"), "leverandoer": facit_line.get("supplier"),
                    "tekst": facit_line.get("text"),
                })
        else:
            per_fund_recall[facit_fund]["facit_antal"] += 1
            if pred_fund == facit_fund:
                per_fund_recall[facit_fund]["fundet"] += 1
                correct_total += 1
            else:
                wrong_total += 1
                if pred_fund == "OK":
                    missed.append({"line_id": lid, "facit_fund": facit_fund})
                else:
                    wrong_fund.append({"line_id": lid, "facit_fund": facit_fund, "predicted": pred_fund})

    for fid, d in per_fund_recall.items():
        d["recall"] = round(d["fundet"] / d["facit_antal"], 3) if d["facit_antal"] else None

    return {
        "antal_scorede_linjer": len(scored_ids),
        "ok_noejagtighed": {
            "total": ok_total, "korrekt": ok_correct,
            "andel": round(ok_correct / ok_total, 3) if ok_total else None,
        },
        "andel_korrekte_total": round(correct_total / max(correct_total + wrong_total, 1), 3),
        "recall_pr_fund_id": dict(sorted(per_fund_recall.items())),
        "falske_positiver_ok_til_fund": false_positives,
        "forkert_fund_id": wrong_fund,
        "misset_fund_som_ok": missed,
        "hallucinerede_fund_id": hallucinated,
        "uden_for_pr_linje_scope": {
            "fund_ids": sorted(excluded_funds),
            "antal_linjer": len(out_of_scope_lines),
            "detaljer": out_of_scope_lines,
        },
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--facit", required=True, type=Path)
    ap.add_argument("--katalog", required=True, type=Path)
    ap.add_argument("--run-dir", required=True, type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    facit = json.loads(args.facit.read_text())
    katalog = json.loads(args.katalog.read_text())
    predictions = load(args.run_dir, "predictions.json")
    batches_meta = load(args.run_dir, "batches_meta.json")

    excluded_funds = {f["id"] for f in katalog["funds"] if f.get("kraever_tvaerlinje_kontekst")}
    valid_fund_ids = {f["id"] for f in katalog["funds"]}

    result = score(facit["lines"], predictions, excluded_funds, valid_fund_ids)
    result["batches"] = batches_meta

    out_path = args.out or (args.run_dir / "scoring.json")
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

    print(f"Scoret {result['antal_scorede_linjer']} linjer")
    print(f"OK-nøjagtighed: {result['ok_noejagtighed']}")
    print(f"Samlet andel korrekte: {result['andel_korrekte_total']*100:.1f}%")
    print("Recall pr. fund-id:")
    for fid, d in result["recall_pr_fund_id"].items():
        print(f"  {fid}: {d['fundet']}/{d['facit_antal']} ({d['recall']})")
    print(f"Falske positiver (OK->fund): {len(result['falske_positiver_ok_til_fund'])}")
    print(f"Forkert fund-id: {len(result['forkert_fund_id'])}")
    print(f"Misset fund som OK: {len(result['misset_fund_som_ok'])}")
    print(f"Hallucinerede fund-id'er: {len(result['hallucinerede_fund_id'])}")
    print(f"Uden for pr.-linje-scope ({sorted(excluded_funds)}): "
          f"{result['uden_for_pr_linje_scope']['antal_linjer']} linjer ikke scoret")
    if batches_meta.get("batches"):
        valid = sum(1 for b in batches_meta["batches"] if b.get("schema_valid"))
        total_s = sum(b.get("sekunder", 0) for b in batches_meta["batches"])
        print(f"Batches: {valid}/{len(batches_meta['batches'])} skema-valide, {total_s:.0f}s total")
    print(f"\nScoring skrevet: {out_path}")


if __name__ == "__main__":
    main()
