#!/usr/bin/env python3
"""Semantik-PoC byggetrin 3: LLM-klassifikation af posteringslinjer.

Vælger en stratificeret population af facit-linjer, deduplikerer dem til
unikke grupper (dedup.py), og sender grupperne i batches til en LOKAL
Ollama-model sammen med fundkataloget. Modellen returnerer pr. gruppe
{fund_id | "OK", begrundelse_kort}, som spredes til alle gruppens
medlemslinjer.

Ingen kundedata forlader maskinen (lokal Ollama). Rå svar og
mellemresultater skrives til --out-dir, som skal pege UDENFOR repoet.

Brug (fuld kørsel):
  python3 run_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \\
      --out-dir <scratch>/run_qwen27b --model qwen3.8:27b

Brug (røgtest — kun første batch):
  python3 run_poc.py --facit ... --katalog ... --out-dir <scratch>/smoke_14b \\
      --model qwen3:14b --max-batches 1
"""
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from pathlib import Path

from dedup import group_lines, reduction_stats

OLLAMA_URL = "http://localhost:11434/api/chat"

SYSTEM_RULES = """Du er momsfaglig reviewer. Du får et FUNDKATALOG (fund-id, navn, regel,
identifikationsmetode) og en batch af POSTERINGSGRUPPER (hver gruppe repræsenterer én
eller flere ensartede bogføringslinjer). Klassificér hver gruppe.

Regler:
1. "OK" er default og et LEGITIMT svar — de fleste linjer er korrekte. Klassificér KUN
   med et fund-id ved klar evidens i konto, leverandørnavn, posteringstekst eller momskode.
2. Opfind ALDRIG fund-id'er, der ikke står i kataloget. Er du i tvivl mellem to fund,
   vælg det mest specifikke og nævn tvivlen kort i begrundelsen.
3. Returner KUN et JSON-objekt: {"classifications": [ ... ]} — intet andet.
4. Ét element pr. gruppe: {"group_id": str, "fund_id": str eller "OK", "begrundelse_kort": str}
   — begrundelse_kort må højst være én kort sætning, på dansk.
5. Brug KUN de oplyste felter for gruppen. Gæt aldrig på information, der ikke er givet."""


def build_katalog_text(katalog: dict) -> str:
    lines = []
    for f in katalog["funds"]:
        if f["status"] != "Fund":
            continue  # "Ingen fund"-kontroller (F24/F25) er populationstjek, ikke linje-mønstre
        note = " [KUN til info — kræver tværlinje-kontekst, brug sparsomt]" if f["kraever_tvaerlinje_kontekst"] else ""
        metode = f" Identifikation: {f['identifikationsmetode']}" if f.get("identifikationsmetode") else ""
        lines.append(f"- {f['id']} ({f['navn']}){note}: {f['regel_kort']}.{metode}")
    return "\n".join(lines)


def build_group_text(group: dict) -> str:
    rep = group["representative"]
    n = group["member_count"]
    extra = f" [gruppe af {n} lignende linjer]" if n > 1 else ""
    return (
        f"- group_id: {group['group_id']}{extra}\n"
        f"  konto: {rep.get('account')} ({rep.get('account_name')})\n"
        f"  leverandør: {rep.get('supplier')}\n"
        f"  posteringstekst: {rep.get('text')}\n"
        f"  momskode: {rep.get('vat_code')}\n"
        f"  effektiv momssats: {rep.get('effective_vat_rate')}\n"
        f"  valuta: {rep.get('currency') or 'DKK'}\n"
        f"  grundlag (repræsentativ linje): {rep.get('amount_excl_vat')}\n"
        f"  momsbeløb (repræsentativ linje): {rep.get('vat_amount')}\n"
        f"  moms fratrukket: {'ja' if rep.get('moms_fratrukket') else 'nej'}"
    )


def build_prompt(katalog: dict, batch: list[dict]) -> str:
    katalog_txt = build_katalog_text(katalog)
    grupper_txt = "\n".join(build_group_text(g) for g in batch)
    return f"""{SYSTEM_RULES}

FUNDKATALOG:
{katalog_txt}

POSTERINGSGRUPPER:
{grupper_txt}"""


def call_ollama(model: str, prompt: str, num_ctx: int, timeout: int = 1800) -> tuple[str, float]:
    body = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "format": "json",
        "options": {"temperature": 0, "num_ctx": num_ctx},
    }
    # Undertrykker "thinking"-output for modeller der understøtter det (fx qwen3-familien) —
    # hurtigere og undgår at tænke-tekst forurener det tvungne JSON-svar.
    body["think"] = False
    req = urllib.request.Request(
        OLLAMA_URL, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    return resp["message"]["content"], time.time() - t0


def select_population(facit: dict, sample_ok: int, seed: int) -> tuple[list[dict], dict]:
    """Stratificeret PoC-population: ALLE fund-linjer + tilfældig stikprøve OK-linjer.

    Fuld population (alle 3.010 linjer) er en senere kørsel — se README.
    """
    lines = facit["lines"]
    fund_lines = [l for l in lines if l["fund_id"] != "OK"]
    ok_lines = [l for l in lines if l["fund_id"] == "OK"]
    rng = random.Random(seed)
    sampled_ok = rng.sample(ok_lines, min(sample_ok, len(ok_lines)))
    population = fund_lines + sampled_ok
    provenance = {
        "antal_fund_linjer": len(fund_lines),
        "antal_ok_linjer_total": len(ok_lines),
        "antal_ok_stikproeve": len(sampled_ok),
        "seed": seed,
        "population_size": len(population),
    }
    return population, provenance


def batched(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--facit", required=True, type=Path)
    ap.add_argument("--katalog", required=True, type=Path)
    ap.add_argument("--out-dir", required=True, type=Path, help="UDENFOR repo — indeholder kundedata")
    ap.add_argument("--model", default="qwen3.8:27b")
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument("--num-ctx", type=int, default=16384)
    ap.add_argument("--sample-ok", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-batches", type=int, default=None, help="Begræns til N batches (røgtest)")
    args = ap.parse_args()

    facit = json.loads(args.facit.read_text())
    katalog = json.loads(args.katalog.read_text())
    args.out_dir.mkdir(parents=True, exist_ok=True)

    population, provenance = select_population(facit, args.sample_ok, args.seed)
    groups = group_lines(population)
    stats = reduction_stats(population, groups)
    print(f"Population: {provenance}")
    print(f"Deduplikering: {stats}")

    (args.out_dir / "population_meta.json").write_text(
        json.dumps({"provenance": provenance, "reduction": stats}, ensure_ascii=False, indent=2)
    )
    (args.out_dir / "groups.json").write_text(
        json.dumps(groups, ensure_ascii=False, indent=2, default=str)
    )

    batches = list(batched(groups, args.batch_size))
    if args.max_batches:
        batches = batches[: args.max_batches]
        print(f"RØGTEST: begrænset til {len(batches)} batch(es) af {args.batch_size} grupper")

    all_predictions: dict[str, dict] = {}  # line_id -> {fund_id, begrundelse_kort, group_id}
    batch_meta = []
    for bi, batch in enumerate(batches, start=1):
        prompt = build_prompt(katalog, batch)
        print(f"== Batch {bi}/{len(batches)} ({len(batch)} grupper, model={args.model}) ...", flush=True)
        try:
            raw, dur = call_ollama(args.model, prompt, args.num_ctx)
        except Exception as e:
            batch_meta.append({"batch": bi, "schema_valid": False, "error": str(e)})
            print(f"   FEJL: {e}", flush=True)
            continue

        (args.out_dir / f"batch{bi:03d}_prompt.txt").write_text(prompt)
        (args.out_dir / f"batch{bi:03d}_raw.json").write_text(raw)

        valid = False
        classifications = []
        try:
            parsed = json.loads(raw)
            classifications = parsed.get("classifications", parsed if isinstance(parsed, list) else [])
            valid = isinstance(classifications, list) and all(
                "group_id" in c and "fund_id" in c for c in classifications
            )
        except Exception:
            pass

        batch_meta.append({
            "batch": bi, "antal_grupper": len(batch), "sekunder": round(dur, 1),
            "schema_valid": valid, "antal_klassifikationer": len(classifications),
        })
        print(f"   {dur:.0f}s, schema_valid={valid}, klassifikationer={len(classifications)}", flush=True)

        if valid:
            by_group = {c["group_id"]: c for c in classifications}
            for g in batch:
                c = by_group.get(g["group_id"])
                if c is None:
                    continue
                for line_id in g["member_line_ids"]:
                    all_predictions[line_id] = {
                        "fund_id": c.get("fund_id"),
                        "begrundelse_kort": c.get("begrundelse_kort"),
                        "group_id": g["group_id"],
                    }

    (args.out_dir / "predictions.json").write_text(
        json.dumps(all_predictions, ensure_ascii=False, indent=2)
    )
    (args.out_dir / "batches_meta.json").write_text(
        json.dumps({"model": args.model, "batch_size": args.batch_size, "batches": batch_meta}, ensure_ascii=False, indent=2)
    )

    valid_batches = [b for b in batch_meta if b.get("schema_valid")]
    total_time = sum(b.get("sekunder", 0) for b in batch_meta)
    print(f"\nFærdig: {len(valid_batches)}/{len(batch_meta)} batches skema-valide, "
          f"{len(all_predictions)} linjer klassificeret, {total_time:.0f}s total.")
    print(f"Output: {args.out_dir}")


if __name__ == "__main__":
    main()
