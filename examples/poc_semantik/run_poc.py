#!/usr/bin/env python3
"""Semantik-PoC byggetrin 3: LLM-klassifikation af posteringslinjer.

Vælger en stratificeret population af facit-linjer, deduplikerer dem til
unikke grupper (dedup.py), og sender grupperne i batches til en LOKAL
LLM-harness (OpenAI-kompatibelt /v1/chat/completions-endpoint) sammen med
fundkataloget. Modellen returnerer pr. gruppe {fund_id | "OK",
begrundelse_kort}, som spredes til alle gruppens medlemslinjer.

Harness-note (2026-09-22): oprindeligt kørt mod lokal Ollama (se
docs/CHANGELOG.md/README.md for de historiske Ollama-kørsler — de resultater
står ved magt). Bal er siden skiftet til LM Studio som lokal LLM-harness;
denne fil taler nu OpenAI-kompatibelt chatformat i stedet for Ollamas eget
/api/chat. Samme kaldskode virker også mod Ollamas nyere /v1-facade
(--base-url http://localhost:11434/v1) — se --base-url-hjælpeteksten.

Ingen kundedata forlader maskinen (lokal harness). Rå svar og
mellemresultater skrives til --out-dir, som skal pege UDENFOR repoet.

Brug (fuld kørsel, LM Studio):
  python3 run_poc.py --facit <scratch>/facit.json --katalog <scratch>/katalog.json \\
      --out-dir <scratch>/run_qwen27b --model qwen3.8-27b

Brug (røgtest — kun første batch):
  python3 run_poc.py --facit ... --katalog ... --out-dir <scratch>/smoke_27b \\
      --model qwen3.8-27b --max-batches 1

Brug (mod Ollamas OpenAI-facade i stedet for LM Studio):
  python3 run_poc.py --facit ... --katalog ... --out-dir <scratch>/run_ollama \\
      --base-url http://localhost:11434/v1 --model qwen3.8:27b
"""
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from pathlib import Path

from dedup import group_lines, reduction_stats

DEFAULT_BASE_URL = "http://localhost:1234/v1"  # LM Studio default

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


def build_request_body(model: str, prompt: str) -> dict:
    """Bygger OpenAI-kompatibel chat/completions-payload.

    Ingen Ollama-specifikke felter (`format`, `options.num_ctx`, `think`) — se
    call_llm()'s docstring for den empiriske begrundelse. "Thinking"-styring sker via
    et /no_think-præfiks i selve brugerbeskeden (Qwen3-konventionen), ikke et
    body-niveau-felt — OpenAI-chatformatet har intet sådant felt.
    """
    return {
        "model": model,
        "messages": [{"role": "user", "content": f"/no_think\n{prompt}"}],
        "temperature": 0,
        "stream": False,
    }


def extract_message_text(message: dict) -> str:
    """Udtrækker svarteksten fra en chat/completions-besked.

    Faldback til 'reasoning_content', hvis 'content' er tomt: empirisk observeret mod
    den kørende LM Studio (qwen3.8-27b, 2026-09-22) at nogle svar — særligt med
    response_format={"type": "json_schema"} — lander HELT i 'reasoning_content' i
    stedet for 'content'. Vi bruger IKKE response_format (se call_llm()), men
    faldbacken er en billig robusthedsgevinst mod samme observerede model-kvirk.
    """
    content = (message.get("content") or "").strip()
    if content:
        return content
    return (message.get("reasoning_content") or "").strip()


def call_llm(model: str, prompt: str, base_url: str, timeout: int = 1800) -> tuple[str, float]:
    """Kalder et OpenAI-kompatibelt /v1/chat/completions-endpoint.

    Default er LM Studio (http://localhost:1234/v1) — Bal er skiftet fra Ollama til
    LM Studio som lokal LLM-harness 2026-09-22. Samme kaldskode virker også mod
    Ollamas nyere OpenAI-facade (--base-url http://localhost:11434/v1).

    JSON-tvang — EMPIRISK afprøvet mod den kørende LM Studio (qwen3.8-27b,
    2026-09-22), IKKE antaget:
      - response_format={"type": "json_object"} → HTTP 400: "'response_format.type'
        must be 'json_schema' or 'text'". LM Studios OpenAI-facade understøtter ikke
        OpenAIs ældre json_object-tvang.
      - response_format={"type": "json_schema", ...} → HTTP 200, MEN hele svaret
        landede i message['reasoning_content'] i stedet for 'content' (uafhængigt af
        om /no_think var sat) — ubrugeligt uden ekstra parsing/kompleksitet for en
        marginal gevinst.
      - Uden response_format (prompt-instruktion, SYSTEM_RULES pkt. 3) → 'content'
        indeholder gyldig JSON (evt. med foranstillet whitespace, som json.loads
        tolererer). Dette er den valgte vej — samme disciplin som hos Ollama: promptet
        TVINGER skemaet, og main()'s eksisterende batchvalidering (schema_valid) er
        sikkerhedsnettet, ikke en API-garanti.
    """
    body = build_request_body(model, prompt)
    url = f"{base_url.rstrip('/')}/chat/completions"
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    message = resp["choices"][0]["message"]
    return extract_message_text(message), time.time() - t0


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
    ap.add_argument(
        "--base-url", default=DEFAULT_BASE_URL,
        help="OpenAI-kompatibel base-URL uden '/chat/completions'. Default: LM Studio "
             f"({DEFAULT_BASE_URL}). Virker også mod Ollamas nyere OpenAI-facade "
             "(http://localhost:11434/v1) — samme kaldskode, blot anden URL/modelid "
             "(Ollamas modelid bruger ':', fx qwen3.8:27b; LM Studios bruger '-').",
    )
    ap.add_argument("--model", default="qwen3.8-27b", help="LM Studios modelid (bindestreg). Se --base-url for Ollama-formatet (kolon).")
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument(
        "--num-ctx", type=int, default=None,
        help="IGNORERES — Ollama-specifikt felt uden modstykke i OpenAI-chatformatet. "
             "LM Studio sætter kontekstlængden server-side ved model-load (se LM Studios "
             "model-indstillinger). Bevaret kun for bagudkompatibilitet med gamle "
             "kommandoer; en angivet værdi udløser en advarsel og bruges ikke.",
    )
    ap.add_argument("--sample-ok", type=int, default=300)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-batches", type=int, default=None, help="Begræns til N batches (røgtest)")
    args = ap.parse_args()

    if args.num_ctx is not None:
        print(
            f"ADVARSEL: --num-ctx {args.num_ctx} ignoreres (no-op) — LM Studio har ingen "
            "per-request kontekst-parameter i OpenAI-chatformatet. Sæt kontekstlængden i "
            "LM Studios model-indstillinger, hvis den nuværende server-side værdi ikke er "
            "tilstrækkelig til batch-størrelsen/kataloget."
        )

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
            raw, dur = call_llm(args.model, prompt, args.base_url)
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
