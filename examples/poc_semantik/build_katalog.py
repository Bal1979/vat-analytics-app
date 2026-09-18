#!/usr/bin/env python3
"""Semantik-PoC byggetrin 2: fundkatalog til LLM-prompten.

Læser arkene "Fundkatalog" (F01-F31 med navne/status/niveau/beskrivelse/
identifikationsmetode) og "Metode og forudsætninger" (ekspertens generelle
regler: praksis pr. konto/leverandør, absolutte momslovs-kontroller,
60/40-forudsætning, netting, risikoniveau-definitioner) og destillerer dem
til et katalog-JSON: pr. fund-id navn + kort klassifikationsregel.

Kundefilen ligger UDENFOR repoet. Katalog-JSON'en indeholder ikke kundedata
(ingen posteringstekster/leverandørnavne/beløb) — kun de generiske
fund-definitioner eksperten selv har skrevet — men skrives alligevel til
--out UDENFOR repoet for at holde metoden generisk og adskilt fra kundens
konkrete gennemgang (samme disciplin som facit).

Brug:
  python3 build_katalog.py --xlsx <sti/til/AI\\ Transaktionsgennemgang.xlsx> \\
      --out <scratchpad>/katalog.json
"""
from __future__ import annotations

import argparse
import datetime
import json
import re
from pathlib import Path

import openpyxl

_FUND_ID_RE = re.compile(r"^F\d+$")

FUNDKATALOG_SHEET = "Fundkatalog"
METODE_SHEET = "Metode og forudsætninger"

# Fund, hvor en isoleret pr.-linje-klassifikation ikke retfærdigt kan afgøre
# sagen, fordi selve fundet ER en tværlinje- eller virksomhedsniveau-
# vurdering (jf. opgavebeskrivelsen 2026-09-18, punkt 5). Dokumenteret her
# fremfor forsøgt genkendt heuristisk, fordi det er en metodisk afgrænsning,
# ikke et mønster i dataene.
CROSS_LINE_FUNDS = {
    "F01": (
        "Dobbeltbogføring afgøres ved at sammenholde ALLE 3.010 linjer parvis "
        "(leverandør+tekst+beløb+moms) og netting mod kreditnotaer på tværs af "
        "bilag — en enkelt linje indeholder ikke i sig selv information om, at "
        "en anden linje er en dublet."
    ),
    "F10": (
        "Fundet er et virksomhedsniveau-metodevalg (kantineordningens momsmodel), "
        "ikke en fejl på den enkelte linje — linjen matcher blot kriterierne for "
        "at være omfattet af metodevalget."
    ),
}


def _rows(ws):
    for row in ws.iter_rows(values_only=True):
        yield row


def load_fundkatalog(xlsx_path: Path) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[FUNDKATALOG_SHEET]
    rows = list(_rows(ws))
    header_idx = next(i for i, r in enumerate(rows) if r and r[0] == "Fund-ID")
    header = rows[header_idx]
    cols = {name: i for i, name in enumerate(header) if name}

    merged: dict[str, dict] = {}
    order: list[str] = []
    for row in rows[header_idx + 1:]:
        if not row or not row[cols["Fund-ID"]]:
            continue
        fid = str(row[cols["Fund-ID"]]).strip()
        if not _FUND_ID_RE.match(fid):
            continue  # springer forklarende slutnote (fri tekst i kol. A) over
        entry = merged.get(fid)
        niveau = row[cols.get("Risikoniveau")] if "Risikoniveau" in cols else None
        beskrivelse = row[cols.get("Beskrivelse")] if "Beskrivelse" in cols else None
        metode = row[cols.get("Sådan er kontrollen udført")] if "Sådan er kontrollen udført" in cols else None
        if entry is None:
            entry = {
                "id": fid,
                "navn": row[cols.get("Fund")],
                "status": row[cols.get("Status")],
                "niveauer": [],
                "lovgrundlag": row[cols.get("Lovgrundlag")],
                "beskrivelse": beskrivelse,
                "identifikationsmetode": metode,
            }
            merged[fid] = entry
            order.append(fid)
        if niveau and niveau not in entry["niveauer"]:
            entry["niveauer"].append(niveau)
        # Ved dubletrækker (fx F16: samme fund optræder med to niveauer/
        # delmængder) er beskrivelse/lovgrundlag i praksis identiske på tværs
        # af rækkerne — første forekomst er dækkende, niveauerne samles ovenfor.

    funds = []
    for fid in order:
        e = merged[fid]
        regel_kort = (e["beskrivelse"] or "").strip()
        if e["lovgrundlag"]:
            regel_kort = f"{regel_kort} ({e['lovgrundlag']})".strip()
        cross = fid in CROSS_LINE_FUNDS
        funds.append({
            "id": fid,
            "navn": e["navn"],
            "status": e["status"],
            "niveauer": e["niveauer"],
            "regel_kort": regel_kort,
            "identifikationsmetode": (e["identifikationsmetode"] or "").strip() or None,
            "kraever_tvaerlinje_kontekst": cross,
            "tvaerlinje_begrundelse": CROSS_LINE_FUNDS.get(fid),
        })
    return funds


def load_generelle_regler(xlsx_path: Path) -> list[str]:
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb[METODE_SHEET]
    regler = []
    for row in _rows(ws):
        vals = [str(v).strip() for v in row if v is not None and str(v).strip()]
        if not vals:
            continue
        text = " ".join(vals)
        if text in ("EY", "EY Metode og forudsætninger"):
            continue
        regler.append(text)
    return regler


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--xlsx", required=True, type=Path, help="Sti til kunde-Excel (UDENFOR repo)")
    ap.add_argument("--out", required=True, type=Path, help="Katalog-JSON output (UDENFOR repo, fx scratchpad)")
    args = ap.parse_args()

    funds = load_fundkatalog(args.xlsx)
    regler = load_generelle_regler(args.xlsx)

    aktive = [f for f in funds if f["status"] == "Fund"]
    ingen_fund = [f for f in funds if f["status"] != "Fund"]
    cross = [f["id"] for f in funds if f["kraever_tvaerlinje_kontekst"]]

    out = {
        "meta": {
            "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "source_file": str(args.xlsx),
            "antal_fund_ids": len(funds),
            "antal_aktive_fund": len(aktive),
            "antal_ingen_fund_kontroller": len(ingen_fund),
            "tvaerlinje_undtaget": cross,
        },
        "funds": funds,
        "generelle_regler": regler,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, ensure_ascii=False, indent=2))

    print(f"Katalog skrevet: {args.out}")
    print(f"  {len(funds)} fund-id'er ({len(aktive)} aktive, {len(ingen_fund)} 'ingen fund'-kontroller)")
    print(f"  Undtaget pr.-linje-scope: {cross}")
    print(f"  {len(regler)} tekstlinjer fra 'Metode og forudsætninger'")


if __name__ == "__main__":
    main()
