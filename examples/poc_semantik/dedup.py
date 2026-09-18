"""Deduplikering af posteringslinjer til semantik-PoC'en (arbejdspapirets §5.4).

Grupperer linjer på et normaliseret fingeraftryk af de felter, LLM'en faktisk
FÅR AT SE (konto, leverandør, tekst-mønster, momskode, valuta, om der er
fratrukket moms) — ALDRIG på facit-fund-id'et, da det ville "løse" opgaven i
selve grupperingen. Én LLM-vurdering pr. unik gruppe spredes bagefter til alle
gruppens medlemslinjer.

Bemærk bevidst medtagne felter i nøglen ud over konto/leverandør/tekst:
- momskode (Momsvirksomhedsgruppe/Momsvaregruppe) og "moms > 0": to ellers
  identiske linjer, der er kodet forskelligt, er netop den slags afvigelse
  flere fund (fx F04) handler om — de må IKKE smeltes sammen.
- valuta: afgørende for F29 (udenlandsk valuta med dansk moms) — to ellers
  identiske linjer i hhv. DKK og EUR skal klassificeres hver for sig.

Ikke medtaget: beløb, datoer, bilagsnr. — varierer pr. postering uden at ændre
den momsmæssige vurdering (undtagen tærskelfølsomme fund som F31 "over 1.000
kr.", som er en kendt begrænsning — se README).
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

_DIGITS = re.compile(r"\d+")
_PUNCT = re.compile(r"[^\w\s]", flags=re.UNICODE)
_SPACES = re.compile(r"\s+")


def normalize_text(value: Any, max_len: int = 50) -> str:
    """Normaliserer leverandør/posteringstekst til gruppering.

    Fjerner cifre (fakturanumre, datoer, beløb indlejret i teksten),
    tegnsætning og dobbelt-whitespace, lowercaser og trunkerer.
    """
    if value is None:
        return ""
    s = str(value).lower()
    s = _DIGITS.sub(" ", s)
    s = _PUNCT.sub(" ", s)
    s = _SPACES.sub(" ", s).strip()
    return s[:max_len]


def group_key(line: dict) -> tuple:
    """Bygger gruppenøglen for én facit-linje (kun pre-klassifikations-felter)."""
    return (
        str(line.get("account") or ""),
        normalize_text(line.get("supplier")),
        normalize_text(line.get("text")),
        str(line.get("vat_business_group") or ""),
        str(line.get("vat_product_group") or ""),
        str(line.get("currency") or ""),
        bool(line.get("vat_amount") not in (None, 0, 0.0)),
    )


def group_lines(lines: list[dict]) -> list[dict]:
    """Grupperer en liste facit-linjer. Returnerer liste af grupper:

    {"group_id": str, "member_line_ids": [...], "representative": <line dict>,
     "member_count": int, "amount_min": float, "amount_max": float,
     "vat_amount_sum": float}

    Repræsentanten er den FØRSTE linje i gruppen (stabilt efter input-rækkefølge),
    så gruppens vist beløb ikke er tilfældigt valgt ved hver kørsel.
    """
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    order: list[tuple] = []
    for line in lines:
        key = group_key(line)
        if key not in buckets:
            order.append(key)
        buckets[key].append(line)

    groups = []
    for idx, key in enumerate(order, start=1):
        members = buckets[key]
        rep = members[0]
        amounts = [m.get("amount_excl_vat") or 0 for m in members]
        vat_amounts = [m.get("vat_amount") or 0 for m in members]
        groups.append({
            "group_id": f"G{idx:04d}",
            "member_line_ids": [m["line_id"] for m in members],
            "member_count": len(members),
            "representative": rep,
            "amount_min": min(amounts),
            "amount_max": max(amounts),
            "vat_amount_sum": round(sum(vat_amounts), 2),
        })
    return groups


def reduction_stats(lines: list[dict], groups: list[dict]) -> dict:
    n_lines = len(lines)
    n_groups = len(groups)
    return {
        "antal_linjer": n_lines,
        "antal_unikke_grupper": n_groups,
        "reduktionsfaktor": round(n_lines / n_groups, 2) if n_groups else None,
        "reduktion_pct": round(100 * (1 - n_groups / n_lines), 1) if n_lines else None,
    }
