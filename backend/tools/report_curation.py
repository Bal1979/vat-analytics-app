"""
report_curation.py — kunderapportens kuraterings-mekanisme (byggetrin ~10,
Bal-godkendt 2026-09-18, designoplæg alle fire spørgsmål godkendt).

Sektion 4 ("Observationer & spørgsmål") i ``tools/generate_report.py`` viser
ALDRIG fund-data direkte — den viser altid en kurationsfil, som denne fil
seeder/opdaterer/loader. Grundtanken: motoren foreslår, rådgiveren bestemmer,
og et redigeret felt overlever for evigt (indtil rådgiveren selv ændrer det),
også når analysen genkøres på en opdateret fil.

KURATIONSFIL-FORMAT (JSON), ``kurationsfil_version``: "1.0"::

    {
      "kurationsfil_version": "1.0",
      "sidst_seedet": "<ISO 8601 UTC — første seed>",
      "sidst_opdateret": "<ISO 8601 UTC — seneste seed/merge>",
      "grupper": {
        "<tema-nøgle>": {
          "tema": "<tema-nøgle>",           # se tools/report_themes.py
          "tema_navn": "<dansk visningsnavn>",
          "status": "ny_ikke_kurateret" | "kurateret" | "ingen_fund_i_seneste_koersel",
          "medtag": true | false,           # RÅDGIVERENS til-/fravalg — styrer
                                             # om gruppen vises i sektion 4
          "horisont": "0-3" | "3-12" | "12-24" | null,   # bruges i sektion 6
          "spoergsmaal": "...",             # RÅDGIVER-EJET, seedet med udkast
          "hvorfor": "...",                 # RÅDGIVER-EJET, seedet med udkast
          "anbefaling": "...",              # RÅDGIVER-EJET, seedet med udkast
          "advisor_notes": "",              # RÅDGIVER-EJET, frit notatfelt
          "auto": {                         # ALTID REGENERERET — IKKE rådgiver-ejet.
            "kilde_kontroller": [19],        #   Overskrives ubetinget ved hver
            "fund_antal": 21,                #   seed/genkørsel, uanset hvad der
            "beloeb_i_alt": 12345.67,        #   stod der før.
            "severity_counts": {"high": 5, "medium": 16},
            "evidens": [{"konto": "...", "bilag": "...", "dato": "...",
                         "beloeb": 0.0, "beskrivelse": "..."}, ...],  # cap 10
            "seneste_koersel": "<ISO 8601 UTC>"
          }
        }
      }
    }

LIVSCYKLUS (Bal-godkendt mekanik):

1. **Filen findes ikke** (første kørsel, eller ``--curation`` udeladt):
   ``seed_curation`` bygger ÉT gruppeforslag pr. tema, der har mindst ét fund
   i denne kørsel. Auto-forfremmelse (``medtag``): temaet "timing" er ALTID
   forfremmet; enhver gruppe med mindst ét kritisk/højt fund forfremmes;
   medium-only-grupper forfremmes, når det samlede beløb når
   ``materiality.REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD`` (konfigurerbar via
   ``MATERIALITY_REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD``-env). Alle andre
   grupper optages i filen med ``medtag: false``, så rådgiveren kan slå dem
   til manuelt uden at skrive teksten fra bunden.

2. **Filen findes** (genkørsel, fx efter en motorkalibrering):
   ``merge_curation`` bevarer HVER EKSISTERENDE gruppe fuldstændig uændret
   PÅ NÆR dens ``auto``-blok (som altid regenereres — det er data, ikke
   rådgiverindhold). Rådgiverens redigerede spørgsmål/prosa/anbefaling/
   til-/fravalg/horisont/noter overlever ordret. Et tema, der IKKE havde
   fund før men har det nu, tilføjes som en helt ny gruppe (samme
   seed-behandling som pkt. 1, inkl. auto-forfremmelse) — DESIGNBESLUTNING
   (dokumenteret her, jf. opgavens "afvig ikke uden at notere det
   eksplicit"-krav): den får status ``"ny_ikke_kurateret"``, så rådgiveren
   kan se i filen, at den er ny og endnu ikke bevidst gennemgået, men den er
   IKKE tvangs-skjult (``medtag`` følger samme forfremmelsesregel som en
   frisk seed) — ellers ville en ægte ny høj-fund-observation kunne forsvinde
   stille fra rapporten efter en motorændring, hvilket strider mod "RØD =
   handling krævet, ingen falske alarmer". Et tema, der HAVDE fund før, men
   ikke har det i denne kørsel, beholder sin gruppe (rådgiverens tekst
   slettes ALDRIG automatisk — "slet aldrig filer/indhold uden Bals
   tilladelse"-disciplinen udvidet hertil), men får status
   ``"ingen_fund_i_seneste_koersel"`` og en tom auto-blok; renderingslaget
   (generate_report.py) viser den ikke i sektion 4 (intet at vise), men den
   ligger klar, hvis fundet skulle dukke op igen.

INGEN evidensrække viser mere end de facto findes i fundets egne
transaktionsreferencer — ingen fabrikerede eksempler.
"""

from __future__ import annotations

import copy
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

from analytics import materiality
from tools import report_themes

CURATION_VERSION = "1.0"
_MAX_EVIDENCE_ROWS = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --- Auto-blok (data, aldrig rådgiver-ejet) ---------------------------------

def build_auto_block(theme_key: str, findings: list, now: str | None = None) -> dict:
    """Byg den regenererede 'auto'-blok for én tema-gruppe. Evidensen tager
    (bevidst, for spredning på tværs af kontrollens fund frem for dybde i ét
    enkelt fund) den FØRSTE transaktionsreference pr. fund, cap'et til
    ``_MAX_EVIDENCE_ROWS`` rækker i alt for gruppen."""
    meta = report_themes.THEMES.get(theme_key, {})
    sev_counts = Counter(f.get("severity", "low") for f in findings)
    total_amount = round(sum(f.get("estimated_amount") or 0.0 for f in findings), 2)

    evidence = []
    for f in findings:
        refs = f.get("transactions") or []
        if not refs:
            continue
        ref = refs[0]
        evidence.append({
            "konto": ref.get("account_id", "") or "",
            "bilag": ref.get("transaction_id") or ref.get("journal_id") or "",
            "dato": ref.get("date", "") or "",
            "beloeb": ref.get("amount", f.get("estimated_amount", 0.0)),
            "beskrivelse": ref.get("description") or f.get("description", "") or "",
        })
        if len(evidence) >= _MAX_EVIDENCE_ROWS:
            break

    return {
        "kilde_kontroller": list(meta.get("kontroller", ())),
        "fund_antal": len(findings),
        "beloeb_i_alt": total_amount,
        "severity_counts": dict(sev_counts),
        "evidens": evidence,
        "seneste_koersel": now or _now_iso(),
    }


def should_promote(theme_key: str, auto_block: dict) -> bool:
    """Auto-forfremmelsesregel for seedning — se modulets docstring."""
    if theme_key in report_themes.ALWAYS_PROMOTED_THEMES:
        return True
    sev = auto_block.get("severity_counts", {})
    if sev.get("critical") or sev.get("high"):
        return True
    return auto_block.get("beloeb_i_alt", 0.0) >= materiality.REPORT_MEDIUM_GROUP_PROMOTION_THRESHOLD


def seed_group(theme_key: str, findings: list, now: str | None = None) -> dict:
    now = now or _now_iso()
    meta = report_themes.THEMES[theme_key]
    auto = build_auto_block(theme_key, findings, now=now)
    draft = report_themes.draft_content(theme_key, findings)
    return {
        "tema": theme_key,
        "tema_navn": meta["navn"],
        "status": "ny_ikke_kurateret",
        "medtag": should_promote(theme_key, auto),
        "horisont": meta["default_horisont"],
        "spoergsmaal": draft["spoergsmaal"],
        "hvorfor": draft["hvorfor"],
        "anbefaling": draft["anbefaling"],
        "advisor_notes": "",
        "auto": auto,
    }


def group_findings_by_theme(all_findings: list) -> dict:
    by_theme = defaultdict(list)
    for f in all_findings:
        tk = report_themes.theme_of(f.get("test_id"))
        if tk:
            by_theme[tk].append(f)
    return by_theme


def seed_curation(all_findings: list, now: str | None = None) -> dict:
    """Byg en HELT NY kurationsstruktur (ingen eksisterende fil) — ét
    gruppeforslag pr. tema med mindst ét fund i denne kørsel."""
    now = now or _now_iso()
    by_theme = group_findings_by_theme(all_findings)
    groups = {
        tk: seed_group(tk, by_theme[tk], now=now)
        for tk in report_themes.THEME_ORDER
        if by_theme.get(tk)
    }
    return {
        "kurationsfil_version": CURATION_VERSION,
        "sidst_seedet": now,
        "sidst_opdateret": now,
        "grupper": groups,
    }


def merge_curation(existing: dict, all_findings: list, now: str | None = None) -> dict:
    """Slå en genkørsels fund sammen med en EKSISTERENDE kurationsfil —
    rådgiverens indhold vinder for kendte grupper (kun 'auto' regenereres);
    nye temaer tilføjes som ``ny_ikke_kurateret``; temaer uden fund i denne
    kørsel beholdes, men nulstilles til tom 'auto' + status
    ``ingen_fund_i_seneste_koersel`` (se modulets docstring for begrundelsen)."""
    now = now or _now_iso()
    fresh = seed_curation(all_findings, now=now)

    merged_groups = copy.deepcopy(existing.get("grupper", {}))
    for tk, fresh_group in fresh["grupper"].items():
        if tk in merged_groups:
            merged_groups[tk]["auto"] = fresh_group["auto"]
        else:
            merged_groups[tk] = fresh_group

    stale_now = {
        "kilde_kontroller": [],
        "fund_antal": 0,
        "beloeb_i_alt": 0.0,
        "severity_counts": {},
        "evidens": [],
        "seneste_koersel": now,
    }
    for tk, grp in merged_groups.items():
        if tk not in fresh["grupper"]:
            meta = report_themes.THEMES.get(tk, {})
            stale = dict(stale_now)
            stale["kilde_kontroller"] = list(meta.get("kontroller", ()))
            grp["auto"] = stale
            grp["status"] = "ingen_fund_i_seneste_koersel"

    return {
        "kurationsfil_version": existing.get("kurationsfil_version", CURATION_VERSION),
        "sidst_seedet": existing.get("sidst_seedet", now),
        "sidst_opdateret": now,
        "grupper": merged_groups,
    }


# --- Fil-I/O -----------------------------------------------------------------

def load_curation(path: str) -> dict | None:
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_curation(path: str, data: dict) -> None:
    out_dir = os.path.dirname(os.path.abspath(path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def resolve_curation(path: str | None, all_findings: list, now: str | None = None) -> tuple:
    """Højniveau-indgang brugt af ``generate_report.py``s CLI: seed/merge +
    (hvis ``path`` er angivet) persistér til disk. Returnerer
    ``(curation_dict, blev_skrevet_til_disk)``. Uden ``path``: kun in-memory
    (rapporten kan stadig bygges, men ingen fil overlever kørslen — brugbart
    til engangs-forhåndsvisning)."""
    now = now or _now_iso()
    if not path:
        return seed_curation(all_findings, now=now), False

    existing = load_curation(path)
    if existing is None:
        fresh = seed_curation(all_findings, now=now)
        save_curation(path, fresh)
        return fresh, True

    merged = merge_curation(existing, all_findings, now=now)
    save_curation(path, merged)
    return merged, True


def promoted_groups(curation: dict) -> list:
    """Grupper klar til visning i sektion 4: ``medtag`` er sat OG der er
    reelt fund at vise i denne kørsel (en 'ingen_fund_i_seneste_koersel'-
    gruppe har intet at vise, uanset et tidligere ``medtag: true``)."""
    groups = (curation or {}).get("grupper", {})
    ordered = [groups[tk] for tk in report_themes.THEME_ORDER if tk in groups]
    # Grupper for temaer uden fast rækkefølge (bør ikke forekomme, men
    # robusthed > exception) føjes til sidst.
    ordered += [g for tk, g in groups.items() if tk not in report_themes.THEME_ORDER]
    return [
        g for g in ordered
        if g.get("medtag") and (g.get("auto") or {}).get("fund_antal", 0) > 0
    ]
