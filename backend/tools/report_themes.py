"""
report_themes.py — den faste tema-gruppering af kontrolfund til
kunderapportens sektion 4 ("Observationer & spørgsmål"), byggetrin ~10
(Bal-godkendt 2026-09-18, designoplæg alle fire spørgsmål godkendt).

Kunden ser ALDRIG kontrolnumre (jf. generate_report.py's disciplin — kun
lineage-footeren og Excel-arbejdsbilaget må vise dem). Denne fil er broen:
den binder de interne kontrolnumre til et tema, kunden KAN se, og til
deterministiske tekst-UDKAST (spørgsmål/hvorfor/anbefaling), som rådgiveren
altid kan redigere via kurationsfilen (se ``tools/report_curation.py``).

Tema -> kontrol-mapping (Bal-godkendt, ikke konfigurerbar pr. kørsel — en
ændring her er en produktbeslutning, ikke en engagement-indstilling):
    kodeopsaetning            -> 19 (sats afviger fra vat_setup)
    momsbehandling_pr_konto   -> 80 (indtægt uden momsbehandling, pr. konto)
    dataanomalier             -> 1, 7, 24, 60 (moms-genberegning,
                                 nulværdi-transaktioner, implicit sats
                                 ugyldig — residual efter 2026-09-18-
                                 kalibreringen, negativt momsbeløb —
                                 samme residual)
    proces                    -> 46, 14 (faktura-/bogføringslag,
                                 genbrugt fakturanummer)
    timing                    -> 82, 5 (periode-/rubrikafstemning,
                                 dato-/periodekonsistens)

Rækkefølgen i ``THEMES`` er visnings-rækkefølgen i rapporten.
"""

from __future__ import annotations

import re
from collections import Counter

# --- Tema-metadata + kontrol-mapping ---------------------------------------

THEMES = {
    "kodeopsaetning": {
        "navn": "Kodeopsætning",
        "kontroller": (19,),
        "default_horisont": "0-3",
    },
    "momsbehandling_pr_konto": {
        "navn": "Momsbehandling pr. konto",
        "kontroller": (80,),
        "default_horisont": "3-12",
    },
    "dataanomalier": {
        "navn": "Dataanomalier",
        "kontroller": (1, 7, 24, 60),
        "default_horisont": "0-3",
    },
    "proces": {
        "navn": "Proces",
        "kontroller": (46, 14),
        "default_horisont": "3-12",
    },
    "timing": {
        "navn": "Timing",
        "kontroller": (82, 5),
        "default_horisont": "0-3",
    },
}

# Temaer, der ALTID forfremmes ("medtag": true) i den auto-seedede kuration,
# uanset severity/beløb — jf. designoplægget ("alle høj-fund-grupper
# forfremmet + timing-temaet").
ALWAYS_PROMOTED_THEMES = {"timing"}

THEME_ORDER = list(THEMES.keys())


def theme_of(test_id: int) -> str | None:
    """Temanøgle for en kontrol, eller None hvis kontrollen ikke indgår i
    nogen kurateret tema-gruppe (den optræder da kun i Excel-arbejdsbilaget,
    ikke i kundens sektion 4)."""
    for key, meta in THEMES.items():
        if test_id in meta["kontroller"]:
            return key
    return None


# --- Deterministiske tekst-udkast -------------------------------------------
# Ren skabelon-tekst — INGEN sprogmodel, INGEN kundedata-fortolkning ud over
# simple frekvens-optællinger (mest hyppige konto/momskode i gruppens fund).
# Formuleret spørgende/vi-bemærker (aldrig anklagende), jf. opgavens formkrav.

def _top_field(findings: list, field: str, limit: int = 1) -> list:
    """Mest hyppige ikke-tomme værdi af ``field`` på tværs af gruppens fund
    (kigger kun på FØRSTE transaktionsreference pr. fund, så en enkelt
    aggregeret kontrol-80-gruppe med mange refs ikke dominerer optællingen)."""
    counter: Counter = Counter()
    for f in findings:
        refs = f.get("transactions") or []
        if not refs:
            continue
        value = refs[0].get(field)
        if value:
            counter[str(value)] += 1
    return [v for v, _ in counter.most_common(limit)]


def _stats(findings: list) -> dict:
    total_amount = round(sum(f.get("estimated_amount") or 0.0 for f in findings), 2)
    return {"antal": len(findings), "beloeb": total_amount}


_CODE_IN_DESCRIPTION = re.compile(r"momskode ['’]([^'’]+)['’]", re.IGNORECASE)


def _top_code_from_descriptions(findings: list, limit: int = 1) -> list:
    """Kontrol 19's transaktionsreferencer bærer IKKE selve momskoden
    strukturelt (kun kontonummer/beløb/dato, jf. cat03_vat_rate_validation.
    _txn_ref) — koden optræder kun i den danske fritekstbeskrivelse
    ("Momskode 'X' ... afviger fra opsætningens sats ..."). Udtrækkes derfor
    herfra, med en tom liste (-> generisk spørgsmål) hvis mønsteret ikke
    matcher noget (fx en anden kontrols beskrivelsestekst)."""
    counter: Counter = Counter()
    for f in findings:
        m = _CODE_IN_DESCRIPTION.search(f.get("description") or "")
        if m:
            counter[m.group(1)] += 1
    return [v for v, _ in counter.most_common(limit)]


def draft_kodeopsaetning(findings: list) -> dict:
    codes = _top_code_from_descriptions(findings)
    stats = _stats(findings)
    if codes:
        spg = (f"Hvad anvendes momskoden {codes[0]} til, og er opsætningen "
               "korrekt sat op i jeres system?")
    else:
        spg = "Er der momskoder i jeres opsætning, som bør gennemgås sammen med jer?"
    return {
        "spoergsmaal": spg,
        "hvorfor": (f"{stats['antal']} posteringer bruger en momskode, hvis faktiske sats "
                    "afviger fra det, jeres eget momssetup angiver. Det kan enten betyde en "
                    "forældet kodeopsætning eller en postering, der reelt bør bogføres på en "
                    "anden kode."),
        "anbefaling": ("Gennemgå den/de fremhævede momskoder sammen med jeres bogholderi, og "
                        "ret enten koden eller opsætningen, så de stemmer overens."),
    }


def draft_momsbehandling_pr_konto(findings: list) -> dict:
    accounts = _top_field(findings, "account_id")
    stats = _stats(findings)
    if accounts:
        spg = (f"Hvordan behandles moms på konto {accounts[0]} — er posteringerne her "
               "momspligtige, momsfrie eller udenlandske?")
    else:
        spg = "Er der konti i kontoplanen, hvis momsbehandling bør bekræftes?"
    return {
        "spoergsmaal": spg,
        "hvorfor": (f"{stats['antal']} konti har posteringer for i alt "
                    f"{stats['beloeb']:,.0f} DKK bogført uden moms og uden momskode. Det er "
                    "ofte en bevidst kontobrug (fx interne allokeringskonti), men bør "
                    "bekræftes konto for konto — ikke antaget."),
        "anbefaling": ("Bekræft momsbehandlingen for de fremhævede konti, og opdatér "
                        "kontoplanens momsopsætning, hvis den ikke afspejler praksis."),
    }


def draft_dataanomalier(findings: list) -> dict:
    stats = _stats(findings)
    return {
        "spoergsmaal": ("Er der en forretningsmæssig forklaring på de fremhævede "
                         "posteringer, som afviger fra det forventede mønster?"),
        "hvorfor": (f"{stats['antal']} posteringer for i alt {stats['beloeb']:,.0f} DKK er "
                    "markeret som datamæssige afvigelser (fx nulværdi-transaktioner, en "
                    "implicit momssats der ikke matcher nogen kendt sats, eller et negativt "
                    "momsbeløb uden en tydelig modpost). Dette er OBSERVATIONER, ikke "
                    "konklusioner."),
        "anbefaling": ("Gennemgå de fremhævede posteringer stikprøvevis, og bekræft om "
                        "afvigelsen er en fejl, en særlig forretningsgang eller en "
                        "datakvalitetsting, der bør rettes ved kilden."),
    }


def draft_proces(findings: list) -> dict:
    stats = _stats(findings)
    return {
        "spoergsmaal": "Hvordan er jeres proces for fakturanumre og bogføringstiming organiseret i dag?",
        "hvorfor": (f"{stats['antal']} posteringer peger på genbrugte fakturanumre eller et "
                    "usædvanligt stort tidsmæssigt lag mellem faktura- og bogføringsdato. "
                    "Det er ikke nødvendigvis en fejl, men bør kunne forklares proces-mæssigt."),
        "anbefaling": ("Bekræft rutinen for fakturanummerering og bogføringsfrister, og "
                        "overvej om den bør strammes op."),
    }


def draft_timing(findings: list) -> dict:
    return {
        "spoergsmaal": ("Stemmer den bogførte moms overens med jeres indberetninger hen over "
                         "året, og er de resterende periodeafvigelser ren timing?"),
        "hvorfor": ("Afstemningen mod den indberettede momsangivelse (se afsnittet "
                    "'Afstemningen') viser enkelte periodeafvigelser, der typisk udlignes "
                    "over årstotalen (fx fordi købsmoms angives på afregningsbasis, mens "
                    "bogføringen følger periode), samt evt. dato-/periodemæssige "
                    "observationer omkring årsskiftet."),
        "anbefaling": ("Bekræft at periodeafvigelserne er kendte timingforskelle og ikke en "
                        "reel angivelsesfejl — årstotalen er tillidsankeret i rapporten."),
    }


_DRAFT_BUILDERS = {
    "kodeopsaetning": draft_kodeopsaetning,
    "momsbehandling_pr_konto": draft_momsbehandling_pr_konto,
    "dataanomalier": draft_dataanomalier,
    "proces": draft_proces,
    "timing": draft_timing,
}


def draft_content(theme_key: str, findings: list) -> dict:
    """Byg {spoergsmaal, hvorfor, anbefaling}-udkast for et tema ud fra dets
    fund. Ukendt tema (bør ikke forekomme, THEMES er den lukkede liste) ->
    generisk udkast, aldrig en exception."""
    builder = _DRAFT_BUILDERS.get(theme_key)
    if builder is None:
        return {
            "spoergsmaal": "Er der forhold i denne gruppe, vi bør drøfte sammen?",
            "hvorfor": "Se evidenstabellen for de konkrete posteringer.",
            "anbefaling": "Gennemgå de fremhævede posteringer sammen med jeres bogholderi.",
        }
    return builder(findings)
