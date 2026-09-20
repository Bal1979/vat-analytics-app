"""
Bilagsniveau-momsmodellen — form-detektion og bilagsaggregering (F3,
gap-analyse-runde 2, Bal-godkendt 2026-09-20).

Baggrund
--------
Motoren har hidtil (implicit) antaget BC/NAV-formen: momsbeløb OG
-grundlag bor på SAMME linje (``tax_code``+``tax_amount``+``tax_base`` på
én postering). Nogle ERP'er (empirisk bekræftet: IFS, kunde 2s IFS-datasæt)
bogfører i stedet KONTOBASERET: en grundlagslinje (omkostnings-/
indtægtskonto) bærer momskoden UDEN sit eget momsbeløb, og et SEPARAT
momskonto-linje i samme bilag bærer det faktiske momsbeløb (typisk med
samme kode) — koblet UDELUKKENDE via bilaget (samme ``transactions[]``,
efter GAP-12s bilagsgruppering), aldrig via et kontonummer/-plan-signal.

En kontrol der (som hidtil) forudsætter linje-formen, bliver på kontobaseret
data vildledt af strukturel støj: en momskonto-linjes EGEN "grundlag"
(dens debet/kredit-beløb) er reelt momsbeløbet selv, ikke et momsgrundlag —
og en grundlagslinje har (korrekt) 0 kr. moms, fordi momsen ligger et andet
sted i bilaget. Se cat01_transaction_integrity.test_01_vat_recalculation og
cat03_vat_rate_validation.test_22_missing_output_vat/test_24_implied_rate
for hvordan formen konsumeres.

Formdetektion — GENERISK, ikke IFS-hardkodet
---------------------------------------------
``detect_form``/``get_form`` ser UDELUKKENDE på det statistiske mønster i
tax_code/tax_amount/debet/kredit-kombinationen på tværs af HELE datasættet —
aldrig på specifikke kontonumre, -navne eller kontoplan-metadata (kontoplanen
skelner i praksis ikke pålideligt momskonti fra andre "Other Payables"-konti,
jf. empirisk afprøvning af kunde 2s IFS-datasæts chart_of_accounts.csv). Samme
mekanisme vil derfor også opdage en fremtidig Oracle-/SAP-udtræk med samme
bogføringsmønster, uden kodeændring.

Heuristikken (se ``_compute_form``):
  1. Blandt alle linjer med en udfyldt momskode: hvor stor en andel bærer
     KODEN UDEN sit eget momsbeløb (en "kode-uden-beløb"-linje med et reelt
     debet-/kreditbeløb)? En høj andel er kontobaseret formens signatur —
     på ægte BC-form er denne andel ~0 (kode og beløb sidder altid sammen).
  2. Er de linjer, der RENT FAKTISK bærer et momsbeløb, koncentreret på få
     distinkte konti (dedikerede momskonti), sammenlignet med det brede
     antal konti, der bærer "kode-uden-beløb"-linjer (almindelige omkostnings-
     /indtægtskonti)? Kontobaseret form har typisk et lille håndfuld
     momskonti og hundredvis af grundlagskonti.

Population-guard (samme filosofi som ``readiness.MIN_TX_FOR_GATING``): et
lille datasæt (fx valideringssuitens et-/få-linjers scenarier, eller
enhedstests med håndbyggede fixtures) klassificeres ALTID som
``FORM_LINE_BASED`` — den hidtidige antagelse, og dermed 100% uændret
adfærd for ALLE eksisterende tests/scenarier/den virkelige BC/NAV-/Excel-/
SAF-T-fil. Kun et datasæt stort nok til at vise et statistisk pålideligt
mønster kan udløse ``FORM_ACCOUNT_BASED``.
"""

from __future__ import annotations

FORM_LINE_BASED = "line_based"
FORM_ACCOUNT_BASED = "account_based"

# Guard: minimum antal momskodede linjer, før formen overhovedet vurderes
# statistisk. Under denne grænse: altid FORM_LINE_BASED (uændret adfærd for
# små datasæt/tests/valideringsscenarier).
MIN_CODE_LINES_FOR_DETECTION = 200

# Andel af momskodede linjer der skal være "kode uden beløb" (grundlags-
# linjer), før kontobaseret form overhovedet er sandsynliggjort.
CODE_ONLY_SHARE_THRESHOLD = 0.30

# Momsbeløbet skal være koncentreret på FÅ konti (dedikerede momskonti) —
# forholdet mellem antal distinkte "beløbsbærende" konti og antal distinkte
# "kode-uden-beløb"-konti skal være lavt for at klassificere som kontobaseret.
AMOUNT_ACCOUNT_CONCENTRATION_RATIO = 0.25


def _lines(data: dict) -> list:
    return [line for txn in data.get("transactions", []) for line in txn.get("lines", [])]


def _compute_form(data: dict) -> dict:
    """Beregn formen ÉN gang for hele datasættet (se modulets docstring for
    heuristikken). Rent, sideeffektfrit — cache håndteres af ``get_form``."""
    code_lines = 0
    amount_bearing = 0
    code_only = 0
    amount_accounts = set()
    code_only_accounts = set()

    for line in _lines(data):
        code = line.get("tax_code")
        if not code:
            continue
        code_lines += 1
        vat = line.get("tax_amount") or 0
        own_amount = (line.get("debit_amount") or 0) - (line.get("credit_amount") or 0)
        acc = line.get("account_id", "")
        if vat:
            amount_bearing += 1
            if acc:
                amount_accounts.add(acc)
        elif own_amount:
            code_only += 1
            if acc:
                code_only_accounts.add(acc)

    diagnostik = {
        "momskodede_linjer": code_lines,
        "beloebsbaerende_linjer": amount_bearing,
        "kode_uden_beloeb_linjer": code_only,
        "distinkte_beloebskonti": len(amount_accounts),
        "distinkte_kodeuden_beloeb_konti": len(code_only_accounts),
    }

    if code_lines < MIN_CODE_LINES_FOR_DETECTION:
        return {"form": FORM_LINE_BASED, "grundlag": "for_faa_linjer", **diagnostik}

    code_only_share = code_only / code_lines if code_lines else 0.0
    diagnostik["kode_uden_beloeb_andel"] = round(code_only_share, 3)

    if code_only_share < CODE_ONLY_SHARE_THRESHOLD:
        return {"form": FORM_LINE_BASED, "grundlag": "lav_kode_uden_beloeb_andel", **diagnostik}

    if not amount_accounts:
        # Ingen linjer bærer overhovedet et momsbeløb -- kan ikke være
        # kontobaseret (der findes ingen "momskonto" at pege på).
        return {"form": FORM_LINE_BASED, "grundlag": "ingen_beloebsbaerende_linjer", **diagnostik}

    concentration = len(amount_accounts) / max(len(code_only_accounts), 1)
    diagnostik["koncentrationsforhold"] = round(concentration, 3)

    if concentration <= AMOUNT_ACCOUNT_CONCENTRATION_RATIO:
        return {"form": FORM_ACCOUNT_BASED, "grundlag": "koncentreret_momskonti", **diagnostik}

    return {"form": FORM_LINE_BASED, "grundlag": "ikke_koncentreret", **diagnostik}


def get_form(data: dict) -> dict:
    """Cache'et formdetektion pr. datasæt — beregnes én gang pr. ``data``-dict
    og genbruges af alle kontroller i samme kørsel (samme "beregn én gang,
    læs mange steder"-mønster som ``header['vat_setup_loaded']``). Cache-
    nøglen (``_vat_form_cache``) er bevidst IKKE en del af datakontrakten
    (canonical-only, samme status som fx ``line['source_row']``)."""
    cached = data.get("_vat_form_cache")
    if cached is not None:
        return cached
    result = _compute_form(data)
    data["_vat_form_cache"] = result
    return result


def is_account_based(data: dict) -> bool:
    """True hvis datasættet er klassificeret som kontobaseret momsform (se
    modulets docstring)."""
    return get_form(data)["form"] == FORM_ACCOUNT_BASED


def code_rate_lookup(data: dict) -> dict:
    """``{tax_code: tax_table-opslag}`` — fælles hjælper for kontroller der
    skal slå en kodes opsætningssats op (samme mønster som allerede brugt
    separat i cat03_vat_rate_validation's test_19/test_24)."""
    return {t["tax_code"]: t for t in data.get("tax_table", [])}


def voucher_code_aggregates(txn: dict, account_based: bool) -> dict:
    """Aggregér et bilags linjer pr. momskode til (grundlag, moms).

    ``account_based=False`` (linjebaseret/BC-form): grundlag og moms bor på
    SAMME linje — en linje med et momsbeløb bidrager sin EGEN ``tax_base``
    til grundlaget (præcis som den hidtidige pr.-linje-logik i cat01/cat03).

    ``account_based=True`` (kontobaseret/IFS-form): grundlag og moms bor på
    FORSKELLIGE linjer i samme bilag. En linje MED momsbeløb bidrager KUN til
    momssummen (dens eget beløb er momsen selv, ikke et grundlag); en linje
    UDEN momsbeløb, men med et reelt debet-/kreditbeløb, bidrager (netto
    debet-kredit) til grundlaget.

    Returnerer ``{tax_code: {"base": float, "vat": float,
    "base_lines": [...], "vat_lines": [...]}}`` — beløbene er NETTO
    (debet-kredit) og fortegnsbærende (positiv ~ købs-/omkostningsside,
    negativ ~ salgs-/indtægtsside), ligesom resten af motorens linjebeløb."""
    agg: dict = {}
    for line in txn.get("lines", []):
        code = line.get("tax_code")
        if not code:
            continue
        entry = agg.setdefault(code, {
            "base": 0.0, "vat": 0.0, "base_lines": [], "vat_lines": [],
        })
        vat = line.get("tax_amount") or 0
        if vat:
            entry["vat"] += vat
            entry["vat_lines"].append(line)
            if not account_based:
                entry["base"] += line.get("tax_base") or 0
        else:
            own = (line.get("debit_amount") or 0) - (line.get("credit_amount") or 0)
            if own:
                entry["base"] += own
                entry["base_lines"].append(line)
    return agg
