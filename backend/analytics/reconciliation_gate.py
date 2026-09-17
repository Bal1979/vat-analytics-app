"""
Afstemningsgate — BALAI-dataflow-arkitektur.md §8.3: "afstemt mod
kontroltotaler" (byggetrin 8, Del 2, Bal-godkendt 2026-09-17).

Afstemmer transaktionssummer PR. KONTO (``account_id``) i den kanoniske
struktur mod en ekstern kontroltotal-fil, FØR analysen køres. Snitfladen er
aftalt med vat-extract og MÅ IKKE afviges:

    {"reconciliation_version": "1.0.0", "source": "...", "generated": "...",
     "accounts": [{"account_id": "...", "amount": 0.0}, ...]}

Beløbsgrundlag: NETTO signeret beløb pr. konto, ``debit_amount - credit_amount``
summeret over alle linjer for kontoen (BC/NAV-konventionen: debet positiv,
kredit negativ på ét signeret "Amount"-felt -- samme konvention kildesystemet
selv bruger, jf. mapping_og_transformation.md's D1/Amount-felter). Dette er en
DOKUMENTERET ANTAGELSE, ikke en bekræftet kilde-facitliste (opgaven bad
eksplicit om at vurdere ud fra "hvordan Sheet1's 'Sum of Amount'-tal er
dannet" -- der findes i skrivende stund ikke et konkret eksempel på selve
kontroltotal-filen at kalibrere imod). Findes senere en reel kontroltotal-fil
med modsat fortegnskonvention, er dette den ENE linje, der skal ændres
(``_net_amount_per_account``).

v1-adfærd (eksplicit i opgaven): et afstemningsbrud blokerer IKKE analysen.
Gaten er informativ -- rapporten stemples tydeligt, Bal beslutter. Gaten er
desuden helt VALGFRI: ingen kontroltotal-fil givet -> status
"afstemning_ikke_udfoert", intet gate-brud.

Ingen ændring af motorens kontrollogik her -- dette er et selvstændigt,
førkørende lag, samme adskillelse som canonical_parser.py.
"""

from __future__ import annotations

import json

DEFAULT_TOLERANCE = 0.01

RECONCILIATION_VERSION_SUPPORTED = "1.0.0"

STATUS_NOT_PERFORMED = "afstemning_ikke_udfoert"
STATUS_RECONCILED = "afstemt"
STATUS_NOT_RECONCILED = "IKKE_AFSTEMT"
STATUS_INVALID_FILE = "ugyldig_kontroltotal_fil"


def _net_amount_per_account(canonical: dict) -> dict:
    """Netto (debit - credit) pr. account_id, summeret over alle linjer i alle
    transaktioner. Se modulets docstring for fortegnskonvention/antagelse."""
    totals: dict = {}
    for txn in canonical.get("transactions", []):
        for line in txn.get("lines", []):
            acct = line.get("account_id") or ""
            if not acct:
                continue
            debit = line.get("debit_amount") or 0.0
            credit = line.get("credit_amount") or 0.0
            totals[acct] = totals.get(acct, 0.0) + (debit - credit)
    return {acct: round(amt, 2) for acct, amt in totals.items()}


def load_control_totals(path: str) -> tuple:
    """Læs + valider en kontroltotal-fil. Returnerer (dict|None, error|None).
    Fejler ALDRIG med exception -- teknisk fejl skal aldrig fremstå som et
    fagligt afstemningsbrud (jf. brugerens faste regel)."""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, UnicodeDecodeError) as e:
        return None, f"Kontroltotal-filen kunne ikke læses: {e}"
    except json.JSONDecodeError as e:
        return None, f"Kontroltotal-filen er ikke gyldig JSON: {e}"

    if not isinstance(doc, dict):
        return None, "Kontroltotal-filen skal være et JSON-objekt."
    if doc.get("reconciliation_version") != RECONCILIATION_VERSION_SUPPORTED:
        return None, (
            "Ukendt/manglende reconciliation_version (forventede "
            f"{RECONCILIATION_VERSION_SUPPORTED!r}, fik "
            f"{doc.get('reconciliation_version')!r})."
        )
    accounts = doc.get("accounts")
    if not isinstance(accounts, list):
        return None, "Kontroltotal-filen mangler en 'accounts'-liste."
    for entry in accounts:
        if not isinstance(entry, dict) or "account_id" not in entry or "amount" not in entry:
            return None, "Hver post i 'accounts' skal have 'account_id' og 'amount'."
    return doc, None


def reconcile(canonical: dict, control_totals_path: str | None,
              tolerance: float = DEFAULT_TOLERANCE) -> dict:
    """Kør afstemningsgaten. Blokerer ALDRIG analysen (v1) -- returnerer altid
    en rapport, motoren/CLI'en kan vælge at vise/stemple, uanset udfald.

    Returnerer:
        {
          "gate_status": STATUS_*,
          "reconciliation_version": "1.0.0" | None,
          "source": <fra kontroltotal-filen> | None,
          "generated": <fra kontroltotal-filen> | None,
          "tolerance": <float>,
          "accounts": [
              {"account_id", "control_amount", "actual_amount", "difference", "status"}
          ],
          "summary": {"total_accounts_checked", "reconciled_count", "not_reconciled_count"},
          "message": "<menneskelæsbar status>",
        }
    """
    if not control_totals_path:
        return {
            "gate_status": STATUS_NOT_PERFORMED,
            "reconciliation_version": None,
            "source": None,
            "generated": None,
            "tolerance": tolerance,
            "accounts": [],
            "summary": {"total_accounts_checked": 0, "reconciled_count": 0, "not_reconciled_count": 0},
            "message": "Afstemning ikke udført — ingen kontroltotal-fil angivet.",
        }

    control_doc, error = load_control_totals(control_totals_path)
    if error:
        return {
            "gate_status": STATUS_INVALID_FILE,
            "reconciliation_version": None,
            "source": None,
            "generated": None,
            "tolerance": tolerance,
            "accounts": [],
            "summary": {"total_accounts_checked": 0, "reconciled_count": 0, "not_reconciled_count": 0},
            "message": f"Afstemning ikke udført — {error} "
                       "(teknisk fejl, ikke et fagligt afstemningsbrud).",
        }

    actual = _net_amount_per_account(canonical)
    rows = []
    reconciled_count = 0
    for entry in control_doc["accounts"]:
        acct = str(entry["account_id"])
        control_amount = float(entry["amount"])
        actual_amount = actual.get(acct, 0.0)
        difference = round(actual_amount - control_amount, 2)
        ok = abs(difference) <= tolerance
        reconciled_count += 1 if ok else 0
        rows.append({
            "account_id": acct,
            "control_amount": control_amount,
            "actual_amount": actual_amount,
            "difference": difference,
            "status": STATUS_RECONCILED if ok else STATUS_NOT_RECONCILED,
        })

    not_reconciled_count = len(rows) - reconciled_count
    overall = STATUS_RECONCILED if not_reconciled_count == 0 else STATUS_NOT_RECONCILED
    if not rows:
        message = "Afstemning udført — kontroltotal-filen indeholdt ingen konti."
    elif overall == STATUS_RECONCILED:
        message = f"Afstemt: alle {len(rows)} konti matcher kontroltotalerne (tolerance {tolerance})."
    else:
        message = (
            f"IKKE AFSTEMT: {not_reconciled_count} af {len(rows)} konti afviger "
            f"fra kontroltotalerne (tolerance {tolerance}). Se detaljer pr. konto."
        )

    return {
        "gate_status": overall,
        "reconciliation_version": control_doc.get("reconciliation_version"),
        "source": control_doc.get("source"),
        "generated": control_doc.get("generated"),
        "tolerance": tolerance,
        "accounts": rows,
        "summary": {
            "total_accounts_checked": len(rows),
            "reconciled_count": reconciled_count,
            "not_reconciled_count": not_reconciled_count,
        },
        "message": message,
    }
