"""
Standardkontoplan-rolle (nature): balance vs. resultat ud fra StandardAccountID.

Baggrund
--------
En SAF-T-fil mapper hver af virksomhedens egne konti til en dansk standardkonto via
feltet ``StandardAccountID``. Selve `AccountType` er i praksis upålidelig — de
modtagne klientfiler sætter `AccountType="Other"` på ~93–100% af konti. Men
``StandardAccountID`` er udfyldt, og Erhvervsstyrelsens standardkontoplan er skarpt
opdelt i to sektioner:

    1000  RESULTATOPGØRELSE   (resultat: 1000–4999)
    5000  BALANCE
    5001  AKTIVER             (balance: 5000+)
    6500  PASSIVER

Så en konto med ``StandardAccountID`` ≥ 5000 er en **balancekonto** (aktiv/passiv/
egenkapital, inkl. moms-, debitor- og kreditorkonti), og 1000–4999 er
**resultatopgørelse** (omsætning/omkostninger). Det giver et robust momsrelevans-
signal, uafhængigt af den (ofte fejlmærkede) AccountType.

Grænserne er ERST-standardkontoplanens egne sektions-headere (git.erst.dk). Ændres
kun hvis ERST ændrer opdelingen.
"""

from __future__ import annotations

from typing import Optional

# Sektions-grænser fra ERST-standardkontoplanens headere.
RESULT_START = 1000    # RESULTATOPGØRELSE
BALANCE_START = 5000   # BALANCE / AKTIVER / PASSIVER


def _leading_int(std_id) -> Optional[int]:
    """Første heltal i en StandardAccountID-streng (fx '5800' -> 5800). None hvis
    værdien ikke starter med et ciffer."""
    if std_id is None:
        return None
    s = str(std_id).strip()
    digits = ""
    for ch in s:
        if ch.isdigit():
            digits += ch
        else:
            break
    return int(digits) if digits else None


def account_nature(std_id) -> str:
    """Returnér 'balance', 'resultat' eller '' (ukendt) ud fra StandardAccountID.

    Ukendt (tom/ikke-numerisk/uden for planen) → '' så adfærden er uændret, når
    signalet ikke kan bruges.
    """
    n = _leading_int(std_id)
    if n is None:
        return ""
    if n >= BALANCE_START:
        return "balance"
    if n >= RESULT_START:
        return "resultat"
    return ""


def is_balance_account(std_id) -> bool:
    """True hvis StandardAccountID positivt er en balancekonto (≥ 5000)."""
    return account_nature(std_id) == "balance"
