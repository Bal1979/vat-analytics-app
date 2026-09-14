"""
Datakontrakt-drift-gate: den committede catalog/data_contract.json skal matche
det, generatoren producerer fra tools/data_contract_data.py. Fanger en
forældet kontrakt (felt tilføjet/ændret uden regenerering) — analogt til
tests/test_catalog_fresh.py for regelkataloget.

Krydstjekker desuden mod analytics/readiness.py: hvert signal-felt readiness
kræver for en kategori/kontrol skal være beskrevet i kontrakten, så de to ikke
kan drifte fra hinanden uden at et testfald falder.
"""

from tools import build_data_contract as gen
from analytics import readiness


def test_committed_contract_is_fresh():
    contract, _ = gen.build_contract()
    generated = gen.serialize(contract)
    with open(gen._OUT, encoding="utf-8") as f:
        committed = f.read()
    assert committed == generated, (
        "catalog/data_contract.json er forældet — kør: "
        "python tools/build_data_contract.py"
    )


def test_materiality_env_names_match_code():
    _, problems = gen.build_contract()
    assert not problems["materiality_env_mismatch"], (
        "MATERIALITY_RUN_CONFIG i tools/data_contract_data.py nævner env-navne, "
        "der ikke findes i analytics/materiality.py: "
        f"{problems['materiality_env_mismatch']}"
    )


def _all_contract_field_names(contract) -> set:
    names = set()
    for obj in contract["objekter"].values():
        names.update(f["navn"] for f in obj["felter"])
        if "sub_objekt" in obj:
            names.update(f["navn"] for f in obj["sub_objekt"]["felter"])
    return names


def test_readiness_signal_fields_present_in_contract():
    """Ethvert felt, readiness.py bruger som kørbarheds-signal for en kategori
    eller en specifik kontrol, skal være dokumenteret i datakontrakten."""
    contract, _ = gen.build_contract()
    contract_fields = _all_contract_field_names(contract)

    signal_fields = set(readiness.FIELD_INFO)
    for fields in readiness.CATEGORY_REQUIREMENTS.values():
        signal_fields.update(fields)
    for fields in readiness.CONTROL_REQUIREMENTS.values():
        signal_fields.update(fields)

    missing = sorted(signal_fields - contract_fields)
    assert not missing, (
        "readiness.py kræver felter, der ikke er beskrevet i "
        f"tools/data_contract_data.py: {missing}"
    )
