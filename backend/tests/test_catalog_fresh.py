"""
Katalog-drift-gate: den committede catalog/rules.json skal matche det, generatoren
producerer fra koden. Fanger et forældet katalog (kontrol ændret uden regenerering).
"""

from tools import build_rules_catalog as gen


def test_committed_catalog_is_fresh():
    catalog, _ = gen.build_catalog()
    generated = gen.serialize(catalog)
    with open(gen._OUT, encoding="utf-8") as f:
        committed = f.read()
    assert committed == generated, (
        "catalog/rules.json er forældet — kør: python tools/build_rules_catalog.py"
    )


def test_catalog_integrity():
    _, problems = gen.build_catalog()
    assert not problems["missing"], f"Manglende test_id: {problems['missing']}"
    assert not problems["dupes"], f"Dublerede test_id: {problems['dupes']}"
    assert not problems["unknown_notes"], f"rule_notes peger på ukendte test_id: {problems['unknown_notes']}"
