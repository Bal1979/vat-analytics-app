"""
Selvkonsistens-gaten — "momskonto-krydstjekket" (byggetrin ~12, Bal-godkendt
2026-09-23). Se analytics/self_consistency_gate.py for baggrunden
(kontrol 82-sagen, 2026-09-22): motorens beregnede rubrikker (SAMME
kilde-af-sandhed som kontrol 82's _compute_period_rubrics, INGEN parallel
logik) krydstjekkes mod de faktiske posteringer på kundens egne momskonti
(tax_table[].sales_vat_account/purchase_vat_account/
reverse_charge_vat_account) — uden et ekspert-facit.

Alle koder/tal/konti herunder er SYNTETISKE (ingen kundedata) — samme
disciplin som resten af testsuiten.
"""

from analytics import materiality
from analytics import self_consistency_gate as scg
from analytics.categories import cat10_vat_reconciliation as cat10
from validation.builders import mk_data, mk_line, mk_txn


def _vat_setup_extra(entries):
    return {"header": {"vat_setup_loaded": True}, "tax_table": entries}


# --- "Ikke målbar" -----------------------------------------------------------

def test_ikke_maalbar_when_vat_setup_has_no_account_references():
    """Kunde 2/IFS-mønstret: vat_setup uden konto-referencefelter -- gaten må
    ALDRIG gætte, kun rapportere ærligt at den ikke kan måle."""
    extra = _vat_setup_extra([
        {"tax_code": "E0G", "tax_percentage": 0.0, "setup_matched": True},
    ])
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                                  tax_code="E0G", supply_direction="purchase"),
                          period="03", period_year="2024"), **extra)
    result = scg.evaluate(data)
    assert result["status"] == scg.STATUS_NOT_MEASURABLE
    assert all(r["status"] == "ikke_maalbar" for r in result["rubrikker"].values())
    assert all(r["konti"] == [] for r in result["rubrikker"].values())


def test_ikke_maalbar_with_empty_tax_table():
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)))
    result = scg.evaluate(data)
    assert result["status"] == scg.STATUS_NOT_MEASURABLE


# --- Match ("bestået") --------------------------------------------------------

def test_bestaaet_when_purchase_and_sale_rubrics_match_their_gl_accounts():
    extra = _vat_setup_extra([
        {"tax_code": "STANDARD|I25", "tax_percentage": 25.0,
         "sales_vat_account": "961100", "purchase_vat_account": "963100",
         "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            # Den momskodede købslinje (kilde til den BEREGNEDE input_vat-rubrik).
            mk_line(account_id="4000", debit_amount=1000.0, tax_amount=250.0,
                    tax_code="STANDARD|I25", supply_direction="purchase"),
            # Den FAKTISKE GL-postering på købsmomskontoen (intet tax_code/
            # tax_amount -- BC/NAV-mønstret: beløbet ligger i debit_amount).
            mk_line(account_id="963100", debit_amount=250.0),
        ], transaction_id="T1", period="03", period_year="2024"),
        mk_txn([
            mk_line(account_id="4000", credit_amount=2000.0, tax_amount=-500.0,
                    tax_code="STANDARD|I25", supply_direction="sale"),
            mk_line(account_id="961100", credit_amount=500.0),
        ], transaction_id="T2", period="03", period_year="2024"),
    ], **extra)

    result = scg.evaluate(data)
    assert result["status"] == scg.STATUS_OK
    assert result["rubrikker"]["input_vat"] == {
        "konti": ["963100"], "maalbar": True,
        "beregnet_aar": 250.0, "bogfoert_aar": 250.0, "difference_aar": 0.0,
        "status": "match",
    }
    assert result["rubrikker"]["output_vat"]["status"] == "match"
    assert result["rubrikker"]["output_vat"]["beregnet_aar"] == 500.0
    assert result["rubrikker"]["output_vat"]["bogfoert_aar"] == 500.0
    # rc_services/energy_tax har ingen koder i denne opsætning -- ikke målbare,
    # men det trækker IKKE det samlede resultat ned, når andre rubrikker er
    # målbare og matcher.
    assert result["rubrikker"]["rc_services"]["maalbar"] is False
    assert result["perioder"][0]["periode"] == "2024-03"
    assert result["perioder"][0]["rubrikker"]["input_vat"]["status"] == "match"


def test_settlement_postings_are_excluded_from_the_gl_side():
    """VAT-afregningsbatchen nulstiller kontoen periodisk -- den slags
    posteringer skal IKKE indgå i krydstjekket (ville ellers skævvride
    sammenligningen). To signaler dækkes: supply_direction='settlement' OG
    BC/NAV's source_code='MOMSAFREGN' (empirisk: ikke alle afregnings-
    posteringer bærer det første, se modulets docstring)."""
    extra = _vat_setup_extra([
        {"tax_code": "STANDARD|I25", "tax_percentage": 25.0,
         "purchase_vat_account": "963100", "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            mk_line(account_id="4000", debit_amount=1000.0, tax_amount=250.0,
                    tax_code="STANDARD|I25", supply_direction="purchase"),
            mk_line(account_id="963100", debit_amount=250.0),
            # De to afregnings-varianter -- begge skal udelades, ellers ville
            # kontoens nettobeløb blive skævvredet med 999.999,99.
            mk_line(account_id="963100", credit_amount=999999.99, supply_direction="settlement"),
            mk_line(account_id="963100", credit_amount=999999.99, source_code="MOMSAFREGN"),
        ], transaction_id="T1", period="03", period_year="2024"),
    ], **extra)

    result = scg.evaluate(data)
    assert result["rubrikker"]["input_vat"]["status"] == "match"
    assert result["rubrikker"]["input_vat"]["bogfoert_aar"] == 250.0


def test_energy_tax_measured_on_its_own_account_and_excluded_from_input_vat():
    """Elafgift har sin egen rubrik/konto (BC-empiri: samme mekanisme som
    sales_vat_account/purchase_vat_account, blot en anden kontoværdi for den
    energy_tax-klassificerede kode) og må hverken forurene input_vat-
    beregningen eller -kontoen."""
    extra = _vat_setup_extra([
        {"tax_code": "STANDARD|I25", "tax_percentage": 25.0,
         "purchase_vat_account": "963100", "setup_matched": True},
        {"tax_code": "DOMESTIC|ELECTRICITY_TAX", "tax_percentage": 0.0,
         "vat_calculation_type": "Full VAT", "purchase_vat_account": "968100",
         "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            mk_line(account_id="4000", debit_amount=1000.0, tax_amount=250.0,
                    tax_code="STANDARD|I25", supply_direction="purchase"),
            mk_line(account_id="963100", debit_amount=250.0),
            mk_line(account_id="4100", debit_amount=400.0, tax_amount=100.0,
                    tax_code="DOMESTIC|ELECTRICITY_TAX", supply_direction="purchase"),
            mk_line(account_id="968100", debit_amount=100.0),
        ], transaction_id="T1", period="03", period_year="2024"),
    ], **extra)

    result = scg.evaluate(data)
    assert result["rubrikker"]["input_vat"]["beregnet_aar"] == 250.0  # IKKE 350
    assert result["rubrikker"]["input_vat"]["status"] == "match"
    assert result["rubrikker"]["energy_tax"] == {
        "konti": ["968100"], "maalbar": True,
        "beregnet_aar": 100.0, "bogfoert_aar": 100.0, "difference_aar": 0.0,
        "status": "match",
    }


# --- Afvigelse -----------------------------------------------------------------

def test_material_mismatch_between_computed_and_booked_is_flagged():
    extra = _vat_setup_extra([
        {"tax_code": "STANDARD|I25", "tax_percentage": 25.0,
         "purchase_vat_account": "963100", "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            mk_line(account_id="4000", debit_amount=4000.0, tax_amount=1000.0,
                    tax_code="STANDARD|I25", supply_direction="purchase"),
            # Kontoen bærer kun 600 -- 400 forskel, langt over både den
            # absolutte tolerance og materialitetsgrænsen.
            mk_line(account_id="963100", debit_amount=600.0),
        ], transaction_id="T1", period="03", period_year="2024"),
    ], **extra)

    result = scg.evaluate(data)
    assert result["status"] == scg.STATUS_DEVIATION
    assert result["rubrikker"]["input_vat"]["status"] == "afvigelse"
    assert result["rubrikker"]["input_vat"]["difference_aar"] == 400.0
    assert "input_vat" in result["message"] or "Indgående moms" in result["message"]


def test_immaterial_rounding_difference_is_still_a_match():
    extra = _vat_setup_extra([
        {"tax_code": "STANDARD|I25", "tax_percentage": 25.0,
         "purchase_vat_account": "963100", "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            mk_line(account_id="4000", debit_amount=4000.0, tax_amount=1000.0,
                    tax_code="STANDARD|I25", supply_direction="purchase"),
            mk_line(account_id="963100", debit_amount=1000.01),
        ], transaction_id="T1", period="03", period_year="2024"),
    ], **extra)

    result = scg.evaluate(data)
    assert result["status"] == scg.STATUS_OK
    assert result["rubrikker"]["input_vat"]["status"] == "match"


# --- SYNTETISK REGRESSIONSTEST: simulerer gårsdagens kontrol 82-fejl ----------
#
# Baggrund (kontrol 82-sagen, 2026-09-22): en tidligere version af
# _compute_period_rubrics ignorerede fradragsprocenten (non_deductible_vat_pct)
# på fradragssiden og talte HELE momsbeløbet med som fradragsberettiget --
# afveg 326 t.kr. fra virkeligheden, og det blev kun opdaget via kundens EGEN
# 3-vejs-afstemning. Dette er selvkonsistens-gatens raison d'être: den skal
# opdage PRÆCIS denne klasse af fejl UDEN et ekspert-facit. Testen simulerer
# fejlen ved at monkeypatche _deductible_fraction til (fejlagtigt) altid at
# returnere 1.0 -- IKKE ved at rulle den rigtige fix tilbage i produktionskoden.

def test_synthetic_regression_flags_reintroduced_deduction_bug(monkeypatch):
    extra = _vat_setup_extra([
        {"tax_code": "DOMESTIC|REDUCED_PRIVATE_VAT_LIKE", "tax_percentage": 25.0,
         "non_deductible_vat_pct": 40.0, "purchase_vat_account": "963100",
         "setup_matched": True},
    ])
    data = mk_data([
        mk_txn([
            mk_line(account_id="4000", debit_amount=4000.0, tax_amount=1000.0,
                    tax_code="DOMESTIC|REDUCED_PRIVATE_VAT_LIKE", supply_direction="purchase"),
            # Den FAKTISKE bogføring afspejler korrekt kun den fradrags-
            # berettigede andel (60% af 1.000 = 600) -- ERP'et bogfører ikke
            # mere, end kunden reelt må trække fra.
            mk_line(account_id="963100", debit_amount=600.0),
        ], transaction_id="T1", period="03", period_year="2024"),
    ], **extra)

    # FØR (fixet, nuværende produktionskode): motoren regner selv 60% ud --
    # matcher den faktiske bogføring. Gaten er tavs.
    result_fixed = scg.evaluate(data)
    assert result_fixed["status"] == scg.STATUS_OK
    assert result_fixed["rubrikker"]["input_vat"]["beregnet_aar"] == 600.0
    assert result_fixed["rubrikker"]["input_vat"]["status"] == "match"

    # Simulér gårsdagens fejl: fradragsprocenten ignoreres (altid 100%
    # fradrag), PRÆCIS FEJL 2's symptom fra 2026-09-22.
    monkeypatch.setattr(cat10, "_deductible_fraction", lambda tax_code, setup_by_code: 1.0)
    result_buggy = scg.evaluate(data)
    assert result_buggy["rubrikker"]["input_vat"]["beregnet_aar"] == 1000.0  # den genindførte fejl
    assert result_buggy["status"] == scg.STATUS_DEVIATION
    assert result_buggy["rubrikker"]["input_vat"]["status"] == "afvigelse"
    assert result_buggy["rubrikker"]["input_vat"]["difference_aar"] == 400.0


# --- Materialitetskonstanterne er konfigurerbare (samme mønster som resten
# af materiality.py -- ikke hårdkodede magiske tal) --------------------------

def test_materiality_constants_are_env_overridable(monkeypatch):
    monkeypatch.setenv("MATERIALITY_SELF_CONSISTENCY_TOLERANCE", "5.0")
    monkeypatch.setenv("MATERIALITY_SELF_CONSISTENCY_MATERIALITY_PCT", "2.5")
    monkeypatch.setenv("MATERIALITY_SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES", "AFREGNING")
    import importlib
    from analytics import materiality as mat
    importlib.reload(mat)
    try:
        assert mat.SELF_CONSISTENCY_TOLERANCE == 5.0
        assert mat.SELF_CONSISTENCY_MATERIALITY_PCT == 2.5
        assert mat.SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES == ["AFREGNING"]
    finally:
        for var in ("MATERIALITY_SELF_CONSISTENCY_TOLERANCE",
                    "MATERIALITY_SELF_CONSISTENCY_MATERIALITY_PCT",
                    "MATERIALITY_SELF_CONSISTENCY_SETTLEMENT_SOURCE_CODES"):
            monkeypatch.delenv(var, raising=False)
        importlib.reload(mat)


def test_compute_period_rubrics_public_alias_matches_private_function():
    data = mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0,
                                  tax_code="STANDARD|I25", supply_direction="purchase"),
                          period="03", period_year="2024"))
    assert cat10.compute_period_rubrics(data) == cat10._compute_period_rubrics(data)
