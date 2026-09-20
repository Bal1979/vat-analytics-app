"""
Kontrol 84-efterforskningen + K5-b (2026-09-20) — opfølgeren på K4's åbne
tråd (leverandørkartotek-datakvalitet bag kontrol 84's residual) og
landetabel-rundens 70/84-OBS. Se docs/CHANGELOG.md for den fulde empiriske
fordeling (kunde 2/IFS: kontrol 84 330 → 24; kontrol 70 50.804 → 9.438;
BC-v5 byte-identisk).

Tre generiske, strukturelle ændringer — INGEN kundenavne/-numre i koden:

1. Momskode-værn (K5-b) for kontrol 70 + 84: linjer helt uden momskode er
   uden for momsscope (afregning/bank/valutakurs/koncernmellemregning) og
   kan hverken mangle erhvervelsesmoms eller bære et momsfradrag.
2. Kollektivnummer-undtagelsen (kontrol 84): et EU-format-ugyldigt nummer,
   der optræder med >= CONTROL_84_SHARED_VAT_MIN_DESCS distinkte
   beskrivelsestekster, er en samle-/kollektivkonto i kartoteket — ikke én
   skjult handelspartner. Dækkes fortsat af kontrol 28 (format/datakvalitet).
3. Severity pr. fund (kontrol 84): "critical" kun med højrisikovare-faktoren
   (klassisk MTIC-profil); ellers "high" (kartotek-datakvalitetsklassen).
"""

from analytics.categories import cat09_reverse_charge as cat09
from analytics.categories import cat11_fraud_mtic as cat11
from validation.builders import mk_data, mk_txn, mk_line


def _line(**kw):
    defaults = dict(debit_amount=60000.0, country="DE", vat_number="",
                     tax_code="I25", description="")
    defaults.update(kw)
    return mk_line(**defaults)


# === 1) Momskode-værn (K5-b) =================================================

def test_84_codeless_line_is_out_of_scope():
    """En linje helt uden momskode bærer intet momsfradrag -- ingen missing
    trader-eksponering, uanset hvor mange risikofaktorer der ellers matcher."""
    line = _line(tax_code="")
    data = mk_data(mk_txn(line, description="Køb af mobiltelefoner"))
    assert cat11.test_84_missing_trader(data, {}) == []


def test_84_coded_line_still_fires():
    """Samme linje MED momskode rammes fortsat (regression for værnet)."""
    line = _line()
    data = mk_data(mk_txn(line, description="Køb af mobiltelefoner"))
    findings = cat11.test_84_missing_trader(data, {})
    assert len(findings) == 1


def test_70_codeless_line_is_out_of_scope():
    """EU-linje uden moms OG uden momskode: afregnings-/balanceklassen --
    springes over (kendt, dokumenteret residualrisiko, se docstring)."""
    line = mk_line(debit_amount=1000.0, country="DE", tax_code="", tax_amount=0.0)
    assert cat09.test_70_eu_service_no_rc(mk_data(mk_txn(line)), {}) == []


def test_70_coded_unmarked_line_still_fires():
    """Kontrollens kernepopulation -- momskodet, men hverken RC-markeret
    eller momsbelagt -- er uberørt af værnet."""
    line = mk_line(debit_amount=1000.0, country="DE", tax_code="X0", tax_amount=0.0)
    findings = cat09.test_70_eu_service_no_rc(mk_data(mk_txn(line)), {})
    assert len(findings) == 1


def test_70_rc_marked_line_is_clean():
    line = mk_line(debit_amount=1000.0, country="DE", tax_code="RC25", tax_amount=0.0)
    assert cat09.test_70_eu_service_no_rc(mk_data(mk_txn(line)), {}) == []


# === 2) Kollektivnummer-undtagelsen ==========================================

def _shared_vat_data(n_descs, vat_number="DE9208100"):
    """n_descs transaktioner med SAMME ugyldige momsnummer men hver sin
    beskrivelsestekst (kollektivkonto-mønstret fra kunde 2s kartotek)."""
    txns = [
        mk_txn(_line(vat_number=vat_number),
               transaction_id=f"T-{i}", description=f"Leverandør {i} GmbH")
        for i in range(n_descs)
    ]
    return mk_data(txns)


def test_84_shared_invalid_number_is_collective_vendor_artifact():
    """Samme ugyldige nummer på 3 distinkte beskrivelsestekster (default-
    tærsklen): 'ugyldigt momsnr'-faktoren bortfalder, og uden en tredje
    faktor er der intet fund."""
    data = _shared_vat_data(3)
    assert cat11.test_84_missing_trader(data, {}) == []


def test_84_invalid_number_with_one_name_still_fires():
    """En ægte missing trader har ét navn -- under tærsklen rammes nummeret
    fortsat af 'ugyldigt momsnr'-faktoren."""
    data = _shared_vat_data(1)
    findings = cat11.test_84_missing_trader(data, {})
    assert len(findings) == 1
    assert "ugyldigt momsnr" in findings[0]["transactions"][0]["risk_flags"]


def test_84_desc_variants_of_same_text_do_not_reach_threshold():
    """Whitespace-/case-varianter af samme tekst normaliseres til ÉN
    beskrivelse og udløser ikke undtagelsen."""
    txns = [
        mk_txn(_line(vat_number="DE9208100"), transaction_id=f"T-{i}", description=d)
        for i, d in enumerate(["ACME GmbH", "acme  gmbh", " ACME GMBH "])
    ]
    findings = cat11.test_84_missing_trader(mk_data(txns), {})
    assert len(findings) == 3  # én pr. transaktion -- undtagelsen greb IKKE


def test_84_missing_number_is_not_affected_by_collective_logic():
    """Undtagelsen gælder KUN udfyldte, ugyldige numre -- tomme numre
    ('manglende momsnr') grupperes ikke og rammes fortsat."""
    txns = [
        mk_txn(_line(vat_number=""), transaction_id=f"T-{i}",
               description=f"Leverandør {i} ApS")
        for i in range(5)
    ]
    findings = cat11.test_84_missing_trader(mk_data(txns), {})
    assert len(findings) == 5


# === 3) Severity pr. fund ====================================================

def test_84_high_risk_goods_profile_stays_critical():
    """Den klassiske MTIC-profil (højrisikovare-faktoren, jf. BC-v5's to
    ægte fund: mobiltelefoner) forbliver 'critical'."""
    line = _line()
    data = mk_data(mk_txn(line, description="Køb af mobiltelefoner"))
    findings = cat11.test_84_missing_trader(data, {})
    assert len(findings) == 1
    assert findings[0]["severity"] == "critical"


def test_84_data_quality_profile_is_high_not_critical():
    """Uden højrisikovare-faktoren (udenlandsk + manglende nr + beløb) er
    populationen empirisk kartotek-datakvalitet -- 'high', ikke 'critical'."""
    line = _line()
    data = mk_data(mk_txn(line, description="Almindeligt varekøb"))
    findings = cat11.test_84_missing_trader(data, {})
    assert len(findings) == 1
    assert findings[0]["severity"] == "high"
    assert "højrisikovare" not in findings[0]["transactions"][0]["risk_flags"]
