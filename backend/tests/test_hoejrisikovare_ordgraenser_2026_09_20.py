"""
Kalibrering "højrisikovare-nøgleord/leverandørnavne" (2026-09-20, Bal-godkendt).

Baggrund (kunde-empiri — INGEN kundenavne/-tekster i denne fil, kun
SYNTETISKE selskabsnavne der reproducerer de fundne MØNSTRE, se
docs/CHANGELOG.md for de kvantificerede tal): den brede substring-matching i
`vat_rules.MTIC_HIGH_RISK_KEYWORDS` (kontrol 84/87/88) ramte også
leverandør-/betalingsservicenavne, fordi posteringsteksten på denne type
linjer (udenlandsk leverandør uden momsnr., en ERP-kildes frie
posteringstekst) OFTE ER modpartens navn. Observerede mønstre: et nøgleord
som substring i et stednavn der indgår i et forsyningsselskabs navn, i en
udbredt betalingsapp/flere selskabsnavne, i et engelsk branchebegreb for en
overfladebehandlingsproces. Fjernede kunde-datasættets ENESTE "critical"-
fund på kontrol 84 (en udenlandsk leverandør, hvis selskabsnavn indeholdt
"mobil" som substring).

Fix: `vat_rules.mtic_high_risk_match` = ordgrænse-match på
MTIC_HIGH_RISK_KEYWORDS (undgår compound-word-/navne-støj) ELLER
substring-match på MTIC_HIGH_RISK_COMPOUND_TERMS ("mobiltelefon" -- allerede
éntydig, skal fortsat ramme danske bøjningsformer som "mobiltelefonER").
"mobil" er FJERNET fra nøgleordssættet (0 reelle vare-hits på kunde 2s data,
100% navne-/servicenavne-støj).

Kendt, dokumenteret residual (IKKE løst her): et selskabsnavn hvor
nøgleordet optræder som et LØSREVET, selvstændigt ord med mellemrum omkring
(fx et valutavekslingsselskabs eget navn) matcher fortsat -- ordgrænser
løser kun sammensatte ord/navne uden mellemrum, ikke ægte firmanavne der
bærer et løsrevet nøgleord. Se docs/CHANGELOG.md for den fulde
kalibreringsrapport.
"""

from analytics import vat_rules as vr
from analytics.categories import cat11_fraud_mtic as cat11
from validation.builders import mk_data, mk_txn, mk_line


def _line(**kw):
    defaults = dict(debit_amount=60000.0, country="DE", vat_number="",
                     tax_code="I25", description="")
    defaults.update(kw)
    return mk_line(**defaults)


# === vat_rules.mtic_high_risk_match — enhedstests ============================

def test_compound_word_noise_does_not_match():
    """Nøgleordet som substring i et sammensat ord/navn UDEN mellemrum
    omkring roden matcher IKKE (ordgrænser) -- syntetiske gengivelser af
    kunde 2s fundne mønstre."""
    assert not vr.mtic_high_risk_match("Betaling via SyntMobilePay")
    assert not vr.mtic_high_risk_match("SyntMobilhus A/S")
    assert not vr.mtic_high_risk_match("isSyntMobile AB")
    assert not vr.mtic_high_risk_match("SyntGuldborgby Vand A/S")
    assert not vr.mtic_high_risk_match("Synt-Tec Plating A/S")
    assert not vr.mtic_high_risk_match("Afskrivning SyntIoTKomponenter-Udgaaet")


def test_genuine_compound_goods_word_still_matches():
    """Den ægte, éntydige varetekst 'mobiltelefon(er/en)' matcher fortsat --
    bøjningsformer inkluderet (dansk plural -er)."""
    assert vr.mtic_high_risk_match("Køb af mobiltelefoner")
    assert vr.mtic_high_risk_match("Salg af 1 mobiltelefon")
    assert vr.mtic_high_risk_match("Moms på mobiltelefonen efterangivelse")


def test_standalone_keyword_still_matches():
    """Et nøgleord som et HELT, selvstændigt ord matcher fortsat (ægte
    vare-tekst-scenarie, fx direkte handel med guld/sølv/platin/chips)."""
    assert vr.mtic_high_risk_match("Salg af 1 kg guld")
    assert vr.mtic_high_risk_match("Køb af sølv til produktion")
    assert vr.mtic_high_risk_match("Investering i platin")
    assert vr.mtic_high_risk_match("10.000 stk chip til lager")


def test_standalone_name_residual_is_known_and_unfixed():
    """Dokumenteret residual: et selskabsnavn hvor nøgleordet står som et
    løsrevet, selvstændigt ord (mellemrum omkring) matcher FORTSAT -- ikke
    løst af ordgrænser alene. Ingen kunde-specifik hack indført."""
    assert vr.mtic_high_risk_match("Synt Chip Exchange Gmbh")


# === Kontrol 84 — regressionstest med syntetiske selskabsnavne ==============

def test_84_supplier_name_containing_keyword_no_longer_upgrades_to_critical():
    """Reproducerer kunde 2s eneste 'critical'-fund syntetisk: en udenlandsk
    leverandør uden momsnr. og med højt beløb, hvor selskabsNAVNET (ikke en
    varetekst) indeholder 'mobil' som substring ('SyntMobile AB'). Fundet
    havde tidligere 3 risikofaktorer (udenlandsk, højt beløb, højrisikovare)
    og blev 'critical' -- efter fixet mister det højrisikovare-faktoren og
    rammer IKKE længere 3-faktor-tærsklen: fundet forsvinder helt (ikke kun
    et severity-skift ned til 'high'). Gyldigt SE-momsnr. sat, så
    'manglende/ugyldigt momsnr' ikke selv bidrager til tærsklen (samme
    faktor-sammensætning som det virkelige fund: udenlandsk + højt beløb +
    højrisikovare -- eksakt 3, dvs. lige på tærsklen)."""
    line = _line(country="SE", vat_number="SE123456789012")
    data = mk_data(mk_txn(line, description="SyntMobile AB, *EUR* *E1G*"))
    findings = cat11.test_84_missing_trader(data, {})
    assert findings == []


def test_84_genuine_high_risk_goods_purchase_stays_critical():
    """Regression: en ægte vare-tekst ('mobiltelefoner') med samme
    risikofaktor-kombination forbliver 'critical' -- fixet ændrer ikke den
    klassiske MTIC-profil."""
    line = _line()
    data = mk_data(mk_txn(line, description="Køb af mobiltelefoner"))
    findings = cat11.test_84_missing_trader(data, {})
    assert len(findings) == 1
    assert findings[0]["severity"] == "critical"
    assert "højrisikovare" in findings[0]["transactions"][0]["risk_flags"]


# === Kontrol 87/88 — regressionstest med syntetiske selskabsnavne ===========

def test_87_supplier_name_does_not_fire():
    """Et selskabsnavn, der tidligere matchede via substring, udløser ikke
    længere kontrol 87 (Højrisikovare)."""
    line = mk_line(debit_amount=30000.0)
    data = mk_data(mk_txn(line, description="SyntGuldborgby Varme A/S"))
    assert cat11.test_87_high_risk_goods(data) == []


def test_87_genuine_goods_text_still_fires():
    line = mk_line(debit_amount=30000.0)
    data = mk_data(mk_txn(line, description="Køb af mobiltelefoner"))
    findings = cat11.test_87_high_risk_goods(data)
    assert len(findings) == 1


def test_88_supplier_name_pair_does_not_fire():
    """Nul-margin-parringen (kontrol 88) kræver højrisikovare-teksten på
    BEGGE ben -- et selskabsnavn på købs-/salgslinjen udløser ikke længere
    et fund."""
    data = mk_data([
        mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1",
               description="SyntMobilhus A/S"),
        mk_txn(mk_line(credit_amount=50000.0), transaction_id="S-1",
               description="SyntMobilhus A/S"),
    ])
    assert cat11.test_88_zero_margin(data) == []


def test_88_genuine_goods_pair_still_fires():
    data = mk_data([
        mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1",
               description="Køb af mobiltelefoner"),
        mk_txn(mk_line(credit_amount=50000.0), transaction_id="S-1",
               description="Salg af mobiltelefoner"),
    ])
    findings = cat11.test_88_zero_margin(data)
    assert len(findings) == 1
