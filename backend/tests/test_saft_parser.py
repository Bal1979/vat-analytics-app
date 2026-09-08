"""
SAF-T-parser: produktions-input til motoren.

Dækker: kanonisk struktur fra en gyldig v2.1-lignende fil, best-effort på en
fejlmærket v1.0-fil (fuldt datasæt, forkert version), tolerance for manglende
sektioner, XML-hærdning (DOCTYPE afvises), at linjen bærer account_type +
standard_account_id (fundament for momsrelevans-scopet), at motoren kan køre på
SAF-T-outputtet, og at upload-routeren vælger den rigtige parser.

Alle fixtures er selvstændige (skrevet i testen) — ingen afhængighed af
søsterprojekternes filer.
"""

from analytics.engine import run_all_tests
from parsers import saft_parser
from parsers import upload_router


# Namespacet er DK-SAF-T — parseren er namespace-agnostisk (matcher på localname).
_NS = 'xmlns="urn:StandardAuditFile-Taxation-Financial:DK"'

SAFT_V21 = f"""<?xml version="1.0" encoding="UTF-8"?>
<AuditFile {_NS}>
  <Header>
    <AuditFileVersion>2.1</AuditFileVersion>
    <DefaultCurrencyCode>DKK</DefaultCurrencyCode>
    <Company><Name>Testfirma ApS</Name></Company>
    <SelectionCriteria>
      <SelectionStartDate>2024-01-01</SelectionStartDate>
      <SelectionEndDate>2024-12-31</SelectionEndDate>
    </SelectionCriteria>
  </Header>
  <MasterFiles>
    <GeneralLedgerAccounts>
      <Account>
        <AccountID>1000</AccountID>
        <AccountDescription>Salg af varer</AccountDescription>
        <AccountType>Sale</AccountType>
        <StandardAccountID>1010</StandardAccountID>
        <ClosingCreditBalance>50000.00</ClosingCreditBalance>
      </Account>
      <Account>
        <AccountID>6900</AccountID>
        <AccountDescription>Bankgaeld</AccountDescription>
        <AccountType>Liability</AccountType>
        <StandardAccountID>5800</StandardAccountID>
        <ClosingCreditBalance>20000.00</ClosingCreditBalance>
      </Account>
    </GeneralLedgerAccounts>
    <Customers>
      <Customer><CustomerID>K1</CustomerID><Name>Kunde A/S</Name>
        <Address><Country>DK</Country></Address>
        <TaxRegistration><TaxRegistrationNumber>DK12345678</TaxRegistrationNumber></TaxRegistration>
      </Customer>
    </Customers>
    <Suppliers>
      <Supplier><SupplierID>L1</SupplierID><Name>Leverandoer GmbH</Name>
        <Address><Country>DE</Country></Address>
        <TaxRegistration><TaxRegistrationNumber>DE123456789</TaxRegistrationNumber></TaxRegistration>
      </Supplier>
    </Suppliers>
    <TaxTable>
      <TaxTableEntry>
        <TaxCodeDetails><TaxCode>U25</TaxCode><Description>Udgaaende 25%</Description><TaxPercentage>25</TaxPercentage></TaxCodeDetails>
      </TaxTableEntry>
    </TaxTable>
  </MasterFiles>
  <GeneralLedgerEntries>
    <Journal>
      <JournalID>Sales</JournalID>
      <Transaction>
        <TransactionID>T1</TransactionID>
        <Period>1</Period>
        <TransactionDate>2024-01-15</TransactionDate>
        <GLPostingDate>2024-01-15</GLPostingDate>
        <CustomerID>K1</CustomerID>
        <Description>Salg faktura 1</Description>
        <Line>
          <RecordID>1</RecordID>
          <AccountID>1000</AccountID>
          <CreditAmount>20000.00</CreditAmount>
          <TaxInformation><TaxCode>U25</TaxCode><TaxPercentage>25</TaxPercentage><TaxAmount>5000.00</TaxAmount><TaxBase>20000.00</TaxBase></TaxInformation>
        </Line>
      </Transaction>
      <Transaction>
        <TransactionID>T2</TransactionID>
        <Period>2</Period>
        <TransactionDate>2024-02-10</TransactionDate>
        <GLPostingDate>2024-02-10</GLPostingDate>
        <Description>Afdrag banklaan</Description>
        <Line>
          <RecordID>1</RecordID>
          <AccountID>6900</AccountID>
          <CreditAmount>20000.00</CreditAmount>
        </Line>
      </Transaction>
    </Journal>
  </GeneralLedgerEntries>
</AuditFile>
"""

# Fejlmærket: deklarerer v1.0, men har fuldt datasæt, og AccountType er "Other"
# på alle konti (som de rigtige klientfiler) — best-effort skal stadig virke.
SAFT_MISLABELED_V1 = SAFT_V21.replace(
    "<AuditFileVersion>2.1</AuditFileVersion>",
    "<AuditFileVersion>1.0</AuditFileVersion>",
).replace("<AccountType>Sale</AccountType>", "<AccountType>Other</AccountType>") \
 .replace("<AccountType>Liability</AccountType>", "<AccountType>Other</AccountType>")

SAFT_HEADER_ONLY = f"""<?xml version="1.0" encoding="UTF-8"?>
<AuditFile {_NS}>
  <Header><AuditFileVersion>2.1</AuditFileVersion><DefaultCurrencyCode>DKK</DefaultCurrencyCode></Header>
</AuditFile>
"""

SAFT_WITH_DOCTYPE = f"""<?xml version="1.0"?>
<!DOCTYPE AuditFile [ <!ENTITY x "expanded"> ]>
<AuditFile {_NS}><Header><AuditFileVersion>2.1</AuditFileVersion></Header></AuditFile>
"""


def _write(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return str(p)


# --- Struktur --------------------------------------------------------------

def test_parse_v21_structure(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, info = saft_parser.parse_saft(path)
    assert data is not None
    s = info["sections"]
    assert s["version"] == "2.1"
    assert s["accounts"] == 2
    assert s["transactions"] == 2
    assert s["lines"] == 2
    assert data["header"]["company_name"] == "Testfirma ApS"
    assert data["header"]["currency"] == "DKK"


def test_lines_carry_account_type_and_standard_id(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, _ = saft_parser.parse_saft(path)
    # Find linjen på balancekontoen 6900.
    bank_line = next(l for t in data["transactions"] for l in t["lines"]
                     if l["account_id"] == "6900")
    assert bank_line["account_type"] == "Liability"
    assert bank_line["standard_account_id"] == "5800"


def test_master_data_enriches_lines(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, _ = saft_parser.parse_saft(path)
    sales_line = next(l for t in data["transactions"] for l in t["lines"]
                      if l["account_id"] == "1000")
    assert sales_line["customer_name"] == "Kunde A/S"
    assert sales_line["country"] == "DK"


def test_period_derived_from_posting_date(tmp_path):
    # SAF-T Period er "1"/"2" (regnskabsperiode); vi udleder kalendermåned af datoen.
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, _ = saft_parser.parse_saft(path)
    t1 = next(t for t in data["transactions"] if t["transaction_id"] == "T1")
    assert t1["period"] == "01"
    assert t1["period_year"] == "2024"


# --- Best-effort / tolerance ----------------------------------------------

def test_best_effort_on_mislabeled_v1(tmp_path):
    path = _write(tmp_path, "v1.xml", SAFT_MISLABELED_V1)
    data, info = saft_parser.parse_saft(path)
    assert data is not None
    assert info["sections"]["version"] == "1.0"        # deklareret (forkert)
    assert info["sections"]["transactions"] == 2        # men fuldt datasæt parses


def test_missing_sections_tolerated(tmp_path):
    path = _write(tmp_path, "hdr.xml", SAFT_HEADER_ONLY)
    data, info = saft_parser.parse_saft(path)
    assert data is not None
    assert info["sections"]["transactions"] == 0
    assert any("transaktioner" in w.lower() for w in info["warnings"])


def test_doctype_rejected(tmp_path):
    path = _write(tmp_path, "bomb.xml", SAFT_WITH_DOCTYPE)
    data, info = saft_parser.parse_saft(path)
    assert data is None
    assert info["errors"] and "DOCTYPE" in info["errors"][0].upper()


# --- Motoren kører på SAF-T ------------------------------------------------

def test_engine_runs_on_saft(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, _ = saft_parser.parse_saft(path)
    report = run_all_tests(data)
    assert isinstance(report["total_findings"], int)
    assert len(report["categories"]) == 12


def test_scope_suppresses_balance_line_via_saft_account_type(tmp_path):
    # T2 er en kreditpostering på en Liability-konto uden moms -> kontrol 80 ville
    # normalt fyre, men momsrelevans-scopet undertrykker den, fordi AccountType er
    # en balancekonto. Beviser scopet end-to-end fra SAF-T.
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    data, _ = saft_parser.parse_saft(path)
    fired = {f["test_id"] for f in run_all_tests(data)["all_findings"]}
    assert 80 not in fired


# --- Upload-routing --------------------------------------------------------

def test_router_detects_saft_by_extension(tmp_path):
    path = _write(tmp_path, "any.xml", SAFT_V21)
    assert upload_router.is_saft(path)


def test_router_detects_saft_by_sniff(tmp_path):
    # Atypisk endelse, men indholdet er en AuditFile -> sniff fanger den.
    path = _write(tmp_path, "udtraek.txt", SAFT_V21)
    assert upload_router.is_saft(path)


def test_router_parse_upload_returns_canonical(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    adapted = upload_router.parse_upload(path)
    assert not adapted.get("parse_info", {}).get("error")
    assert len(adapted["transactions"]) == 2


def test_router_preview_saft(tmp_path):
    path = _write(tmp_path, "v21.xml", SAFT_V21)
    prev = upload_router.preview_upload(path)
    assert prev["type"] == "saft"
    assert prev["sections"]["transactions"] == 2
