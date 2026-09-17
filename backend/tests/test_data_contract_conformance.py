"""
Skema-konformans mellem BEGGE parser-veje og catalog/data_contract.json.

Trin 3 i byggerækkefølgen (BALAI-dataflow-arkitektur.md §7): "Forson de tre
input-kontrakter ... mod den nye kontrakt — skema-valideret, ikke kun efter
konvention." Denne test beviser skema-konformans i CI — den håndhæver INTET
i selve parserne/adapteren (kontrakten forbliver deskriptiv, jf. §7, byggetrin
2, og tools/data_contract_data.py's docstring).

For hvert felt i kontrakten, pr. objekt, checkes på repræsentative fixtures:
  * FELT TIL STEDE, når ``kilder.<vej>`` for feltet ikke er ``False`` — en
    ``False``-markering betyder netop, at kontrakten IKKE lover feltet fra den
    vej (fx ship_from_country på SAF-T-vejen), så fravær der er per definition
    ikke en konformans-fejl.
  * TYPE KORREKT (string/number/int/list), når feltet er til stede og
    kilde-flaget er ``True`` eller ``"partial"`` (en reel, ikke-gættet værdi
    er lovet). ``None`` er altid gyldigt (nullable/"intet signal"), uanset
    kontraktens type-streng.

Fixtures er selvstændige og dækker de fleste felter i kontrakten, inkl. de
nye kolonne-aliaser fra trin 3 (GAP-03/04/05) og SAF-T-siden af GAP-07/09.
"""

import csv
import json

import openpyxl

from parsers.excel_parser import parse_excel
from parsers.data_adapter import adapt_excel_to_saft
from parsers import saft_parser
from parsers import canonical_parser
from tools import build_data_contract as gen


# --- Type-tjek ---------------------------------------------------------------

_TYPE_CHECKERS = {
    "string": lambda v: isinstance(v, str),
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "list": lambda v: isinstance(v, list),
    "object": lambda v: isinstance(v, dict),
}


def _type_ok(contract_type: str, value) -> bool:
    if value is None:
        # Nullable/"intet signal" er altid gyldigt — kontraktens type-streng
        # beskriver formen på en REEL værdi, ikke om feltet må mangle indhold.
        return True
    base = contract_type.split(" ")[0].split("[")[0].split("{")[0]
    checker = _TYPE_CHECKERS.get(base)
    if checker is None:
        return True
    return checker(value)


def _instances_for(obj_name, data):
    if obj_name in ("header", "summary"):
        inst = data.get(obj_name)
        return [inst] if inst is not None else []
    return data.get(obj_name, [])


def _assert_object_conforms(obj_label, felter, instances, source):
    assert instances, f"{source}: ingen instanser af {obj_label} at teste mod fixturen"
    for field in felter:
        navn = field["navn"]
        kilde_flag = field["kilder"].get(source)
        if kilde_flag is False:
            continue  # kontrakten lover intet -- fravær er ikke en fejl her
        for inst in instances:
            assert navn in inst, (
                f"{source}: {obj_label}.{navn} mangler i parser-output "
                f"(kontrakt: kilder.{source}={kilde_flag!r})"
            )
            assert _type_ok(field["type"], inst[navn]), (
                f"{source}: {obj_label}.{navn}={inst[navn]!r} matcher ikke "
                f"kontraktens type {field['type']!r}"
            )


def _assert_canonical_conforms(canonical: dict, source: str, contract: dict):
    objekter = contract["objekter"]
    for obj_name, obj in objekter.items():
        _assert_object_conforms(
            obj_name, obj["felter"], _instances_for(obj_name, canonical), source
        )
        if "sub_objekt" in obj:
            sub = obj["sub_objekt"]
            lines = [
                l for t in canonical.get("transactions", []) for l in t.get("lines", [])
            ]
            _assert_object_conforms(
                f"{obj_name}.{sub['navn']}", sub["felter"], lines, source
            )


def _assert_canonical_source_conforms(canonical: dict, contract: dict):
    """Samme konformans-check som ``_assert_canonical_conforms`` ovenfor (navn
    bevidst forskelligt for ikke at kollidere med den funktion, der validerer
    excel/saft), men for den kanoniske CSV-vej (byggetrin 8).

    Forskel fra excel/saft: den kanoniske vej lover STRUKTURELT INTET for hele
    objekter (suppliers/customers, jf. GAP-11) -- kilder.canonical er False for
    ALLE felter på de objekter. At kræve mindst én fixture-instans dér (som den
    delte ``_assert_object_conforms`` gør) ville tvinge en fabrikeret kunde-/
    leverandørrække ind i fixturen for et objekt, kontrakten selv siger den
    kanoniske vej aldrig udfylder -- så disse objekter springes bevidst helt
    over, når intet felt er lovet.
    """
    objekter = contract["objekter"]
    for obj_name, obj in objekter.items():
        felter = obj["felter"]
        if any(f["kilder"].get("canonical") is not False for f in felter):
            _assert_object_conforms(
                obj_name, felter, _instances_for(obj_name, canonical), "canonical"
            )
        if "sub_objekt" in obj:
            sub = obj["sub_objekt"]
            if any(f["kilder"].get("canonical") is not False for f in sub["felter"]):
                lines = [
                    l for t in canonical.get("transactions", []) for l in t.get("lines", [])
                ]
                _assert_object_conforms(
                    f"{obj_name}.{sub['navn']}", sub["felter"], lines, "canonical"
                )


# --- Fixtures ----------------------------------------------------------------

def _write_excel_fixture(tmp_path):
    """Én række, der rammer stort set alle felter kontrakten kender til Excel-
    vejen — inkl. de nye kolonne-aliaser fra trin 3 (customer_country/
    customer_vat_number, account_type/standard_account_id, opening_balance/
    closing_balance)."""
    headers = [
        "transaction_id", "date", "account_id", "account_description",
        "description", "debit", "credit", "vat_amount", "vat_code",
        "vat_rate", "journal_id", "invoice_number", "supplier_id",
        "supplier_name", "customer_id", "customer_name", "currency",
        "country", "ship_from_country", "ship_to_country", "vat_number",
        "document_date", "tax_base", "period", "year",
        "customer_country", "customer_vat_number",
        "account_type", "standard_account_id",
        "opening_balance", "closing_balance",
    ]
    row = [
        "T1", "2024-03-15", "1000", "Salgskonto", "Salg af varer",
        0.0, 1250.0, 250.0, "U25", 25.0, "SALES",
        "F-100", "L1", "Leverandoer ApS", "K1", "Kunde A/S",
        "DKK", "DE", "DK", "DE", "DE123456789",
        "2024-03-10", 1000.0, "3", "2024",
        "DE", "DE123456789",
        "Revenue", "1010", 0.0, 50000.0,
    ]
    path = str(tmp_path / "conformance.xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    ws.append(row)
    wb.save(path)
    return path


_SAFT_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<AuditFile xmlns="urn:StandardAuditFile-Taxation-Financial:DK">
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
        <OpeningDebitBalance>0.00</OpeningDebitBalance>
        <OpeningCreditBalance>0.00</OpeningCreditBalance>
        <ClosingDebitBalance>0.00</ClosingDebitBalance>
        <ClosingCreditBalance>20000.00</ClosingCreditBalance>
      </Account>
      <Account>
        <AccountID>2100</AccountID>
        <AccountDescription>Koeb af varer</AccountDescription>
        <AccountType>Expense</AccountType>
        <StandardAccountID>2100</StandardAccountID>
        <OpeningDebitBalance>0.00</OpeningDebitBalance>
        <OpeningCreditBalance>0.00</OpeningCreditBalance>
        <ClosingDebitBalance>8000.00</ClosingDebitBalance>
        <ClosingCreditBalance>0.00</ClosingCreditBalance>
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
        <TaxCodeDetails>
          <TaxCode>U25</TaxCode>
          <Description>Udgaaende 25%</Description>
          <TaxPercentage>25</TaxPercentage>
          <StandardTaxCode>1</StandardTaxCode>
          <Country>DK</Country>
        </TaxCodeDetails>
      </TaxTableEntry>
    </TaxTable>
  </MasterFiles>
  <GeneralLedgerEntries>
    <Journal>
      <JournalID>Sales</JournalID>
      <Transaction>
        <TransactionID>T1</TransactionID>
        <Period>1</Period>
        <TransactionDate>2024-01-10</TransactionDate>
        <GLPostingDate>2024-01-15</GLPostingDate>
        <CustomerID>K1</CustomerID>
        <Description>Salg faktura 1</Description>
        <Line>
          <RecordID>1</RecordID>
          <AccountID>1000</AccountID>
          <CreditAmount>20000.00</CreditAmount>
          <TaxInformation>
            <TaxCode>U25</TaxCode>
            <TaxPercentage>25</TaxPercentage>
            <TaxAmount>5000.00</TaxAmount>
            <TaxBase>20000.00</TaxBase>
          </TaxInformation>
        </Line>
      </Transaction>
      <Transaction>
        <TransactionID>T2</TransactionID>
        <Period>1</Period>
        <TransactionDate>2024-01-12</TransactionDate>
        <GLPostingDate>2024-01-14</GLPostingDate>
        <SupplierID>L1</SupplierID>
        <Description>Koebsfaktura 9</Description>
        <Line>
          <RecordID>1</RecordID>
          <AccountID>2100</AccountID>
          <DebitAmount>8000.00</DebitAmount>
          <TaxInformation>
            <TaxCode>U25</TaxCode>
            <TaxPercentage>25</TaxPercentage>
            <TaxAmount>2000.00</TaxAmount>
            <TaxBase>8000.00</TaxBase>
            <Deductibles><NonDeductibleAmount>500.00</NonDeductibleAmount></Deductibles>
          </TaxInformation>
        </Line>
      </Transaction>
    </Journal>
  </GeneralLedgerEntries>
</AuditFile>
"""


def _write_saft_fixture(tmp_path):
    path = tmp_path / "conformance.xml"
    path.write_text(_SAFT_FIXTURE, encoding="utf-8")
    return str(path)


# --- Tests ---------------------------------------------------------------

def test_excel_path_conforms_to_data_contract(tmp_path):
    contract, _ = gen.build_contract()
    path = _write_excel_fixture(tmp_path)
    canonical = adapt_excel_to_saft(parse_excel(path))
    _assert_canonical_conforms(canonical, "excel", contract)


def test_saft_path_conforms_to_data_contract(tmp_path):
    contract, _ = gen.build_contract()
    path = _write_saft_fixture(tmp_path)
    canonical, _info = saft_parser.parse_saft(path)
    assert canonical is not None
    _assert_canonical_conforms(canonical, "saft", contract)


# --- Kanonisk CSV-fixture (byggetrin 8) --------------------------------------

def _write_canonical_fixture(tmp_path, with_summary=True):
    """Lille, selvstændig kanonisk gl_entries-CSV -- samme kolonnenavne som
    dataextract.transform's BC/NAV-mapping producerer i praksis (bekræftet mod
    det rigtige transform_summary.json fra 2026-09-16-kørslen). Ingen
    kundedata -- opdigtede beløb/konti."""
    headers = [
        "posting_dates", "tax_point", "vat_period", "credit_note_flag",
        "invoice_numbers", "gl_accounts", "vat_codes", "vat_amount",
        "debit_amount", "credit_amount", "supply_direction", "currency_fx",
    ]
    rows = [
        ["2024-03-15", "2024-03-10", "2024-03", "false", "F-100", "1000",
         "U25", "250.0", "0", "1250.0", "sale", ""],
        ["2024-03-16", "2024-03-16", "2024-03", "false", "7000123", "2100",
         "", "0", "800.0", "0", "purchase", ""],
    ]
    path = tmp_path / "canonical_gl_entries.csv"
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    if with_summary:
        summary = {
            "mapping_version": "1.0.0",
            "schema_fingerprint": "sha256:testfixture0000000000000000000000000000000000000000000000",
            "source_erp": "Test ERP",
            "profile_version": "1.0.0",
            "rows_in": 2,
            "rows_out": 2,
        }
        (tmp_path / "transform_summary.json").write_text(
            json.dumps(summary), encoding="utf-8"
        )
    return str(path)


def test_canonical_path_conforms_to_data_contract(tmp_path):
    contract, _ = gen.build_contract()
    path = _write_canonical_fixture(tmp_path)
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    _assert_canonical_source_conforms(canonical, contract)


def test_canonical_path_without_summary_still_conforms(tmp_path):
    """Lineage-sidecar er valgfri (§ opgavens Del 1/3) -- ingen
    transform_summary.json må ikke ændre kontrakt-konformansen, kun tømme
    mapping_version/schema_fingerprint."""
    contract, _ = gen.build_contract()
    path = _write_canonical_fixture(tmp_path, with_summary=False)
    canonical, info = canonical_parser.parse_canonical(path)
    assert canonical is not None, info
    assert canonical["header"]["mapping_version"] == ""
    assert canonical["header"]["schema_fingerprint"] == ""
    _assert_canonical_source_conforms(canonical, contract)
