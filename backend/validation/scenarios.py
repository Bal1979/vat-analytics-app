"""Scenarie-register: én plantet defekt pr. kontrol.

Hvert scenarie har en REN baseline (kontrollen må ikke fyre) og en DEFEKT variant
(kontrollen SKAL fyre). Dette er et repræsentativt startsæt (mønster) — det udvides
til at dække alle 98 aktive kontroller. Hold `test_id` i sync med catalog/rules.json.
"""

from validation.builders import mk_data, mk_txn, mk_line

_DE_VAT = "DE123456789"  # gyldigt DE-format (9 cifre)

SCENARIOS = [
    {
        "test_id": 27,
        "navn": "EU-handel uden momsnummer",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=""))),
    },
    {
        "test_id": 29,
        "navn": "EU-erhvervelse med moms pålagt (manglende reverse charge)",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=250.0))),
    },
    {
        "test_id": 30,
        "navn": "Eksport til tredjeland med moms pålagt",
        "clean": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="US", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="US", tax_amount=250.0))),
    },
    {
        "test_id": 31,
        "navn": "EU B2B-salg med dansk moms",
        "clean": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=250.0))),
    },
    {
        "test_id": 36,
        "navn": "Indenlandsk leverance nulsat som udlandssalg (vare forlader ikke DK)",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number=_DE_VAT,
                                        ship_from_country="DK", ship_to_country="DE", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE",
                                         ship_from_country="DK", ship_to_country="DK", tax_amount=0.0))),
    },
    {
        "test_id": 46,
        "navn": "Stort lag mellem faktura- og bogføringsdato",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-02-01", document_date="2024-02-01")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-20", document_date="2024-02-01")),
    },
    {
        "test_id": 79,
        "navn": "Købsmoms uden grundlag",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0, tax_base=1000.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0, tax_base=0.0))),
    },
    {
        "test_id": 80,
        "navn": "Indtægt uden momsbehandling",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DK", tax_code="U25",
                                        tax_percentage=25.0, tax_base=10000.0, tax_amount=2500.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DK", tax_code="",
                                         tax_amount=0.0))),
    },
]
