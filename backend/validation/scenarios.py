"""Scenarie-register: én plantet defekt pr. kontrol.

Hvert scenarie har en REN baseline (kontrollen må ikke fyre) og en DEFEKT variant
(kontrollen SKAL fyre). Det beviser både følsomhed og fravær af falske alarmer.
Hold `test_id` i sync med catalog/rules.json. Udvides kategori for kategori mod
fuld dækning af de 98 aktive kontroller.
"""

from validation.builders import mk_data, mk_txn, mk_line

_DE_VAT = "DE123456789"  # gyldigt DE-format (9 cifre)
_I25 = [{"tax_code": "I25", "tax_percentage": 25.0}]
_ASSET = [{"account_id": "5820", "description": "Bank", "account_type": "Asset",
           "opening_balance": 0.0, "closing_balance": 0.0}]

SCENARIOS = [
    # === cat01: Transaktionsintegritet & datakvalitet (1-10) ===
    {
        "test_id": 1, "navn": "Moms-genberegning",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0,
                                        tax_base=1000.0, tax_amount=250.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0,
                                         tax_base=1000.0, tax_amount=100.0))),
    },
    {
        "test_id": 2, "navn": "Momskode-validering",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25")), tax_table=_I25),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="XYZ")), tax_table=_I25),
    },
    {
        "test_id": 3, "navn": "Momsafrunding (øre-præcision)",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=100.12))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=100.123))),
    },
    {
        "test_id": 4, "navn": "Faktura-feltfuldstændighed",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), description="Køb af varer")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), description="")),
    },
    {
        "test_id": 5, "navn": "Dato/periode-konsistens",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-15", period="03", period_year="2024")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-15", period="01", period_year="2024")),
    },
    {
        "test_id": 6, "navn": "Negative linjebeløb",
        "clean": mk_data(mk_txn(mk_line(debit_amount=500.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=-500.0))),
    },
    {
        "test_id": 7, "navn": "Nul-værdi transaktioner",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_base=1000.0, tax_amount=250.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_base=0.0, tax_amount=250.0))),
    },
    {
        "test_id": 8, "navn": "Valutakurs-konsistens",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, currency="DKK"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, currency="EUR"))),
    },
    {
        "test_id": 9, "navn": "Leveringstidspunkt (uden for periode)",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-06-15")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2025-06-15")),
    },
    {
        "test_id": 10, "navn": "Transaktionsbalance",
        "clean": mk_data(mk_txn([mk_line(debit_amount=1000.0),
                                 mk_line(credit_amount=1000.0, record_id="L2")])),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0))),
    },

    # === cat02: Dubletdetektion (11-18) ===
    {
        "test_id": 11, "navn": "Eksakt dubletfaktura",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                                transaction_id="T-1")),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-2", date="2024-03-15"),
        ]),
    },
    {
        "test_id": 12, "navn": "Fuzzy dubletfaktura",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-200"),
                   transaction_id="T-2", date="2024-03-15"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F 100"),
                   transaction_id="T-2", date="2024-03-15"),
        ]),
    },
    {
        "test_id": 13, "navn": "Samme beløb, samme leverandør (≤30 dage)",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1"), transaction_id="T-1", date="2024-01-01"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1"), transaction_id="T-2", date="2024-06-01"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1"), transaction_id="T-1", date="2024-03-01"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1"), transaction_id="T-2", date="2024-03-15"),
        ]),
    },
    {
        "test_id": 14, "navn": "Genbrugt fakturanummer",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=2000.0, supplier_id="L1", source_document_id="F-200"),
                   transaction_id="T-2", date="2024-04-15"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=2000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-2", date="2024-04-15"),
        ]),
    },
    {
        "test_id": 15, "navn": "Dobbeltbetalingsdetektion (≤7 dage)",
        "clean": mk_data([
            mk_txn(mk_line(credit_amount=5000.0, account_id="5820"), transaction_id="T-1", date="2024-03-01"),
            mk_txn(mk_line(credit_amount=5000.0, account_id="5820"), transaction_id="T-2", date="2024-03-20"),
        ], accounts=_ASSET),
        "defect": mk_data([
            mk_txn(mk_line(credit_amount=5000.0, account_id="5820"), transaction_id="T-1", date="2024-03-01"),
            mk_txn(mk_line(credit_amount=5000.0, account_id="5820"), transaction_id="T-2", date="2024-03-05"),
        ], accounts=_ASSET),
    },
    {
        "test_id": 16, "navn": "Kreditnota-dubletter",
        "clean": mk_data(mk_txn(mk_line(debit_amount=500.0, supplier_id="L1", source_document_id="K-1"),
                                transaction_id="T-1", description="Kreditnota")),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=500.0, supplier_id="L1", source_document_id="K-1"),
                   transaction_id="T-1", description="Kreditnota", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=500.0, supplier_id="L1", source_document_id="K-1"),
                   transaction_id="T-2", description="Kreditnota", date="2024-03-15"),
        ]),
    },
    {
        "test_id": 17, "navn": "Tværgående enheds-dubletter",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-2", date="2024-03-16"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", source_document_id="F-100"),
                   transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L2", source_document_id="F-100"),
                   transaction_id="T-2", date="2024-03-16"),
        ]),
    },
    {
        "test_id": 18, "navn": "Sekventielle transaktionsnumre (dublet)",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-100"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-101"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-100"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-100"),
        ]),
    },

    # === cat04: Grænseoverskridende & EU-compliance (udvalg) ===
    {
        "test_id": 27, "navn": "EU-handel uden momsnummer",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=""))),
    },
    {
        "test_id": 29, "navn": "EU-erhvervelse med moms pålagt (manglende reverse charge)",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=250.0))),
    },
    {
        "test_id": 30, "navn": "Eksport til tredjeland med moms pålagt",
        "clean": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="US", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="US", tax_amount=250.0))),
    },
    {
        "test_id": 31, "navn": "EU B2B-salg med dansk moms",
        "clean": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=1000.0, country="DE", vat_number=_DE_VAT, tax_amount=250.0))),
    },
    {
        "test_id": 36, "navn": "Indenlandsk leverance nulsat som udlandssalg",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number=_DE_VAT,
                                        ship_from_country="DK", ship_to_country="DE", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE",
                                         ship_from_country="DK", ship_to_country="DK", tax_amount=0.0))),
    },

    # === cat05: Timing & periode (udvalg) ===
    {
        "test_id": 46, "navn": "Stort lag mellem faktura- og bogføringsdato",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-02-01", document_date="2024-02-01")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-20", document_date="2024-02-01")),
    },

    # === cat10: Ind-/udgående moms-afstemning (udvalg) ===
    {
        "test_id": 79, "navn": "Købsmoms uden grundlag",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0, tax_base=1000.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0, tax_base=0.0))),
    },
    {
        "test_id": 80, "navn": "Indtægt uden momsbehandling",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DK", tax_code="U25",
                                        tax_percentage=25.0, tax_base=10000.0, tax_amount=2500.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DK", tax_code="", tax_amount=0.0))),
    },
]
