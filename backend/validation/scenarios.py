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

    # === cat03: Momssats-validering (19-26) ===
    {
        "test_id": 19, "navn": "Ugyldig momssats",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=25.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=12.0))),
    },
    {
        "test_id": 20, "navn": "Sats afviger fra momstabel",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0)), tax_table=_I25),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=10.0)), tax_table=_I25),
    },
    {
        "test_id": 21, "navn": "Reduceret/udenlandsk momssats",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=25.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="X", tax_percentage=19.0, tax_amount=190.0))),
    },
    {
        "test_id": 22, "navn": "Manglende salgsmoms",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code="U25", tax_percentage=25.0, tax_amount=2500.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code="U25", tax_percentage=25.0, tax_amount=0.0))),
    },
    {
        "test_id": 23, "navn": "Inkonsistent sats pr. momskode",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0), transaction_id="T-1"),
            mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0), transaction_id="T-2"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=25.0), transaction_id="T-1"),
            mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_percentage=0.0), transaction_id="T-2"),
        ]),
    },
    {
        "test_id": 24, "navn": "Implicit sats ugyldig",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_base=1000.0, tax_amount=250.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_base=1000.0, tax_amount=120.0))),
    },
    {
        "test_id": 25, "navn": "Nulsats på indenlandsk handel",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="N0", tax_percentage=25.0, tax_base=1000.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="N0", tax_percentage=0.0, tax_base=1000.0, country=""))),
    },
    {
        "test_id": 26, "navn": "Momsbeløb uden momskode",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="I25", tax_amount=250.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_code="", tax_amount=250.0))),
    },

    # === cat09: Reverse charge & selvangivelse (70-75) ===
    {
        "test_id": 70, "navn": "EU-køb uden reverse charge-markering",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", tax_code="RC25", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", tax_code="", tax_amount=0.0))),
    },
    {
        "test_id": 71, "navn": "Reverse charge på indenlandsk handel",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="I25"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="RC25"))),
    },
    {
        "test_id": 72, "navn": "RC-vare med moms i stedet for omvendt betalingspligt",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="RC25", tax_amount=250.0),
                                description="Køb af mobiltelefoner")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="I25", tax_amount=250.0),
                                 description="Køb af mobiltelefoner")),
    },
    {
        "test_id": 73, "navn": "Asymmetrisk reverse charge",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="I25"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="RC25"))),
    },
    {
        "test_id": 74, "navn": "Byggeydelse uden omvendt betalingspligt",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="RC25", tax_amount=250.0),
                                description="Byggeydelse og montage")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", tax_code="I25", tax_amount=250.0),
                                 description="Byggeydelse og montage")),
    },
    {
        "test_id": 75, "navn": "Reverse charge uden modparts-momsnr",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", tax_code="RC25", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", tax_code="RC25", vat_number=""))),
    },

    # === cat04: Grænseoverskridende (resten: 28, 32-35, 37, 38) ===
    {
        "test_id": 28, "navn": "Ugyldigt momsnummer-format",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number="DE12"))),
    },
    {
        "test_id": 32, "navn": "Manglende landekode på udenlandsk part",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="", vat_number=_DE_VAT))),
    },
    {
        "test_id": 33, "navn": "Valuta/land-uoverensstemmelse",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", currency="DKK"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", currency="EUR"))),
    },
    {
        "test_id": 34, "navn": "Dansk momsnummer på udenlandsk part",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number="DK12345678"))),
    },
    {
        "test_id": 35, "navn": "Momsnr-præfiks matcher ikke land",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number="SE123456789012"))),
    },
    {
        "test_id": 37, "navn": "VIES-verifikation anbefales",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", vat_number=""))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DE", vat_number=_DE_VAT))),
    },
    {
        "test_id": 38, "navn": "Import fra tredjeland uden dokumentation",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="US", tax_amount=0.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="US", tax_amount=250.0))),
    },

    # === cat05: Timing & periode (39-45) ===
    {
        "test_id": 39, "navn": "Bogføring efter periodeslut",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-06-15")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2025-01-15")),
    },
    {
        "test_id": 40, "navn": "Bogføring før periodestart",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-06-15")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2023-12-15")),
    },
    {
        "test_id": 41, "navn": "Weekend-bogføring",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-15")),   # fredag
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-16")),  # lørdag
    },
    {
        "test_id": 42, "navn": "Ophobning ved periodeslut",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-03-31")),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"T-{i}", date="2024-03-31")
                           for i in range(5)]),
    },
    {
        "test_id": 43, "navn": "Fremtidig dato",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2024-06-15")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0), date="2027-01-01")),
    },
    {
        "test_id": 44, "navn": "Salg på kvartalsgrænse",
        "clean": mk_data(mk_txn(mk_line(credit_amount=1000.0), date="2024-05-15")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=1000.0), date="2024-03-31")),
    },
    {
        "test_id": 45, "navn": "Sekvens/dato-inkonsistens",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-1", date="2024-01-01"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-2", date="2024-02-01"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-3", date="2024-03-01"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-1", date="2024-03-01"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-2", date="2024-02-01"),
            mk_txn(mk_line(debit_amount=1000.0), transaction_id="T-3", date="2024-01-01"),
        ]),
    },

    # === cat06: Leverandør- & kundevalidering (47-54) ===
    {
        "test_id": 47, "navn": "Manglende parts-navn",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name=""))),
    },
    {
        "test_id": 48, "navn": "Dublerede leverandører",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS"),
                                transaction_id="T-1")),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS"), transaction_id="T-1"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L2", supplier_name="ABC ApS"), transaction_id="T-2"),
        ]),
    },
    {
        "test_id": 49, "navn": "Manglende CVR på dansk leverandør",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                                        country="DK", vat_number="DK12345674"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                                         country="DK", vat_number=""))),
    },
    {
        "test_id": 50, "navn": "Ugyldigt CVR-nummer",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                                        country="DK", vat_number="DK12345674"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                                         country="DK", vat_number="DK12345678"))),
    },
    {
        "test_id": 51, "navn": "Engangsleverandør, højt beløb",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=60000.0, supplier_id="L1", supplier_name="ABC ApS"))),
    },
    {
        "test_id": 52, "navn": "Stor kunde uden momsnummer",
        "clean": mk_data(mk_txn(mk_line(credit_amount=120000.0, customer_id="K1", customer_name="Kunde A/S",
                                        vat_number="DE123456789"))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=120000.0, customer_id="K1", customer_name="Kunde A/S",
                                         vat_number=""))),
    },
    {
        "test_id": 53, "navn": "Samme momsnr på flere parter",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                                        vat_number="DK12345674"))),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L1", supplier_name="ABC ApS",
                           vat_number="DK12345674"), transaction_id="T-1"),
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="L2", supplier_name="ABC2 ApS",
                           vat_number="DK12345674"), transaction_id="T-2"),
        ]),
    },
    {
        "test_id": 54, "navn": "Part i begge roller",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="P1", supplier_name="ABC ApS"), transaction_id="T-1"),
            mk_txn(mk_line(credit_amount=1000.0, customer_id="P2", customer_name="XYZ A/S"), transaction_id="T-2"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, supplier_id="P1", supplier_name="ABC ApS"), transaction_id="T-1"),
            mk_txn(mk_line(credit_amount=1000.0, customer_id="P1", customer_name="ABC ApS"), transaction_id="T-2"),
        ]),
    },

    # === cat07: Beløbs- & tærskeltest (55-62) ===
    {
        "test_id": 55, "navn": "Rundt beløb",
        "clean": mk_data(mk_txn(mk_line(debit_amount=50123.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=50000.0))),
    },
    {
        "test_id": 56, "navn": "Beløb lige under grænse",
        "clean": mk_data(mk_txn(mk_line(debit_amount=45123.0))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=49321.0))),
    },
    {
        "test_id": 57, "navn": "Kontant over grænsen",
        "clean": mk_data(mk_txn(mk_line(debit_amount=25000.0), description="Bankoverførsel")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=25000.0), description="Kontant betaling")),
    },
    {
        "test_id": 58, "navn": "Usædvanligt stort beløb (outlier)",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"S-{i}") for i in range(5)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"S-{i}") for i in range(20)]
                          + [mk_txn(mk_line(debit_amount=100000.0), transaction_id="BIG")]),
    },
    {
        "test_id": 59, "navn": "Stort momsbeløb uden bilag",
        "clean": mk_data(mk_txn(mk_line(debit_amount=24000.0, tax_amount=6000.0, source_document_id="F-1"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=24000.0, tax_amount=6000.0, source_document_id=""))),
    },
    {
        "test_id": 60, "navn": "Negativt momsbeløb",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=-500.0), description="Kreditnota")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=-500.0), description="Postering")),
    },
    {
        "test_id": 61, "navn": "Konto-beløb outlier",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0, account_id="4000"), transaction_id=f"A-{i}")
                          for i in range(5)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0, account_id="4000"), transaction_id=f"A-{i}")
                           for i in range(20)]
                          + [mk_txn(mk_line(debit_amount=100000.0, account_id="4000"), transaction_id="A-BIG")]),
    },
    {
        "test_id": 62, "navn": "Mulig strukturering",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=3000.0, supplier_id="L1"), transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=3000.0, supplier_id="L1"), transaction_id="T-2", date="2024-03-15"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=6000.0, supplier_id="L1"), transaction_id="T-1", date="2024-03-15"),
            mk_txn(mk_line(debit_amount=6000.0, supplier_id="L1"), transaction_id="T-2", date="2024-03-15"),
        ]),
    },

    # === cat08: Statistisk anomalidetektion (63-69) — kræver store datasæt ===
    {
        "test_id": 63, "navn": "Benford-afvigelse (første ciffer)",
        "clean": mk_data([mk_txn(mk_line(debit_amount=90.0), transaction_id=f"B-{i}") for i in range(40)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=90.0), transaction_id=f"B-{i}") for i in range(60)]),
    },
    {
        "test_id": 64, "navn": "Mange runde beløb",
        "clean": mk_data([mk_txn(mk_line(debit_amount=50123.0), transaction_id=f"R-{i}") for i in range(30)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=50000.0), transaction_id=f"R-{i}") for i in range(30)]),
    },
    {
        "test_id": 65, "navn": "Sjælden momskode",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0, tax_code="I25"), transaction_id=f"C-{i}")
                          for i in range(151)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0, tax_code="I25"), transaction_id=f"C-{i}")
                           for i in range(150)]
                          + [mk_txn(mk_line(debit_amount=1000.0, tax_code="RARE"), transaction_id="C-RARE")]),
    },
    {
        "test_id": 66, "navn": "Gentaget beskrivelse",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"D-{i}",
                                 description=f"Postering nr {i}") for i in range(20)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"D-{i}",
                                  description="Diverse omkostninger") for i in range(20)]),
    },
    {
        "test_id": 67, "navn": "Posteringsspike",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"E-{i}",
                                 date=f"2024-01-{i:02d}") for i in range(1, 13)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"E-{i}",
                                  date=f"2024-01-{i:02d}") for i in range(1, 21)]
                          + [mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"SP-{j}",
                                    date="2024-02-01") for j in range(20)]),
    },
    {
        "test_id": 68, "navn": "Sjældent brugt konto",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.0, account_id="4000"), transaction_id=f"F-{i}")
                          for i in range(51)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0, account_id="4000"), transaction_id=f"F-{i}")
                           for i in range(50)]
                          + [mk_txn(mk_line(debit_amount=30000.0, account_id="9999"), transaction_id="F-RARE")]),
    },
    {
        "test_id": 69, "navn": "Skæv øre-fordeling",
        "clean": mk_data([mk_txn(mk_line(debit_amount=1000.37), transaction_id=f"G-{i}") for i in range(50)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=1000.0), transaction_id=f"G-{i}") for i in range(50)]),
    },

    # === cat11: Svindel & karrusel/MTIC (84, 86-89, 91-93; 85/90 inaktive) ===
    {
        "test_id": 84, "navn": "Missing trader-indikator",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="DK", vat_number="DK12345674"),
                                description="Postering")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=60000.0, country="DE", vat_number=""),
                                 description="Køb af mobiltelefoner")),
    },
    {
        "test_id": 86, "navn": "Hurtig gennemstrømning",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1", date="2024-03-01"),
            mk_txn(mk_line(credit_amount=50000.0), transaction_id="S-1", date="2024-04-15"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1", date="2024-03-01"),
            mk_txn(mk_line(credit_amount=50000.0), transaction_id="S-1", date="2024-03-05"),
        ]),
    },
    {
        "test_id": 87, "navn": "Højrisikovare",
        "clean": mk_data(mk_txn(mk_line(debit_amount=30000.0), description="Køb af kontorartikler")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=30000.0), description="Køb af mobiltelefoner")),
    },
    {
        "test_id": 88, "navn": "Nul-margin på højrisikovare",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1", description="Køb af mobiltelefoner"),
            mk_txn(mk_line(credit_amount=60000.0), transaction_id="S-1", description="Salg af mobiltelefoner"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=50000.0), transaction_id="P-1", description="Køb af mobiltelefoner"),
            mk_txn(mk_line(credit_amount=50000.0), transaction_id="S-1", description="Salg af mobiltelefoner"),
        ]),
    },
    {
        "test_id": 89, "navn": "Nystartet høj-volumen leverandør",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-1", date="2024-01-01"),
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-2", date="2024-06-01"),
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-3", date="2024-12-01"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-1", date="2024-03-01"),
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-2", date="2024-03-10"),
            mk_txn(mk_line(debit_amount=70000.0, supplier_id="L1"), transaction_id="N-3", date="2024-03-20"),
        ]),
    },
    {
        "test_id": 91, "navn": "Mellemregnings-/gennemstrømningskonto",
        "clean": mk_data([mk_txn(mk_line(debit_amount=20000.0, account_id="9999"),
                                 transaction_id=f"M-{i}", description="Postering") for i in range(3)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=20000.0, account_id="9999"),
                                  transaction_id=f"M-{i}", description="Mellemregning") for i in range(3)]),
    },
    {
        "test_id": 92, "navn": "Falsk faktura-indikator",
        "clean": mk_data(mk_txn(mk_line(debit_amount=20000.0, tax_amount=3000.0, source_document_id="F-1",
                                        supplier_id="L1", supplier_name="ABC ApS"))),
        "defect": mk_data(mk_txn(mk_line(debit_amount=20000.0, tax_amount=3000.0, source_document_id="",
                                         supplier_id="L1", supplier_name="ABC ApS"))),
    },
    {
        "test_id": 93, "navn": "Identisk beløb hos mange parter",
        "clean": mk_data([mk_txn(mk_line(debit_amount=50000.0, supplier_id="L1"), transaction_id=f"I-{i}")
                          for i in range(4)]),
        "defect": mk_data([mk_txn(mk_line(debit_amount=50000.0, supplier_id=f"L{i}"), transaction_id=f"I-{i}")
                           for i in range(4)]),
    },

    # === cat10: Ind-/udgående moms-afstemning (resten: 76, 77, 78, 81) ===
    {
        "test_id": 76, "navn": "Højt købsmoms/salgsmoms-forhold",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0), transaction_id="P"),
            mk_txn(mk_line(credit_amount=1000.0, tax_amount=250.0), transaction_id="S"),
        ]),
        "defect": mk_data([
            mk_txn(mk_line(debit_amount=10000.0, tax_amount=1000.0), transaction_id="P"),
            mk_txn(mk_line(credit_amount=1000.0, tax_amount=100.0), transaction_id="S"),
        ]),
    },
    {
        "test_id": 77, "navn": "Momskonto afstemmer ikke",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
                         accounts=[{"account_id": "7010", "description": "Momsafregning",
                                    "account_type": "", "opening_balance": 0.0, "closing_balance": -250.0}]),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0)),
                          accounts=[{"account_id": "7010", "description": "Momsafregning",
                                     "account_type": "", "opening_balance": 0.0, "closing_balance": 9999.0}]),
    },
    {
        "test_id": 78, "navn": "Negativt momstilsvar",
        "clean": mk_data([
            mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0), transaction_id="P"),
            mk_txn(mk_line(credit_amount=2000.0, tax_amount=500.0), transaction_id="S"),
        ]),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, tax_amount=250.0))),
    },
    {
        "test_id": 81, "navn": "Stor andel momsfri omsætning",
        "clean": mk_data(mk_txn(mk_line(credit_amount=20000.0, tax_code="U25", tax_percentage=25.0,
                                        tax_base=20000.0, tax_amount=5000.0))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=20000.0, tax_code="N0", tax_percentage=0.0))),
    },

    # === cat12: E-handel, digitale ydelser & særordninger (94-98, 100-103; 99 inaktiv) ===
    {
        "test_id": 94, "navn": "EU-forbrugersalg (OSS)",
        "clean": mk_data(mk_txn(mk_line(credit_amount=20000.0, country="DE", vat_number=_DE_VAT))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=20000.0, country="DE", vat_number=""))),
    },
    {
        "test_id": 95, "navn": "Fjernsalgstærskel overskredet",
        "clean": mk_data(mk_txn(mk_line(credit_amount=20000.0, country="DE", vat_number=""))),
        "defect": mk_data(mk_txn(mk_line(credit_amount=80000.0, country="DE", vat_number=""))),
    },
    {
        "test_id": 96, "navn": "Digital ydelse med dansk moms til EU-forbruger",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number="",
                                        tax_percentage=0.0), description="Software licens")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number="",
                                         tax_percentage=25.0), description="Software licens")),
    },
    {
        "test_id": 97, "navn": "Dansk moms på EU-forbrugersalg",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number="",
                                        tax_percentage=0.0, tax_amount=0.0), description="Varesalg")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, country="DE", vat_number="",
                                         tax_percentage=25.0, tax_amount=2500.0), description="Varesalg")),
    },
    {
        "test_id": 98, "navn": "Lavværdiimport uden importmoms (IOSS)",
        "clean": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="US", tax_amount=0.0),
                                description="Postering")),
        "defect": mk_data(mk_txn(mk_line(debit_amount=1000.0, country="US", tax_amount=0.0),
                                 description="Import fra tredjeland")),
    },
    {
        "test_id": 100, "navn": "Elektronisk ydelse uden momskode",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code="U25"),
                                description="Software abonnement")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code=""),
                                 description="Software abonnement")),
    },
    {
        "test_id": 101, "navn": "Teleydelse uden momskode",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code="U25"), description="Teleydelse")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_code=""), description="Teleydelse")),
    },
    {
        "test_id": 102, "navn": "Rejseydelse med fuld moms",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_percentage=0.0, tax_amount=0.0),
                                description="Pakkerejse til Spanien")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_percentage=25.0, tax_amount=2500.0),
                                 description="Pakkerejse til Spanien")),
    },
    {
        "test_id": 103, "navn": "Brugtmoms-vare med fuld moms",
        "clean": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_percentage=0.0, tax_amount=0.0),
                                description="Salg af brugt udstyr")),
        "defect": mk_data(mk_txn(mk_line(credit_amount=10000.0, tax_percentage=25.0, tax_amount=2500.0),
                                 description="Salg af brugt udstyr")),
    },
]
