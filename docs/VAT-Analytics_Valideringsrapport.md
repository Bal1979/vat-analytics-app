# VAT Analytics — Valideringsrapport

> Auto-genereret af `python -m validation.run_validation` · 2026-06-16

✅ **BESTÅET** — 40/40 scenarier bestået (plantet defekt fanges + ren baseline er tavs).

| test_id | Kontrol | Fanger defekt | Ren er tavs | Resultat |
|---------|---------|---------------|-------------|----------|
| 1 | Moms-genberegning | ja | ja | ✅ |
| 2 | Momskode-validering | ja | ja | ✅ |
| 3 | Momsafrunding (øre-præcision) | ja | ja | ✅ |
| 4 | Faktura-feltfuldstændighed | ja | ja | ✅ |
| 5 | Dato/periode-konsistens | ja | ja | ✅ |
| 6 | Negative linjebeløb | ja | ja | ✅ |
| 7 | Nul-værdi transaktioner | ja | ja | ✅ |
| 8 | Valutakurs-konsistens | ja | ja | ✅ |
| 9 | Leveringstidspunkt (uden for periode) | ja | ja | ✅ |
| 10 | Transaktionsbalance | ja | ja | ✅ |
| 11 | Eksakt dubletfaktura | ja | ja | ✅ |
| 12 | Fuzzy dubletfaktura | ja | ja | ✅ |
| 13 | Samme beløb, samme leverandør (≤30 dage) | ja | ja | ✅ |
| 14 | Genbrugt fakturanummer | ja | ja | ✅ |
| 15 | Dobbeltbetalingsdetektion (≤7 dage) | ja | ja | ✅ |
| 16 | Kreditnota-dubletter | ja | ja | ✅ |
| 17 | Tværgående enheds-dubletter | ja | ja | ✅ |
| 18 | Sekventielle transaktionsnumre (dublet) | ja | ja | ✅ |
| 19 | Ugyldig momssats | ja | ja | ✅ |
| 20 | Sats afviger fra momstabel | ja | ja | ✅ |
| 21 | Reduceret/udenlandsk momssats | ja | ja | ✅ |
| 22 | Manglende salgsmoms | ja | ja | ✅ |
| 23 | Inkonsistent sats pr. momskode | ja | ja | ✅ |
| 24 | Implicit sats ugyldig | ja | ja | ✅ |
| 25 | Nulsats på indenlandsk handel | ja | ja | ✅ |
| 26 | Momsbeløb uden momskode | ja | ja | ✅ |
| 27 | EU-handel uden momsnummer | ja | ja | ✅ |
| 29 | EU-erhvervelse med moms pålagt (manglende reverse charge) | ja | ja | ✅ |
| 30 | Eksport til tredjeland med moms pålagt | ja | ja | ✅ |
| 31 | EU B2B-salg med dansk moms | ja | ja | ✅ |
| 36 | Indenlandsk leverance nulsat som udlandssalg | ja | ja | ✅ |
| 46 | Stort lag mellem faktura- og bogføringsdato | ja | ja | ✅ |
| 70 | EU-køb uden reverse charge-markering | ja | ja | ✅ |
| 71 | Reverse charge på indenlandsk handel | ja | ja | ✅ |
| 72 | RC-vare med moms i stedet for omvendt betalingspligt | ja | ja | ✅ |
| 73 | Asymmetrisk reverse charge | ja | ja | ✅ |
| 74 | Byggeydelse uden omvendt betalingspligt | ja | ja | ✅ |
| 75 | Reverse charge uden modparts-momsnr | ja | ja | ✅ |
| 79 | Købsmoms uden grundlag | ja | ja | ✅ |
| 80 | Indtægt uden momsbehandling | ja | ja | ✅ |

Dækning: **40** scenarier (repræsentativt startsæt; udvides til alle 98 aktive kontroller).

