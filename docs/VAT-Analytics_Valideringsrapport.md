# VAT Analytics — Valideringsrapport

> Auto-genereret af `python -m validation.run_validation` · 2026-06-16

✅ **BESTÅET** — 98/98 scenarier bestået (plantet defekt fanges + ren baseline er tavs).

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
| 28 | Ugyldigt momsnummer-format | ja | ja | ✅ |
| 29 | EU-erhvervelse med moms pålagt (manglende reverse charge) | ja | ja | ✅ |
| 30 | Eksport til tredjeland med moms pålagt | ja | ja | ✅ |
| 31 | EU B2B-salg med dansk moms | ja | ja | ✅ |
| 32 | Manglende landekode på udenlandsk part | ja | ja | ✅ |
| 33 | Valuta/land-uoverensstemmelse | ja | ja | ✅ |
| 34 | Dansk momsnummer på udenlandsk part | ja | ja | ✅ |
| 35 | Momsnr-præfiks matcher ikke land | ja | ja | ✅ |
| 36 | Indenlandsk leverance nulsat som udlandssalg | ja | ja | ✅ |
| 37 | VIES-verifikation anbefales | ja | ja | ✅ |
| 38 | Import fra tredjeland uden dokumentation | ja | ja | ✅ |
| 39 | Bogføring efter periodeslut | ja | ja | ✅ |
| 40 | Bogføring før periodestart | ja | ja | ✅ |
| 41 | Weekend-bogføring | ja | ja | ✅ |
| 42 | Ophobning ved periodeslut | ja | ja | ✅ |
| 43 | Fremtidig dato | ja | ja | ✅ |
| 44 | Salg på kvartalsgrænse | ja | ja | ✅ |
| 45 | Sekvens/dato-inkonsistens | ja | ja | ✅ |
| 46 | Stort lag mellem faktura- og bogføringsdato | ja | ja | ✅ |
| 47 | Manglende parts-navn | ja | ja | ✅ |
| 48 | Dublerede leverandører | ja | ja | ✅ |
| 49 | Manglende CVR på dansk leverandør | ja | ja | ✅ |
| 50 | Ugyldigt CVR-nummer | ja | ja | ✅ |
| 51 | Engangsleverandør, højt beløb | ja | ja | ✅ |
| 52 | Stor kunde uden momsnummer | ja | ja | ✅ |
| 53 | Samme momsnr på flere parter | ja | ja | ✅ |
| 54 | Part i begge roller | ja | ja | ✅ |
| 55 | Rundt beløb | ja | ja | ✅ |
| 56 | Beløb lige under grænse | ja | ja | ✅ |
| 57 | Kontant over grænsen | ja | ja | ✅ |
| 58 | Usædvanligt stort beløb (outlier) | ja | ja | ✅ |
| 59 | Stort momsbeløb uden bilag | ja | ja | ✅ |
| 60 | Negativt momsbeløb | ja | ja | ✅ |
| 61 | Konto-beløb outlier | ja | ja | ✅ |
| 62 | Mulig strukturering | ja | ja | ✅ |
| 63 | Benford-afvigelse (første ciffer) | ja | ja | ✅ |
| 64 | Mange runde beløb | ja | ja | ✅ |
| 65 | Sjælden momskode | ja | ja | ✅ |
| 66 | Gentaget beskrivelse | ja | ja | ✅ |
| 67 | Posteringsspike | ja | ja | ✅ |
| 68 | Sjældent brugt konto | ja | ja | ✅ |
| 69 | Skæv øre-fordeling | ja | ja | ✅ |
| 70 | EU-køb uden reverse charge-markering | ja | ja | ✅ |
| 71 | Reverse charge på indenlandsk handel | ja | ja | ✅ |
| 72 | RC-vare med moms i stedet for omvendt betalingspligt | ja | ja | ✅ |
| 73 | Asymmetrisk reverse charge | ja | ja | ✅ |
| 74 | Byggeydelse uden omvendt betalingspligt | ja | ja | ✅ |
| 75 | Reverse charge uden modparts-momsnr | ja | ja | ✅ |
| 76 | Højt købsmoms/salgsmoms-forhold | ja | ja | ✅ |
| 77 | Momskonto afstemmer ikke | ja | ja | ✅ |
| 78 | Negativt momstilsvar | ja | ja | ✅ |
| 79 | Købsmoms uden grundlag | ja | ja | ✅ |
| 80 | Indtægt uden momsbehandling | ja | ja | ✅ |
| 81 | Stor andel momsfri omsætning | ja | ja | ✅ |
| 84 | Missing trader-indikator | ja | ja | ✅ |
| 86 | Hurtig gennemstrømning | ja | ja | ✅ |
| 87 | Højrisikovare | ja | ja | ✅ |
| 88 | Nul-margin på højrisikovare | ja | ja | ✅ |
| 89 | Nystartet høj-volumen leverandør | ja | ja | ✅ |
| 91 | Mellemregnings-/gennemstrømningskonto | ja | ja | ✅ |
| 92 | Falsk faktura-indikator | ja | ja | ✅ |
| 93 | Identisk beløb hos mange parter | ja | ja | ✅ |
| 94 | EU-forbrugersalg (OSS) | ja | ja | ✅ |
| 95 | Fjernsalgstærskel overskredet | ja | ja | ✅ |
| 96 | Digital ydelse med dansk moms til EU-forbruger | ja | ja | ✅ |
| 97 | Dansk moms på EU-forbrugersalg | ja | ja | ✅ |
| 98 | Lavværdiimport uden importmoms (IOSS) | ja | ja | ✅ |
| 100 | Elektronisk ydelse uden momskode | ja | ja | ✅ |
| 101 | Teleydelse uden momskode | ja | ja | ✅ |
| 102 | Rejseydelse med fuld moms | ja | ja | ✅ |
| 103 | Brugtmoms-vare med fuld moms | ja | ja | ✅ |

Dækning: **98** scenarier (repræsentativt startsæt; udvides til alle 98 aktive kontroller).

