# VAT Analytics — Sporbarhedsrapport

> Auto-genereret af `tools/build_traceability.py` · katalogversion **1.1.0** · 2026-09-08

## Dækning

- Kontroller i alt: **103** (aktive: **98**, inaktive: **5**)
- Præcis kilde udfyldt: **0 / 103** (resten viser kategoriens retsområde indtil den fagansvarlige pinner paragraffen)
- Dækkende valideringstest udfyldt: **98 / 103** (valideringssuiten dækker alle 98 aktive kontroller; de 5 uden test er de inaktive)

## Analyse-moduler (momsrelevans-slankning)

| Modul | Default | Kontroller | Beskrivelse |
|-------|---------|------------|-------------|
| Momskerne | TIL | 60 | Kontroller med direkte momsfaglig konsekvens: sats, fradragsret, angivelse, reverse charge, place-of-supply og momsafstemning. Fundamentet i en momsgennemgang. |
| Forensic & statistik | FRA | 26 | Statistisk anomalidetektion, beløbs-outliers, timing-anomalier og karrusel/MTIC-indikatorer. Hører til et JE-/besvigelsesmandat — ikke en momsgennemgang. |
| Stamdata- & datakvalitet | FRA | 4 | Parts-/stamdatavalidering uden direkte fradrags-konsekvens (fx dubletnavne, formatfejl). Datakvalitet, ikke momsfund. |
| Dublet-recovery (bredt) | FRA | 3 | Bredere dublet-/recovery-scanninger (nær-dubletter, beløbsmatch på tværs). Den stærke eksakt-dublet med moms-dobbeltfradrag ligger i momskernen. |
| E-handel & særordninger | FRA | 10 | OSS/fjernsalg, digitale ydelser, margin-/brugtmoms. Momsfagligt korrekte, men kun relevante ved B2C-fjernsalg/særordninger — klientbetinget, default fra. |

## Sporbarhedsmatrix (kontrol → analyse-modul → kilde → test)

| ID | Kontrol | Status | Analyse-modul | Default | Retsområde / kilde | Test |
|----|---------|--------|---------------|---------|--------------------|------|
| VATA-001 | Moms-genberegning | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-002 | Momskode-validering | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-003 | Momsafrunding | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-004 | Faktura-feltfuldstændighed | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-005 | Dato/periode-konsistens | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-006 | Negative linjebeløb | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-007 | Nul-værdi transaktioner | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-008 | Valutakurs-konsistens | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-009 | Leveringstidspunkt | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-010 | Transaktionsbalance | aktiv | Momskerne | TIL | Momsgrundlag og fradragsret (momsloven) — datakvalitet jf. Skattestyrelsens kontrolmetoder | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-011 | Eksakt dubletfaktura | aktiv | Momskerne | TIL | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-012 | Fuzzy dubletfaktura | aktiv | Momskerne | TIL | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-013 | Samme beløb, samme leverandør | aktiv | Dublet-recovery (bredt) | FRA | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-014 | Genbrugt fakturanummer | aktiv | Momskerne | TIL | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-015 | Dobbeltbetalingsdetektion | aktiv | Dublet-recovery (bredt) | FRA | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-016 | Kreditnota-dubletter | aktiv | Momskerne | TIL | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-017 | Tværgående enheds-dubletter | aktiv | Momskerne | TIL | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-018 | Sekventielle transaktionsnumre | aktiv | Dublet-recovery (bredt) | FRA | Dobbelt fradrag / dublethåndtering (momsloven, fradragsret) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-019 | Ugyldig momssats | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-020 | Sats afviger fra momstabel | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-021 | Reduceret/udenlandsk momssats | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-022 | Manglende salgsmoms | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-023 | Inkonsistent sats pr. momskode | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-024 | Implicit sats ugyldig | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-025 | Nulsats på indenlandsk handel | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-026 | Momsbeløb uden momskode | aktiv | Momskerne | TIL | Momssatser: standardsats 25% og 0-sats (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-027 | EU-handel uden momsnummer | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-028 | Ugyldigt momsnummer-format | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-029 | EU-erhvervelse uden reverse charge | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-030 | Eksport med moms pålagt | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-031 | EU-salg med dansk moms | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-032 | Manglende landekode på udenlandsk part | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-033 | Valuta/land-uoverensstemmelse | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-034 | Dansk momsnummer på udenlandsk part | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-035 | Momsnr-præfiks matcher ikke land | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-036 | Indenlandsk leverance nulsat som udlandssalg | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-037 | VIES-verifikation anbefales | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-038 | Import uden dokumentation | aktiv | Momskerne | TIL | Leveringssted, EU-handel og eksport: 0-sats og omvendt betalingspligt (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-039 | Bogføring efter periodeslut | aktiv | Momskerne | TIL | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-040 | Bogføring før periodestart | aktiv | Momskerne | TIL | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-041 | Weekend-bogføring | aktiv | Forensic & statistik | FRA | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-042 | Ophobning ved periodeslut | aktiv | Forensic & statistik | FRA | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-043 | Fremtidig dato | aktiv | Forensic & statistik | FRA | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-044 | Salg på kvartalsgrænse | aktiv | Momskerne | TIL | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-045 | Sekvens/dato-inkonsistens | aktiv | Forensic & statistik | FRA | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-046 | Stort lag mellem faktura- og bogføringsdato | aktiv | Momskerne | TIL | Leveringstidspunkt og angivelsesperiode — periodisering (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-047 | Manglende leverandørnavn | aktiv | Stamdata- & datakvalitet | FRA | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-048 | Dublerede leverandører | aktiv | Stamdata- & datakvalitet | FRA | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-049 | Manglende CVR på dansk leverandør | aktiv | Momskerne | TIL | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-050 | Ugyldigt CVR-nummer | aktiv | Momskerne | TIL | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-051 | Engangsleverandør, højt beløb | aktiv | Stamdata- & datakvalitet | FRA | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-052 | Stor kunde uden momsnummer | aktiv | Momskerne | TIL | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-053 | Samme momsnr på flere parter | aktiv | Momskerne | TIL | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-054 | Part i begge roller | aktiv | Stamdata- & datakvalitet | FRA | Gyldigt momsnummer og parts-/fakturakrav (momsloven, VIES) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-055 | Rundt beløb | aktiv | Forensic & statistik | FRA | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-056 | Beløb lige under grænse | aktiv | Forensic & statistik | FRA | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-057 | Kontant over grænsen | aktiv | Momskerne | TIL | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-058 | Usædvanligt stort beløb | aktiv | Forensic & statistik | FRA | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-059 | Stort momsbeløb uden bilag | aktiv | Momskerne | TIL | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-060 | Negativt momsbeløb | aktiv | Momskerne | TIL | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-061 | Konto-beløb outlier | aktiv | Forensic & statistik | FRA | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-062 | Mulig strukturering | aktiv | Forensic & statistik | FRA | Beløbs- og tærskelkontroller (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-063 | Benford-afvigelse (første ciffer) | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-064 | Mange runde beløb | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-065 | Sjælden momskode | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-066 | Gentaget beskrivelse | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-067 | Posteringsspike | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-068 | Sjældent brugt konto | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-069 | Skæv øre-fordeling | aktiv | Forensic & statistik | FRA | Statistisk anomalidetektion, Benford m.v. (Skattestyrelsens kontrolmetoder) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-070 | EU-køb uden reverse charge-markering | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-071 | Reverse charge på indenlandsk handel | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-072 | RC-vare med moms i stedet for omvendt betalingspligt | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-073 | Asymmetrisk reverse charge | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-074 | Byggeydelse uden omvendt betalingspligt | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-075 | Reverse charge uden modparts-momsnr | aktiv | Momskerne | TIL | Omvendt betalingspligt / reverse charge (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-076 | Højt købsmoms/salgsmoms-forhold | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-077 | Momskonto afstemmer ikke | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-078 | Negativt momstilsvar | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-079 | Købsmoms uden grundlag | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-080 | Indtægt uden momsbehandling | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-081 | Stor andel momsfri omsætning | aktiv | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-082 | test_82_period_declaration | inaktiv_kraever_kildedata | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | (Fase C) |
| VATA-083 | test_83_partial_deduction | inaktiv_kraever_kildedata | Momskerne | TIL | Opgørelse og afstemning af ind-/udgående moms; (delvis) fradragsret (momsloven) | (Fase C) |
| VATA-084 | Missing trader-indikator | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-085 | test_85_carousel_pattern | inaktiv_kraever_kildedata | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | (Fase C) |
| VATA-086 | Hurtig gennemstrømning | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-087 | Højrisikovare | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-088 | Nul-margin på højrisikovare | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-089 | Nystartet høj-volumen leverandør | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-090 | test_90_payment_pattern | inaktiv_kraever_kildedata | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | (Fase C) |
| VATA-091 | Mellemregnings-/gennemstrømningskonto | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-092 | Falsk faktura-indikator | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-093 | Identisk beløb hos mange parter | aktiv | Forensic & statistik | FRA | Svig/MTIC: solidarisk hæftelse og karruselindikatorer (momsloven, EU) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-094 | EU-forbrugersalg (OSS) | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-095 | Fjernsalgstærskel overskredet | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-096 | Digital ydelse med dansk moms til EU-forbruger | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-097 | Dansk moms på EU-forbrugersalg | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-098 | Lavværdiimport uden importmoms (IOSS) | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-099 | test_99_platform_liability | inaktiv_kraever_kildedata | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | (Fase C) |
| VATA-100 | Elektronisk ydelse uden momskode | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-101 | Teleydelse uden momskode | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-102 | Rejseydelse med fuld moms | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |
| VATA-103 | Brugtmoms-vare med fuld moms | aktiv | E-handel & særordninger | FRA | E-handel, digitale ydelser og særordninger: OSS, brugtmoms, rejsebureau (momsloven) | tests/test_validation_suite.py + validation/scenarios.py |

## Inaktive kontroller (beslutning og afhængighed)

| ID | Kontrol | Afhænger af | Beslutning |
|----|---------|-------------|------------|
| VATA-082 | test_82_period_declaration | indberettet momsangivelse for perioden | AKTIVERES via angivelses-input: upload af momsangivelse ELLER positiv indtastning (simuleret dansk momsangivelse). Motoren afstemmer beregnede totaler mod indberettede tal. |
| VATA-083 | test_83_partial_deduction | delvis fradragsret (fordelingsnøgle) | AKTIVERES todelt: (1) fra data — udled og flag anvendt fradragsbrøk pr. linje + flere/uensartede brøker; (2) fuldt via toggle 'Virksomheden har 100% momspligtig aktivitet' (default til). Slås fra -> indtast foreløbig (anvendt i perioden) og evt. endelig fradragsret (efterregulering, kendes i juni-angivelsen året efter). |
| VATA-085 | test_85_carousel_pattern | vareflow på tværs af virksomheder (multi-entity) | UDEN FOR SCOPE for single-entity-analyse — dokumenteret begrænsning. Proxy-signaler gives af de øvrige cat11-kontroller. |
| VATA-090 | test_90_payment_pattern | fuld betalingsdata (modtagerkonto, betalingsdato, tredjepart) | PARKERET: kandidat til valgfrit betalingsdata-input (fraud: faktiske overførsler vs. bogførte tal). |
| VATA-099 | test_99_platform_liability | salgskanal + underliggende sælger (markedsplads-hæftelse) | UDEN FOR SCOPE for bogføringsbaseret analyse — dokumenteret begrænsning. |

