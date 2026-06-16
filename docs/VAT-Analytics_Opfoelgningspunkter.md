# VAT Analytics — opfølgningspunkter (åbne)

Levende liste over åbne punkter fra EY-løftet. Hold den opdateret.

## Fase B — regelkatalog & sporbarhed
- **Udfyld præcis `kilde` (momslov-paragraf) pr. kontrol.** Mekanikken er på plads
  (`catalog/rules.json` + `catalog/rule_notes.json` → matrix/rapport). Pt. viser
  matricen kategoriens *retsområde* som fallback (0/103 præcise kilder udfyldt).
  Den fagansvarlige skriver de præcise paragraffer ind i `rule_notes.json` (felt
  `"kilde"` pr. test_id); katalog + matrix regenereres af generatorerne.
  Claude kan lave et kategori-udkast til gennemsyn, men opfinder ikke paragraf-numre.

## Fase C — uafhængig valideringssuite (i gang)
- Plantet defekt pr. aktiv kontrol (98), gated i CI. Udfylder samtidig `test`-kolonnen
  i sporbarhedsmatricen.

## Inaktive kontroller — besluttede features
- **82 (periode vs. angivelse):** byg angivelses-input (upload af momsangivelse ELLER
  simuleret dansk momsangivelse) → afstem beregnede totaler mod indberettede tal.
- **83 (delvis fradragsret):** (1) udled/flag anvendt fradragsbrøk fra data; (2) toggle
  "Virksomheden har 100% momspligtig aktivitet" (default til) → slås fra: indtast
  foreløbig + evt. endelig fradragsret (efterregulering, kendes i juni-angivelsen).
- **90 (betalingsmønstre):** parkeret — valgfrit upload af fuld betalingsdata (fraud:
  faktiske overførsler vs. bogførte tal).
- **85, 99:** uden for scope for single-entity-analyse — dokumenterede begrænsninger.

## Drift (tværgående)
- **EU data-residency for søsterværktøjer:** VIES (`efficient-amazement`) og Data
  Extract (`powerful-endurance`) ligger formentlig stadig med volumen i US. Migrér
  til EU som VAT Analytics (slet US-volumen → sæt EU → genopret volumen). Noteret i
  playbook'ens drifts-afsnit.

## Senere faser
- **Fase E:** godkendelses-dokumentationspakke (4 docx) efter SAF-T-skabelon.
- **Fase F:** stress-test på rigtige klientdata; tæl røde → reelle (jf. VIES 37→4).
