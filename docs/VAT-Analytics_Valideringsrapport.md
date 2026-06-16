# VAT Analytics — Valideringsrapport

> Auto-genereret af `python -m validation.run_validation` · 2026-06-16

✅ **BESTÅET** — 8/8 scenarier bestået (plantet defekt fanges + ren baseline er tavs).

| test_id | Kontrol | Fanger defekt | Ren er tavs | Resultat |
|---------|---------|---------------|-------------|----------|
| 27 | EU-handel uden momsnummer | ja | ja | ✅ |
| 29 | EU-erhvervelse med moms pålagt (manglende reverse charge) | ja | ja | ✅ |
| 30 | Eksport til tredjeland med moms pålagt | ja | ja | ✅ |
| 31 | EU B2B-salg med dansk moms | ja | ja | ✅ |
| 36 | Indenlandsk leverance nulsat som udlandssalg (vare forlader ikke DK) | ja | ja | ✅ |
| 46 | Stort lag mellem faktura- og bogføringsdato | ja | ja | ✅ |
| 79 | Købsmoms uden grundlag | ja | ja | ✅ |
| 80 | Indtægt uden momsbehandling | ja | ja | ✅ |

Dækning: **8** scenarier (repræsentativt startsæt; udvides til alle 98 aktive kontroller).

