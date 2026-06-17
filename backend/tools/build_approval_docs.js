#!/usr/bin/env node
/* =========================================================================
 * build_approval_docs.js — genererer godkendelses-dokumentationspakken (4 docx)
 * for VAT Analytics, efter SAF-T/VIES/Data Extract-skabelonen.
 *
 * Output (docs/):
 *   VAT-Analytics_Godkendelses-overblik.docx        (START HER)
 *   VAT-Analytics_Solution_Architecture.docx
 *   VAT-Analytics_Sikkerhed_og_databehandling.docx
 *   VAT-Analytics_Hosting_og_drift.docx
 *
 * Kør (mest pålideligt — lokal install i repo-roden):
 *   cd <repo-rod> && npm install && node backend/tools/build_approval_docs.js
 * (package.json + node_modules ligger i repo-roden, UDEN FOR Railway-servicens
 *  backend/-rod, og er gitignored — så de ikke påvirker deploy-builden.)
 * ========================================================================= */

const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, LevelFormat, HeadingLevel, BorderStyle, WidthType,
  ShadingType, PageBreak,
} = require("docx");

const REPO = path.resolve(__dirname, "..", "..");
const DOCS = path.join(REPO, "docs");
const CATALOG = path.join(REPO, "backend", "catalog", "rules.json");
const DATE = "16. juni 2026";
const CONTENT_W = 9360; // US Letter, 1" margins
const NAVY = "1B365D";

// Tal hentes fra det versionerede regelkatalog, så dokumenterne ikke kan drifte.
let CAT_VERSION = "1.0.0", N_TOTAL = 103, N_ACTIVE = 98, N_INACTIVE = 5, INACTIVE_IDS = "82, 83, 85, 90, 99";
try {
  const c = JSON.parse(fs.readFileSync(CATALOG, "utf-8"));
  CAT_VERSION = c.catalog_version;
  N_TOTAL = c.antal_kontroller;
  const inact = c.regler.filter((r) => r.status !== "aktiv");
  N_INACTIVE = inact.length;
  N_ACTIVE = N_TOTAL - N_INACTIVE;
  INACTIVE_IDS = inact.map((r) => r.test_id).join(", ");
} catch (e) { /* defaults */ }

// ---------------------------------------------------------------------------
const H1 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(t)] });
const H2 = (t) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(t)] });
const P = (t, opts = {}) => new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text: t, ...opts })] });

function bullets(items) {
  return items.map((t) => new Paragraph({ numbering: { reference: "bullets", level: 0 }, spacing: { after: 60 }, children: [new TextRun(t)] }));
}
function numbered(items, ref) {
  return items.map((t) => new Paragraph({ numbering: { reference: ref, level: 0 }, spacing: { after: 60 }, children: [new TextRun(t)] }));
}

const BORD = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const BORDERS = { top: BORD, bottom: BORD, left: BORD, right: BORD };

function cell(text, width, { head = false, bold = false } = {}) {
  return new TableCell({
    borders: BORDERS,
    width: { size: width, type: WidthType.DXA },
    shading: head ? { fill: NAVY, type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 80, bottom: 80, left: 120, right: 120 },
    children: [new Paragraph({ children: [new TextRun({ text, bold: head || bold, color: head ? "FFFFFF" : undefined })] })],
  });
}
function table(headers, rows, widths) {
  const headRow = new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, widths[i], { head: true })) });
  const bodyRows = rows.map((r) => new TableRow({ children: r.map((c, i) => cell(String(c), widths[i])) }));
  return new Table({ width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: widths, rows: [headRow, ...bodyRows] });
}
function titleBlock(title, subtitle, note) {
  const out = [
    new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: "VAT Analytics", bold: true, color: NAVY, size: 28 })] }),
    new Paragraph({ spacing: { after: 60 }, children: [new TextRun({ text: title, bold: true, size: 44, color: NAVY })] }),
    new Paragraph({ spacing: { after: 200 }, children: [new TextRun({ text: subtitle, italics: true, size: 24, color: "555555" })] }),
  ];
  if (note) out.push(new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text: note, size: 20, color: "555555" })] }));
  out.push(new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text: `Bal AI · ${DATE} · katalogversion ${CAT_VERSION}`, size: 20, color: "777777" })] }));
  out.push(new Paragraph({ children: [new PageBreak()] }));
  return out;
}
function makeDoc(children) {
  return new Document({
    styles: {
      default: { document: { run: { font: "Arial", size: 22 } } },
      paragraphStyles: [
        { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 30, bold: true, font: "Arial", color: NAVY }, paragraph: { spacing: { before: 280, after: 140 }, outlineLevel: 0 } },
        { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 25, bold: true, font: "Arial", color: "2E5C8A" }, paragraph: { spacing: { before: 180, after: 100 }, outlineLevel: 1 } },
      ],
    },
    numbering: {
      config: [
        { reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] },
        ...["n1", "n2", "n3", "n4"].map((ref) => ({ reference: ref, levels: [{ level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 720, hanging: 360 } } } }] })),
      ],
    },
    sections: [{ properties: { page: { size: { width: 12240, height: 15840 }, margin: { top: 1440, right: 1440, bottom: 1440, left: 1440 } } }, children }],
  });
}

// ===========================================================================
// 1) GODKENDELSES-OVERBLIK
// ===========================================================================
const overblik = makeDoc([
  ...titleBlock("Godkendelses-overblik", "Forside- og statusdokument for værktøjsgodkendelse",
    "Dette dokument er indgangen til pakken: det opsummerer hvad der findes, hvad der er dækket, og hvad der udestår. Det er skrevet efter best practice og skal indpasses i EY's konkrete godkendelsesskabelon."),

  H1("1. Produktet kort fortalt"),
  P(`VAT Analytics er et webbaseret værktøj, der kører ${N_TOTAL} automatiserede momskontroller (baseret på Skattestyrelsens kontrolmetoder) mod et momsdataudtræk fra en virksomheds bogføring (Excel/CSV). Kontrollerne er fordelt på 12 kategorier — fra transaktionsintegritet og dubletter over momssats, grænseoverskridende handel og reverse charge til afstemning, statistisk anomalidetektion, svindel/MTIC og e-handelssærordninger.`),
  P("For hvert fund angives en sporbar reference til den eller de berørte kildetransaktioner (bilag, dato, konto, beløb, det felt der udløste fundet), en impact-klassifikation (økonomisk, renterisiko eller compliance) og en sværhedsgrad. Resultatet præsenteres som en prioriteret liste frem for en mur af flag."),
  P(`Værktøjet behandler kundedata (selve momsudtrækket), men er bevidst designet som et kontrolværktøj — ikke et arkiv: inputfilen slettes straks efter kørslen, resultaterne holdes kun i hukommelsen for den enkelte kørsel, og revisionsloggen indeholder udelukkende metadata. Af de ${N_TOTAL} kontroller er ${N_ACTIVE} aktive på et fladt bogføringsudtræk; de resterende ${N_INACTIVE} (test ${INACTIVE_IDS}) er bevidst inaktive, fordi de kræver kildedata en flad eksport ikke bærer — hver med en dokumenteret beslutning.`),

  H1("2. Dokumentationspakke"),
  P("Alle artefakter ligger versionsstyret i repoets docs/-mappe (og koden i backend/):"),
  table(["Fil", "Indhold"], [
    ["VAT-Analytics_Godkendelses-overblik.docx", "Dette dokument — status og indgang til pakken."],
    ["VAT-Analytics_Solution_Architecture.docx", "Formål, scope, arkitektur, kontrol-/regelmodel, dataflow, QA, hosting, begrænsninger, åbne punkter."],
    ["VAT-Analytics_Sikkerhed_og_databehandling.docx", "Datakategorier, dataflow, GDPR/ikke-arkiv, adgang, trusselsmodel, logning."],
    ["VAT-Analytics_Hosting_og_drift.docx", "Nuværende drift (EU), EY-platform, migration, env-inventar, backup, CI, roller."],
    ["VAT-Analytics_Regel-sporbarhedsmatrix.xlsx", "Kontrol → kilde → modul → test (auto-genereret fra regelkataloget)."],
    ["VAT-Analytics_Sporbarhedsrapport.md", "Dækningsrapport: aktive/inaktive, kilde- og testdækning."],
    ["VAT-Analytics_Valideringsrapport.md", "Auto-genereret resultat af den uafhængige valideringssuite."],
  ], [4200, 5160]),
  P("«Udkast» = skrevet efter best practice, men skal gennemgås/godkendes af de relevante funktioner og indpasses i EY's skabelon.", { italics: true }),

  H1("3. Godkendelsesparathed pr. område"),
  table(["Område", "Status", "Bemærkning"], [
    ["Arkitektur & testbarhed", "Grøn", "Al kontrol-logik i en netværksfri pakke (backend/analytics/); tyndt FastAPI web-lag."],
    ["Auth & adgang", "Grøn", "Session-auth (pbkdf2), /setup, invitationer, rate-limit, CSRF — porteret til FastAPI."],
    ["HTTP-sikkerhed", "Grøn", "Stram CSP uden CDN'er/unsafe-*, HSTS, fuld header-pakke; UI er self-hostet."],
    ["Regelkatalog & sporbarhed", "Grøn*", `Versioneret katalog (${N_TOTAL} kontroller) + matrix. *Præcis momslov-kilde pr. kontrol pinnes af fagansvarlig.`],
    ["Uafhængig validering", "Grøn", `Valideringssuite: ${N_ACTIVE}/${N_ACTIVE} aktive kontroller med plantet defekt, gated i CI.`],
    ["Datapolitik", "Grøn", "Ikke-arkiv: inputfil slettes efter kørsel; resultater kun i hukommelsen; audit kun metadata."],
    ["Importkontrakt", "Grøn", "Udvidet med ship_from/ship_to, adskilt faktura-/bogføringsdato og tax_base; drevet af delt feltkontrakt."],
    ["Drift / hosting", "Gul", "Live i EU (Railway, 1 worker/1 replica). Produktionsdrift på EY-platform er en forudsætning."],
    ["Stress-test på rigtige data", "Gul", "Kørsel på et reelt klientudtræk (tæl røde → reelle) udestår."],
  ], [2700, 1100, 5560]),

  H1("4. Åbne punkter (kræver EY / eksterne / faglige beslutninger)"),
  ...bullets([
    "Mapning mod EY's konkrete godkendelsesskabelon (denne pakke er skrevet efter best practice).",
    "Produktionsdrift på EY-godkendt infrastruktur (EU-dataplacering) + databehandleraftale.",
    "Faglig pinning af præcis momslov-kilde pr. kontrol i regelkataloget (matricen viser i dag kategoriens retsområde).",
    "Stress-test på et rigtigt klientudtræk; verificér at RØD = action krævet (tæl røde → reelle).",
    "Uafhængig sikkerhedsgennemgang / penetrationstest.",
    "Beslutning om de besluttede, endnu ikke byggede features for inaktive kontroller (82: angivelses-input; 83: delvis-fradragsret-toggle; 90: valgfrit betalingsdata).",
    "Support- og vedligeholdelsesmodel samt rolle-/ansvarsbeskrivelse.",
  ]),

  H1("5. Anbefalet rækkefølge mod godkendelse"),
  ...numbered([
    "Forelæg pakken for EY's tool-governance og få den konkrete godkendelsesskabelon.",
    "Indpas de fire dokumenter + matrix/rapporter i EY's skabelon og afklar manglende afsnit.",
    "Aftal driftsplatform og igangsæt platformsflytning + databehandleraftale parallelt.",
    "Pin de præcise momslov-kilder pr. kontrol; matrix/rapport regenereres automatisk.",
    "Stress-test mod rigtige klientudtræk; ret driftsfund ved roden og kalibrér materialitet.",
    "Bestil penetrationstest og indstil til formel godkendelse, når åbne punkter er lukket eller planlagt.",
  ], "n1"),
  P(`Status pr. ${DATE}: Alt der kan lukkes i kode og dokumentation internt, er på plads — versioneret regelkatalog, sporbarhedsmatrix, uafhængig valideringssuite (${N_ACTIVE}/${N_ACTIVE} gated i CI), porteret session-auth, stram HTTP-sikkerhed, udvidet importkontrakt og drift i EU. De resterende punkter er bevidst markeret som åbne, fordi de kræver EY-beslutninger, faglig pinning eller eksterne parter.`, { bold: true }),
]);

// ===========================================================================
// 2) SOLUTION ARCHITECTURE
// ===========================================================================
const arch = makeDoc([
  ...titleBlock("Løsnings- og arkitekturbeskrivelse", "Grundlag for produkt-/værktøjsgodkendelse",
    "Bemærk: Dette dokument er udarbejdet som et best practice-grundlag og er endnu ikke mappet mod EY's konkrete godkendelsesskabelon. Afsnit 13 lister de åbne punkter."),

  H1("1. Introduktion og formål"),
  P(`VAT Analytics er et software-værktøj, der analyserer et momsdataudtræk fra en virksomheds bogføring og kører ${N_TOTAL} automatiserede momskontroller imod det. Værktøjet er målrettet revisorer, controllere og momskonsulenter, der skal kvalitetssikre en virksomheds momsbehandling og afdække fejl, risici og indikationer på svindel.`),
  P("Dette dokument beskriver værktøjets arkitektur, kontrol- og regelmodel, datahåndtering, sikkerhed, kvalitetssikring og driftsmodel, som grundlag for en formel værktøjsgodkendelse. Det ledsages af en separat regel-sporbarhedsmatrix (Excel), der for hver kontrol angiver dens retsområde/kilde, det modul der håndhæver den, og den test der dækker den."),

  H1("2. Kerneudfordring"),
  P("Den dominerende udfordring er korrekthed og sporbarhed: at de beregninger og aggregater værktøjet udleder er korrekte, og at hvert fund kan spores fra det rapporterede tal tilbage til den konkrete kildetransaktion. Hertil kommer materialitet (at et rødt flag reelt betyder «action krævet», ikke støj) og inputdatakvalitet/-bredde (at kontrollerne fodres med de rette felter frem for at gætte). Designprincippet er konservativt: en kontrol fyrer kun, når data understøtter det, så falske alarmer undgås."),
  P("For at undgå dobbelttælling rapporteres de økonomiske totaler både som BRUTTO (sum af per-kontrol-estimater, kan overlappe når flere kontroller flager samme transaktion) og DISTINKT (transaktions-dedupliceret, hvor hver transaktion tæller én gang med det største per-kontrol-bidrag). Et automatiseret testet invariant sikrer, at det distinkte tal aldrig overstiger bruttotallet."),

  H1("3. Afgrænsning (scope)"),
  H2("3.1 I scope"),
  ...bullets([
    `${N_TOTAL} momskontroller i 12 kategorier (heraf ${N_ACTIVE} aktive på et fladt bogføringsudtræk).`,
    "Import af momsudtræk fra Excel/CSV med fleksibel, alias-baseret kolonnegenkendelse (dansk + engelsk + ERP-navne).",
    "En prioriteret findings-rapport med impact-/sværhedsklassifikation og sporbarhed til kildetransaktioner.",
    "Et versioneret regelkatalog + en uafhængig valideringssuite (plantet defekt pr. kontrol), gated i CI.",
  ]),
  H2("3.2 Uden for scope"),
  ...bullets([
    "Værktøjet afgiver ikke en endelig momsfaglig konklusion; det leverer kvalificerede fund til revisorens/konsulentens vurdering.",
    `${N_INACTIVE} kontroller (test ${INACTIVE_IDS}) er inaktive på et fladt udtræk og kræver enten ekstra kildedata eller falder uden for en single-entity-analyse (se afsnit 12).`,
    "Selve udtrækket fra ERP-systemet foretages uden for værktøjet (se søsterværktøjet Data Extract).",
  ]),

  H1("4. Arkitekturoverblik"),
  P("Værktøjet er en web-applikation i Python (FastAPI) med et bevidst tyndt web-lag. Al kontrol-logik ligger i en testbar pakke (backend/analytics/) uden netværksafhængighed, så den kan valideres deterministisk i CI. Importlaget (backend/parsers/) genkender kolonner og tilpasser de flade rækker til den linje-/transaktionsstruktur, motoren forventer."),
  H2("4.1 Teknologistak"),
  table(["Lag", "Teknologi"], [
    ["Web-lag", "Python 3.12 + FastAPI/uvicorn (ruter, jobstyring, auth, sikkerhedsheaders)"],
    ["Kontrol-logik", "backend/analytics/: engine + 12 kategorimoduler + delte momsregler (ren Python)"],
    ["Import", "backend/parsers/: excel_parser (kolonne-aliaser) + data_adapter (SAF-T-lignende struktur)"],
    ["Auth/governance", "Session-auth (Starlette SessionMiddleware) + revisionslog (SQLite), porteret fra SAF-T/VIES"],
    ["Frontend", "Self-hostet HTML/CSS/JS (ingen CDN'er); resultater renderes via event-delegation under stram CSP"],
    ["CI", "GitHub Actions: pytest + katalogvalidering + uafhængig valideringssuite (gated) + pip-audit"],
  ], [2400, 6960]),
  H2("4.2 Komponenter"),
  ...bullets([
    "backend/analytics/engine.py — orkestrerer alle kontroller og bygger den klassificerede rapport (scores, impact, severity).",
    "backend/analytics/categories/cat01–cat12 — de 12 kategorimoduler med kontrollerne.",
    "backend/analytics/vat_rules.py — delte danske momsregler/helpers (satser, EU-lande, momsnr-format, CVR mod-11, statistik).",
    "backend/parsers/ — excel_parser.py (import + alias-genkendelse) og data_adapter.py (struktur + tax_base).",
    "backend/catalog/ — versioneret rules.json + rule_notes.json (kilde/test/beslutninger).",
    "backend/validation/ — uafhængig valideringssuite (ren/defekt-scenarie pr. kontrol).",
    "backend/tools/ — generatorer: build_rules_catalog.py, build_traceability.py, build_approval_docs.js.",
  ]),

  H1("5. Behandlingsmodel og dataflow"),
  ...numbered([
    "Brugeren logger ind og uploader et momsudtræk (Excel/CSV). Små filer analyseres synkront; store filer (≥50 MB, op til 2 GB) kører som et asynkront baggrundsjob med statuspolling.",
    "Importlaget genkender kolonnerne (alias-baseret) og tilpasser rækkerne til transaktions-/linjestruktur.",
    "Motoren kører alle kontroller og bygger en rapport med fund, impact-/sværhedsklassifikation og sporbarhed til kildetransaktioner.",
    "Resultatet vises i browseren. Inputfilen slettes straks efter kørslen; resultatet holdes kun i hukommelsen for kørslen og persisteres ikke til disk.",
  ], "n2"),
  P("Jobtilstand holdes in-memory, hvilket bevidst binder driften til én proces/instans (se Hosting og drift). Det understøtter dataminimering: ingen kundedata efterlades på disk efter kørslen."),

  H1("6. Kontrol- og regelmodel"),
  P(`Kontrollerne er katalogiseret i et versioneret regelkatalog (catalog/rules.json, katalogversion ${CAT_VERSION}), der auto-genereres direkte fra de ${N_TOTAL} kontrolfunktioner i koden — så kataloget aldrig kan drifte fra implementeringen. Hver post bærer: id, kategori, impact-type, sværhedsgrad, status (aktiv/inaktiv), modul, funktion samt felterne kilde (retsområde/paragraf) og test (dækkende valideringstest). For inaktive kontroller registreres desuden afhængighed og en eksplicit scope-beslutning.`),
  P("Sporbarhedsmatricen (Excel) og dækningsrapporten genereres fra kataloget, så kontrol → kilde → modul → test holder sig synkront. Resultatmodellen skelner mellem et fagligt fund og en teknisk fejl: kontrollerne «springer pænt over», når data mangler, frem for at producere falske positiver."),

  H1("7. Uafhængig valideringssuite"),
  P(`Hver aktiv kontrol har et scenarie med en REN baseline (kontrollen må ikke fyre) og en DEFEKT variant med én plantet defekt (kontrollen SKAL fyre). Det beviser både følsomhed (defekten fanges) og fravær af falske alarmer (ren baseline er tavs). Suiten dækker alle ${N_ACTIVE} aktive kontroller og køres som en gated portbevogter i CI sammen med den øvrige testsuite.`),

  H1("8. Determinisme, versionering og reproducerbarhed"),
  ...bullets([
    "Regelkataloget er versioneret (catalog_version) og auto-genereres fra koden via tools/build_rules_catalog.py.",
    "Sporbarhedsmatrix, sporbarhedsrapport og valideringsrapport genereres deterministisk fra kataloget/suiten.",
    "Runtime-afhængigheder er versionspinnede; runtime og test er adskilt (requirements.txt vs. requirements-dev.txt).",
    "Alt er i git; ændringer er sporbare via commits, og katalogintegriteten gates i CI.",
  ]),

  H1("9. Importkontrakt og inputdatabredde"),
  P("Importen genkender et bredt sæt kolonnenavne (danske, engelske og ERP-specifikke aliaser) drevet af et delt kanonisk feltkontrakt med søsterværktøjet Data Extract. Kontrakten er udvidet med tre selvstændige, prioriterede vareflow-/datofelter: ship_from_country og ship_to_country (forsendelsesland) samt en fakturadato adskilt fra bogføringsdatoen. Hertil et importerbart momsgrundlag (tax_base), der foretrækkes over det udledte, når kilden leverer det."),
  P("Udvidelsen aktiverer kontroller, der ellers ville hvile på heuristik: place-of-supply/trekantshandel (kontrol 36) kan nu fange fx uberettiget nulrating, når en vare ikke forlader Danmark men sælges til udlandet, samt levering til et andet land end modparten; og faktura/bogførings-lag (kontrol 46) kan afdække periodiseringsrisiko."),

  H1("10. Sikkerhed og databehandling (resumé)"),
  P("Adgang kræver login (session-auth, pbkdf2, /setup ved første kørsel, invitationer, roller, rate-limit, CSRF). Alle svar bærer en stram CSP uden CDN'er samt HSTS og øvrige sikkerhedsheaders. Inputfilen slettes straks efter kørslen, resultater persisteres ikke, og en append-only revisionslog registrerer kun metadata. Detaljer i det separate dokument Sikkerhed og databehandling."),

  H1("11. Kvalitetssikring og test"),
  P(`Værktøjet er dækket af en automatiseret testsuite (parser-, motor-, auth- og sikkerhedstests) samt den uafhængige valideringssuite (${N_ACTIVE}/${N_ACTIVE} aktive kontroller med plantet defekt). CI kører pytest, katalogvalidering og valideringssuiten (gated) samt pip-audit --strict på runtime-afhængigheder ved hver push/PR.`),

  H1("12. Kendte begrænsninger og forudsætninger"),
  ...bullets([
    `${N_INACTIVE} kontroller er inaktive på et fladt udtræk: test 82 (kræver indberettet momsangivelse), 83 (delvis fradragsret/fordelingsnøgle), 85 (karrusel — kræver vareflow på tværs af virksomheder), 90 (kræver fuld betalingsdata) og 99 (kræver salgskanal/markedsplads-data). Hver har en dokumenteret beslutning (aktivér via input / parkeret / uden for scope).`,
    "Analysen kvalitetssikrer momsbehandlingen, men erstatter ikke et fagligt skøn.",
    "Resultaternes værdi afhænger af inputdatakvaliteten; manglende felter kan gøre enkelte kontroller mindre præcise.",
    "Materialitetstærskler er p.t. faste i koden; engagement-styret materialitet er en mulig udvidelse.",
  ]),

  H1("13. Åbne punkter for EY-godkendelse"),
  ...bullets([
    "Mapning mod EY's konkrete godkendelsesskabelon.",
    "Produktionsdrift på EY-godkendt infrastruktur (EU) + databehandleraftale.",
    "Faglig pinning af præcis momslov-kilde pr. kontrol.",
    "Stress-test mod rigtige klientudtræk + materialitetskalibrering.",
    "Uafhængig sikkerhedsgennemgang / penetrationstest.",
    "Support-/vedligeholdelsesmodel og rolle-/ansvarsbeskrivelse.",
  ]),

  H1("Appendiks A — Modulinventar"),
  table(["Modul", "Ansvar"], [
    ["main.py", "Tyndt FastAPI web-lag: ruter, upload/jobstyring, auth-wiring, sikkerhedsheaders."],
    ["auth.py", "Session-auth: /setup, login, invitationer, roller, rate-limit, CSRF (porteret til FastAPI)."],
    ["audit_log.py", "Append-only revisionslog (kun metadata)."],
    ["analytics/engine.py", "Orkestrering + rapportbygning (scores, impact, severity)."],
    ["analytics/categories/*", "De 12 kategorimoduler med kontrollerne."],
    ["analytics/vat_rules.py", "Delte danske momsregler/helpers."],
    ["parsers/*", "Import (alias-genkendelse) + adapter til motorens struktur."],
    ["validation/*", "Uafhængig valideringssuite (ren/defekt pr. kontrol)."],
    ["tools/*", "Generatorer: regelkatalog, sporbarhed, docx."],
  ], [3000, 6360]),
]);

// ===========================================================================
// 3) SIKKERHED OG DATABEHANDLING
// ===========================================================================
const sec = makeDoc([
  ...titleBlock("Sikkerhed og databehandling", "Datakategorier, dataflow, GDPR, adgang og logning",
    "Best practice-grundlag. Den centrale pointe er, at værktøjet er et kontrolværktøj — ikke et arkiv: kundedata behandles kun under selve kørslen og efterlades ikke."),

  H1("1. Datakategorier"),
  table(["Kategori", "Indhold", "Opbevaring"], [
    ["Kundedata (momsudtræk)", "Den uploadede Excel/CSV med transaktioner — kan indeholde modparts-navne, momsnumre m.v.", "Slettes straks efter kørslen."],
    ["Analyseresultat", "Fund + sporbarhed til kildetransaktioner, genereret for kørslen.", "Kun i hukommelsen; persisteres ikke til disk."],
    ["Auth-data", "Brugernavne + saltede password-hashes (pbkdf2), roller, invitationer.", "SQLite på persistent EU-volumen."],
    ["Revisionslog", "Kun metadata: hændelse, aktør, tidspunkt, IP, filstørrelse, udfald.", "SQLite, append-only, EU-volumen."],
  ], [2400, 4800, 2160]),

  H1("2. Dataflow"),
  P("Brugeren logger ind og uploader et momsudtræk. Filen gemmes midlertidigt med et tilfældigt (UUID-baseret) filnavn under en valideret upload-mappe, parses og analyseres (synkront for små filer, som asynkront baggrundsjob for store). Resultatet vises i browseren. Inputfilen slettes straks efter kørslen (også ved fejl), og resultatet holdes kun i hukommelsen for kørslen — det skrives ikke til disk og arkiveres ikke."),

  H1("3. Databeskyttelse (GDPR) — ikke-arkiv-princippet"),
  ...bullets([
    "Dataminimering: kundedata behandles kun under kørslen. Inputfilen slettes straks efter, og resultater persisteres ikke.",
    "Personhenførbare data i udtrækket (fx modparts-navne/momsnumre) opbevares ikke efter kørslen.",
    "De eneste vedvarende persondata er brugerkonti (navn/brugernavn) og revisionslog-metadata til adgangsstyring og governance — begge på et EU-volumen.",
    "Passwords gemmes kun som saltede hashes (pbkdf2:sha256). Revisionsloggen indeholder aldrig momsdata/kundedata — kun metadata.",
    "Transport sker over HTTPS. Databehandleraftale og endelig dataplacering fastlægges af driftsplatformen (EY).",
  ]),

  H1("4. Adgang"),
  ...bullets([
    "Adgang kræver login. Første kørsel opretter administrator via en /setup-side; yderligere brugere oprettes via tidsbegrænsede engangs-invitationslinks. Adskilte roller (administrator/bruger).",
    "Der findes ingen credentials i kode eller miljøvariabler — den tidligere hardcodede HTTP Basic Auth er fjernet.",
    "Adgangskoder gemmes som saltede hashes (pbkdf2:sha256, stdlib). Login er timing-sikkert og beskyttet af rate limiting (pr. bruger og pr. IP) i SQLite.",
    "Alle tilstandsændrende handlinger er CSRF-beskyttede (session-token, konstant-tid-sammenligning; header eller form-felt).",
    "Analyse-API'erne kræver login; uautoriserede kald får 401 (API) eller omdirigeres til login (web).",
  ]),

  H1("5. HTTP-sikkerhed"),
  P("Alle svar bærer en stram Content-Security-Policy uden CDN'er og uden 'unsafe-inline'/'unsafe-eval' — alle aktiver (CSS/JS) er self-hostet, og det dynamiske UI bruger event-delegation frem for inline-handlers. Desuden sættes X-Content-Type-Options (nosniff), X-Frame-Options (DENY), Referrer-Policy, Permissions-Policy og Strict-Transport-Security (HSTS). Session-cookies er HttpOnly, SameSite=Lax og Secure i produktion. Runtime-afhængigheder er versionspinnede og scannes med pip-audit i CI."),

  H1("6. Trusselsmodel (resumé)"),
  ...bullets([
    "Læk af kundedata efter kørsel: afbødet af ikke-arkiv-princippet — inputfil slettes, resultater persisteres ikke.",
    "Path traversal ved upload: filnavne saniteres og erstattes af UUID; den resolverede sti verificeres at ligge i upload-mappen; filtype og -størrelse valideres.",
    "XSS i resultat-UI: data renderes via tekst-escaping og event-delegation under stram CSP (ingen inline scripts).",
    "Brute force mod login: afbødet af rate limiting (pr. bruger/IP) + timing-sikker verifikation.",
    "Kompromittering af auth-db: passwords er kun hashes; admin-backup bruger SQLites konsistente backup-API og kræver admin + CSRF.",
  ]),

  H1("7. Revisionslog"),
  P("En append-only revisionslog (egen SQLite-database) registrerer sikkerhedsrelevante hændelser — login (succes/fejl/lockout), logout, oprettelse af administrator/invitationer, brugeradministration og analysekørsler — som metadata (hvem, hvornår, filstørrelse, udfald). Den giver et governance-egnet «hvem gjorde hvad, hvornår»-spor og indeholder aldrig momsnumre, beløb, navne eller andet kundedata."),

  H1("8. Underdatabehandlere og åbne punkter"),
  ...bullets([
    "Driftsplatform/hosting: Railway (EU-region) til dev/demo; produktionsplatform fastlægges som led i EY-flytningen (databehandleraftale).",
    "Penetrationstest / uafhængig sikkerhedsgennemgang udestår.",
    "Formel retention-/sletteproces for auth- og audit-data fastlægges med EY (inputdata slettes allerede pr. kørsel).",
  ]),
]);

// ===========================================================================
// 4) HOSTING OG DRIFT
// ===========================================================================
const hosting = makeDoc([
  ...titleBlock("Hosting og drift", "Nuværende drift (EU), EY-platform, migration, env-inventar og CI",
    "Best practice-grundlag. Den nuværende drift kører i EU, men er midlertidig; produktionsdrift på EY-godkendt platform er en forudsætning for godkendelse."),

  H1("1. Nuværende drift"),
  P("Værktøjet driftes p.t. midlertidigt på Railway (projekt airy-light, domæne vat.balai.dk) — til udvikling og demonstration. Appen er en standard Python/ASGI-applikation (FastAPI/uvicorn) bygget fra en Dockerfile. Servicen kører i EU-regionen (EU West, Amsterdam) med præcis 1 worker og 1 replica, da jobtilstanden holdes in-memory."),

  H1("2. Procesmodel (kritisk: 1 worker / 1 replica)"),
  P("Analysejobs holdes i hukommelsen i app-processen. Derfor SKAL der køre præcis én proces/instans — ellers kan et job blive startet på én instans og pollet på en anden. startCommand er pinnet i railway.json (uvicorn --workers 1), og skalering er låst til 1 replica. Bemærk at en tilknyttet volumen i sig selv binder servicen til én region og udelukker horisontal skalering, hvilket passer med denne model.", { bold: true }),

  H1("3. EY-platform (forudsætning)"),
  P("Det er en udtrykkelig forudsætning for godkendelse, at produktet driftes på EY's egen godkendte platform med kontroller for dataplacering (EU), kryptering, adgang, logning, backup og beredskab. Arkitekturen er platform-agnostisk (standard ASGI/WSGI), så flytningen kan ske uden ændringer i kontrol-logikken; ved behov kan SQLite-auth/-audit flyttes til en managed database."),

  H1("4. Migrationsplan / opsætning"),
  ...numbered([
    "Sæt servicens region til EU og opret en persistent volumen monteret på /data (region følger volumenet; volumen → 1 replica).",
    "Sæt db-stierne på volumenet: AUTH_DB_PATH=/data/auth.db og AUDIT_DB_PATH=/data/audit.db.",
    "Sæt en stærk SECRET_KEY (påkrævet i produktion; ellers brudte sessions). Sæt IKKE SESSION_COOKIE_SECURE=0 i produktion.",
    "Deploy via Dockerfile; startCommand (uvicorn --workers 1, $PORT) er pinnet i railway.json.",
    "Åbn /setup én gang og opret administrator — kontoen ligger nu på det persistente EU-volumen.",
    "Verificér /health, login-/setup-flow, at en genindlæsning bevarer sessionen, sikkerhedsheaders og CI-grøn.",
    "Etabler backup af /data-volumenet (auth + audit) og en dokumenteret sletteproces.",
  ], "n3"),
  P("KRITISK (verificeret 2026-06-15/16): uden persistent volumen + AUTH_DB_PATH/AUDIT_DB_PATH ligger SQLite-databaserne på containerens flygtige filsystem og nulstilles ved HVER redeploy — administratoren forsvinder, og sessions går i stykker. Med en stabil SECRET_KEY og databaserne på et monteret /data-volumen i EU overlever både brugere og sessions deploys og genstarter.", { bold: true }),
  P("DRIFTSFUND (verificeret 2026-06-16): railway.json's startCommand køres uden shell, så et bart \"$PORT\" ikke ekspanderes (uvicorn fejler med 'Invalid value for --port: $PORT'). Kommandoen pakkes derfor i 'sh -c', så $PORT ekspanderes korrekt. Region kunne ikke skiftes, mens en volumen var tilknyttet (volumen er region-bundet): volumenet blev fjernet, regionen sat til EU, og volumenet genoprettet i EU.", { bold: true }),

  H1("5. Miljø-inventar"),
  table(["Variabel", "Formål", "Default"], [
    ["SECRET_KEY", "Session-signering. SKAL sættes i produktion (ellers brudte sessions/genstart).", "(tilfældig — kun dev)"],
    ["AUTH_DB_PATH", "Sti til auth-databasen. SKAL pege på det persistente volumen.", "data/auth.db"],
    ["AUDIT_DB_PATH", "Sti til revisionsloggen. SKAL pege på det persistente volumen.", "(ved siden af auth.db)"],
    ["SESSION_COOKIE_SECURE", "Secure-flag på session-cookie. Lad være usat (=secure) i prod.", "1 (secure)"],
    ["CORS_ORIGINS", "Tilladte origins (komma-separeret).", "https://vat.balai.dk,http://localhost:3000"],
    ["MAX_UPLOAD_MB", "Maksimal uploadstørrelse.", "2048"],
    ["PORT", "Lytteport (sættes af platformen).", "5003 / 8080"],
  ], [2500, 4660, 2200]),

  H1("6. Backup og beredskab (BCDR)"),
  ...bullets([
    "Auth-databasen kan tages som et konsistent øjebliksbillede via en admin-beskyttet backup-funktion (SQLites backup-API).",
    "Det persistente /data-volumen (auth + audit) bør indgå i platformens backup-rutine.",
    "Da kundedata ikke opbevares (input slettes pr. kørsel, resultater persisteres ikke), er gendannelsesomfanget begrænset til brugerkonti og revisionslog.",
  ]),

  H1("7. CI/CD"),
  ...bullets([
    "GitHub Actions kører ved hver push/PR: pytest, katalogvalidering (python tools/build_rules_catalog.py), den uafhængige valideringssuite (python -m validation.run_validation, gated) og pip-audit --strict på runtime-afhængigheder.",
    "Runtime- og test-afhængigheder er adskilt (requirements.txt vs. requirements-dev.txt) og versionspinnede.",
    "Regelkatalog, sporbarhedsmatrix, valideringsrapport og denne dokumentationspakke kan regenereres deterministisk via scripts i backend/tools/.",
    "Build-hygiejne: docx-generatorens Node-artefakter (package.json, node_modules) ligger i repo-roden uden for Railway-servicens backend/-rod og er gitignored, så de ikke påvirker deploy-builden.",
  ]),

  H1("8. Roller, support og åbne punkter"),
  ...bullets([
    "Rolle-/ansvarsbeskrivelse (drift, fagansvarlig for regelkataloget/momslov-kilder, sikkerhed) fastlægges med EY.",
    "Support- og vedligeholdelsesmodel (SLA, opdatering af kontroller ved regelændringer) fastlægges.",
    "Logning/alarmering kobles til EY-platformens overvågning.",
  ]),
]);

// ---------------------------------------------------------------------------
const FILES = [
  ["VAT-Analytics_Godkendelses-overblik.docx", overblik],
  ["VAT-Analytics_Solution_Architecture.docx", arch],
  ["VAT-Analytics_Sikkerhed_og_databehandling.docx", sec],
  ["VAT-Analytics_Hosting_og_drift.docx", hosting],
];

fs.mkdirSync(DOCS, { recursive: true });
(async () => {
  for (const [name, doc] of FILES) {
    const buf = await Packer.toBuffer(doc);
    fs.writeFileSync(path.join(DOCS, name), buf);
    console.log("Skrev docs/" + name + " (" + buf.length + " bytes)");
  }
})();
