# Designnotat — databerigelse via to-fils-join (VAT Analytics)

> Beslutningsoplæg. Formål: at kunne tilføre data, som ét udtræk ikke bærer
> (fx afsender-/modtagerland), på en **korrekt og sporbar** måde — uden at gætte.
> Ingen kode er ændret som følge af dette notat.

## 1. Problemet

Datagrundlags-analysen fortæller nu ærligt, *hvilke* kontroller der ikke kan køre,
og *hvad* der mangler. På en ren SAF-T Financial-fil er det typiske hul
**afsenderland/modtagerland** (kontrol 36, place-of-supply). SAF-T Financial
beskriver *pengestrømmen* (hovedbogen), ikke *vareflowet*. Fysisk afsender-/
modtagerland bor i logistik-/forsendelsesdata, følgesedler, ordre-/fakturamodulet
eller Intrastat — aldrig i selve finans-SAF-T'en. Vi kan derfor ikke udlede det af
SAF-T alene; vi skal **tilføre** en anden kilde.

Dette er ikke et særtilfælde: samme mekanik kan tilføre manglende CVR
(leverandørstamdata), betalingsdata (kontrol 90) m.v. Notatet beskriver derfor en
**generel berigelses-/join-motor**, ikke kun en ship-land-patch.

## 2. Grundmodel: primær kilde + valgfri sekundær kilde

- **Primær kilde:** det momsudtræk brugeren allerede uploader (SAF-T eller Excel/CSV)
  → den kanoniske transaktions-/linjestruktur.
- **Sekundær kilde (valgfri):** en berigelsesfil (Excel/CSV), fx en forsendelses-
  eller fakturaeksport, der bærer de manglende felter **plus en join-nøgle**.
- **Join:** de to sættes sammen på en **eksplicit, delt nøgle** — ikke fuzzy-gæt.
  Kun matchede rækker beriges; resten står uberiget (og datagrundlaget viser det).

Berigelsen sker i import-laget (før motoren), så de eksisterende kontroller kører
uændret på det berigede datasæt.

## 3. Join-nøglen er hele knasten

En troværdig 1:1-berigelse kræver en ren, delt nøgle. Kandidater i prioriteret
rækkefølge:

| Nøgle | Styrke | Bemærkning |
|---|---|---|
| **Faktura-/bilagsnummer** | Stærkest | Findes ofte i SAF-T som `SourceDocumentID`/`Description` (fritekst → skal normaliseres) og i fakturaeksporten. |
| **Ordre-/følgeseddelnummer** | Stærk | Kun hvis begge filer bærer det. |
| **Transaction-/RecordID (SAF-T)** | Stærk, men lukket | Kun hvis den sekundære fil er eksporteret fra samme ERP med samme id'er. |
| **Sammensat: modpart + dato + beløb** | Svag (fuzzy) | Kun som *mærket* fallback — se §6. |

**Nøgle-normalisering (kritisk):** fakturanumre er notorisk inkonsistente på tværs
af systemer. Før join: trim, versaler, fjern førende nuller/præfikser, ensret
separatorer. Normaliseringen skal være synlig og testbar (ikke skjult magi).

**Kardinalitet:** én faktura ↔ mange hovedbogslinjer. Beslut kornstørrelsen:
- Berig **pr. faktura** (nøgle = fakturanr): alle linjer på samme bilag arver
  afsender-/modtagerland. Enkelt og robust — anbefales som udgangspunkt.
- Berig **pr. linje** (nøgle = fakturanr + linjeref): kun hvis den sekundære fil er
  linjespecifik. Sjældnere.

## 4. Match-kvalitet skal rapporteres (som datagrundlaget)

Berigelsen må aldrig være en black box. Efter join vises et **match-panel** (samme
ånd som datagrundlags-panelet):

- **Match-rate:** X af Y primære transaktioner beriget.
- **U-matchede (venstre):** primære rækker uden match i berigelsesfilen.
- **U-matchede (højre):** rækker i berigelsesfilen uden match i udtrækket.
- **Dubletter på nøglen:** samme nøgle flere gange (tvetydig join → flag, berig ikke).

Så kan brugeren (og en revisor) *stole* på berigelsen — eller se præcis hvor den
ikke slog til.

## 5. Proveniens pr. felt

Hvert beriget felt mærkes med **kilde**: `saft` / `excel` / `berigelsesfil` /
`udledt`. Det passer med hele sporbarheds-filosofien: et fund, der bygger på et
beriget felt, kan spores tilbage til *hvor tallet kom fra*. Konkret bæres en lille
proveniens-markering med på linjen (fx `_kilde: {ship_to_country: "berigelsesfil"}`),
og den kan vises i finding-detaljen.

## 6. Fuzzy fallback — kun mærket, aldrig som fakta

Hvis der ikke findes en ren nøgle, kan en sammensat match (modpart + dato + beløb
inden for tolerance) foreslås — men **kun som "sandsynlig match"**, tydeligt mærket,
og aldrig brugt til at udløse et RØD-fund uden brugerens bekræftelse. Samme princip
som resten af værktøjet: vi påstår ikke noget, vi ikke kan bevise.

## 7. Alternativer uden en sekundær fil (lavere tillid)

- **Udled af parternes land:** SAF-T *har* leverandørens/kundens land i stamdata.
  For varer kan man tilnærme: DK-leverandør → DE-kunde ≈ forsendelse DK→DE. Det er
  en **heuristik** (ydelser, drop-shipping, trekantshandel bryder den) og skal
  mærkes som "**udledt**" med lav konfidens. Fordel: nul ekstra data; lader kontrol
  36 køre best-effort med forbehold i stedet for at være helt blokeret.
- **Intrastat-afstemning (ikke per-linje):** Intrastat rummer varebevægelser med
  land, men **aggregeret pr. periode/varekode** — kan ikke joines 1:1 til
  hovedbogslinjer. Egner sig til *afstemning* (matcher summen af EU-varer i
  bogføringen mod Intrastat?) — det er hul **G5** fra Fabian-dialogen, en
  selvstændig, værdifuld kontrol, men en anden ting end per-linje-berigelse.

## 8. Datapolitik og sikkerhed

Den sekundære fil er **også kundedata** og følger samme ikke-arkiv-politik:
slettes straks efter kørslen, holdes kun i hukommelsen for kørslen, revisionslog
kun metadata. Ingen ny persistens. Samme upload-hærdning (filtype/-størrelse,
UUID-navn) som primærfilen.

## 9. Foreslået MVP + faser

- **MVP:** SAF-T/Excel (primær) + én valgfri berigelsesfil, join på **ét normaliseret
  fakturanummer**, berigelse **pr. faktura** af afsender-/modtagerland, match-panel
  + proveniens. Kun eksakt nøgle-match (ingen fuzzy).
- **Fase 2:** flere berige-felter (CVR, land) og flere nøgler; "udledt af parternes
  land" som særskilt, mærket kilde.
- **Fase 3:** fuzzy fallback (mærket) og Intrastat-afstemning (G5) som egen kontrol.

## 10. Åbne spørgsmål til Fabian

1. Hvilken nøgle kan vi realistisk regne med i praksis — er **fakturanummer**
   pålideligt til stede i både SAF-T-eksporten og en forsendelses-/fakturafil?
2. Hvilken sekundær kilde er nemmest for klienten at levere (fragt-/WMS-eksport,
   fakturamodul, ordredata)?
3. Er "**udledt af parternes land**" (mærket lav konfidens) acceptabelt som
   best-effort for kontrol 36, eller vil vi hellere holde den blokeret, til der er
   rigtige forsendelsesdata?
4. Skal Intrastat-afstemning (G5) prioriteres som selvstændig kontrol frem for
   per-linje-berigelse?
