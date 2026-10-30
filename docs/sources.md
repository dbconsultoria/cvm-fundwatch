# Sources

Everything here was observed on real files downloaded on 2026-10-29. Nothing is taken from memory.
Inspection scripts were throwaway and are not part of the repo; the facts they produced are below.

## CVM Informe Diario

Dataset page: https://dados.cvm.gov.br/dataset/fi-doc-inf_diario

### Files

| Path | Granularity | Range seen |
|---|---|---|
| `/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.zip` | one month, one CSV member `inf_diario_fi_YYYYMM.csv` | 202101 to 202610 |
| `/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_YYYY.zip` | one year, one CSV member per month (`inf_diario_fi_YYYYMM.csv`) | 2000 to 2020 |
| `/dados/FI/DOC/INF_DIARIO/META/meta_inf_diario_fi.txt` | field dictionary (Latin-1, CRLF) | |

A month that does not exist yet returns HTTP 404 (checked for 202611). HEAD requests work
and return `Last-Modified`, `ETag` and `Content-Length`.

HIST zip members differ by year:

| Years | Members |
|---|---|
| 2000 to 2004 | one yearly CSV, `inf_diario_fi_YYYY.csv` (2004 is 106 MB uncompressed) |
| 2005 to 2020 | one CSV per month, `inf_diario_fi_YYYYMM.csv` |

Older data quirks:

- 2000 and 2001 are almost empty: 256 rows each, from 6 and 5 CNPJs (files of about 3.5 KB
  zipped). Coverage starts in 2002 (148,589 rows, 820 CNPJs) and 2003 (175,882 rows, 793
  CNPJs). Checked 2026-10-30.
- HIST 2000 has empty `VL_TOTAL` and `NR_COTST` fields (`FITVM;01.465.738/0001-97;2000-01-03;;2.597231300000;2588000.00;91000.00;0.00;`).
- 200501 has 31 distinct dates: besides business days there are 5 Saturday and 5 Sunday rows.
  Recent months (202003, 202610) only have weekdays.

### Format (all layouts)

- Separator `;`, no quoting seen, CRLF line endings, header on the first line, no BOM.
- Decimal separator is `.`, no thousands separator. Dates are `YYYY-MM-DD`.
- Encoding: the daily files contain only ASCII in practice (zero bytes > 0x7F in 202309, 202409,
  202410, 202610), so the encoding cannot be proven from the data. The metadata file and all
  registry files are Latin-1, so the reader decodes with `latin-1`, which is lossless for ASCII.
- Every row has exactly the header's field count (checked on all inspected months).
- A monthly file only contains dates of its own month.
- Numbers come with fixed scale: money with 2 decimals, `VL_QUOTA` with 12 decimals.
- Negative equity and zero quota exist in real data: 202309 has 424 rows with negative
  `VL_PATRIM_LIQ` and 1,781 rows with `VL_QUOTA <= 0`; 202610 has 485 and 1,071.

### Three layouts, not two

| Layout | Header | Where it appears |
|---|---|---|
| `hist_v0` | `CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST` | HIST yearly files 2005 to 2020 (checked 2005, 2010, 2015, 2019, 2020) |
| `v1` (pre 175) | `TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST` | HIST 2000 and 2004; monthly 202101 to 202311 |
| `v2` (post 175) | `TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST` | monthly 202312 to 202610 |

**The layout depends on when CVM generated the file, not on the data month.** All monthly files
regenerated on or after 2024-11-30 use `v2`; the last `v1` file (202311) was regenerated on
2024-10-26. 202312 to 202409 carry the `v2` header but `ID_SUBCLASSE` is always empty and
`TP_FUNDO_CLASSE` only has the old values. Consequence: the parser must detect the layout from the
header of each file, never from the month.

### Columns

From `meta_inf_diario_fi.txt` (which documents only the `v2` names) and observed data:

| Column | Type (meta) | Notes |
|---|---|---|
| `TP_FUNDO` / `TP_FUNDO_CLASSE` | varchar(20) | Report type, not the investment class. Values seen below. Absent in `hist_v0`. |
| `CNPJ_FUNDO` / `CNPJ_FUNDO_CLASSE` | varchar(18) | Formatted `00.017.024/0001-53`. In `v2` it is the CNPJ of the class. |
| `ID_SUBCLASSE` | varchar(15) | `v2` only. Empty for classes without subclasses. Looks like `SG3VT1727814030`. |
| `DT_COMPTC` | date | Reference date. |
| `VL_TOTAL` | numeric(17,2) | Total portfolio value. |
| `VL_QUOTA` | numeric(27,12) | Quota value. |
| `VL_PATRIM_LIQ` | numeric(17,2) | Net equity. |
| `CAPTC_DIA` | numeric(17,2) | Subscriptions of the day. |
| `RESG_DIA` | numeric(17,2) | Redemptions of the day. |
| `NR_COTST` | int | Number of shareholders. |

`TP_FUNDO_CLASSE` values over time:

| File | Values (rows) |
|---|---|
| 202309 (`v1`) | `FI` 505,392, `FAPI` 260 |
| 202409 (`v2`) | `FI`, `FAPI` |
| 202410 (`v2`) | `FI`, `CLASSES - FIF`, `FAPI` |
| 202610 (`v2`) | `CLASSES - FIF` 458,459, `CLASSE FIF/FAPI` 230, `FI` 120 |

### Keys and volumes

| File | Rows | Distinct CNPJ | Business days | Rows with subclass |
|---|---|---|---|---|
| 202309 | 505,652 | 25,796 | 21 | n/a |
| 202409 | 538,493 | 25,992 | 21 | 0 |
| 202410 | 593,077 | 25,977 | 23 | 89 |
| 202610 | 458,809 | 25,433 | 20 (up to 2026-10-28) | 18,113 (1,053 subclasses) |

- `v1`: `(CNPJ_FUNDO, DT_COMPTC)` is unique.
- `v2`: `(CNPJ_FUNDO_CLASSE, ID_SUBCLASSE, DT_COMPTC)` is unique. `(CNPJ, DT_COMPTC)` alone is not
  (5,155 duplicates in 202610), because a class with subclasses reports one row per subclass.
- When a class has subclasses there is **no** class level row on the same date (0 cases in
  202610). Class totals must be computed by summing subclasses.
- Monthly zip is about 10 to 12 MB, CSV about 45 to 55 MB uncompressed.

### Reporting lag

The newest dates in a file are incomplete. The 202610 file downloaded on 2026-10-29 had
25,097 classes on 10-23, 24,796 on 10-26, 23,770 on 10-27 and 28 on 10-28 (ADR 0021).

### Refresh behaviour (from `Last-Modified`)

Observed on 2026-10-29 (BRT = UTC-3):

- 202609 and 202610 (M-1 and M): `Last-Modified` 2026-10-29 03:50 UTC, i.e. about 00:50 BRT.
- 202511 to 202608: all regenerated on Saturday 2026-10-24 around 09:00 UTC.
- 202510 and older: frozen. Each month seems to get its last regeneration about 12 months after
  the reference month (202510 last on 2026-09-26, 202509 on 2026-08-29).
- Responses come through Cloudflare (`cf-cache-status: HIT`, `Cache-Control: max-age=14400`) and
  carry both `Last-Modified` and `ETag`.

Consequences: restatements can show up for about 12 months back, and the weekly regeneration
changes `Last-Modified` even when the content may be identical. See ADR 0007.

## CVM registry

Directory: `/dados/FI/CAD/DADOS/`. All files Latin-1 (`\xda`, `\xc7` and similar bytes found),
`;` separator, CSV quoting compatible.

### Options found

| Option | File | Rows | Key | Type/peer fields | Notes |
|---|---|---|---|---|---|
| A. Legacy snapshot | `cad_fi.csv` (17 MB) | 46,806 | `CNPJ_FUNDO` formatted, **not unique** (5,703 duplicate rows) | `TP_FUNDO`, `CLASSE`, `CLASSE_ANBIMA` | Only funds not yet adapted to 175. Covers 6,233 of 25,796 CNPJs in 202309 and only 1,454 of 25,433 in 202610. Dying. |
| B. Post 175 registry | `registro_fundo_classe.zip` (7 MB): `registro_fundo.csv`, `registro_classe.csv`, `registro_subclasse.csv` | 90,912 / 37,034 / 11,556 | `CNPJ_Fundo`, `CNPJ_Classe`, `ID_Subclasse` (14 digit CNPJ, no punctuation) | `registro_classe.Classificacao` (Multimercado, Renda Fixa, Acoes, Cambial, FMP-FGTS), `Classificacao_Anbima` (66 values), `Tipo_Classe` | Covers 25,323 of 25,433 CNPJs in 202610 (99.6%) and 1,047 of 1,052 subclasses. Current snapshot only, no history. `registro_fundo` covers 100% of 202309 CNPJs but has no investment class. |
| C. Legacy history | `cad_fi_hist.zip` (19 MB), member `cad_fi_hist_classe.csv` | 62,070 | `CNPJ_FUNDO` formatted + `DT_INI_CLASSE` / `DT_FIM_CLASSE` | `CLASSE` (Multimercado, Renda Fixa, Acoes, Referenciado, Curto Prazo, Cambial, FMP-FGTS, Divida Externa) | Type 2 history. Covers 25,783 of 25,796 CNPJs in 202309, 25,632 with an interval valid on 2023-09-15. The zip has 19 members (admin, gestor, sit, taxa_adm, ...). |

Key points:

- Daily reports use formatted CNPJ, registry B uses digits only. Silver will store the 14 digit
  form. No alphanumeric CNPJ seen yet in any file, but the column stays a string (ADR 0006).
- Old classes `Referenciado`, `Curto Prazo` and `Divida Externa` do not exist post 175. They
  are mapped to `Renda Fixa` (ADR 0017).
- `registro_classe` repeats 13 CNPJs (some rows are exact copies, others a cancelled and an
  active registration). `registro_fundo` repeats 8,166 CNPJs and even some `ID_Registro_Fundo`.
- `cad_fi_hist_classe` has 56 overlapping intervals for the same CNPJ, no empty start dates,
  and most histories start in 2005 (first interval in 2004 for only 248 CNPJs).
- `registro_classe.CNPJ_Classe` equals the parent `registro_fundo.CNPJ_Fundo` for 37,612 of
  37,762 classes, so the pre 175 fund CNPJ in the daily reports joins to the post 175 class.
- 9,443 of the 25,323 classes reporting in 202610 are FIC (`Classe_Cotas = S`), so summed AUM
  double counts money invested through funds of funds.

**Decision (ADR 0008):** use **B** (`registro_classe.Classificacao`) as the
current peer group and **C** (`cad_fi_hist_classe.csv`) for dates before a fund adapted to 175
(`registro_fundo.Data_Adaptacao_RCVM175`). Snapshot both daily into bronze so we build our own
history of B. Drop A.

## BCB PTAX

- OData v4 service: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- No key. `$metadata` lists `CotacaoDolarPeriodo(dataInicial, dataFinalCotacao)`, both strings.
- **Date parameters are `MM-DD-YYYY`.** Working call:

  ```
  .../CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)
    ?@dataInicial='03-01-2020'&@dataFinalCotacao='03-31-2020'&$format=json
  ```

- Response `application/json;charset=UTF-8`, shape `{"@odata.context": ..., "value": [...]}`.
  Each item: `cotacaoCompra` (Decimal, scale 5), `cotacaoVenda` (Decimal, scale 5),
  `dataHoraCotacao` (string, `YYYY-MM-DD HH:MM:SS.fff`, fractional digits vary from 1 to 6).
- Business days only. One closing rate per day, published around 13:05 to 13:12 BRT
  (2026-10-29 was available at 13:11).
- A 2005-01-01 to 2026-12-31 request returned 5,481 rows in one response, so no paging is
  needed. Before 2005 timestamps look different (`2005-01-03 18:35:00.0`).
- **Duplicate seen:** 2025-04-23 appears twice with identical rates and timestamps 27 ms apart.
  Silver keeps one row per date (latest timestamp).
- No `Last-Modified` or `ETag` headers. Change detection must use content hashing.
- Parse decimals as strings in bronze to avoid float rounding.
- Large moves exist in the data as published: 5.0377 on 2026-10-23, 5.5007 on 2026-10-26,
  5.3346 on 2026-10-27 (sell rate). The pipeline keeps them as is.
