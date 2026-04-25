# Audit: `coords` — 20260424_151349

- **Status:** **FAIL**
- **Total files:** 66
- **OK:** 51
- **Soft dup (multi-foco genuino, <1.05x):** 5
- **Duplicated (bug, >=1.05x):** 10
- **No keys:** 0
- **Error:** 0
- **Empty:** 0

## `base_D_with_rad_drop_rows_calculated`  (21/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 37,756 | 37,756 | 1.0x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 33,306 | 33,306 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 57,871 | 57,871 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 77,746 | 77,746 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 315,991 | 315,991 | 1.0x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 561,208 | 561,208 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 616,380 | 616,380 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 613,852 | 613,852 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 602,702 | 602,702 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 623,015 | 623,015 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 620,753 | 620,753 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 601,348 | 601,348 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 606,569 | 606,569 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 617,633 | 617,633 | 1.0x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 656,328 | 656,328 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 754,530 | 754,530 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 38,546 | 38,359 | 1.0049x | OK |  |
| `inmet_bdq_2020_cerrado.parquet` | 48,474 | 48,059 | 1.0086x | OK |  |
| `inmet_bdq_2021_cerrado.parquet` | 29,936 | 28,169 | 1.0627x | DUPLICATED |  |
| `inmet_bdq_2022_cerrado.parquet` | 581,504 | 565,346 | 1.0286x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2023_cerrado.parquet` | 404,244 | 404,244 | 1.0x | OK |  |
| `inmet_bdq_2024_cerrado.parquet` | 589,553 | 578,486 | 1.0191x | SOFT_DUP | minor multi-foco overhead (genuino) |

## `base_E_with_rad_knn_calculated`  (16/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 99,044 | 99,014 | 1.0003x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 99,122 | 99,122 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 149,485 | 149,476 | 1.0001x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 151,077 | 151,065 | 1.0001x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 591,015 | 590,793 | 1.0004x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 1,000,189 | 1,000,183 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,092,782 | 1,092,758 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,105,813 | 1,105,807 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,149,052 | 1,149,052 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,221,338 | 1,221,338 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,252,984 | 1,252,957 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,255,234 | 1,255,216 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,231,513 | 1,231,363 | 1.0001x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,291,905 | 1,291,620 | 1.0002x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,367,056 | 1,367,050 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,526,778 | 1,526,766 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 1,794,582 | 1,684,823 | 1.0651x | DUPLICATED |  |
| `inmet_bdq_2020_cerrado.parquet` | 1,914,627 | 1,695,119 | 1.1295x | DUPLICATED |  |
| `inmet_bdq_2021_cerrado.parquet` | 2,097,874 | 1,708,005 | 1.2283x | DUPLICATED |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,794,302 | 1,567,861 | 1.1444x | DUPLICATED |  |
| `inmet_bdq_2023_cerrado.parquet` | 1,040,676 | 972,249 | 1.0704x | DUPLICATED |  |
| `inmet_bdq_2024_cerrado.parquet` | 1,726,910 | 1,572,157 | 1.0984x | DUPLICATED |  |

## `base_F_full_original_calculated`  (19/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 99,024 | 99,014 | 1.0001x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 99,122 | 99,122 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 149,479 | 149,476 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 151,069 | 151,065 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 590,867 | 590,793 | 1.0001x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 1,000,185 | 1,000,183 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,092,766 | 1,092,758 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,105,809 | 1,105,807 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,149,052 | 1,149,052 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,221,338 | 1,221,338 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,252,966 | 1,252,957 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,255,222 | 1,255,216 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,231,413 | 1,231,363 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,291,715 | 1,291,620 | 1.0001x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,367,052 | 1,367,050 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,526,770 | 1,526,766 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 1,730,147 | 1,684,823 | 1.0269x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2020_cerrado.parquet` | 1,786,132 | 1,695,119 | 1.0537x | DUPLICATED |  |
| `inmet_bdq_2021_cerrado.parquet` | 1,855,732 | 1,708,005 | 1.0865x | DUPLICATED |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,660,882 | 1,567,861 | 1.0593x | DUPLICATED |  |
| `inmet_bdq_2023_cerrado.parquet` | 995,058 | 972,249 | 1.0235x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2024_cerrado.parquet` | 1,641,685 | 1,572,157 | 1.0442x | SOFT_DUP | minor multi-foco overhead (genuino) |

## Action required

- Arquivos com `dup_ratio > 1.01x` detectados. Se estiver em `modeling/`, rode `make dedupe` e depois regenere `make physics-features`, `make pipeline-coords` e `make champion-overwrite`.

