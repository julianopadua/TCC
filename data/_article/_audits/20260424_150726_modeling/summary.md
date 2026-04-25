# Audit: `modeling` — 20260424_150726

- **Status:** **FAIL**
- **Total files:** 132
- **OK:** 120
- **Soft dup (multi-foco genuino, <1.05x):** 10
- **Duplicated (bug, >=1.05x):** 2
- **No keys:** 0
- **Error:** 0
- **Empty:** 0

## `base_A_no_rad`  (22/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 99,014 | 99,014 | 1.0x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 99,122 | 99,122 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 149,476 | 149,476 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 151,065 | 151,065 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 590,793 | 590,793 | 1.0x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 1,000,183 | 1,000,183 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,092,758 | 1,092,758 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,105,807 | 1,105,807 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,149,052 | 1,149,052 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,221,338 | 1,221,338 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,252,957 | 1,252,957 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,255,216 | 1,255,216 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,231,363 | 1,231,363 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,291,620 | 1,291,620 | 1.0x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,367,050 | 1,367,050 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,526,766 | 1,526,766 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 1,689,190 | 1,684,823 | 1.0026x | OK |  |
| `inmet_bdq_2020_cerrado.parquet` | 1,703,902 | 1,695,119 | 1.0052x | OK |  |
| `inmet_bdq_2021_cerrado.parquet` | 1,716,764 | 1,708,005 | 1.0051x | OK |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,576,620 | 1,567,861 | 1.0056x | OK |  |
| `inmet_bdq_2023_cerrado.parquet` | 972,249 | 972,249 | 1.0x | OK |  |
| `inmet_bdq_2024_cerrado.parquet` | 1,580,940 | 1,572,157 | 1.0056x | OK |  |

## `base_B_no_rad_knn`  (21/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 99,022 | 99,014 | 1.0001x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 99,122 | 99,122 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 149,477 | 149,476 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 151,070 | 151,065 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 590,802 | 590,793 | 1.0x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 1,000,183 | 1,000,183 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,092,759 | 1,092,758 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,105,807 | 1,105,807 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,149,052 | 1,149,052 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,221,338 | 1,221,338 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,252,957 | 1,252,957 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,255,216 | 1,255,216 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,231,409 | 1,231,363 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,291,636 | 1,291,620 | 1.0x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,367,050 | 1,367,050 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,526,767 | 1,526,766 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 1,719,564 | 1,684,823 | 1.0206x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2020_cerrado.parquet` | 1,760,288 | 1,695,119 | 1.0384x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2021_cerrado.parquet` | 1,831,958 | 1,708,005 | 1.0726x | DUPLICATED |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,642,895 | 1,567,861 | 1.0479x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2023_cerrado.parquet` | 993,960 | 972,249 | 1.0223x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2024_cerrado.parquet` | 1,621,888 | 1,572,157 | 1.0316x | SOFT_DUP | minor multi-foco overhead (genuino) |

## `base_C_no_rad_drop_rows`  (22/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 92,238 | 92,238 | 1.0x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 98,771 | 98,771 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 142,096 | 142,096 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 141,168 | 141,168 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 577,154 | 577,154 | 1.0x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 972,964 | 972,964 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,040,552 | 1,040,552 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,054,820 | 1,054,820 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,065,169 | 1,065,169 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,139,455 | 1,139,455 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,151,559 | 1,151,559 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,123,521 | 1,123,521 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,121,948 | 1,121,948 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,139,568 | 1,139,568 | 1.0x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,205,699 | 1,205,699 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,372,499 | 1,372,499 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 97,624 | 97,623 | 1.0x | OK |  |
| `inmet_bdq_2020_cerrado.parquet` | 117,336 | 117,333 | 1.0x | OK |  |
| `inmet_bdq_2021_cerrado.parquet` | 65,239 | 65,221 | 1.0003x | OK |  |
| `inmet_bdq_2022_cerrado.parquet` | 955,555 | 951,827 | 1.0039x | OK |  |
| `inmet_bdq_2023_cerrado.parquet` | 654,692 | 654,692 | 1.0x | OK |  |
| `inmet_bdq_2024_cerrado.parquet` | 985,127 | 984,184 | 1.001x | OK |  |

## `base_D_with_rad_drop_rows`  (22/22 OK)

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
| `inmet_bdq_2019_cerrado.parquet` | 38,360 | 38,359 | 1.0x | OK |  |
| `inmet_bdq_2020_cerrado.parquet` | 48,062 | 48,059 | 1.0001x | OK |  |
| `inmet_bdq_2021_cerrado.parquet` | 28,187 | 28,169 | 1.0006x | OK |  |
| `inmet_bdq_2022_cerrado.parquet` | 569,074 | 565,346 | 1.0066x | OK |  |
| `inmet_bdq_2023_cerrado.parquet` | 404,244 | 404,244 | 1.0x | OK |  |
| `inmet_bdq_2024_cerrado.parquet` | 579,429 | 578,486 | 1.0016x | OK |  |

## `base_E_with_rad_knn`  (21/22 OK)

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
| `inmet_bdq_2019_cerrado.parquet` | 1,721,402 | 1,684,823 | 1.0217x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2020_cerrado.parquet` | 1,767,733 | 1,695,119 | 1.0428x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2021_cerrado.parquet` | 1,837,452 | 1,708,005 | 1.0758x | DUPLICATED |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,643,294 | 1,567,861 | 1.0481x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2023_cerrado.parquet` | 995,058 | 972,249 | 1.0235x | SOFT_DUP | minor multi-foco overhead (genuino) |
| `inmet_bdq_2024_cerrado.parquet` | 1,622,986 | 1,572,157 | 1.0323x | SOFT_DUP | minor multi-foco overhead (genuino) |

## `base_F_full_original`  (22/22 OK)

| File | Rows | Unique keys | Ratio | Status | Notes |
|---|---:|---:|---:|---|---|
| `inmet_bdq_2003_cerrado.parquet` | 99,014 | 99,014 | 1.0x | OK |  |
| `inmet_bdq_2004_cerrado.parquet` | 99,122 | 99,122 | 1.0x | OK |  |
| `inmet_bdq_2005_cerrado.parquet` | 149,476 | 149,476 | 1.0x | OK |  |
| `inmet_bdq_2006_cerrado.parquet` | 151,065 | 151,065 | 1.0x | OK |  |
| `inmet_bdq_2007_cerrado.parquet` | 590,793 | 590,793 | 1.0x | OK |  |
| `inmet_bdq_2008_cerrado.parquet` | 1,000,183 | 1,000,183 | 1.0x | OK |  |
| `inmet_bdq_2009_cerrado.parquet` | 1,092,758 | 1,092,758 | 1.0x | OK |  |
| `inmet_bdq_2010_cerrado.parquet` | 1,105,807 | 1,105,807 | 1.0x | OK |  |
| `inmet_bdq_2011_cerrado.parquet` | 1,149,052 | 1,149,052 | 1.0x | OK |  |
| `inmet_bdq_2012_cerrado.parquet` | 1,221,338 | 1,221,338 | 1.0x | OK |  |
| `inmet_bdq_2013_cerrado.parquet` | 1,252,957 | 1,252,957 | 1.0x | OK |  |
| `inmet_bdq_2014_cerrado.parquet` | 1,255,216 | 1,255,216 | 1.0x | OK |  |
| `inmet_bdq_2015_cerrado.parquet` | 1,231,363 | 1,231,363 | 1.0x | OK |  |
| `inmet_bdq_2016_cerrado.parquet` | 1,291,620 | 1,291,620 | 1.0x | OK |  |
| `inmet_bdq_2017_cerrado.parquet` | 1,367,050 | 1,367,050 | 1.0x | OK |  |
| `inmet_bdq_2018_cerrado.parquet` | 1,526,766 | 1,526,766 | 1.0x | OK |  |
| `inmet_bdq_2019_cerrado.parquet` | 1,689,190 | 1,684,823 | 1.0026x | OK |  |
| `inmet_bdq_2020_cerrado.parquet` | 1,703,902 | 1,695,119 | 1.0052x | OK |  |
| `inmet_bdq_2021_cerrado.parquet` | 1,716,764 | 1,708,005 | 1.0051x | OK |  |
| `inmet_bdq_2022_cerrado.parquet` | 1,576,620 | 1,567,861 | 1.0056x | OK |  |
| `inmet_bdq_2023_cerrado.parquet` | 972,249 | 972,249 | 1.0x | OK |  |
| `inmet_bdq_2024_cerrado.parquet` | 1,580,940 | 1,572,157 | 1.0056x | OK |  |

## Action required

- Arquivos com `dup_ratio > 1.01x` detectados. Se estiver em `modeling/`, rode `make dedupe` e depois regenere `make physics-features`, `make pipeline-coords` e `make champion-overwrite`.

