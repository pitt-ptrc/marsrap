
<!-- README.md is generated from README.Rmd. Please edit that file -->

# marsrap

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![CRAN
status](https://www.r-pkg.org/badges/version/marsrap)](https://CRAN.R-project.org/package=marsrap)
<!-- badges: end -->

The goal of `marsrap` is to make retrieving raw data (in “bar” format)
from the UPMC Medical Archive System (MARS) easier than devising the
equivalent query in it’s proprietary, arcane, and old query language
(ESP). It is a [reproducible analytic
pipeline](https://raps-with-r.dev/) (RAP).

## Installation

You can install the development version of marsrap like so:

``` r
remotes::install_github("pitt-ptrc/marsrap")
```

## Example

``` r
library(marsrap)

# Helper function for demostration
create_project(copy_files = TRUE)
#> Base directory already exists: /Users/matt/repos/marsrap/raw
#> Subdirectory already exists: /Users/matt/repos/marsrap/raw/lb
#> Subdirectory already exists: /Users/matt/repos/marsrap/raw/lc
#> Subdirectory already exists: /Users/matt/repos/marsrap/raw/mpax
#> Subdirectory already exists: /Users/matt/repos/marsrap/raw/charges
#> Subdirectory already exists: /Users/matt/repos/marsrap/raw/icd
#> Copied fake_bar_bloodlab.out to /Users/matt/repos/marsrap/raw/lb
#> Copied fake_bar_culturelab.out to /Users/matt/repos/marsrap/raw/lc
#> Directory structure setup complete.
list.files("raw", full.names = TRUE, recursive = TRUE)
#> [1] "raw/lb/fake_bar_bloodlab.out"   "raw/lc/fake_bar_culturelab.out"

result_paths <- main_pipeline("raw", clean = FALSE, save = "arrow")
#> parse.sh executed successfully.
#> Joining with `by = join_by(acc_id)`
#> Adding missing grouping variables: `org_ind` and `mtyp_ind`
#> Warning: Missing values are always removed in SQL aggregation functions.
#> Use `na.rm = TRUE` to silence this warning
#> This warning is displayed once every 8 hours.
#> Warning: ORDER BY is ignored in subqueries without LIMIT
#> ℹ Do you need to move arrange() later in the pipeline or use window_order() instead?
#> Joining with `by = join_by(acc_id_h, acc_num_h)`
#> Joining with `by = join_by(bat_id_h)`
#> Joining with `by = join_by(acc_id_h, bat_id_h)`
#> Joining with `by = join_by(acc_id_h, acc_num_h)`
#> Joining with `by = join_by(bat_id_h)`
#> Joining with `by = join_by(acc_id_h, bat_id_h)`
#> Adding missing grouping variables: `entry_grp`
#> Joining with `by = join_by(acc_id_h, acc_num_h)`
#> Joining with `by = join_by(bat_id_h)`
#> Joining with `by = join_by(acc_id_h, bat_id_h)`
#> Adding missing grouping variables: `entry_grp`
#> Warning: ORDER BY is ignored in subqueries without LIMIT
#> ℹ Do you need to move arrange() later in the pipeline or use window_order() instead?

marts <- lapply(result_paths, arrow::read_ipc_file)

marts
#> $path_lc_rpt
#> # A tibble: 17 × 13
#>    entry_grp Value Description  acc_id_h acc_num_h bat_id_h rpt      
#>        <dbl> <chr> <chr>        <chr>    <chr>     <chr>    <chr>    
#>  1         4 GS    RARE YST     e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  2         4 SREQ  TRACH        e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  3         4 CULT  MOD PSAR     e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  4         4 SDES  SPUT         e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  5         4 GS    MANY GPR     e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  6         4 CULT  MOD NRSF     e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  7         4 GS    MOD WBCS     e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  8         4 CULT  MOD PSAR NO2 e2eee5   9a6a67    7aa382   FNL ,,,,F
#>  9         5 CNT   CC100 CML    496dec   fe884c    9efe18   FNL ,,,,F
#> 10         5 SDES  FOLEY        496dec   fe884c    9efe18   FNL ,,,,F
#> 11         5 CULT  CALB         496dec   fe884c    9efe18   FNL ,,,,F
#> 12         6 CULT  NMRS2        496dec   d75962    d2a2b3   FNL ,,,,F
#> 13         6 SDES  NASAL        496dec   d75962    d2a2b3   FNL ,,,,F
#> 14         7 SDES  NASAL        496dec   d75962    349e98   PENDING  
#> 15         7 CULT  NMRS1        496dec   d75962    349e98   PENDING  
#> 16         8 SDES  NASAL        496dec   d75962    c2eb3e   PENDING  
#> 17         8 CULT  PENDING      496dec   d75962    c2eb3e   PENDING  
#> # ℹ 6 more variables: acc_dt_s <dttm>, bat_type <chr>, bat_dt_s <dttm>,
#> #   mrn_h <chr>, dob_s <date>, sex <chr>
#> 
#> $path_lc_sens
#> # A tibble: 2 × 19
#>   entry_grp org_ind mtyp_ind org      mtyp  Value val     val_num val_extr sens 
#>       <dbl>   <dbl>    <dbl> <chr>    <chr> <chr> <chr>     <dbl> <chr>    <chr>
#> 1         4       1        1 MOD PSAR MIC03 AMIK  "<=16 "      16 <=       SS   
#> 2         4       1        1 MOD PSAR MIC03 AZTR  "<=4 "        4 <=       SS   
#> # ℹ 9 more variables: acc_id_h <chr>, acc_num_h <chr>, bat_id_h <chr>,
#> #   acc_dt_s <dttm>, bat_type <chr>, bat_dt_s <dttm>, mrn_h <chr>,
#> #   dob_s <date>, sex <chr>
#> 
#> $path_lb
#> # A tibble: 30 × 10
#>    entry_grp Value  val   val_num unit    range     acc_dt_s            mrn_h 
#>        <dbl> <chr>  <chr>   <dbl> <chr>   <chr>     <dttm>              <chr> 
#>  1         2 PHVP   7.24     7.24 ""      7.32-7.43 2002-08-23 01:29:00 c58017
#>  2         2 PHVP   7.24     7.24 ""      7.32-7.43 2002-08-23 01:29:00 c58017
#>  3         3 GLU14  122    122    "mg/dL" 70-99     2018-01-15 04:37:00 bbae37
#>  4         2 PCO2VP 71      71    "mm Hg" 41-51     2002-08-23 01:29:00 c58017
#>  5         2 PCO2VP 71      71    "mm Hg" 41-51     2002-08-23 01:29:00 c58017
#>  6         3 BUN14  60      60    "mg/dL" 8-26      2018-01-15 04:37:00 bbae37
#>  7         2 PO2VP  46      46    "mm Hg" 30-50     2002-08-23 01:29:00 c58017
#>  8         2 PO2VP  46      46    "mm Hg" 30-50     2002-08-23 01:29:00 c58017
#>  9         3 CREA14 0.9      0.9  "mg/dL" 0.5-1.4   2018-01-15 04:37:00 bbae37
#> 10         2 HCO3VP 30      30    "mEq/L" 19-25     2002-08-23 01:29:00 c58017
#> # ℹ 20 more rows
#> # ℹ 2 more variables: dob_s <date>, sex <chr>
```
