
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
#> Created base directory: /Users/matt/repos/marsrap/raw
#> Created subdirectory: /Users/matt/repos/marsrap/raw/lb
#> Created subdirectory: /Users/matt/repos/marsrap/raw/lc
#> Created subdirectory: /Users/matt/repos/marsrap/raw/mpax
#> Created subdirectory: /Users/matt/repos/marsrap/raw/charges
#> Created subdirectory: /Users/matt/repos/marsrap/raw/icd
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

marts <- lapply(result_paths, arrow::read_ipc_file)

marts
#> [[1]]
#> # A tibble: 20 × 13
#>    src         entry_grp org_ind mtyp_ind Term  Value Description rowid acc_type
#>    <chr>           <dbl>   <dbl>    <dbl> <chr> <chr> <chr>       <int> <chr>   
#>  1 fake_bar_c…         7       0        0 DAT   CULT  NMRS1          45 CULT    
#>  2 fake_bar_c…         5       0        0 DAT   CULT  CALB           29 CULT    
#>  3 fake_bar_c…         7       0        0 DAT   SDES  NASAL          43 CULT    
#>  4 fake_bar_c…         5       0        0 DAT   CNT   CC100 CML      27 CULT    
#>  5 fake_bar_c…         9       0        0 DAT   CULT  SCNG AEANBT    63 CULT    
#>  6 fake_bar_c…         9       0        0 DAT   SREQ  R PICC         61 CULT    
#>  7 fake_bar_c…         9       0        0 DAT   SREQ  POSI           61 CULT    
#>  8 fake_bar_c…         9       0        0 DAT   SREQ  RCRB           61 CULT    
#>  9 fake_bar_c…         9       0        0 DAT   SDES  BLOOD          59 CULT    
#> 10 fake_bar_c…         6       0        0 DAT   CULT  NMRS2          37 CULT    
#> 11 fake_bar_c…         6       0        0 DAT   SDES  NASAL          35 CULT    
#> 12 fake_bar_c…         5       0        0 DAT   SDES  FOLEY          25 CULT    
#> 13 fake_bar_c…         4       0        0 DAT   CULT  MOD PSAR        9 CULT    
#> 14 fake_bar_c…         4       0        0 DAT   CULT  MOD PSAR N…     9 CULT    
#> 15 fake_bar_c…         4       0        0 DAT   CULT  MOD NRSF        9 CULT    
#> 16 fake_bar_c…         4       0        0 DAT   GS    MOD WBCS        7 CULT    
#> 17 fake_bar_c…         4       0        0 DAT   GS    MANY GPR        7 CULT    
#> 18 fake_bar_c…         4       0        0 DAT   GS    RARE YST        7 CULT    
#> 19 fake_bar_c…         4       0        0 DAT   SREQ  TRACH           5 CULT    
#> 20 fake_bar_c…         4       0        0 DAT   SDES  SPUT            3 CULT    
#> # ℹ 4 more variables: acc_id_h <chr>, acc_num_h <chr>, bat_id_h <chr>,
#> #   rpt <chr>
```
