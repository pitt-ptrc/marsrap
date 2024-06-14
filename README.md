
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
(ESP).

## Installation

You can install the development version of marsrap like so:

``` r
remotes::install_github("pitt-ptrc/marsrap")
```

## Example

``` r
library(marsrap)

create_project(copy_files = TRUE)
list.files("raw", full.names = TRUE, recursive = TRUE)
```
