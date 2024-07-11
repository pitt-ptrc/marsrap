#' Annotate Lab Entries
#'
#' This function processes an arrow dataset of lab entries, creating an annotated version with additional grouping information.
#'
#' @param arrow_lab A character string specifying the path to the arrow dataset of lab entries.
#'
#' @return A duckdb object with annotated lab entries, including an `entry_grp` column that groups entries based on a specified condition.
#'
#' @export
#'
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select if_else compute
#' @importFrom dbplyr window_order
anno_entry <- function(arrow_lab){

  dir_name <- dirname(arrow_lab)
  file_name <- basename(arrow_lab)

  arrow_lab |>
    open_dataset(format = "arrow") |>
    to_duckdb() |>
    window_order(src, rowid) |>
    mutate(entry_grp = if_else(Term == "ACC", 1, 0), .before = 2) |>
    mutate(entry_grp = cumsum(entry_grp)) |>
    compute()
}

#' Annotate Lab Report Entries
#'
#' This function processes a duckdb dataset of lab entries, adding annotations for report indicators, organization indicators, and material type indicators.
#' It assumes that the dataset has been processed by the `anno_entry` function first.
#'
#' @param duck A duckdb dataset containing lab entries with an `entry_grp` column.
#'
#' @return A duckdb object with additional columns: `rpt_ind`, `org_ind`, and `mtyp_ind` for report, organization, and material type indicators respectively.
#'
#' @export
#'
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select if_else compute arrange
#' @importFrom dbplyr window_order
anno_report <- function(duck){

  if (!"entry_grp" %in% colnames(duck)) {
    stop("Please use anno_entry first.")
  }

  duck |>
    group_by(entry_grp) |>
    window_order(rowid) |>
    mutate(rpt_ind = if_else(Term == "DAT" & Value == "RPT", 1, 0), .before = Term) |>
    mutate(rpt_ind = cumsum(rpt_ind)) |>
    mutate(org_ind = if_else(Term == "DAT" & Value == "ORG", 1, 0), .before = Term) |>
    mutate(org_ind = cumsum(org_ind)) |>
    group_by(entry_grp, org_ind) |>
    mutate(mtyp_ind = if_else(Term == "DAT" & Value == "MTYP", 1, 0), .before = Term) |>
    mutate(mtyp_ind = cumsum(mtyp_ind)) |>
    arrange(entry_grp, rpt_ind, org_ind, mtyp_ind) |>
    compute()

}


# Annotate Metadata Groups
#'
#' Processes an Arrow table of laboratory data, performing several transformations
#' and annotations to prepare the data for analysis. This includes converting the
#' Arrow table to a DuckDB table, ordering the data, grouping entries, and removing
#' unnecessary columns.
#'
#' @param arrow_lab An Arrow table containing laboratory data. This table is expected
#'   to have certain columns (`CKSUM`, `TermLine`, `Term`, etc.) necessary for the
#'   processing steps within the function.
#'
#' @return Returns a DuckDB table after applying a series of transformations. The
#'   transformations include ordering by `CKSUM` and `TermLine`, creating entry groups
#'   based on the `Term` column values, and removing the `Source` column. The table
#'   is further processed with DuckDB-specific computations before being returned.
#'
#' @details The function starts by converting the input Arrow table to a DuckDB table
#'   for efficient processing. It then applies a window order based on `CKSUM` and
#'   `TermLine` columns. Grouping is performed by creating entry groups where each
#'   group starts with a row where `Term` is "ACC". The `Source` column is removed
#'   from the final output, and the table is computed in DuckDB before being returned.
#'
#' @export
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select if_else compute
#' @importFrom dbplyr window_order
anno_group <- function(arrow_lab){

  dir_name <- dirname(arrow_lab)
  file_name <- basename(arrow_lab)

  arrow_lab |>
    anno_entry() |>
    anno_report() |>
    arrow::to_arrow() |>
    write_ipc_file(file.path(dir_name, paste0("anno_", file_name)))
}
