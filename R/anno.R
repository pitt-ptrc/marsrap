#' Annotate Laboratory Data in Arrow Format
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
anno_entry <- function(arrow_lab){

  dir_name <- dirname(arrow_lab)
  file_name <- basename(arrow_lab)

  arrow_lab |>
    open_dataset(format = "arrow") |>
    to_duckdb() |>
    window_order(rowid) |>
    mutate(entry_grp = if_else(Term == "ACC", 1, 0), .before = 2) |>
    mutate(entry_grp = cumsum(entry_grp)) |>
    compute() |>
    arrow::to_arrow() |>
    write_ipc_file(file.path(dir_name, paste0("anno_", file_name)))
}
