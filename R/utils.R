#' Split a Table by Groups with Named List Output
#'
#' This function takes a table, groups it by one or more variables, and then
#' splits the grouped data into a list. The resulting list has names derived
#' from the unique combinations of the grouping variables.
#'
#' @param .tbl A data table or tibble.
#' @param ... One or more unquoted variables by which to group and then split `.tbl`.
#'            Variables can be separated by commas.
#'
#' @return A named list of tibbles/data tables, where each item corresponds to a unique combination
#'         of the grouping variables. The names of the list items are derived from the unique
#'         combinations of the grouping variables, separated by " / ".
#'
#' @importFrom dplyr group_by group_split group_keys
#' @importFrom rlang inject set_names
named_group_split <- function(.tbl, ...) {
  grouped <- group_by(.tbl, ...)
  names <- rlang::inject(paste(!!!group_keys(grouped), sep = " / "))

  grouped |>
    group_split() |>
    rlang::set_names(names)
}



#' Generate a Hashed Column in a DuckDB Table
#'
#' This function adds a new column to a DuckDB table with hashed values based on an existing column.
#'
#' @param ddb A DuckDB connection or table.
#' @param col The name of the column to hash.
#' @param len The length of the hash to be used. Defaults to 6.
#' @param salt An optional salt to be appended to the column values before hashing. Defaults to NULL.
#' @param new_name An optional new name for the hashed column. Defaults to the original column name with "_h" appended.
#' @importFrom dplyr mutate
#' @importFrom dbplyr sql
#' @importFrom glue glue
#' @importFrom stringr str_c str_sub
#' @importFrom rlang ensym as_string sym
#' @return A modified DuckDB table with the hashed column added.
ddb_col_hash <- function(ddb, col, len = 6, salt = NULL, new_name = NULL) {
  col_sym <- ensym(col)
  col_name <- as_string(col_sym)
  new_col_name <- if (is.null(new_name)) paste0(col_name, "_h") else new_name

  query <- sql(glue::glue("md5({col})"))

  ddb |>
    mutate(!!sym(col) := str_c(!!sym(col), salt)) |>
    mutate(!!sym(new_col_name) := !!sql(query)) |>
    mutate(!!sym(new_col_name) := str_sub(!!sym(new_col_name), 1, len))
}

#' Calculate Date Shifts Based on MRN in a DuckDB Table
#'
#' This function adds a date shift column to a DuckDB table based on the modulus of the MRN (Medical Record Number).
#'
#' @param ddb A DuckDB connection or table.
#' @param salt An optional salt to be appended to the MRN values before calculating the shift. Defaults to NULL.
#' @importFrom dplyr mutate select
#' @importFrom rlang sym
#' @return A modified DuckDB table with the date shift column added.
ddb_ds_get <- function(ddb, salt = NULL) {
  ddb |>
    mutate(mrn_int = as.integer(mrn)) |>
    mutate(date_shift = as.integer(mrn_int %% 365 + 1)) |>
    select(-mrn_int)
}

#' Shift Dates in a DuckDB Table Based on Calculated Shifts
#'
#' This function shifts dates in a specified column of a DuckDB table based on pre-calculated date shifts.
#'
#' @param ddb A DuckDB connection or table.
#' @param col The name of the column containing dates to be shifted.
#' @param new_name An optional new name for the shifted date column. Defaults to the original column name with "_s" appended.
#' @importFrom dplyr mutate
#' @importFrom dbplyr sql
#' @importFrom glue glue
#' @importFrom rlang ensym as_string sym
#' @return A modified DuckDB table with the date-shifted column added.
ddb_col_shift <- function(ddb, col, new_name = NULL){
  col_sym <- ensym(col)
  col_name <- as_string(col_sym)
  new_col_name <- if (is.null(new_name)) paste0(col_name, "_s") else new_name

  query <- sql(glue::glue("date_add({col}::TIMESTAMP, to_days(date_shift))"))

  ddb |>
    mutate(!!sym(new_col_name) := !!sql(query))
}


#' Redact Sensitive Information in a DuckDB Table Column
#'
#' This function redacts sensitive information in a specified column of a DuckDB table. It replaces dates, account numbers, and MRNs with a placeholder.
#'
#' @param ddb A DuckDB connection or table.
#' @param col The name of the column containing sensitive information to be redacted.
#' @param new_name An optional new name for the redacted column. Defaults to the original column name with "_r" appended.
#' @importFrom dplyr mutate
#' @importFrom dbplyr sql
#' @importFrom glue glue
#' @importFrom stringr str_c
#' @importFrom rlang ensym as_string sym
#' @return A modified DuckDB table with the redacted column added.
ddb_col_redact <- function(ddb, col, new_name = NULL){
  col_sym <- ensym(col)
  col_name <- as_string(col_sym)
  new_col_name <- if (is.null(new_name)) paste0(col_name, "_r") else new_name

  date_pat <- "\\b\\d{4}[-/]\\d{2}[-/]\\d{2}\\b|\\b\\d{2}[-/]\\d{2}[-/]\\d{4}\\b"
  acc_num_pat <- "\\b[A-Za-z]+\\d+\\b"
  mrn_pat <- "\\b\\d{9}\\b"

  pat <- str_c(date_pat, acc_num_pat, mrn_pat, sep = "|")
  query <- sql(glue::glue("regexp_replace({col}, '{pat}', 'PTRC-RDCT', 'g')"))

  ddb |>
    mutate(!!sym(new_col_name) := !!sql(query))
}
