#' Parse Laboratory Data
#'
#' This function parses laboratory data, separating descriptions into multiple components
#' and further processing the values into numeric and character categories.
#'
#' @param lab_dat_base A dataframe containing the base laboratory data with a column named 'Description'.
#' @return A list containing three dataframes: `char` for character values, `num` for numeric values, and `err` for erroneous values.
#' @importFrom dplyr filter mutate select any_of ends_with
#' @importFrom stringr str_detect str_remove str_replace str_trim
#' @importFrom tidyr separate
#' @importFrom purrr map
#' @importFrom rlang := enquo quo_name
#' @importFrom dbplyr sql
#' @export
parse_lb_base <- function(lab_dat_base){

  lab_dat_base |>
    filter(!str_detect(Description, "PTRC")) |>
    parse_semicolon() |>
    separate_duckdb(result, into = c("val", "unit", "range", "note", "rpt_status"), sep = ",") |>
    classify_and_extract() |>
    select(!c(Term, Description, result))

}

parse_lc_sens <- function(lc_sens){
  lc_sens |>
    separate_duckdb(Description, into = c("result", "sens"), sep = " ~ ") |>
    separate_duckdb(result, into = c("val", "unit", "range", "note", "rpt_status"), sep = ",") |>
    classify_and_extract() |>
    select(entry_grp, org, mtyp, Value, val, val_num, val_extr, sens, ends_with("_h"))

}

# parse_lc_rpt <- function(lc_rpt){
#   lc_rpt |>
#     separate_duckdb(RPT, into = c("val", "unit", "range", "note", "rpt_status"), sep = ",") |>
#     select(!any_of(c("unit", "range", "note")))
#     # parse_tilde(col = CULT, type = "lc_rpt") |>
#     # parse_tilde(col = CULT, type = "lc_rpt") |>
#     # parse_comma("RPT")
#
# }

#' Parse Comma-Separated Values in a Column
#'
#' This function parses a specified column in a data frame, splitting its values by commas
#' and creating new columns for each part.
#'
#' @param data A data frame or tibble.
#' @param col The name of the column to be parsed.
#' @return A data frame or tibble with new columns created from the comma-separated values.
#' @importFrom dplyr mutate
#' @importFrom dbplyr sql
#' @importFrom rlang sym
parse_comma <- function(data, col) {
  col_sql <- dbplyr::sql(col)
  data |>
    mutate(
      val = sql(paste0("split_part(", col_sql, ", ',', 1)")),
      unit = sql(paste0("split_part(", col_sql, ", ',', 2)")),
      range = sql(paste0("split_part(", col_sql, ", ',', 3)")),
      note = sql(paste0("split_part(", col_sql, ", ',', 4)")),
      rpt_status = sql(paste0("split_part(", col_sql, ", ',', 5)")),
      .after = !!rlang::sym(col)
    )
}

parse_semicolon <- function(data) {
  data |>
    mutate(
      result = dbplyr::sql('split_part(Description, \';\', 1)'),
      unproc = dbplyr::sql('split_part(Description, \';\', 2)'),
      .after = Description
    )
}

classify_and_extract <- function(data) {
  data |>
    mutate(
      # Classify the val strings
      val_class = case_when(
        str_detect(val, "^(\\d+|\\d+\\.\\d+)$") ~ "num",
        str_detect(val, "(>=|<=|<|>|\\+)") ~ "ineq",
        str_detect(val, ".*:.*") ~ "ratio",
        str_detect(val, "%") ~ "pct",
        str_detect(val, "[A-Za-z]") ~ "az",
        TRUE ~ "other"
      ),

      # Extract classified characters
      val_extr = case_when(
        val_class == "ineq" ~ sql("regexp_extract(val, '(>=|<=|<|>|\\+)', 0)"),
        val_class == "ratio" ~ sql("regexp_extract(val, '.*:.*', 0)"),
        val_class == "pct" ~ sql("regexp_extract(val, '%', 0)"),
        val_class == "az" ~ sql("regexp_extract(val, '[A-Za-z]', 0)"),
        TRUE ~ NA_character_
      ),

      # Remove classified characters
      val_num = case_when(
        val_class == "ineq" ~ sql("regexp_replace(val, '(>=|<=|<|>|\\+)', '')"),
        val_class == "ratio" ~ sql("regexp_replace(val, '.*:.*', '')"),
        val_class == "pct" ~ sql("regexp_replace(val, '%', '')"),
        val_class == "az" ~ sql("regexp_replace(val, '[A-Za-z]', '')"),
        TRUE ~ val
      ),

      # Convert to numeric if possible
      val_num = case_when(
        val_class == "num" ~ as.numeric(val),
        val_class %in% c("ineq", "ratio", "pct") ~ as.numeric(val_num),
        TRUE ~ NA_real_
      ),
      .after = val
    )
}

#' Separate Delimited Values into Multiple Columns
#'
#' This function separates a specified column in a data frame, splitting its values by a given delimiter
#' and creating new columns for each part.
#'
#' @param data A data frame or tibble.
#' @param col The name of the column to be parsed.
#' @param into A vector of column names to be created from the split values.
#' @param sep The delimiter used to split the values.
#' @return A data frame or tibble with new columns created from the delimited values.
#' @importFrom dplyr mutate
#' @importFrom dbplyr sql
#' @importFrom rlang enquo !! quo_name sym

separate_duckdb <- function(data, col, into, sep) {
  col <- rlang::enquo(col)
  col_sql <- dbplyr::sql(rlang::quo_name(col))
  mutates <- setNames(
    lapply(seq_along(into), function(i) {
      dbplyr::sql(paste0("split_part(", col_sql, ", '", sep, "', ", i, ")"))
    }),
    into
  )
  data |>
    mutate(!!!mutates, .after = !!col)
}

