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
