#' Read Delimited Text File with Bars as Delimiters
#'
#' This function reads a delimited text file using a specified delimiter (bar "|"),
#' selects non-empty columns, and renames them.
#'
#' @param out.txt The path to the input text file.
#' @param col_names A character vector of column names to set for the output data frame.
#' @return A tibble with columns named according to \code{col_names}.
#' @importFrom readr read_delim
#' @importFrom dplyr select where
#' @importFrom purrr map map2 set_names
#' @importFrom stringr str_remove str_trim
#' @export
read_delim_barx <- function(out.txt, col_names) {
  out.txt |>
    read_delim(delim = "|", col_names = FALSE) |>
    select(where(~ !(all(is.na(.))))) |>
    set_names(col_names)
}

#' Read Specific Sections from a Text File
#'
#' This function reads different sections from a text file based on the section type.
#' It uses custom column names for each section type.
#'
#' @param out.txt The path to the input text file.
#' @param type The type of section to read, one of "head", "acc", "bat", "body".
#' @return A tibble corresponding to the section read, with appropriate column names and modifications.
#' @importFrom readr read_delim read_table
#' @importFrom dplyr mutate row_number
#' @importFrom stringr str_remove str_trim str_sub
#' @importFrom lubridate ymd
#' @export
read_outtxt <- function(out.txt, type) {
  # Column names for different sections
  head_names <- c("CKSUM", "hosp_info", "name", "datetime", "rec_type", "bat_id", "acc_type", "acc_id", "ward", "dob", "hosp", "sex")
  acc_names <- c("key", "acc_num", "acc_date", "acc_time", "acc_id", "acc_type", "hosp_info")
  bat_names <- c("key", "bat_type", "bat_date", "bat_time", "bat_id")
  body_names <- c("Term", "Value", "Description")

  # Suppress column type messages
  options("readr.show_col_types" = FALSE)

  # Read and process the specified section
  # ad hoc for now, clean up later
  switch(
    type,
    head = read_delim_barx(out.txt, head_names) |>
      mutate(mrn = str_sub(hosp_info, end = 9L),
             date = as.Date(datetime),
             time = format(datetime, "%H:%M:%S"),
             bat_id = as.character(bat_id)),
    acc = read_table(out.txt, acc_names) |>
      mutate(acc_date = ymd(acc_date)),
    bat = read_table(out.txt, bat_names) |>
      mutate(bat_id = as.character(bat_id)) |>
      mutate(bat_date = ymd(bat_date)),
    body = read_delim(out.txt, col_names = body_names, delim = "|") |>
      mutate(Value = str_remove(str_trim(Value), ":"),
             Term = str_remove(Term, ":"),
             rowid = row_number())
  )
}

#' Clean Directory and Output Data in Specified Format
#'
#' This function reads all files from a directory, processes them, and outputs the data
#' in a specified format (CSV or Apache Arrow).
#'
#' @param dir The input directory containing the files to be processed.
#' @param outdir The output directory where the processed files will be saved. Defaults to "data".
#' @param format The format to save the processed data in. Possible values are "csv" and "arrow".
#' @return Invisible NULL. This function is used for its side effects of reading, processing,
#'         and saving data.
#' @importFrom dplyr bind_rows
#' @importFrom readr write_csv
#' @importFrom purrr map map2
#' @importFrom tibble tibble
#' @importFrom stringr str_extract str_c
#' @importFrom arrow write_ipc_file
#' @export
clean_dir <- function(dir, outdir = "data", format = "arrow") {
  # Ensure output directory exists
  if (!dir.exists("data")) {
    dir.create("data")
  }

  # Process files in the input directory
  meta <-
    tibble(path = list.files(dir, full.names = TRUE), file = list.files(dir)) |>
    mutate(type = str_extract(file, "^[^_]+")) |>
    mutate(src = str_extract(file, "(?<=_).+?(?=\\.out)")) |>
    named_group_split(type)

  tbl <-
    meta |>
    map(~ map2(.x$path, .x$type, ~ read_outtxt(.x, .y)) |> set_names(.x$src)) |>
    map(~ bind_rows(.x, .id = "src"))

  # Output the processed data in the specified format
  switch(
    format,
    csv = map2(tbl, names(tbl), ~ write_csv(.x, file.path(outdir, str_c(.y, ".csv")))),
    arrow = map2(tbl, names(tbl), ~ arrow::write_ipc_file(.x, file.path(outdir, str_c(.y, ".arrow"))))
  )
}
