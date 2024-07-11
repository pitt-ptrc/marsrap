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
  acc_names <- c("key", "acc_num", "acc_date", "acc_time", "acc_id", "ward", "hosp_info")
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
             bat_id = as.character(bat_id),
             dob = as.Date(as.character(dob), format = "%Y%m%d"),
             sex = as.character(sex)),
    acc = read_table(out.txt, acc_names) |>
      mutate(
        acc_date = ymd(acc_date),
        acc_dt = as.POSIXct(paste(acc_date, acc_time))
             ) |>
      select(-acc_time, -acc_date),
    bat = read_table(out.txt, bat_names) |>
      mutate(bat_id = as.character(bat_id)) |>
      mutate(
        bat_date = ymd(bat_date),
        bat_dt = as.POSIXct(paste(bat_date, bat_time))
      ) |>
      select(-bat_time, -bat_date),
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
#' @param .keep removes files in directory prepped
#' @return Invisible NULL. This function is used for its side effects of reading, processing,
#'         and saving data.
#' @importFrom dplyr bind_rows
#' @importFrom readr write_csv
#' @importFrom purrr map map2
#' @importFrom tibble tibble
#' @importFrom stringr str_extract str_c
#' @importFrom arrow write_ipc_file
#' @export
clean_dir <- function(dir, outdir = "data", format = "arrow", .keep = FALSE) {
  # Ensure output directory exists
  if (!dir.exists(outdir)) {
    dir.create(outdir)
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

  if(!.keep) {
    # Remove a directory
    unlink(dir, recursive = TRUE)
  }
}

#' Clean Routine
#'
#' For internal use in docs, removes all files created in RAP.
#'
#' @return Invisible NULL. This function is used for its side effects
#' @export
cleanup <- function(){

  proc_dirs <- c("raw", "prepped", "data", "deid")
  unlink(proc_dirs, recursive = TRUE)

}


#' Save Data in Specified Format
#'
#' This function saves a given dataset (duck) into a specified directory and file format (either "arrow" or "csv").
#'
#' @param duck The dataset to be saved.
#' @param dir_name A character string specifying the directory where the file will be saved. Default is "mart".
#' @param file_name A character string specifying the name of the file to be saved.
#' @param format A character vector specifying the format to save the file. Options are "arrow" and "csv". Default is "arrow".
#'
#' @return A character string representing the file path of the saved file.
#'
#' @export
#'
save_duck <- function(duck, dir_name = "mart", file_name, format = c("arrow", "csv")) {

  format <- match.arg(format)

  if (format == "arrow") {

    file_name <- paste0(file_name, ".arrow")
    file_path <- file.path(dir_name, file_name)

    duck |>
      to_arrow() |>
      write_ipc_file(file_path)

  } else {

    file_name <- paste0(file_name, ".csv")
    file_path <- file.path(dir_name, file_name)

    duck |>
      write_csv(path = file_path)
  }

  return(file_path)
}
