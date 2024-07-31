#' Create Project Directory Structure
#'
#' Sets up a project directory structure in a specified base directory. Optionally,
#' copies predefined files to their respective subdirectories.
#'
#' @param base_dir The base directory relative to the current working directory.
#'        Defaults to "raw".
#' @param copy_files Logical flag to indicate whether specific files should be copied
#'        from a source directory to their respective new subdirectories. Defaults to FALSE.
#' @return Invisible NULL. This function is primarily used for its side effects of creating
#'         directories and optionally copying files.
#' @export
create_project <- function(base_dir = "raw", copy_files = FALSE) {
  base_path <- file.path(getwd(), base_dir)
  sub_dirs <- c("lb", "lc", "mpax", "charges", "icd")

  if (!dir.exists(base_path)) {
    dir.create(base_path, recursive = TRUE)
    message(sprintf("Created base directory: %s", base_path))
  } else {
    message(sprintf("Base directory already exists: %s", base_path))
  }

  for (sub_dir in sub_dirs) {
    full_sub_dir_path <- file.path(base_path, sub_dir)
    if (!dir.exists(full_sub_dir_path)) {
      dir.create(full_sub_dir_path, recursive = TRUE)
      message(sprintf("Created subdirectory: %s", full_sub_dir_path))
    } else {
      message(sprintf("Subdirectory already exists: %s", full_sub_dir_path))
    }
  }

  if (copy_files) {
    source_folder <- system.file("extdata", package = "marsrap")
    files_to_copy <- list("fake_bar_bloodlab.out" = "lb", "fake_bar_culturelab.out" = "lc")

    for (file_name in names(files_to_copy)) {
      source_file <- file.path(source_folder, file_name)
      destination_dir <- file.path(base_path, files_to_copy[[file_name]])
      destination_file <- file.path(destination_dir, file_name)

      if (file.exists(source_file)) {
        file.copy(source_file, destination_file)
        message(sprintf("Copied %s to %s", file_name, destination_dir))
      } else {
        message(sprintf("File %s does not exist in %s", file_name, source_folder))
      }
    }
  }

  message("Directory structure setup complete.")
}

#' Execute Parsing Script
#'
#' Executes a shell script (`parse.sh`) located in the `extdata` directory of the
#' `marsrap` package. This script is intended to parse and process files in a specified
#' base directory.
#'
#' @param base_dir The base directory where the script will perform its operations.
#'        Defaults to "raw".
#' @return Invisible NULL. This function is primarily used for its side effect of executing
#'         a shell script.
#' @examples
#' \dontrun{
#'   parse_out(base_dir = "project_data")
#' }
#' @export
parse_out <- function(base_dir = "raw") {
  script_path <- system.file("extdata", "parse.sh", package = "marsrap")

  if (file.exists(script_path)) {
    cmd <- paste("bash", shQuote(script_path), base_dir)
    system(cmd)
    message("parse.sh executed successfully.")
  } else {
    stop("parse.sh does not exist in the expected location.")
  }
}

#' Parses preformatted files from a data manager.
#'
#' @importFrom readr read_csv
#' @importFrom dplyr dense_rank mutate filter distinct rename case_when
#' @importFrom stringr str_c
#' @importFrom arrow to_duckdb open_csv_dataset
#' @export
parse_csv_to_arrow <- function(base_dir = "raw", sub_dir) {

  sub_dir <- match.arg(sub_dir, c("lc", "lb"))

  if (sub_dir %in% c("lc", "lb")){
    lab_ddb <-
      base_dir |>
      file.path(sub_dir) |>
      list.files(full.names = TRUE) |>
      open_csv_dataset() |>
      to_duckdb()

    lab_head_parse <-
      lab_ddb |>
      rename(rowid = TermLine, src = Source) |>
      mutate(entry_grp = sql("REGEXP_EXTRACT(CKSUM, '\\d+$')")) |>
      mutate(entry_grp = md5(entry_grp)) |>
      mutate(entry_grp = str_sub(entry_grp, end = 8)) |>
      anno_report() |>
      mutate(mrn = str_sub(CKSUM, start = 12, end = 20)) |>
      mutate(mrn_int = as.integer(mrn)) |>
      mutate(date_shift = as.integer(mrn_int %% 365 + 1)) |>
      mutate(mrn_h = md5(mrn)) |>
      mutate(mrn_h = str_sub(mrn_h, end = 6))

    lab_head_deid <-
      lab_head_parse |>
      select(entry_grp, mrn_h) |>
      distinct()
    # duckdb `to_days`
    # RD doesn't have dob
    # mutate(dob_s = dob + to_days(date_shift)) |>
    # mutate(dob_s = as.Date(dob_s)) |>
    # mutate(date_s = date + to_days(date_shift))

    lab_crosswalk <-
      lab_head_parse |>
      select(entry_grp, mrn, mrn_h, date_shift) |>
      distinct()

    lab_acc_deid <-
      lab_head_parse |>
      select(entry_grp, Term, Value, Description, mrn_h, date_shift) |>
      filter(Term == "ACC") |>
      mutate(acc_num_h = md5(Value)) |>
      mutate(acc_num_h = str_sub(acc_num_h, end = 6)) |>
      # TODO `hosp_info` doesn't work since multiple spaces
      # TEMP `sql()` below until better method with `separate_duckdb()` with delim found
      # mutate(Description = if_else(str_detect(Description, "^TIME\\sNOT\\sGIVEN|NODATA"), NA, Description)) |>
      # mutate(Description = if_else(str_detect(Description, "^\\d{8} \\d{2}:\\d{2}"), Description, NA)) |>
      # separate_duckdb(col = Description, into = c("acc_date", "acc_time", "acc_id", "ward", "hosp_info"), sep = " ") |>
      # mutate(acc_dt = paste(acc_date, acc_time, recycle0 = TRUE)) |>
      mutate(acc_dt = sql("regexp_extract(Description, '^\\d{8} \\d{2}:\\d{2}')")) |>
      mutate(acc_id = sql("regexp_extract(Description, '\\d{13}')")) |>
      # mutate(hosp = sql("regexp_extract(Description, '[^;]+$')")) |>
      # mutate(dept_code = sql("regexp_extract(Description, '(\\S+)\\s*\\S+$')")) |>
      mutate(hosp_info = sql("regexp_extract(Description, '(\\S+)\\s+\\S+\\s*;\\S+$')")) |>
      mutate(hosp_info = sql("regexp_replace(hosp_info, '\\d{6}', '')")) |>
      mutate(acc_id_h = md5(acc_id)) |>
      mutate(acc_id_h = str_sub(acc_id_h, end = 6)) |>
      mutate(acc_dt = if_else(acc_dt == "", NA, sql("strptime(acc_dt, '%Y%m%d %H:%M')"))) |>
      mutate(acc_dt_s = acc_dt + to_days(date_shift)) |>
      select(entry_grp, ends_with("_h"), ends_with("_s"), hosp_info)

    lab_bat_deid <-
      lab_head_parse |>
      select(entry_grp, Term, Value, Description, mrn_h, date_shift) |>
      filter(Term == "BAT") |>
      rename(bat_type = Value) |>
      # # TODO `hosp_info` doesn't work since multiple spaces
      separate_duckdb(col = Description, into = c("bat_date", "bat_time", "bat_id"), sep = " ") |>
      mutate(bat_id_h = md5(bat_id)) |>
      mutate(bat_id_h = str_sub(bat_id_h, end = 6)) |>
      mutate(bat_dt = paste(bat_date, bat_time)) |>
      mutate(bat_dt = strptime(bat_dt, '%Y%m%d %H:%M')) |>
      mutate(bat_dt_s = bat_dt + to_days(date_shift)) |>
      select(entry_grp, ends_with("_h"), ends_with("_s"))


    lab_dat_deid <-
      lab_head_parse |>
      # use CKSUM here to verify results
      select(entry_grp, rpt_ind, org_ind, mtyp_ind, rowid, Term, Value, Description, mrn_h, src) |>
      filter(Term == "DAT") |>
      deid_lab_body_data()

    list_lab_comp <-
      list(
        lab_crosswalk = lab_crosswalk,
        lab_head_deid = lab_head_deid,
        lab_acc_deid = lab_acc_deid,
        lab_bat_deid = lab_bat_deid,
        lab_dat_deid = lab_dat_deid
      )

    return(list_lab_comp)
  } else if (sub_dir == "mpax"){
    return(NULL)
  }
}

