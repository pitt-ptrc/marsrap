#' Deid Head Files
#' @param arrow coherently parsed and PHI data
#' @param save_crosswalk option to save crosswalk containing identifying mappings
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute
#' @export
deid_head <- function(arrow, save_crosswalk = TRUE){

  dir_name <- dirname(arrow)
  file_name <- basename(arrow)

  prep <-
    arrow |>
    open_dataset(format = "arrow") |>
    to_duckdb() |>
    mutate(mrn_int = as.integer(mrn)) |>
    mutate(date_shift = as.integer(mrn_int %% 365 + 1)) |>
    # duckdb `to_days`
    mutate(dob_s = dob + to_days(date_shift)) |>
    mutate(dob_s = as.Date(dob_s)) |>
    mutate(date_s = date + to_days(date_shift)) |>
    # duckdb hash `md5`
    mutate(mrn_h = md5(mrn)) |>
    mutate(acc_id = as.character(acc_id)) |>
    mutate(bat_id = as.character(bat_id)) |>
    mutate(acc_id_h = md5(acc_id)) |>
    mutate(bat_id_h = md5(bat_id)) |>
    mutate(mrn_h = str_sub(mrn_h, end = 6)) |>
    mutate(acc_id_h = str_sub(acc_id_h, end = 6)) |>
    mutate(bat_id_h = str_sub(bat_id_h, end = 6))
  cw <-
    prep |>
    select(mrn, acc_id, bat_id, date_shift, acc_type)

  if(save_crosswalk){
    cw |>
      to_arrow() |>
      write_ipc_file(file.path(dir_name, "crosswalk.arrow"))
  }

  head <-
    prep |>
    # remove PHI
    select(!c(
      CKSUM,
      hosp_info,
      datetime,
      date,
      name,
      mrn,
      dob,
      mrn_int,
      date_shift,
      acc_id,
      bat_id
    ))

  return(list(head = head, cw = cw))

}

#' Deid Battery Files
#' @param arrow coherently parsed and PHI data
#' @param crosswalk crosswalk containing identifying mappings
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct collect
#' @export
deid_bat <- function(arrow, crosswalk){

  dir_name <- dirname(arrow)
  file_name <- basename(arrow)

  cw <- open_dataset(crosswalk, format = "arrow") |>
    select(bat_id, date_shift) |>
    distinct()

  arrow |>
    open_dataset(format = "arrow") |>
    left_join(cw) |>
    to_duckdb() |>
    # duckdb hash `md5`
    mutate(bat_id_h = md5(bat_id)) |>
    mutate(bat_id_h = str_sub(bat_id_h, end = 6)) |>
    # duckdb `to_days`
    mutate(date_shift = to_days(date_shift)) |>
    mutate(bat_dt_s = bat_dt + date_shift) |>
    # # remove PHI
    select(!c(
      date_shift,
      bat_id,
      bat_dt
    ))

}


#' Deid Accession Files
#' @param arrow coherently parsed and PHI data
#' @param crosswalk crosswalk containing identifying mappings
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct
#' @export
deid_acc <- function(arrow, crosswalk){

  dir_name <- dirname(arrow)
  file_name <- basename(arrow)

  cw <- open_dataset(crosswalk, format = "arrow") |>
    select(acc_id, date_shift) |>
    distinct()

  arrow |>
    open_dataset(format = "arrow") |>
    left_join(cw) |>
    to_duckdb() |>
    # duckdb hash `md5`
    mutate(acc_id_h = md5(acc_id)) |>
    mutate(acc_id_h = str_sub(acc_id_h, end = 6)) |>
    mutate(acc_num_h = md5(acc_num)) |>
    mutate(acc_num_h = str_sub(acc_num_h, end = 6)) |>
    # duckdb `to_days`
    mutate(date_shift = to_days(date_shift)) |>
    mutate(acc_dt_s = acc_dt + date_shift) |>
    # # remove PHI
    select(!c(
      date_shift,
      acc_id,
      acc_num,
      acc_dt
    ))

}


#' Deid Body Metadata of Files
#'
#' Unlike other deid_**, here we parse first to get keys to join on.
#' @param arrow coherently parsed and PHI data
#' @param crosswalk crosswalk containing identifying mappings
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
#' @export
deid_lab_body_meta <- function(arrow, crosswalk){

  cw <- open_dataset(crosswalk, format = "arrow") |>
    select(acc_id, acc_type) |>
    distinct() |>
    to_duckdb()

  prep <-
    arrow |>
    open_dataset(format = "arrow") |>
    # not here, nothing available yet
    # left_join(cw) |>
    to_duckdb() |>
    window_order(src, rowid) |>
    group_by(entry_grp) |>
    # Accessions
    mutate(acc_num = if_else(Term == "ACC", Value, NA)) |>
    mutate(acc_num = first(acc_num)) |>
    mutate(acc_id = if_else(Term == "ACC", str_sub(Description, 16, 28), NA)) |>
    mutate(acc_id = first(acc_id)) |>
    filter(Term != "ACC") |>
    # Batteries
    mutate(bat_id = if_else(Term == "BAT", str_sub(Description, 16, 31), NA)) |>
    mutate(bat_id = first(bat_id)) |>
    filter(Term != "BAT")

  prep |>
    left_join(cw) |>
    # duckdb hash `md5` |>
    mutate(acc_id_h = md5(acc_id)) |>
    mutate(acc_num_h = md5(acc_num)) |>
    select(-acc_num, -acc_id) |>
    # duckdb hash `md5`
    mutate(bat_id_h = md5(bat_id)) |>
    select(-bat_id) |>
    mutate(acc_id_h = str_sub(acc_id_h, end = 6)) |>
    mutate(acc_num_h = str_sub(acc_num_h, end = 6)) |>
    mutate(bat_id_h = str_sub(bat_id_h, end = 6))



}

#' Deid Body Text of Files
#'
#' To be completed
#' @param duck duckdb object
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
deid_lab_body_text <- function(duck){

  duck |>
    filter(Term == "TXT") |>
    mutate(Description = if_else(
      str_detect(
        Description,
        "^DR | DR |Dr |Dr\\.|DR\\.|MD$|MD.|^NP | NP|nurse|Nurse|NURSE"
      ),
      "PTRC identity removal",
      Description
    )) |>
    # screen numbers
    mutate(Description = if_else(
      str_detect(
        Description,
        "\\d{10}|\\d{3}\\-\\d{3}\\-\\d{4}|\\(\\d{3}\\)\\s\\d{3}\\-\\d{4}|fax|FAX"
      ),
      "PTRC contact removal",
      Description
    )) |>
    # screen date
    # cases "07.22.2016 @ 1135", "07.06.16 1125", TODO write down other patterns
    mutate(Description = if_else(
      str_detect(
        Description,
        "\\d\\/\\d*\\/\\d|\\d\\-\\d*\\-\\d|\\d{2}\\.\\d{2}\\.\\d{4}"
      ),
      "PTRC date removal",
      Description
    )) |>
    # screen fields
    mutate(Description = if_else(
      str_detect(Value, "RCVB|RCVC|RCDT|FAX"),
      "PTRC data type removal",
      Description
    ))

}


#' Deid Body Data of Files
#' @param duck duckdb object
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
#' @export
deid_lab_body_data <- function(duck){

  duck |>
    filter(Term == "DAT") |>
    mutate(Description = if_else(
      str_detect(
        Description,
        "^DR | DR |Dr |Dr\\.|DR\\.|MD$|MD.|^NP | NP|nurse|Nurse|NURSE"
      ),
      "PTRC removal - identity",
      Description
    )) |>
    # screen numbers
    mutate(Description = if_else(
      str_detect(
        Description,
        "\\d{10}|\\d{3}\\-\\d{3}\\-\\d{4}|\\(\\d{3}\\)\\s\\d{3}\\-\\d{4}|fax|FAX"
      ),
      "PTRC removal - contact",
      Description
    )) |>
    # screen date
    # cases "07.22.2016 @ 1135", "07.06.16 1125", TODO write down other patterns
    mutate(Description = if_else(
      str_detect(
        Description,
        "\\d\\/\\d*\\/\\d|\\d\\-\\d*\\-\\d|\\d{2}\\.\\d{2}\\.\\d{4}"
      ),
      "PTRC removal - date",
      Description
    )) |>
    # screen fields
    mutate(Description = if_else(
      str_detect(Value, "RCVB|RCVC|RCDT|FAX"),
      "PTRC removal - data type",
      Description
    )) |>
    # long DAT text
    mutate(Description = if_else(
      str_length(Description) > 40,
      "PTRC removal - free text",
      Description
    ))


}


#' De-identify and Parse Body Data
#'
#' This function de-identifies and parses body data based on the specified type.
#' It handles laboratory data (`lab`) and ICD data (`icd`).
#'
#' @param anno_path The path to the annotation file.
#' @param cw_path The path to the codebook file.
#' @param type The type of data to process, either `"lab"` for laboratory data or `"icd"` for ICD data. Default is `"lab"`.
#' @return A list containing the de-identified and parsed data.
#' @export
deid_body <- function(anno_path, cw_path, type = "lab") {

  if (type == "lab") {
    labs <-
      anno_path |>
      deid_lab_body_meta(cw_path) |>
      deid_lab_body_data()

    lb <- labs |>
      reshape_lb()

    lb <-
      lb$base |>
      parse_lb_base()

    lc <- labs |>
      reshape_lc()

    lc_rpt <- lc$lc_rpt
      # parse_lc_rpt()

    lc_sens <- lc$lc_sens |>
      parse_lc_sens()

    list_lab <- list(lc_rpt = lc_rpt, lc_sens = lc_sens, lb = lb)

    # lb_path <- save_arrow(lb)
    # lc_path <- save_arrow(lc)

    return(list_lab)

  } else if (type == "icd") {

    # TODO: process all other files

  }
}
