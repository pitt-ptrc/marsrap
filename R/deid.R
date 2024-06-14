#' Deid Head Files
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
    mutate(date_s = date + to_days(date_shift)) |>
    # duckdb hash `md5`
    mutate(mrn_h = md5(mrn)) |>
    mutate(acc_id = as.character(acc_id)) |>
    mutate(bat_id = as.character(bat_id)) |>
    mutate(acc_id_h = md5(acc_id)) |>
    mutate(bat_id_h = md5(bat_id))

  if(save_crosswalk){
    prep |>
      select(mrn, acc_id, bat_id, date_shift) |>
      to_arrow() |>
      write_ipc_file(file.path(dir_name, "crosswalk.arrow"))
  }


  prep |>
    # remove PHI
    select(!c(
      CKSUM,
      hosp_info,
      datetime,
      date,
      name,
      mrn,
      mrn_int,
      date_shift,
      acc_id,
      bat_id
    ))
}

#' Deid Battery Files
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
    # duckdb `to_days`
    mutate(date_shift = to_days(date_shift)) |>
    mutate(bat_date_s = bat_date + date_shift)
    # # remove PHI
    # select(!c(
    #   date_shift,
    #   bat_id,
    #   bat_date
    # ))

}


#' Deid Accession Files
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
    mutate(acc_num_h = md5(acc_num)) |>
    # duckdb `to_days`
    mutate(date_shift = to_days(date_shift)) |>
    mutate(acc_date_s = acc_date + date_shift) |>
    # # remove PHI
    select(!c(
      date_shift,
      acc_id,
      acc_num,
      acc_date
    ))

}


#' Deid Body Metadata of Files
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
#' @export
deid_body_meta <- function(arrow, crosswalk){

  dir_name <- dirname(arrow)
  file_name <- basename(arrow)

  cw <- open_dataset(crosswalk, format = "arrow") |>
    select(acc_id, date_shift) |>
    distinct()

  arrow |>
    open_dataset(format = "arrow") |>
    # left_join(cw) |>
    to_duckdb() |>
    window_order(src, rowid) |>
    group_by(entry_grp) |>
    # Accessions
    mutate(acc_num = if_else(Term == "ACC", Value, NA)) |>
    mutate(acc_num = first(acc_num)) |>
    mutate(acc_id = if_else(Term == "ACC", str_sub(Description, 16, 28), NA)) |>
    mutate(acc_id = first(acc_id)) |>
    # duckdb hash `md5`
    mutate(acc_id_h = md5(acc_id)) |>
    mutate(acc_num_h = md5(acc_num)) |>
    filter(Term != "ACC") |>
    select(-acc_num, -acc_id) |>
    # Batteries
    mutate(bat_id = if_else(Term == "BAT", str_sub(Description, 16, 31), NA)) |>
    mutate(bat_id = first(bat_id)) |>
    # duckdb hash `md5`
    mutate(bat_id_h = md5(bat_id)) |>
    filter(Term != "BAT") |>
    select(-bat_id)

}

#' Deid Body Text of Files
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
#' @export
deid_body_text <- function(duck){

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
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct arrange if_else filter
#' @importFrom dbplyr window_order
#' @importFrom stringr str_sub
#' @export
deid_body_data <- function(duck){

  duck |>
    filter(Term == "DAT") |>
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
