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
    mutate(bat_date_s = bat_date + date_shift) |>
    # # remove PHI
    select(!c(
      date_shift,
      bat_id,
      bat_date
    ))

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
