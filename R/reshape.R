#' Reshape Blood Labs
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct collect
#' @export
reshape_lb <- function(arrow, crosswalk){

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

#' Reshape Culture Labs
#' @importFrom arrow to_duckdb open_dataset to_arrow write_ipc_file
#' @importFrom dplyr mutate select compute left_join distinct collect any_of
#' @importFrom tidyr pivot_wider
#' @export
reshape_lc <- function(ddb){

  # dir_name <- dirname(arrow)
  # file_name <- basename(arrow)

  # cw <- open_dataset(crosswalk, format = "arrow") |>
  #   select(bat_id, date_shift) |>
  #   distinct()

  pivot_values_meta <- c("SDES", "SREQ", "CULT", "RPT", "GS", "CNT", "AF")

  lab_dat_rpt <-
    ddb |>
    filter(max(rpt_ind) == 1) |>
    select(-rpt_ind)

  lab_dat_wo_org <-
    lab_dat_rpt |>
    filter(max(org_ind) == 0) |>
    # filter(Value %in% pivot_values_meta) |>
    select(-rowid) |>
    pivot_wider(names_from = Value, values_from = Description) |>
    select(entry_grp, any_of(pivot_values_meta), ends_with("h")) |>
    arrange(entry_grp)

  lab_dat_w_org <-
    lab_dat_rpt |>
    filter(max(org_ind) > 0)

  lab_dat_w_meta <-
    lab_dat_w_org |>
    # this should be sufficient, but not due to error
    filter(org_ind == 0) |>
    select(-rowid) |>
    pivot_wider(names_from = Value, values_from = Description) |>
    select(entry_grp, any_of(pivot_values_meta), ends_with("h")) |>
    arrange(entry_grp)

  lab_dat_w_res <-
    lab_dat_w_org |>
    filter(mtyp_ind > 0) |>
    filter(Value != "MTYP") |>
    select(entry_grp, org_ind, mtyp_ind, Value, Description) |>
    arrange(entry_grp)

  list(wo = lab_dat_wo_org, w_meta = lab_dat_meta, w_res = lab_dat_w_res)

}
