#' Reshape Blood Labs
#' @importFrom dplyr mutate select compute left_join distinct collect any_of
#' @importFrom tidyr pivot_wider
#' @export
reshape_lb <- function(ddb){

  ddb <- filter(ddb, acc_type == "LAB")

  lab_dat_base <-
    ddb |>
    filter(max(rpt_ind) == 0 & max(org_ind) == 0) |>
    select(!c(rpt_ind, org_ind, mtyp_ind, rowid, acc_type, src))

  lab_dat_rpt <-
    ddb |>
    filter(max(rpt_ind) == 1 & max(org_ind) == 0)

  lab_dat_org <-
    ddb |>
    filter(max(rpt_ind) == 0 & max(org_ind) == 1)

  lab_dat_both <-
    ddb |>
    filter(max(rpt_ind) == 1 & max(org_ind) == 1)

  list(base = lab_dat_base, rpt = lab_dat_rpt, org = lab_dat_org, both = lab_dat_both)

}


#' Reshape Culture Labs
#' @importFrom dplyr mutate select compute left_join distinct collect any_of
#' @importFrom dbplyr window_order
#' @importFrom tidyr pivot_wider fill
#' @export
reshape_lc <- function(ddb){

  # TODO: ddb is a grouped df already, but should not be

  lab_dat_rpt <-
    ddb |>
    filter(acc_type == "CULT") |>
    # TODO: ddb is a grouped df already, but should not be
    # filter(max(rpt_ind) == 1, .by = entry_grp) |>
    filter(max(rpt_ind) == 1) |>
    select(-rpt_ind)

  lc_rpt <-
    lab_dat_rpt |>
    filter(org_ind == 0) |>
    window_order(entry_grp) |>
    mutate(rpt = if_else(Value == "RPT", Description, NA_character_)) |>
    fill(rpt, .direction = "down") |>
    filter(Value != "RPT") |>
    mutate(Description = sql("UNNEST(STR_SPLIT(Description, ' ~ '))"))

  lab_dat_w_org <-
    lab_dat_rpt |>
    filter(max(org_ind) > 0)

  # after viewing results, maybe not needed
  # lc_sens_meta <-
  #   lab_dat_w_org |>
  #   # this should be sufficient, but not due to error
  #   filter(org_ind == 0) |>
  #   select(-rowid) |>
  #   pivot_wider(names_from = Value, values_from = Description) |>
  #   select(entry_grp, any_of(pivot_values_meta), ends_with("h")) |>
  #   arrange(entry_grp)

  lc_sens <-
    lab_dat_w_org |>
    filter(mtyp_ind > 0 | Value == "ORG") |>
    window_order(entry_grp, org_ind, mtyp_ind) |>
    mutate(org = if_else(Value == "ORG", Description, NA_character_)) |>
    mutate(mtyp = if_else(Value == "MTYP", Description, NA_character_)) |>
    group_by(entry_grp, org_ind) |>
    fill(org, .direction = "down") |>
    group_by(entry_grp, org_ind, mtyp_ind) |>
    fill(mtyp, .direction = "down") |>
    filter(Value != "MTYP") |>
    filter(Value != "ORG") |>
    arrange(entry_grp)

  list(
    lc_rpt = lc_rpt,
    # lc_sens_meta = lc_sens_meta,
    lc_sens = lc_sens
  )

}
