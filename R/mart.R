#' Join and present prepared Blood Laboratory Data
#'
#' @param duck A DuckDB object containing laboratory data.
#' @param acc A data frame or tibble containing de-identified accession data.
#' @param bat A data frame or tibble containing de-identified accession bat.
#' @param head A data frame or tibble containing de-identified header data.
#' @param .type Option for non-standard data processing. Only RD supported.
#' @return A data frame or tibble with selected and processed laboratory data.
#' @importFrom dplyr left_join select mutate distinct
#' @importFrom stringr str_sub
#' @export
mart_lb <- function(duck, acc, bat, head, .type = NULL) {

  if(!is.null(.type) && .type == "RD"){

    duck |>
      left_join(acc |> select(entry_grp, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(entry_grp, bat_type, bat_dt_s)) |>
      # head info already present
      select(Value, val, val_num, unit, range, acc_dt_s, mrn_h)

  } else {
    duck |>
      left_join(acc |> select(acc_id_h, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(bat_id_h, bat_type, bat_dt_s)) |>
      left_join(head |> select(bat_id_h, acc_id_h, mrn_h, dob_s, sex)) |>
      select(Value, val, val_num, unit, range, acc_dt_s, mrn_h, dob_s, sex)
  }
}

#' Join and present prepared Sensitivity Laboratory Data
#'
#' @param duck A DuckDB object containing laboratory data.
#' @param acc A data frame or tibble containing de-identified accession data.
#' @param bat A data frame or tibble containing de-identified accession bat.
#' @param head A data frame or tibble containing de-identified header data.
#' @param .type Option for non-standard data processing. Only RD supported.
#' @return A data frame or tibble with selected and processed laboratory data.
#' @importFrom dplyr left_join select mutate distinct
#' @importFrom stringr str_sub
#' @export
mart_lc_sens <- function(duck, acc, bat, head, .type = NULL){

  if(!is.null(.type) && .type == "RD"){

    duck |>
      left_join(acc |> select(entry_grp, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(entry_grp, bat_type, bat_dt_s))
      # head info already present

  } else {
    duck |>
      left_join(acc |> select(acc_id_h, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(bat_id_h, bat_type, bat_dt_s)) |>
      left_join(head |> select(bat_id_h, acc_id_h, mrn_h, dob_s, sex)) |>
      select(-entry_grp)
  }

}

mart_lc_rpt <- function(duck, acc, bat, head, .type = NULL){

  if(!is.null(.type) && .type == "RD"){

    duck |>
      left_join(acc |> select(entry_grp, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(entry_grp, bat_type, bat_dt_s)) |>
      # head info already present
      select(-src, -org_ind, -mtyp_ind, -Term, -acc_type, -rowid) |>
      arrange(entry_grp) |>
      # TODO: this should not be needed
      distinct()

  } else {
    duck |>
      left_join(acc |> select(acc_id_h, acc_num_h, acc_dt_s)) |>
      left_join(bat |> select(bat_id_h, bat_type, bat_dt_s)) |>
      left_join(head |> select(bat_id_h, acc_id_h, mrn_h, dob_s, sex)) |>
      select(-src, -org_ind, -mtyp_ind, -Term, -acc_type, -rowid) |>
      arrange(entry_grp) |>
      # TODO: this should not be needed
      distinct()
  }


}
