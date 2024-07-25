#' Main Data Processing Pipeline
#'
#' This function serves as a wrapper around several internal functions to process and manage data.
#' It handles raw data parsing, cleaning, anonymization, and saving of processed data in various formats.
#'
#' @param dir A character string specifying the directory containing the data to be processed.
#' @param type A character string indicating the type of data to be processed. Default is "raw".
#' @param data_type A character string specifying the type of data. Default is "labs".
#' @param clean A logical value indicating whether to clean up intermediate files. Default is TRUE.
#' @param save A character vector specifying the format to save the processed data. Options are "arrow" and "csv". Default is NULL.
#' @param salt A numeric value used for anonymization purposes. Default is 123.
#'
#' @return A list containing file paths of saved processed data.
#' @importFrom dplyr bind_rows collect
#' @export
#'
main_pipeline <- function(dir, type = "raw", data_type = "labs", clean = TRUE, save = c(NULL, "arrow", "csv"), salt = 123){

  match.arg(type, c("raw", "RD"))

  # create dir deid

  if (!dir.exists("deid")){
    dir.create("deid")
  }

  if (type == "RD"){
    list_lc <- parse_csv_to_arrow(base_dir = dir, sub_dir = "lc")
    list_lb <- parse_csv_to_arrow(base_dir = dir, sub_dir = "lb")


    # head$head <- union_all(
    #   list_lb$lab_head_deid |>  mutate(rec_type = "LAB"),
    #   list_lc$lab_head_deid |>  mutate(rec_type = "CULT")
    # )
    #
    # head$cw <- union_all(
    #   list_lb$lab_crosswalk %>% mutate(rec_type = "LAB"),
    #   list_lc$lab_crosswalk %>% mutate(rec_type = "CULT")
    # )
    #
    # acc <- union_all(
    #   list_lb$lab_acc_deid %>% mutate(rec_type = "LAB"),
    #   list_lc$lab_acc_deid %>% mutate(rec_type = "CULT")
    # )
    #
    # bat <- union_all(
    #   list_lb$lab_bat_deid %>% mutate(rec_type = "LAB"),
    #   list_lc$lab_bat_deid %>% mutate(rec_type = "CULT")
    # )

    lb <- list_lb$lab_dat_deid |>
      mutate(acc_type = "LAB") |>
      reshape_lb()

    lb <-
      lb$base |>
      parse_lb_base()

    lc <- list_lc$lab_dat_deid |>
      mutate(acc_type = "CULT") |>
      reshape_lc()

    lc_rpt <- lc$lc_rpt |>
      ungroup() |>
      select(-mtyp_ind, -org_ind, -rowid)

    lc_sens <- lc$lc_sens |>
      parse_lc_sens()

    lab <- list(lc_rpt = lc_rpt, lc_sens = lc_sens, lb = lb)

    # return(lab)


  } else if (type == "raw"){
    parse_out(dir)
    clean_dir("prepped")

    # should return file path of saved file
    if (data_type == "labs"){
      anno_path <- anno_group("data/body.arrow")
    }

    # create crosswalk and deid_head
    head <- deid_head("data/head.arrow")

    # create deid meta files
    acc <- deid_acc("data/acc.arrow", crosswalk = "data/crosswalk.arrow")
    bat <- deid_bat("data/bat.arrow", crosswalk = "data/crosswalk.arrow")

    # save
    if(!is.null(save)){
      option <- match.arg(save, c("arrow", "csv"))

    }

    # create deid lab file
    lab <- deid_body("data/anno_body.arrow", "data/crosswalk.arrow", type = "lab")

  }

  # save
  if(!is.null(save)){
    option <- match.arg(save, c("arrow", "csv"))

    # save_duck(head$head, dir_name = "deid", file_name = "deid_head", format = option)
    # save_duck(acc, dir_name = "deid", file_name = "deid_acc", format = option)
    # save_duck(bat, dir_name = "deid", file_name = "deid_bat", format = option)



    save_duck(lab$lb, dir_name = "deid", file_name = "deid_lb", format = option)
    save_duck(lab$lc_sens, dir_name = "deid", file_name = "deid_lc_sens", format = option)
    # TODO: investigate why `collect()` is necessary, and `to_arrow()` in `save_duck()` aborts
    lab$lc_rpt |>
      collect() |>
      write_ipc_file(sink = file.path("deid", "deid_lc_rpt.arrow"))

  }

  # marts

  # if (!dir.exists("mart")){
  #   dir.create("mart")
  # }
  #
  # mart_lc_rpt <- mart_lc_rpt(lab$lc_rpt, acc, bat, head$head)
  # mart_lc_sens <- mart_lc_sens(lab$lc_sens, acc, bat, head$head)
  # mart_lb <- mart_lb(lab$lb, acc, bat, head$head)
  #
  # # save marts
  #
  # path_lc_rpt <- save_duck(mart_lc_rpt, dir_name = "mart", "lc_rpt")
  # path_lc_sens <- save_duck(mart_lc_sens, dir_name = "mart", "lc_sens")
  # path_lb <- save_duck(mart_lb, dir_name = "mart", "lb")
  #
  # if(clean) cleanup()
  #
  # list_mart_files <-list(
  #   path_lc_rpt = path_lc_rpt,
  #   path_lc_sens = path_lc_sens,
  #   path_lb = path_lb
  # )
  #
  # return(list_mart_files)

}
