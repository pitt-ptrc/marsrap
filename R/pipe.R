#' @export
main_pipeline <- function(dir, type = "raw", data_type = "labs", clean = TRUE, save = c(NULL, "arrow", "csv"), salt = 123){

  if (type == "raw"){
    parse_out(dir)
    clean_dir("prepped")
  }

  # should return file path of saved file
  if (data_type == "labs"){
    anno_path <- anno_group("data/body.arrow")
  }

  # create dir deid

  if (!dir.exists("deid")){
    dir.create("deid")
  }

  # create crosswalk and deid_head
  head <- deid_head("data/head.arrow")

  # create deid meta files
  acc <- deid_acc("data/acc.arrow", crosswalk = "data/crosswalk.arrow")
  bat <- deid_bat("data/bat.arrow", crosswalk = "data/crosswalk.arrow")

  # save
  if(!is.null(save)){
    option <- match.arg(save, c("arrow", "csv"))
    save_duck(head$head, dir_name = "deid", file_name = "deid_head", format = option)
    save_duck(acc, dir_name = "deid", file_name = "deid_acc", format = option)
    save_duck(bat, dir_name = "deid", file_name = "deid_bat", format = option)
  }

  # create deid lab file
  lab <- deid_body("data/anno_body.arrow", "data/crosswalk.arrow", type = "lab")

  # save
  if(!is.null(save)){
    option <- match.arg(save, c("arrow", "csv"))
    save_duck(lab$lb, dir_name = "deid", file_name = "deid_lb", format = option)
    save_duck(lab$lc_rpt, dir_name = "deid", file_name = "deid_lc_rpt", format = option)
    save_duck(lab$lc_sens, dir_name = "deid", file_name = "deid_lc_sens", format = option)
  }


  # marts

  if (!dir.exists("mart")){
    dir.create("mart")
  }

  mart_lc_rpt <- mart_lc_rpt(lab$lc_rpt, acc, bat, head$head)
  mart_lc_sens <- mart_lc_sens(lab$lc_sens, acc, bat, head$head)
  mart_lb <- mart_lb(lab$lb, acc, bat, head$head)

  # upload function for onedrive

  path_lc_rpt <- save_duck(mart_lc_rpt, dir_name = "mart", "lc_rpt")
  path_lc_sens <- save_duck(mart_lc_sens, dir_name = "mart", "lc_sens")
  path_lb <- save_duck(mart_lb, dir_name = "mart", "lb")



  if(clean) cleanup()

  list_files <-list(
    path_lc_rpt = path_lc_rpt,
    path_lc_sens = path_lc_sens,
    path_lb = path_lb
  )

  return(list_files)

}
