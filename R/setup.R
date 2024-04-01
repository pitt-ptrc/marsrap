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
