test_that("read_delim_barx correctly reads and processes bar-delimited data", {
  # Create a temporary file and write sample data to it
  temp_file <- tempfile()
  writeLines(c("1|2|NA", "4|NA|NA", "7|8|NA"), temp_file)

  # Define expected data.frame for comparison
  expected_df <- tibble::tibble(
    A = c(1, 4, 7),
    B = c(2, NA, 8)
  )

  # Use read_delim_barx to read the temp file
  result_df <- read_delim_barx(temp_file, col_names = c("A", "B"))

  # Check if result matches expected
  expect_equal(result_df, expected_df)

  # Clean up the temporary file
  unlink(temp_file)
})
#
