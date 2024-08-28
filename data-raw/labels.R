## code to prepare `labels` dataset goes here
library(tidyr)

raw_labels <- read_csv(system.file("extdata", "master_lab.csv", package = "marsrap"))

labels <-
  raw_labels |>
  pivot_longer(cols = !c(Hospital, Code), names_to = "lab_name", values_to = "lab_label")


usethis::use_data(labels, overwrite = TRUE)
