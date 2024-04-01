# mut_extract <- function(df, col, index, col_name, patt){
#   mutate(df, "{col_name}" := regexp_extract({{col}}, !!patt, index))
# }
#
# separate_duck <- function(data, col, into, sep, extra){
#
#   n_cols <- length(into)
#   index <- 1:n_cols
#
#   base_patt <- glue::glue("([^{sep}]*){sep}?")
#   merge_patt <- "(.*)"
#
#   patt <- strrep(base_patt, n_cols)
#   if(extra == "merge"){
#     patt <- strrep(base_patt, n_cols - 1)
#     patt <- paste0(patt, merge_patt)
#   }
#   # print(patt)
#
#
#   data |>
#     reduce2(
#       index,
#       into,
#       ~ mut_extract(..1, {{col}}, ..2, ..3, patt),
#       .init = _
#     )
#
# }
