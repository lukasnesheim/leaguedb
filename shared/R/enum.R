library(here)
library(jsonlite)

get_enum <- function(enum) {
  enums <- get_enums()
  if (!enum %in% names(enums)) stop("Invalid enum does not exist in enum.json.")
  enums[[enum]]
}

get_enums <- function() {
  enum_path <- here::here("shared", "enum.json")
  fromJSON(enum_path, simplifyVector = TRUE)
}