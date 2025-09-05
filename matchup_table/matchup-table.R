# tidyverse imports
library(tidyverse)

# package imports
library(baseliner)
library(gt)
library(gtUtils)
library(here)
library(jsonlite)

# global variables
style <- read_json(system.file("style.json", package = "baseliner"))
color <- read_json(system.file("color.json", package = "baseliner"))

# read the matchup data
data <- read_csv(here("matchup_table", "data", "matchup_table.csv"))
week <- data %>% pull("week") %>% first(default = 0)