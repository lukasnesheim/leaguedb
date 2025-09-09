# tidyverse imports
library(tidyverse)

# package imports
library(baseliner)
library(gt)
library(gtUtils)
library(here)
library(jsonlite)
library(scales)

# global variables
style <- read_json(system.file("style.json", package = "baseliner"))
color <- read_json(system.file("color.json", package = "baseliner"))

# read the standings data
data <- read_csv(here("league_table", "data", "league_table.csv"), col_types = cols(move = col_character())) # nolint
week <- data %>% pull("week") %>% first(default = 0)

# tidy the standings data
data <- data %>%
  mutate(
    manager = str_c(word(manager, 1), " ", str_sub(word(manager, 2), 1, 1), "."), # nolint
    move = case_when(
      str_detect(move, "\\+") ~ paste0("<sup><span style='color: #3f8f29;'>", move, "</span></sup>"), # nolint
      str_detect(move, "-") ~ paste0("<sup><span style='color: #bf1029;'>", move, "</span></sup>"), # nolint
      TRUE ~ ""
    ),
    club = paste0(club, move, "<br><span style='font-size:75%;", "color:", style$table$font$color$subtitle, "'>", manager, "</span>"), # nolint
  ) %>%
  select(standing, club, win, loss, ortg, drtg, mpf, eff, -move, -week) # nolint

# min/max offensive rating
min_ortg <- min(data$ortg)
max_ortg <- max(data$ortg)

# min/max defensive rating
min_drtg <- min(data$drtg)
max_drtg <- max(data$drtg)

# min/max max points for
min_mpf <- min(data$mpf)
max_mpf <- max(data$mpf)

# min/max efficiency
min_eff <- min(data$eff)
max_eff <- max(data$eff)

# create the league table
table <- data %>%
  gt() %>%
  theme_baseline_gt() %>%
  tab_header(
    title = "League Table",
    subtitle = paste0("Week ", week, " Standings")
  ) %>%
  tab_source_note(
    source_note = "Table: Lukas Nesheim"
  ) %>%
  cols_label(
    "standing" ~ "",
    "club" ~ "CLUB",
    "win" ~ "WIN",
    "loss" ~ "LOSS",
    "ortg" ~ "OFF RTG",
    "drtg" ~ "DEF RTG",
    "mpf" ~ "MAX PTS",
    "eff" ~ "EFF"
  ) %>%
  tab_style(
    locations = cells_column_labels(columns = club),
    style = cell_text(align = "left")
  ) %>%
  tab_style(
    locations = cells_title(groups = "subtitle"),
    style = cell_text(weight = style$table$font$weight$label)
  ) %>%
  tab_style(
    locations = cells_body(columns = standing:loss),
    style = cell_text(weight = style$table$font$weight$label)
  ) %>%
  tab_style(
    locations = cells_body(columns = win:eff),
    style = cell_text(align = "center")
  ) %>%
  tab_style(
    locations = cells_body(rows = 6),
    style = cell_borders(
      sides = "bottom",
      color = color$london[[5]],
      weight = px(0.5),
      style = "solid"
    )
  ) %>%
  fmt_markdown(columns = club) %>%
  gt_color_pills(
    columns = ortg,
    palette = c(color$london[[6]], color$london[[3]]),
    domain = c(min_ortg, max_ortg),
    digits = 2,
    pill_height = 12
  ) %>%
  gt_color_pills(
    columns = drtg,
    palette = c(color$london[[3]], color$london[[6]]),
    domain = c(min_drtg, max_drtg),
    digits = 2,
    pill_height = 12
  ) %>%
  gt_color_pills(
    columns = mpf,
    palette = c(color$london[[6]], color$london[[3]]),
    domain = c(min_mpf, max_mpf),
    digits = 2,
    pill_height = 12
  ) %>%
  gt_color_pills(
    columns = eff,
    palette = c(color$dublin[[4]], color$dublin[[3]], color$dublin[[2]], color$dublin[[1]]), # nolint
    domain = c(min_eff, max_eff),
    format_type = "percent",
    digits = 1,
    pill_height = 12
  )

# save the table graphic
gt_save_crop(
  table,
  "league_table.png",
  bg = color$background,
  whitespace = 40,
  zoom = 600 / 96
)