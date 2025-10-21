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

# read the matchup data
data <- read_csv(here("matchup_table", "data", "matchup_table.csv")) # nolint
week <- data %>% pull("week") %>% first(default = 0)

# clean up data
data <- data %>%
  mutate(
    manager_x = str_c(word(manager_x, 1), " ", str_sub(word(manager_x, 2), 1, 1), "."), # nolint
    manager_y = str_c(word(manager_y, 1), " ", str_sub(word(manager_y, 2), 1, 1), "."), # nolint
    club_x = paste0(name_x, "<br><span style='font-size:75%;", "color:", style$table$font$color$subtitle, "'>", manager_x, "</span>"), # nolint
    club_y = paste0(name_y, "<br><span style='font-size:75%;", "color:", style$table$font$color$subtitle, "'>", manager_y, "</span>"), # nolint
    wr_x = wins_x / (wins_x + wins_y + draws),
    wr_y = wins_y / (wins_x + wins_y + draws),
    w_score = pmax(score_x, score_y),
    score_x = if_else(score_x == w_score, paste0("<span style='color:#3f8f29'>", sprintf("%.2f", score_x), "</span>"), paste0("<span style='color:#bf1029'>", sprintf("%.2f", score_x), "</span>")), # nolint
    score_y = if_else(score_y == w_score, paste0("<span style='color:#3f8f29'>", sprintf("%.2f", score_y), "</span>"), paste0("<span style='color:#bf1029'>", sprintf("%.2f", score_y), "</span>")), # nolint
    #score_x = "", score_y = "",
    record = paste0(wins_x, " ", "-", " ", wins_y)
  ) %>%
  select(club_x, score_x, wr_x, record, wr_y, score_y, club_y, -week, -wins_x, -wins_y, -name_x, -name_y, -manager_x, -manager_y, -w_score) # nolint

matchup_table <- data %>%
  gt() %>%
  theme_baseline_gt() %>%
  cols_hide(columns = c(wr_x, wr_y)) %>%
  tab_header(
    title = "Matchup Results",
    subtitle = paste0("Week ", week)
  ) %>%
  tab_source_note(
    source_note = "Table: Lukas Nesheim"
  ) %>%
  cols_label(
    "club_x" ~ "CLUB",
    "score_x" ~ "SCORE",
    "record" ~ "HEAD-TO-HEAD",
    "score_y" ~ "SCORE",
    "club_y" ~ "CLUB"
  ) %>%
  text_transform(
    locations = cells_body(columns = record),
    fn = function(x) {
      purrr::pmap_chr(
        list(data$wr_x, data$wr_y, data$record),
        function(wrx, wry, rec) {
          if (wrx > wry) {
            lcolor <- "rgba(63, 143, 41, 0.75)"
            rcolor <- "rgba(191, 16, 41, 0.75)"
          } else if (wrx < wry) {
            lcolor <- "rgba(191, 16, 41, 0.75)"
            rcolor <- "rgba(63, 143, 41, 0.75)"
          } else {
            lcolor <- "rgba(179, 179, 179, 0.75)"
            rcolor <- "rgba(179, 179, 179, 0.75)"
          }
          paste0(
            "<div style='display: flex; flex-direction: column; align-items: center; width: 100%;'>", # nolint
            "<div style='margin-bottom:2px;'>", rec, "</div>",
            "<div style='position: relative; width: 100%; min-width: 100%; height: 1em;'>", # nolint
            "<div style='position: absolute; left: 50%; width:", wry * 50, "%; background-color: ", rcolor, "; height: 100%; border-top: 0.5px solid #333333; border-bottom: 0.5px solid #333333; border-right: 0.5px solid #333333; box-sizing: border-box;'></div>", # nolint
            "<div style='position: absolute; right: 50%; width:", wrx * 50, "%; background-color: ", lcolor, "; height: 100%; border-top: 0.5px solid #333333; border-bottom: 0.5px solid #333333; border-left: 0.5px solid #333333; box-sizing: border-box;'></div>", # nolint
            "</div>",
            "</div>"
          )
        }
      )
    }
  ) %>%
  fmt_markdown(columns = c(club_x, club_y, score_x, score_y, record)) %>%
  cols_align(
    align = "left",
    columns = c(club_x)
  ) %>%
  cols_align(
    align = "right",
    columns = c(club_y)
  ) %>%
  cols_align(
    align = "center",
    columns = c(score_x, score_y)
  ) %>%
  tab_style(
    locations = cells_column_labels(columns = c(club_x)),
    style = cell_text(align = "left")
  ) %>%
  tab_style(
    locations = cells_column_labels(columns = c(club_y)),
    style = cell_text(align = "right")
  ) %>%
  tab_style(
    locations = cells_title(groups = "subtitle"),
    style = cell_text(weight = style$table$font$weight$label)
  ) %>%
  tab_style(
    locations = cells_body(columns = c(club_x, score_x, record, score_y, club_y)), # nolint
    style = cell_text(weight = style$table$font$weight$label)
  ) %>%
  cols_width(
    c(club_x, club_y) ~ px(135),
    c(record) ~ px(80)
  ) %>%
  tab_style(
    location = cells_body(rows = 1),
    style = cell_borders(
      sides = "top",
      color = color$london[[3]],
      weight = px(1),
      style = "solid"
    )
  ) %>%
  tab_style(
    location = cells_body(columns = everything()),
    style = cell_borders(
      sides = "bottom",
      color = color$london[[5]],
      weight = px(1),
      style = "solid"
    )
  ) %>%
  tab_style(
    location = cells_body(columns = record),
    style = cell_borders(
      sides = c("left", "right"),
      color = color$london[[3]],
      weight = px(1),
      style = "solid"
    )
  )

# save the table graphic
gt_save_crop(
  matchup_table,
  "matchup_table.png",
  bg = color$background,
  whitespace = 40,
  zoom = 600 / 96
)