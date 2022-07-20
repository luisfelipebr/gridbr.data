## IBGE STATISTICAL GRID DATA PROCESSING

# open libraries
library(tidyverse)
library(data.table)
library(sf)

#####
# download files
files <- formatC(seq(1:99), width = 2, format = "d", flag = "0")

for (file in files) {
  tryCatch(download.file(
    url = paste0("http://geoftp.ibge.gov.br/recortes_para_fins_estatisticos/grade_estatistica/censo_2010/grade_id", file, ".zip"),
    destfile = paste0("data-raw/grade_id", file, ".zip"),
    method = "auto"
  ),
  error = function(e) print(paste(file, "did not work"))
  )
}

# unzip files
files <- list.files(path = "data-raw", pattern = "*.zip", full.names = TRUE)

lapply(files,
  unzip,
  exdir = "data-raw/"
)

# delete metadata
file.remove(list.files(path = "data-raw", pattern = "*.xml", full.names = TRUE))

# apply some processing steps to each data set:
# - remove geometry
# - remove shape length and area
# - filter only cells with POP > 0
# - export in csv format
files <- list.files(path = "data-raw", pattern = "*.shp", full.names = TRUE)

for (file in files) {
  data <- read_sf(file) %>%
    st_drop_geometry() %>%
    select(-c("Shape_Leng", "Shape_Area")) %>%
    filter(POP > 0)

  write_csv(data, paste0(substr(file, start = 1, stop = 20), "csv"))
}

# apply processing steps for urban areas file
files <- list.files(path = "data-raw", pattern = "*.shp", full.names = TRUE)

for (file in files) {
  data <- read_sf(file) %>%
    st_drop_geometry() %>%
    select(ID_UNICO) %>%
    filter(grepl("200M", ID_UNICO)) %>%
    rename(id = ID_UNICO)

  write_csv(data, paste0(substr(file, start = 1, stop = 20), "csv"))
}

#####
# POPULATION
# read and bind all files
gridbr_full <- lapply(list.files(path = "data-raw", pattern = "*.csv", full.names = TRUE), fread)
gridbr_full <- Filter(NROW, gridbr_full)
gridbr_full <- gridbr_full %>% bind_rows()

# create final datasets
cellsizes <- c("500KM", "100KM", "50KM", "10KM", "5KM", "1KM")

for (i in cellsizes) {
  assign(
    paste0("gridbr_", i),
    gridbr_full %>%
      group_by(!!sym(paste0("nome_", i))) %>%
      summarise(
        MASC = sum(MASC),
        FEM = sum(FEM),
        POP = sum(POP),
        DOM_OCU = sum(DOM_OCU)
      ) %>%
      rename(id = !!sym(paste0("nome_", i)))
  )
}

gridbr_200M <- gridbr_full %>%
  filter(grepl("200M", ID_UNICO)) %>%
  rename(id = ID_UNICO) %>%
  select(id, MASC, FEM, POP, DOM_OCU)

#####
# URBAN
# read and bind all files
gridbr_urban <- lapply(list.files(path = "data-raw", pattern = "*.csv", full.names = TRUE), fread)
gridbr_urban <- Filter(NROW, gridbr_urban)
gridbr_urban <- gridbr_urban %>% bind_rows()
gridbr_urban <- gridbr_urban$id

#####
# save final data sets
usethis::use_data(gridbr_500KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_100KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_50KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_10KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_5KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_1KM, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_200M, overwrite = TRUE, compress = "xz") # , version = 2)
usethis::use_data(gridbr_urban, overwrite = TRUE, compress = "xz") # , version = 2)

# #### NOT IN USE
# files <- list.files(path = "data-raw", pattern = "*.shp", full.names = TRUE)
#
# get_urban_coverage <- function(file) {
#   data <- read_sf(file) %>%
#     select(ID_UNICO) %>%
#     filter(grepl("200M", ID_UNICO))
# }
#
# gridbr_urban <- lapply(FUN = get_urban_coverage, X = files) %>%
#   bind_rows() %>%
#   st_union(is_coverage = TRUE)
#
# write_sf(gridbr_urban, "data-raw/urban.gpkg")
#
# usethis::use_data(gridbr_urban, overwrite = TRUE)#, compress="xz")#, version = 3)
