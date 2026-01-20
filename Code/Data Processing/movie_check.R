###############################
## TMDB TOP 50 BY BUDGET, 1970–2024
## - Movies (budget, revenue, IP)
## - Genres
## - Full cast
## - Directors
###############################

## 0. Setup ----
required_pkgs <- c(
  "httr", "jsonlite", "dplyr", "purrr",
  "lubridate", "tidyr", "qs", "tibble"
)

invisible(lapply(required_pkgs, function(p) {
  if (!requireNamespace(p, quietly = TRUE)) install.packages(p)
}))

library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(tidyr)
library(qs)
library(tibble)

## (Optional) Clear old cached files if you're restarting completely
## unlink(c(
##   "seed_movies.qs", "movie_details_raw.qs", "movies_tbl.qs",
##   "top50_movies.qs", "credits_raw.qs",
##   "genres_tbl.qs", "cast_full_tbl.qs", "directors_tbl.qs",
##   "tmdb_1970_2024_top50_by_budget.qs"
## ))

## 1. API setup ----
Sys.setenv(TMDB_API_KEY = "31faa36e4d8c4168f5224039af6618b3")
API_KEY <- Sys.getenv("TMDB_API_KEY")

if (API_KEY == "") {
  stop("TMDB_API_KEY is not set. Use Sys.setenv(TMDB_API_KEY = 'your_key') or add to .Renviron.")
}

tmdb_get <- function(endpoint, query = list(), sleep = 0.25) {
  res <- GET(
    paste0("https://api.themoviedb.org/3/", endpoint),
    query = c(list(api_key = API_KEY), query)
  )
  Sys.sleep(sleep)
  stop_for_status(res)
  fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)
}


tmdb_get <- function(endpoint, query = list(), sleep = 0.25) {
  res <- GET(
    paste0("https://api.themoviedb.org/3/", endpoint),
    query = c(list(api_key = API_KEY), query)
  )
  Sys.sleep(sleep)
  stop_for_status(res)
  fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)
}

search_res <- tmdb_get(
  endpoint = "search/movie",
  query = list(
    query = "Skyfall",
    year = 2012
  )
)

search_res$results[, c("id", "title", "release_date")]

movie_id <- search_res$results$id[1]

movie_details <- tmdb_get(
  endpoint = paste0("movie/", movie_id)
)

#Check
movie_check <- qread("D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Tables/movies_tbl.qs")
movie_check |> filter(str_detect(collection_name,"James"))

year = 2023
get_seed_movies <- function(year, pages = 10) {
  message("Discovering movies for year: ", year)
  map_dfr(1:pages, function(p) {
    out <- tmdb_get(
      "discover/movie",
      query = list(
        primary_release_year = year,
        with_original_language = "en",  # <-- ADD (English originals),
        sort_by = "revenue.desc",
        page = p
      )
    )
    out$results
  })
}

seed_movies <- map_dfr(year, get_seed_movies) |>
  distinct(id, .keep_all = TRUE)

movie_ids <- seed_movies$id

get_movie_details <- function(id) {
  tmdb_get(paste0("movie/", id))
}


movie_details_raw <- setNames(
  map(movie_ids, safely(get_movie_details)),
  movie_ids
)

normalize_details <- function(x) {
  # If we used safely(), the actual payload is in x$result
  if (!is.null(x$result)) x$result else x
}

details_list <- movie_details_raw |> purrr::map(normalize_details)

# Quick sanity check
str(details_list[[1]], max.level = 1)

movies_tbl <- details_list |>
  purrr::map_dfr(~ {
    rd <- .x$release_date
    rd_date <- if (!is.null(rd) && rd != "") as.Date(rd) else as.Date(NA)
    
    tibble(
      movie_id        = .x$id,
      title           = .x$title,
      release_date    = rd_date,
      year            = year(rd_date),
      budget          = .x$budget,
      revenue         = .x$revenue,
      popularity      = .x$popularity,
      collection_id   = .x$belongs_to_collection$id,
      collection_name = .x$belongs_to_collection$name
    )
  })

movies_tbl |> filter(str_detect(collection_name,"Wonka"))
