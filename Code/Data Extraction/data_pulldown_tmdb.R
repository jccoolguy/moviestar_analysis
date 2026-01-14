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

## 2. Discover movies by year (seed set) ----
years <- 1970:2024   # <--- your requested range

get_seed_movies <- function(year, pages = 5) {
  message("Discovering movies for year: ", year)
  map_dfr(1:pages, function(p) {
    out <- tmdb_get(
      "discover/movie",
      query = list(
        primary_release_year = year,
        sort_by = "revenue.desc",
        page = p
      )
    )
    out$results
  })
}

if (!file.exists("seed_movies.qs")) {
  seed_movies <- map_dfr(years, get_seed_movies) |>
    distinct(id, .keep_all = TRUE)
  qsave(seed_movies, "seed_movies.qs")
} else {
  seed_movies <- qread("seed_movies.qs")
}

## 3. Detailed movie info (budget, revenue, IP, etc.) ----
get_movie_details <- function(id) {
  tmdb_get(paste0("movie/", id))
}

movie_ids <- seed_movies$id

if (!file.exists("movie_details_raw.qs")) {
  message("Pulling detailed movie info for ", length(movie_ids), " movies...")
  movie_details_raw <- setNames(
    map(movie_ids, safely(get_movie_details)),
    movie_ids
  )
  qsave(movie_details_raw, "movie_details_raw.qs")
} else {
  movie_details_raw <- qread("movie_details_raw.qs")
}

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

qsave(movies_tbl, "movies_tbl.qs")

## 4. Top 50 movies by budget per year ----
top50_movies <- movies_tbl |>
  filter(!is.na(year), year >= 1970, year <= 2024) |>
  filter(budget > 0) |>
  group_by(year) |>
  slice_max(budget, n = 50, with_ties = FALSE) |>
  ungroup()

qsave(top50_movies, "top50_movies.qs")

## 5. Genres (robust parsing) ----
message("Extracting genres...")

genres_tbl <- details_list |>
  keep(~ .x$id %in% top50_movies$movie_id) |>
  imap_dfr(function(x, movie_id) {
    g <- x$genres
    
    # Must be a list and non-empty
    if (!is.list(g) || length(g) == 0) return(NULL)
    
    # Keep only proper genre objects with id and name
    g <- g[
      purrr::map_lgl(g, ~ is.list(.x) &&
                       !is.null(.x$id) &&
                       !is.null(.x$name))
    ]
    
    if (length(g) == 0) return(NULL)
    
    tibble(
      movie_id   = as.integer(movie_id),
      genre_id   = purrr::map_int(g, "id"),
      genre_name = purrr::map_chr(g, "name")
    )
  })

qsave(genres_tbl, "genres_tbl.qs")
nrow(genres_tbl)

## 6. Credits (cast + directors) for top 50 movies ----
get_credits <- function(id) {
  tmdb_get(paste0("movie/", id, "/credits"))
}

top_movie_ids <- top50_movies$movie_id

if (!file.exists("credits_raw.qs")) {
  message("Pulling credits (cast + crew) for ", length(top_movie_ids), " movies...")
  credits_raw <- setNames(
    map(top_movie_ids, safely(get_credits)),
    top_movie_ids
  )
  qsave(credits_raw, "credits_raw.qs")
} else {
  credits_raw <- qread("credits_raw.qs")
}

## 6a. Full cast table ----
message("Building full cast table...")


normalize_credits <- function(x) {
  # If we used safely(), the real data is in x$result
  if (!is.null(x$result)) x$result else x
}

credits_list <- credits_raw |> purrr::map(normalize_credits)

cast_full_tbl <- imap_dfr(
  credits_list,
  function(x, movie_id) {
    
    # Skip if there's no cast element
    if (is.null(x$cast)) return(NULL)
    
    cast <- x$cast
    if (!is.list(cast) || length(cast) == 0) return(NULL)
    
    # Keep only proper cast entries with id and name
    cast <- cast[
      map_lgl(cast, ~ is.list(.x) &&
                !is.null(.x$id) &&
                !is.null(.x$name))
    ]
    
    if (length(cast) == 0) return(NULL)
    
    tibble(
      movie_id      = as.integer(movie_id),
      actor_id      = map_int(cast, "id"),
      actor_name    = map_chr(cast, "name"),
      billing_order = map_int(cast, "order", .default = NA_integer_),
      character     = map_chr(cast, "character", .default = NA_character_)
    )
  }
)

qsave(cast_full_tbl, "cast_full_tbl.qs")
nrow(cast_full_tbl)


## 6b. Directors table (from crew) ----
message("Extracting directors from crew...")

directors_tbl <- imap_dfr(
  credits_list,
  function(x, movie_id) {
    
    if (is.null(x$crew)) return(NULL)
    
    crew <- x$crew
    if (!is.list(crew) || length(crew) == 0) return(NULL)
    
    crew <- crew[
      map_lgl(crew, ~ is.list(.x) &&
                !is.null(.x$id) &&
                !is.null(.x$name) &&
                !is.null(.x$job))
    ]
    
    if (length(crew) == 0) return(NULL)
    
    directors <- crew[map_lgl(crew, ~ .x$job == "Director")]
    if (length(directors) == 0) return(NULL)
    
    tibble(
      movie_id      = as.integer(movie_id),
      director_id   = map_int(directors, "id"),
      director_name = map_chr(directors, "name")
    )
  }
)

qsave(directors_tbl, "directors_tbl.qs")
nrow(directors_tbl)

## 7. IP / franchise flags ----
ip_tbl <- top50_movies |>
  mutate(
    is_ip = !is.na(collection_id)
  ) |>
  select(movie_id, is_ip, collection_id, collection_name)

qsave(ip_tbl, "ip_tbl.qs")

## 8. Bundle everything into a single object ----
tmdb_data <- list(
  movies    = top50_movies,
  genres    = genres_tbl,
  cast      = cast_full_tbl,
  directors = directors_tbl,
  ip        = ip_tbl
)

qsave(tmdb_data, "tmdb_1970_2024_top50_by_budget.qs")

message("Done. Saved combined data to 'tmdb_1970_2024_top50_by_budget.qs'.")

## 9. (Optional) quick sanity checks ----
# tmdb <- qread("tmdb_1970_2024_top50_by_budget.qs")
# with(tmdb, nrow(movies))
# with(tmdb, nrow(genres))
# with(tmdb, nrow(cast))
# with(tmdb, nrow(directors))
# with(tmdb, all(cast$movie_id %in% movies$movie_id))
# with(tmdb, all(genres$movie_id %in% movies$movie_id))
# with(tmdb, all(directors$movie_id %in% movies$movie_id))

