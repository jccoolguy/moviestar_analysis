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

get_seed_movies <- function(year, pages = 10) {
  message("Discovering movies for year: ", year)
  map_dfr(1:pages, function(p) {
    out <- tmdb_get(
      "discover/movie",
      query = list(
        primary_release_year = year,
        with_original_language = "en",  # <-- ADD (English originals),
        with_origin_country    = "US",  # <-- ADD (US among production countries),
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

#movie_ids <- top50_movies$movie_id

qsave(top50_movies, "top50_movies.qs")

## 5. Genres (robust parsing) ----
message("Extracting genres...")


genres_tbl <- imap_dfr(
  details_list,
  function(x, idx) {
    g <- x$genres
  #  print(g)
   # print(g[1,2])
    
    # No genres at all
    if (length(g) == 0) {
      print("here")
      return(tibble(
        movie_id  = x$id,
        top_genre = NA_character_
      ))
    }
    
    # Make sure first genre has a name
    top_name <- tryCatch(
      g$name[1],
      error = function(e) NA_character_
    )
    
    tibble(
      movie_id  = x$id,
      top_genre = top_name
    )
  }
)



qsave(genres_tbl, "genres_tbl.qs")
nrow(genres_tbl)

## 6. Credits (cast + directors) for top 50 movies ----
get_credits <- function(id) {
  tmdb_get(paste0("movie/", id, "/credits"))
}


if (!file.exists("credits_raw.qs")) {
credits_raw <- setNames(
  map(top50_movies$movie_id, safely(get_credits)),
  top50_movies$movie_id
)
  
qsave(credits_raw, "credits_raw.qs")

} else {
  credits_raw <- qread("credits_raw.qs")
}

## 6a. Full cast table ----
message("Building full cast table...")

#Reformatting
actors_tbl <- imap_dfr(
  credits_raw,
  function(x, movie_id) {
    
    # Skip failed API calls
    if (is.null(x$result) || is.null(x$result$cast)) return(NULL)
    
    as_tibble(x$result$cast) |>
      mutate(movie_id = as.integer(movie_id))
      
  }
) |> 
  filter(order <= 5) |> 
  mutate(movie_id = as.integer(movie_id)) |>
  select(
    movie_id,
    actor_id = id,
    actor_name = name,
    billing_order = order,
    character
  ) |> 
  filter(movie_id %in% top50_movies$movie_id)

qsave(actors_tbl, "actors_tbl.qs")
nrow(actors_tbl)


## 6b. Directors table (from crew) ----
message("Extracting directors from crew...")


directors_tbl <- imap_dfr(
  credits_raw,
  function(x, movie_id) {
    
    # Skip failed API calls
    if (is.null(x$result) || is.null(x$result$crew)) return(NULL)
    
    as_tibble(x$result$crew) |>
      mutate(movie_id = as.integer(movie_id))
    
  }
) |> 
  filter(job == "Director") |> 
  select(movie_id, director_id = id,director_name = name)|> 
  filter(movie_id %in% top50_movies$movie_id)


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
  actors      = actors_tbl,
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

