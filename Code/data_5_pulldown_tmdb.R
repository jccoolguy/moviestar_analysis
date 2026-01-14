library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(tidyr)
library(qs)

#API WRAPPER
Sys.setenv(TMDB_API_KEY = "31faa36e4d8c4168f5224039af6618b3")
API_KEY <- Sys.getenv("TMDB_API_KEY")

tmdb_get <- function(endpoint, query = list(), sleep = 0.25) {
  res <- GET(
    paste0("https://api.themoviedb.org/3/", endpoint),
    query = c(list(api_key = API_KEY), query)
  )
  Sys.sleep(sleep)
  stop_for_status(res)
  fromJSON(content(res, "text", encoding = "UTF-8"), flatten = TRUE)
}

#Discover Movies by Year
get_seed_movies <- function(year, pages = 5) {
  map_dfr(1:pages, function(p) {
    tmdb_get(
      "discover/movie",
      query = list(
        primary_release_year = year,
        sort_by = "revenue.desc",
        page = p
      )
    )$results
  })
}

years <- 1975:2024
seed_movies <- map_dfr(years, get_seed_movies)

#Pulling Movie Details

get_movie_details <- function(id) {
  tmdb_get(paste0("movie/", id))
}

movie_details <- map(
  seed_movies$id,
  safely(get_movie_details)
)

qsave(movie_details, "movie_details.qs")

#Building a Movie Table
movies_tbl <- movie_details |>
  map("result") |>
  discard(is.null) |>
  map_dfr(~ tibble(
    movie_id = .x$id,
    title = .x$title,
    release_date = as.Date(.x$release_date),
    year = year(as.Date(.x$release_date)),
    budget = .x$budget,
    revenue = .x$revenue,
    popularity = .x$popularity,
    collection_id = .x$belongs_to_collection$id,
    collection_name = .x$belongs_to_collection$name
  ))

#selecting top 50 movies per year
top50_movies <- movies_tbl |>
  filter(!is.na(budget), budget > 0) |>
  group_by(year) |>
  slice_max(budget, n = 50, with_ties = FALSE) |>
  ungroup()

get_credits <- function(id) {
  tmdb_get(paste0("movie/", id, "/credits"))
}

credits_raw <- setNames(
  map(movies_tbl$movie_id, safely(get_credits)),
  movies_tbl$movie_id
)

qsave(credits_raw, "credit_details.qs")

#Reformatting
cast_tbl <- imap_dfr(
  credits_raw,
  function(x, movie_id) {
    
    # Skip failed API calls
    if (is.null(x$result) || is.null(x$result$cast)) return(NULL)
    
    as_tibble(x$result$cast) |>
      filter(order <= 5) |>
      mutate(movie_id = as.integer(movie_id)) |>
      select(
        movie_id,
        actor_id = id,
        actor_name = name,
        billing_order = order,
        character
      )
  }
)

movie_genres <- movie_details |>
  map("result") |>
  discard(is.null) |>
  map_dfr(function(x) {
    # x is the parsed JSON for a single movie
    if (is.null(x$genres) || nrow(x$genres) == 0) {
      tibble(
        movie_id   = x$id,
        genre_id   = NA_integer_,
        genre_name = NA_character_
      )
    } else {
      tibble(
        movie_id   = x$id,
        genre_id   = x$genres$id,
        genre_name = x$genres$name
      )
    }
  })

# 2. Collapse to a single primary_genre per movie (pick first non-NA)
movie_primary_genre <- movie_genres |>
  group_by(movie_id) |>
  summarize(
    primary_genre = first(genre_name[!is.na(genre_name)]),
    .groups = "drop"
  )

# 3. Join primary_genre back to your movie-level table
movies_tbl <- movies_tbl |>
  left_join(movie_primary_genre, by = "movie_id")

# If you want genres only for the top 50 per year:
top50_movies <- top50_movies |>
  left_join(movie_primary_genre, by = "movie_id")

# If you also want the primary_genre on the cast table:
cast_tbl <- cast_tbl |>
  left_join(
    movies_tbl |> select(movie_id, primary_genre),
    by = "movie_id"
  )

#anti_join(
 # movies |> select(movie_id),
  #cast_tbl |> select(movie_id),
  #by = "movie_id"
#) |> nrow()

