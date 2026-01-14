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

#Genres
genres_tbl <- movie_details |>
  map("result") |>
  discard(is.null) |>
  keep(~ .x$id %in% top50_movies$movie_id) |>
  map_dfr(~ {
    
    g <- .x$genres
    
    # 1. Must be a list
    if (!is.list(g) || length(g) == 0) return(NULL)
    
    # 2. Keep only proper genre objects
    g <- g[
      map_lgl(g, ~
                is.list(.x) &&
                !is.null(.x$id) &&
                !is.null(.x$name)
      )
    ]
    
    if (length(g) == 0) return(NULL)
    
    tibble(
      movie_id  = .x$id,
      genre_id  = map_int(g, "id"),
      genre_name = map_chr(g, "name")
    )
  })

#Cast
get_credits <- function(id) {
  tmdb_get(paste0("movie/", id, "/credits"))
}

credits_raw <- map(
  top50_movies$movie_id,
  safely(get_credits)
)

qsave(credits_raw, "credits_raw.qs")

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
#IP indicators
ip_tbl <- top50_movies |>
  mutate(
    is_ip = !is.na(collection_id),
    is_sequel = is_ip
  ) |>
  select(movie_id, is_ip, collection_id, collection_name)

franchise_strength <- movies_tbl |>
  filter(!is.na(collection_id)) |>
  group_by(collection_id) |>
  summarise(
    avg_revenue = mean(revenue, na.rm = TRUE),
    n_films = n()
  )

qsave(
  list(
    movies = top50_movies,
    genres = genres_tbl,
    cast = cast_tbl,
    ip = ip_tbl
  ),
  "tmdb_top50_budget_movies.qs"
)