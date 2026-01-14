library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(tidyr)
library(qs)

data <- qread("tmdb_top50_budget_movies.qs")
movies <- data$movies
movie_ids <- movies$movie_id

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

#CAST PULL
get_credits <- function(id) {
  tmdb_get(paste0("movie/", id, "/credits"))
}

credits_raw <- setNames(
  map(movies$movie_id, safely(get_credits)),
  movies$movie_id
)


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

anti_join(
  movies |> select(movie_id),
  cast_tbl |> select(movie_id),
  by = "movie_id"
) |> nrow()

qsave(
  list(cast_corrected = cast_tbl),
  "cast_correceted.qs"
)
