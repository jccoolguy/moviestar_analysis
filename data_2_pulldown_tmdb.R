#Loading Libraries
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(qs)

#Loading Data from API Pull
movie_details <- readRDS("movie_details.rds")

#Turning Map to DF (not working yet)
movies_tbl <- movie_details |>
  map("result") |>
  discard(is.null) |>
  map_dfr(~ tibble(
    movie_id = .x$id,
    title = .x$title,
    release_date = .x$release_date,
    budget = .x$budget,
    revenue = .x$revenue,
    popularity = .x$popularity,
    collection_id = .x$belongs_to_collection$id
  ))

genres_tbl <- movie_details |>
  map("result") |>
  discard(is.null) |>
  map_dfr(~ {
    genres <- .x$genres
    
    if (is.null(genres) || length(genres) == 0) {
      return(NULL)
    }
    
    genres <- genres[
      map_lgl(genres, ~ !is.null(.x$id) && !is.null(.x$name))
    ]
    
    if (length(genres) == 0) {
      return(NULL)
    }
    
    tibble(
      movie_id = .x$id,
      genre_id = map_int(genres, "id"),
      genre_name = map_chr(genres, "name")
    )
  })

