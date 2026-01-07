#Loading Libraries
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(lubridate)
library(qs)

#API Wrapper
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

#Discovering Movies

discover_page <- function(page) {
  tmdb_get(
    "discover/movie",
    query = list(
      sort_by = "popularity.desc",
      include_adult = FALSE,
      language = "en-US",
      page = page
    )
  )$results
}

movies_index <- map_dfr(1:50, discover_page)

#Core Movie Metadata
get_movie_details <- function(id) {
  tmdb_get(paste0("movie/", id))
}

movie_details <- map(
  movies_index$id,
  safely(get_movie_details)
)

#Saving down movie_details object
saveRDS(movie_details, file = "movie_details.rds")

#Turning Map to DF (not working yet)
movies_tbl <- movie_details |>
  map("result") |>
  discard(is.null) |>
  map_dfr(as_tibble)

