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