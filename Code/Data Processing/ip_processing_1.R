library(qs)
library(tidyverse)

#CHANGES MADE FROM ORIGINAL FILE
  #Filtering to only movies with IPs
  #Adding first movie date for each IP, with a Boolean for each movie to indicate if it is the first in the franchise
  #Adding time between sequels
#NOTES
  #

#Reading Data
data <- qs::qread(
  "D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Combined/tmdb_1970_2024_top50_by_budget.qs"
)
movie_df <- qread("D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Adjustments/movie_df_1.qs")
ip_df <- data$ip

#Left Joining with movies
ip_complete_df <- ip_df |> 
  left_join(movie_df |> select(-collection_id,-collection_name),"movie_id")

#Filtering out movies that do not have an IP associated
ip_associated_df <- ip_complete_df |> 
  filter(!is.na(collection_id))
head(ip_associated_df)

#Creating a column for first movie date in each franchise, and a flag to note if a movie is the first in a franchise
ip_associated_df <- ip_associated_df |> 
  group_by(collection_id) |> 
  mutate(first_movie_date = min(release_date),
         is_first_in_franchise = release_date == first_movie_date) |> 
  ungroup()

#Creating a column that tracks time between sequels
ip_associated_df <- ip_associated_df |> 
  group_by(collection_id) |> 
  arrange(release_date) |> 
  mutate(
    prev_release = lag(release_date),
    gap_days = as.numeric(release_date - prev_release),
    gap_years = gap_days / 365
  ) |> 
  ungroup()

#Saving Data
qsave(ip_associated_df, "Data/Processed_Data/Adjustments/ip_df_1.qs")
