library(qs)
library(tidyverse)
library(ggrepel)
library(priceR)

#CHANGES MADE FROM ORIGINAL FILE
  #Adding profit 
  #Adding flag for whether a movie is associated with an established IP
  #Adding inflationary adjusted revenues and budgets

#Reading Data
data <- qs::qread(
  "D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Combined/tmdb_1970_2024_top50_by_budget.qs"
)
movie_df <- data$movies

#Adding profit feature (revenue - 2.5*budget)
movie_df <- movie_df |> 
  mutate(profit = revenue - 2.5*budget)

#Adding established IP (flag for whether a movie is associated with IP)
movie_df <- movie_df |>
  mutate(established_ip = !is.na(collection_name))

#Adding inflationary adjusted revenues and budgets
movie_df$revenue_2024_dollars <- 
  adjust_for_inflation(movie_df$revenue,
                       from_date = movie_df$year,
                       to_date = 2024,
                       country = "US")

movie_df$budget_2024_dollars <- 
  adjust_for_inflation(movie_df$budget,
                       from_date = movie_df$year,
                       to_date = 2024,
                       country = "US")
#Saving Data
qsave(movie_df, "Data/Processed_Data/Adjustments/movie_df_1.qs")
