library(qs)
library(tidyverse)
library(ggrepel)
library(priceR)

#CHANGES MADE FROM ORIGINAL FILE
  #Adding profit 
  #Adding flag for whether a movie is associated with an established IP
  #Adding inflationary adjusted revenues, budgets, profits
  #Adding log profitability measure
  #Fixing huge outlier movie The Man Who Would be King. This movie had a mistake in its revenue

#Reading Data
data <- qs::qread(
  "D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Combined/tmdb_1970_2024_top50_by_budget.qs"
)
movie_df <- data$movies

#Fixing the man who would be king
movie_df <- movie_df |> 
  mutate(
    revenue = if_else(
      movie_id == 983,
      11000000,
      revenue
    )
  )

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

#Adding inflationary adjusted Profits
movie_df <- movie_df |> 
  mutate(profit_2024_dollars = revenue_2024_dollars - 2.5*budget_2024_dollars)
#Adding log profitability measure
movie_df <- movie_df |> 
  mutate(log_profitability = log(revenue/(2.5*budget)))
movie_df <- movie_df |> 
  filter(revenue != 0)



#Saving Data
qsave(movie_df, "Data/Processed_Data/Adjustments/movie_df_1.qs")
