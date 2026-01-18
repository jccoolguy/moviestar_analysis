library(qs)
data <- qs::qread(
  "D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Combined/tmdb_1970_2024_top50_by_budget.qs"
)
movie_df <- qread("D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Adjustments/movie_df_1.qs")
ip_df <- qread("D:/Portfolio Projects/moviestar_analysis/Data/Processed_Data/Adjustments/ip_df_1.qs")
genres_df <- data$genres
actors_df <- data$actors
directors_df <- data$directors

write.csv(movie_df, file = "Data/Processed_Data/CSV/movie_df_1.csv")
write.csv(ip_df, file = "Data/Processed_Data/CSV/ip_df_1.csv")
write.csv(genres_df, file = "Data/Processed_Data/CSV/genres_df.csv")
write.csv(actors_df, file = "Data/Processed_Data/CSV/actors_df.csv")
write.csv(directors_df, file = "Data/Processed_Data/CSV/directors_df.csv")