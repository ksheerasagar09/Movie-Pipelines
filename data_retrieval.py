




import pandas as pd
movie_path = r"~/downloads/airflow-project3/files/movie.csv"
rating_path = r"~/downloads/airflow-project3/files/rating.csv"
tag_path = r"~/downloads/airflow-project3/files/tag.csv"

def load_files():
    movie_df = pd.read_csv(movie_path,nrows = 10000)
    rating_df = pd.read_csv(rating_path,nrows = 10000)
    tag_df = pd.read_csv(tag_path,nrows = 10000)
    return movie_df,rating_df,tag_df

def merge_df(movie_df,rating_df,tag_df):
    movie_ratings_df = movie_df.merge(rating_df,how="inner",on="movieId")
    movie_tags_df = movie_df.merge(tag_df,how="inner",on="movieId")
    movie_tags_ratings_df = movie_ratings_df.merge(movie_tags_df,how="inner",on="userId")
    return movie_ratings_df, movie_tags_df ,movie_tags_ratings_df



