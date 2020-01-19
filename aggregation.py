

import pandas as pd

def agg_rating_movie(movie_ratings_df):
    df = movie_ratings_df[["movieId","title","rating"]]
    df_agg_rating = df.groupby(["movieId","title"]).mean().sort_values("rating",ascending=False)
    return df_agg_rating

def agg_rating_tag(movie_tags_ratings_df):
    df = movie_tags_ratings_df[["tag","genres_x","rating"]]
    df_agg_rating_tags = df.groupby(["tag","genres_x"]).mean().sort_values("rating",ascending=False)
    return df_agg_rating_tags
