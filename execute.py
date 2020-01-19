from Movie.data_retrieval import load_files,merge_df
from Movie.aggregation import agg_rating_movie, agg_rating_tag
from Movie.connect import connect_db,create_table,insert_to_db,MetaData


def load_data_core(**kwargs):
    task_instance = kwargs["ti"]
    movie_df, rating_df, tag_df = load_files()
    movie_ratings_df, movie_tags_df, movie_tags_ratings_df = merge_df(movie_df, rating_df, tag_df)
    task_instance.xcom_push(key = "movie_ratings_df",value=movie_ratings_df)
    task_instance.xcom_push(key="movie_tags_ratings_df", value=movie_tags_ratings_df)

def aggregate_data_core(**kwargs):
    task_instance = kwargs["ti"]
    movie_ratings_df = task_instance.xcom_pull(key = "movie_ratings_df",task_ids='load_data')
    movie_tags_ratings_df = task_instance.xcom_pull(key="movie_tags_ratings_df", task_ids='load_data')
    df_agg_rating = agg_rating_movie(movie_ratings_df)
    df_agg_rating_tags = agg_rating_tag(movie_tags_ratings_df)
    task_instance.xcom_push(key = "df_agg_rating",value=df_agg_rating)
    task_instance.xcom_push(key="df_agg_rating_tags", value=df_agg_rating_tags)

def insert_db_core(**kwargs):
    task_instance = kwargs["ti"]
    df_agg_rating = task_instance.xcom_pull(key = "df_agg_rating",task_ids='aggregate_data')
    df_agg_rating_tags = task_instance.xcom_pull(key="df_agg_rating_tags", task_ids='aggregate_data')
    engine = connect_db()
    meta = MetaData()
    create_table(meta, engine)
    insert_to_db(engine, df_agg_rating, "tb_agg_movie_ratings")
    insert_to_db(engine, df_agg_rating_tags, "tb_agg_movie_genres")