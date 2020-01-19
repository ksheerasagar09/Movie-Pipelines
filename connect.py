import pandas as pd
import psycopg2


from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
MetaData()

def connect_db():
    db_engine = create_engine('postgres://postgres:postgres@localhost/postgres',echo=False)
    return db_engine

def insert_to_db(db_engine,df,table_name):
    df.to_sql(table_name,con=db_engine,if_exists="append")

def create_table(meta,db_engine):
    tb_agg_movie_ratings = Table(
        'tb_agg_movie_ratings', meta,
        Column('movieId', Integer, primary_key=True),
        Column('title', String),
        Column('rating', String),
    )
    tb_agg_movie_genres = Table(
        'tb_agg_movie_genres', meta,
        Column('tag', String),
        Column('genres_x', String),
        Column('rating', String),
    )
    meta.create_all(db_engine)
