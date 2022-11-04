import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):
    print(table_name, parquet_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    df = pd.read_parquet(parquet_file)

    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)

    print("First insert")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("After first insert")

    print("Second insert")
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100)
    except Exception as e:
        print(e)
    t_end = time()
    print('Inserted all data, took %.3f second' % (t_end - t_start))


def ingest_callable_zones(user, password, host, port, db, table_name, csv_file, execution_date):
    print(table_name, csv_file, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    df = pd.read_csv(csv_file)

    print("First insert")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("After first insert")

    print("Second insert")
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=100)
    except Exception as e:
        print(e)
    t_end = time()
    print('Inserted all data, took %.3f second' % (t_end - t_start))