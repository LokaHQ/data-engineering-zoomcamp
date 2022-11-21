import os

from time import time

import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(
    user, password, host, port, db, table_name, csv_file, execution_date
):
    print(table_name, csv_file, execution_date)
    print(user, password, host, port, db, table_name)

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    print("connection established successfully, inserting data...")

    t_start = time()
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)

    df = next(df_iter)

    # This will force the dates which are outside the bounds to NaT
    if getattr(df, "pickup_datetime", None) and getattr(df, "dropOff_datetime", None):
        df.pickup_datetime = pd.to_datetime(df.pickup_datetime, errors="coerce")
        df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime, errors="coerce")

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    t_end = time()
    print("inserted the first chunk, took %.3f second" % (t_end - t_start))

    while True:
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        if getattr(df, "pickup_datetime", None) and getattr(
            df, "dropOff_datetime", None
        ):
            df.pickup_datetime = pd.to_datetime(df.pickup_datetime, errors="coerce")
            df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime, errors="coerce")

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("inserted another chunk, took %.3f second" % (t_end - t_start))
