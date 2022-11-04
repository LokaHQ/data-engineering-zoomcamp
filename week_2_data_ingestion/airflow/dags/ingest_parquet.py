from time import time

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine

# MAX_US_TS = pa.array([33106123800000000], pa.timestamp("us"))[0]
MAX_US_TS = pa.array([29956490948000000], pa.timestamp("us"))[0]
BATCH_SIZE = 100000


def batch(iterable, n=1):
    """Batch an iterable into chunks of n"""
    le = len(iterable)
    for ndx in range(0, le, n):
        yield iterable[ndx : min(ndx + n, le)]



def ingest_callable(user, password, host, port, db, table_name, file, execution_date, protect_ts_cols=[]):
    print(table_name, file, execution_date)

    db_con = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine = create_engine(db_con)
    engine.connect()

    print('connection established successfully...')

    t_start = time()

    # read file
    df = pd.read_parquet(file, filters=[(col, "<", MAX_US_TS) for col in protect_ts_cols])
    print(f'file read successfully (shape: {df.shape})...')

    # coerce datetime fields (datetime64[ns]) to compatible version
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col])
    print('data transformed successfully...')

    # get header, write to DB
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print('table created successfully...')

    # dump data
    # df.to_sql(name=table_name, con=engine, if_exists='append')
    for chunk in batch(df, n=BATCH_SIZE):
        chunk.to_sql(name=table_name, con=engine, if_exists='append')
        print(f'inserted {len(chunk)} records successfully...')

    t_end = time()
    print('ingested, took %.3f second' % (t_end - t_start))
