
import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os

def main(args):

    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    url = args.url
    
    if url.startswith('http'):
        if url.endswith('.csv.gz'):
            file_name =  'output.csv.gz'
        else:
            file_name = 'output.csv'
        os.system(f"wget {url} -O {file_name}")
    else:
        file_name = url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    if 'tpep_pickup_datetime' in df:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time.time()

            df = next(df_iter)

            if 'tpep_pickup_datetime' in df:
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time.time()

            print('inserted input_data lump, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting input_data into the postgres database")
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV input_data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)