import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5442/ny_taxi")


def main():
    df = pd.read_parquet("data/yellow_tripdata_2022-01.parquet")
    df.columns = [c.lower() for c in df.columns]
    print('Ingesting...')
    # df.to_sql("ny_taxi", engine, chunksize=100000, if_exists="append")


if __name__ == "__main__":
    main()
