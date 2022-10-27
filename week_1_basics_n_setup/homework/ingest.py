import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:postgres@localhost:5442/ny_taxi")


def main():
    df = pd.read_parquet("data/yellow_tripdata_2021-01.parquet")
    df_zones = pd.read_csv("data/taxi+_zone_lookup.csv")
    df.columns = [c.lower() for c in df.columns]
    df_zones.columns = [c.lower() for c in df_zones.columns]
    df.to_sql("ny_taxi", engine, chunksize=100000, if_exists="replace", index=False)
    df_zones.to_sql("zones", engine, if_exists="replace", index=False)


if __name__ == "__main__":
    main()
