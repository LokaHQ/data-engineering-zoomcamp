import os
import pyarrow.parquet as pq


def format_to_csv(src_file, output_file, execution_date):
    if not src_file.endswith(".parquet"):
        print("Can only accept source files in parquet format, for the moment")
        return

    df = pq.read_table(src_file).to_pandas(timestamp_as_object=True)
    df.to_csv(output_file)
