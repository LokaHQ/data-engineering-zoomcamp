FROM apache/spark-py:latest

WORKDIR /

# RUN pip install pyspark
# RUN wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

COPY . .

EXPOSE 8000

# CMD ["spark.version"]
CMD ["spark.range(1000 * 1000 * 1000).count()"]
