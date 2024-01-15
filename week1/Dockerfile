FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow wheel parquet

WORKDIR /app

# COPY pipeline.py pipeline.py

# RUN wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
# RUN wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

COPY ingest_data.py ingest_data.py
# COPY ingest_taxi_zone_data.py ingest_taxi_zone_data.py

ENTRYPOINT ["python", "ingest_data.py"]
# ENTRYPOINT ["python", "pipeline.py"]

# ENTRYPOINT ["bash"]


