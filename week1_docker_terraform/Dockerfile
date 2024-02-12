FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow wheel parquet

WORKDIR /app

# COPY pipeline.py pipeline.py


# RUN wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz
# RUN wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv


# docker run -it \
# --network week1_default \
# taxi_ingest:v001 \
# --user=root \
# --password=root \
# --host=pgdatabase \
# --port=5432 \
# --db=ny_taxi \
# --table=yellow_taxi_data \
# --url=${URL}


COPY ingest_data.py ingest_data.py
# COPY ingest_taxi_zone_data.py ingest_taxi_zone_data.py

ENTRYPOINT ["python", "ingest_data.py"]


# ENTRYPOINT ["bash"]


