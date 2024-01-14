FROM python:3.9.1

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow wheel parquet

WORKDIR /app

COPY pipeline.py pipeline.py

# COPY ingest_data.py ingest_data.py
# COPY ingest_taxi_zone_data.py ingest_taxi_zone_data.py

# ENTRYPOINT ["python", "ingest_data.py"]
ENTRYPOINT ["python", "pipeline.py"]

# ENTRYPOINT ["bash"]


