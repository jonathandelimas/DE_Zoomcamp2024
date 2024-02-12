import io
import pandas as pd
import requests
import pyarrow.parquet as pq

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet'
    # url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz '

    response = requests.get(url)

    # Check if the response is OK (200 status)
    if response.status_code == 200:
        # Write the content to a local file
        with open('temp.parquet', 'wb') as file:
            file.write(response.content)

        # Now read from the saved file
        return pd.read_parquet('temp.parquet')
    else:
        # Handle errors or return an appropriate message
        return None


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
