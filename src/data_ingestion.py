import click
import pandas as pd
import os
from google.cloud import storage

@click.command()
@click.option('--sa_path', help='Path to the service account json file')
@click.option('--project_id', help='Project ID of you GCP project')
@click.option('--year', default=2021, help='Year to download')
@click.option('--bucket', help='Name of the bucket to upload the data')
def data_ingestion(sa_path, project_id, year, bucket):
    
    color='green'
    for month in range(1, 4):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
        
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"

        print("Loading data from url...")
        df_taxi = pd.read_parquet(url)
        print('Uploading data to GCS...')
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f'green_taxi/{file_name}').upload_from_string(df_taxi.to_parquet(), 'text/parquet', timeout=300)
        print("Successfully uploaded the data!")
    
    
    
    
if __name__ == '__main__':
    data_ingestion()