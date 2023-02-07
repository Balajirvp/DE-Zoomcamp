from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    return Path(f"./data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning Example"""
    df = pd.read_parquet(path)
    # print(f"Pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"Post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Writing data into BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    df.to_gbq(destination_table="yellow_trips.rides", 
              project_id = "composed-sun-375018", 
              credentials = gcp_credentials_block.get_credentials_from_service_account(), 
              chunksize=100000, 
              if_exists="append")

@flow(log_prints=True)
def etl_gcs_to_bq(color: str = "yellow", year: int = 2019, months: list[int] = [2,3]):
    """Main ETL Flow to load data to Big Query data warehouse"""    
    for month in months:
        path = extract_from_gcs(color, year, month)
        df = transform(path)
        print(f"Row count: {len(df)}")
        write_bq(df)

if __name__ == '__main__':
    color = "yellow"
    year = 2019
    months = [2,3]
    etl_gcs_to_bq(color, year, months)