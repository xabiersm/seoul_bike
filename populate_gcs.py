#import Prefect to orchestrate and load data to gcp
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket, GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect.tasks import task_input_hash
from prefect.client.schemas.schedules import IntervalSchedule

#import kaggle to download the data
from kaggle.api.kaggle_api_extended import KaggleApi

#import pandas
import polars as pl

#import pathlib
from pathlib import Path

#import os to work with folders
import os

#import timedelta
from datetime import timedelta, datetime

@task(name="Download_data",description="Checks if the file already exists and download it to a local path.",retries=3,retry_delay_seconds=1,
      cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def download_data(dataset_owner: str, dataset_name: str, path: str, file_name: str):
    api = KaggleApi()
    api.authenticate()
    
    try:
        os.mkdir(path)
    except OSError as error:
        print("Directory already exists, no need to create it")
        
    my_file = Path(f"{path}{file_name}")
    #print(my_file)
    
    if my_file.is_file():
        print("File already exists, no need to download")
    else:
        api.dataset_download_files(dataset=f"{dataset_owner}/{dataset_name}", path=path,unzip=True)

@task(name="Read_data",description="Read data from the csv file and create a DataFrame")
def read_data(file_path: str) -> pl.DataFrame:
    schema = {
        "Duration": pl.Int8,
        "Distance": pl.Int16,
        "PLong": pl.Float32,
        "PLatd": pl.Float32,
        "DLong": pl.Float32,
        "Dlatd": pl.Float32,
        "Haversine": pl.Float32,
        "Pmonth": pl.Int8,
        "Pday": pl.Int8,
        "Phour": pl.Int8,
        "Pmin": pl.Int8,
        "PDweek": pl.Int8,
        "Dmonth": pl.Int8,
        "Dday": pl.Int8,
        "Dhour": pl.Int8,
        "Dmin": pl.Int8,
        "DDweek": pl.Int8,
        "Temp": pl.Float32,
        "Precip": pl.Float32,
        "Wind": pl.Float32,
        "Humid": pl.Float32,
        "Solar": pl.Float32,
        "Snow": pl.Float32,
        "GroundTemp": pl.Float32,
        "Dust": pl.Float32
    }
    df = pl.read_csv(file_path,schema=schema,separator=",")
    #print(df.head(10))
    #print(df.schema)
    return df
    
@flow(name='Ingest data')
def seoul_bike_trips(dataset_owner: str, dataset_name: str, file_path: str, filename: str):
    #dataset_owner = "tagg27"
    #dataset_name = "seoul-bike-trip"
    #file_path = "./data/"
    #file_name = "cleaned_seoul_bike_data.csv"
    
    download_data(dataset_owner, dataset_name, file_path, filename)
    datafile_path = f"{file_path}{filename}"
    df = read_data(file_path=datafile_path)

if __name__ == "__main__":
    parameters = {"dataset_owner": "tagg27", "dataset_name": "seoul-bike-trip", "file_path": "./data/", "filename": "cleaned_seoul_bike_data.csv"}
    seoul_bike_trips.serve(name="Seoul city bike trips",parameters=parameters,schedule=IntervalSchedule(interval=timedelta(days=1),anchor_date=datetime(2024,1,1,0,0),timezone="Europe/Berlin"))

    #seoul_bike_trips(parameters)
   