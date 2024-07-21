#import Prefect to orchestrate and load data to gcp
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket, GcpCredentials
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from kaggle.api.kaggle_api_extended import KaggleApi
from prefect.tasks import task_input_hash

#import kaggle to download the data
from kaggle.api.kaggle_api_extended import KaggleApi

#import polars
import polars as pl

#import pathlib
from pathlib import Path

#import os to work with folders
import os

def download_data(dataset_owner: str, dataset_name: str, path: str):
    api = KaggleApi()
    api.authenticate()
    
    try:
        os.mkdir(path)
    except OSError as error:
        print("Directory already exists, no need to create it")
    
    api.dataset_download_files(dataset=f"{dataset_owner}/{dataset_name}", path=path,unzip=True)

if __name__ == "__main__":
    dataset_owner = "tagg27"
    dataset_name = "seoul-bike-trip"
    file_path = "./data/"
    
    download_data(dataset_owner, dataset_name, file_path)
    