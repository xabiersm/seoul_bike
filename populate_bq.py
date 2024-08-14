from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.client.schemas.schedules import IntervalSchedule
from prefect_gcp.cloud_storage import GcsBucket

from pyspark.sql import SparkSession
from pyspark.sql.functions import make_timestamp, lit, concat

#import timedelta
from datetime import timedelta, datetime

#import os to work with folders
import os

#import pathlib
from pathlib import Path

@task(name='Create Spark session')
def create_spark_session(app_name: str):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config('spark.driver.memory','15g') \
        .config("spark.jars.packages","com.google.cloud.spark:spark-3.5-bigquery:0.40.0") \
        .getOrCreate()
    
    return spark

#download data from the data lake and create a spark dataframe from it
@task(name='Download data')
def download_data(bucket_name: str, gcs_path: str, local_path: str, filename:str, spark):    
    try: 
        os.mkdir(local_path)
    except OSError:
        print('Directory already exists, there is no need to create it.')
          
    if Path(f"{local_path}/{filename}").is_file():
        print('File already exists locally, there is no need to download it.')
    else:
        bucket = GcsBucket.load(bucket_name)
        bucket.download_folder_to_path(from_folder=gcs_path,to_folder=local_path)
    
    df = spark.read.parquet(f"{local_path}/{filename}")
    
    
    return df

@task(name='Transform dataframe')
def transform_df(df):
       
    #transformation using pyspark
    
    df_trans = df.withColumn(
        "PickUpDatetime",
        make_timestamp(lit(2023),df.Pmonth,df.Pday,df.Phour,df.Pmin,lit(0))
    ).withColumn(
        "DropOffDatetime",
        make_timestamp(lit(2023),df.Dmonth,df.Dday,df.Dhour,df.Dmin,lit(0))
    )
    
    columns_to_drop = ["Pmonth","Pday","Phour","Pmin","Dmonth","Dday","Dhour","Dmin"]
    
    #passing the names as a list but unpacking it so spark can understand
    df_dropped = df_trans.drop(*columns_to_drop)
    
    #TODO: make sure that 'duration' is equal to 'dropoff_datetime' - 'pickup_datetime', otherwise change the value
    
    df_compared = df_dropped.withColumn("Duration",(df_dropped["DropOffDatetime"] - df_dropped["PickUpDatetime"]).cast("int")/60)
    
    # how to use when otherwise in pyspark, but it is not necessary. easier to just update the column with the substraction
    # df_durationUpdated = df_compared.withColumn(
    #     "Duration",
    #     when(df_compared["Duration"] == df_compared["DateDiff"],df_compared["Duration"]) \
    #     .otherwise(df_compared['DateDiff'])
    # )
    # df_durationUpdated.show(truncate=False)
    
    #change the PDweek and DDweek to weekdays names
    # df_compared = df_compared.withColumn(
    #     "PDweek",
    #     when(df_compared["PDweek"] == 0,"MONDAY").when(df_compared["PDweek"] == 1,"TUESDAY").when(df_compared["PDweek"] == 2,"WEDNESDAY").when(df_compared["PDweek"] == 3,"THURSDAY") \
    #     .when(df_compared["PDweek"] == 4,"FRIDAY").when(df_compared["PDweek"] == 5,"SATURDAY").when(df_compared["PDweek"] == 6,"SUNDAY")
    #     )
    
    # df_compared = df_compared.withColumn(
    #     "DDweek",
    #     when(df_compared["DDweek"] == 0,"MONDAY").when(df_compared["DDweek"] == 1,"TUESDAY").when(df_compared["DDweek"] == 2,"WEDNESDAY").when(df_compared["DDweek"] == 3,"THURSDAY") \
    #     .when(df_compared["DDweek"] == 4,"FRIDAY").when(df_compared["DDweek"] == 5,"SATURDAY").when(df_compared["DDweek"] == 6,"SUNDAY")
    #     )
    
    # df_compared.show(truncate=False)
    
    #to do the same but with sql statements
    
    #register the dataframe as a temporary view so sql queries can be run against it
    # df_compared.createOrReplaceTempView("DaysOfTheWeek")
    # result_df = spark.sql
    # ("""
    #     SELECT
    #         PickUpDatetime,
    #         DropOffDatetime,
    #         CASE
    #             WHEN PDweek = 0 THEN 'MONDAY'
    #             WHEN PDweek = 1 THEN 'TUESDAY'
    #             WHEN PDweek = 2 THEN 'WEDNESDAY'
    #             WHEN PDweek = 3 THEN 'THURSDAY'
    #             WHEN PDweek = 4 THEN 'FRIDAY'
    #             WHEN PDweek = 5 THEN 'SATURDAY'
    #             WHEN PDweek = 6 THEN 'SUNDAY'
    #             ELSE PDweek
    #         END
    #     FROM DaysOfTheWeek
    # """)
    
    #create a column with latitude, longitude data
    df_geo = df_compared.withColumn(
        "PickUpGeolocalization",
        concat(df_compared['PLong'], lit(', '),df_compared['PLatd'])
    ).drop(df_compared['PLatd']).drop(df_compared['PLong']) \
    .withColumn(
        "DropOffGeolocalization",
        concat(df_compared['DLong'],lit(', '),df_compared['Dlatd'])
    ).drop(df_compared['Dlatd']).drop(df_compared['DLong'])
    
    # df_geo.show(5)
    
    return df_geo

@task(name='Upload to BigQuery.')
def upload_to_bq(df, table_id: str, project_id: str):
    #upload to bigquery using pyspark
    df.write \
        .format("bigquery") \
        .option("writeMethod","direct") \
        .mode("overwrite") \
        .save(f"{project_id}.{table_id}")
    
    # these credentials are for pandas method
    # gcp_creds_block = GcpCredentials.load("credentials-xsm")
    # gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    # pandas_gbq.to_gbq(
    #     dataframe=df_pandas,
    #     destination_table=table_id,
    #     project_id=project_id,
    #     if_exists='replace',
    #     credentials=gcp_creds
    # )

@flow(name='Populate BigQuery table',description="Downloads a parquet file from GCS, applies transformations using pyspark and uploads the resulting dataframe to BigQuery.")
def populate_bq(bucket_name: str, gcs_path: str, local_path: str, filename: str, table_id: str, project_id: str) -> None:
    # bucket_name = "seoul-bike-trips-bucket"
    # gcs_path = "bike-data"
    # local_path = "./data/datalake/"
    # table_id = 'seoul_bike_trips_dataset.seoul_bike_trips'
    # project_id = 'useful-circle-430118-u0'
    # filename = 'seoul_bike-0.parquet'
    
    spark = create_spark_session(app_name="SeoulBike")
    
    df = download_data(bucket_name=bucket_name, gcs_path=gcs_path,local_path=local_path, filename=filename, spark=spark)
    df_cleaned = transform_df(df)
    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    upload_to_bq(df=df_cleaned,table_id=table_id,project_id=project_id, )
    
    spark.stop()
    
if __name__ == "__main__":
    parameters={'filename':'seoul_bike-0.parquet','bucket_name':'seoul-bike-trips-bucket','gcs_path':'bike-data','local_path':'./data/datalake',
                'table_id':'seoul_bike_trips_dataset.seoul_bike_trips', 'project_id':'useful-circle-430118-u0'}
    populate_bq.serve(name="Seoul city bike - Populate BQ",parameters=parameters,schedule=IntervalSchedule(interval=timedelta(days=1),anchor_date=datetime(2024,1,1,0,30),timezone="Europe/Berlin"))