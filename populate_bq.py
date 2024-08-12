from prefect_gcp.cloud_storage import GcsBucket

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import make_timestamp, lit


#this script will download data from the data lake (gcs), some transformations will be applied and the cleaned data will be uploaded to the data warehouse (big query)

def create_spark_session(app_name: str):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    return spark

#download data from the data lake and create a spark dataframe from it
def download_data(name: str, gcs_path: str, local_path: str, spark):
    bucket = GcsBucket.load(name)

    bucket.download_folder_to_path(from_folder=gcs_path,to_folder=local_path)
    
    df = spark.read.parquet(f"{local_path}/*")
    
    df.show()
    
    return df

def transform_df(df):
       
    #transformation using pyspark
    
    df_trans = df.withColumn(
        "pickup_datetime",
        make_timestamp(lit(2023),df.Pmonth,df.Pday,df.Phour,df.Pmin,lit(0))
    ).withColumn(
        "dropoff_datetime",
        make_timestamp(lit(2023),df.Dmonth,df.Dday,df.Dhour,df.Dmin,lit(0))
    )
    
    #df_trans.show()
    
    columns_to_drop = ["Pmonth","Pday","Phour","Pmin","Dmonth","Dday","Dhour","Dmin"]
    
    #passing the names as a list but unpacking it so spark can understand
    df_dropped = df_trans.drop(*columns_to_drop)
    
    df_dropped.show()
    
    #TODO: make sure that 'duration' is equal to 'dropoff_datetime' - 'pickup_datetime'
    
    return df_dropped
    
def upload_to_bq(df_cleaned):
    data = []

def populate_bq():
    bucket_name = "seoul-bike-trips-bucket"
    gcs_path = "bike-data"
    local_path = "./data/datalake/"
    spark = create_spark_session(app_name="SeoulBike")
    
    df = download_data(name=bucket_name, gcs_path=gcs_path,local_path=local_path, spark=spark)
    df_cleaned = transform_df(df)
    
    upload_to_bq(df_cleaned)
    
    
    
if __name__ == "__main__":
    populate_bq()