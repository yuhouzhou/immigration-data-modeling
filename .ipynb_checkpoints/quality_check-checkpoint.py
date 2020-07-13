import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
#     import argparse

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark

def check_df(spark, parquet_path):
    df = spark.read.parquet(parquet_path)
    if df.count() > 0:
        print('Data is correct.')

if __name__ == '__main__':
    spark = create_spark_session()
    parquet_path = "s3a://dend-yuhou/output_data/immigration"
    check_df(spark, parquet_path)
    