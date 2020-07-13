import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


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

def process_i94_data(spark, input_data, output_data, use_sample=True):
    if use_sample:
        # read i94 data file
        i94_data = os.path.join(input_data, 'immigration_data_sample.csv')
        df = spark.read.csv(i94_data, header=True)
    else:
        # read i94 data file
        i94_data = os.path.join(input_data, '18-83510-I94-Data-2016/*.sas7bdat')
        df = spark.read.format('com.github.saurfang.sas.spark').load(i94_data)

    # extract columns to create immigration table
    df.createOrReplaceTempView("i94_table")
    immigration_table = spark.sql(
        """ SELECT DISTINCT cicid,
                            i94port AS arr_city_port_code,
                            arrdate AS arr_date,
                            i94mode AS arr_mode,
                            i94addr AS arr_state_code,
                            depdate AS departure_date,
                            dtadfile AS file_date,
                            airline,
                            fltno AS flight_num,
                            entdepa AS arrival_flag,
                            entdepd AS departure_flag,
                            entdepu AS update_flag,
                            matflag AS match_flag
            FROM i94_table 
            WHERE cicid IS NOT NULL
        """)
    
    # write immigration table to parquet files
    immigration_table.write.parquet(
        path=os.path.join(output_data, 'immigration'),
        mode='overwrite'
    )
    
    # extract columns to create immigrant_info table
    immigrant_info_table = spark.sql(
        """ SELECT DISTINCT cicid,
                            i94bir AS age,
                            biryear AS bithyear,
                            i94cit AS country_citizen_code,
                            i94res AS country_residence_code,
                            i94visa AS visa_category,
                            visapost AS visa_issue_dep,
                            visatype AS visa_type,
                            occup AS occupation,
                            dtaddto AS date_admitted,
                            insnum AS ins_num,
                            admnum AS adm_num
            FROM i94_table 
            WHERE cicid IS NOT NULL
        """)
    
    # write immigration table to parquet files
    immigrant_info_table.write.parquet(
        path=os.path.join(output_data, 'immigrant_info'),
        mode='overwrite'
    )

def process_temperature_data(spark, input_data, output_data):
    # read data file
    df = spark.read.csv(input_data, header=True)
    
    # filter by country
    df = df.where(df['Country'] == 'United States')

    # extract columns for table
    df.createOrReplaceTempView("temperature_table")
    city_temperature_table = spark.sql(
        """ SELECT DISTINCT city AS city,
                            dt AS date,
                            AverageTemperature AS average_temperature,
                            AverageTemperatureUncertainty AS average_temperature_uncertainty,
                            Latitude AS latitute,
                            Longitude AS longitute 
            FROM temperature_table
            ORDER BY city
        """)
    
    # write table to parquet files
    city_temperature_table.write.parquet(
        path=os.path.join(output_data, 'temperature'),
        mode='overwrite'
    )

def process_demographic_data(spark, input_data, output_data):
    # read data file
    df = spark.read.csv(input_data, header=True)
    
    # extract columns for table
    df.createOrReplaceTempView("demographic_table")
    city_demographic_table = spark.sql(
        """ SELECT DISTINCT City AS city,
                            State AS state,
                            `Median Age` AS median_age,
                            `Male Population` AS male_population,
                            `Female Population` AS female_population,
                            `Total Population` AS total_population,
                            `Number of Veterans` AS veteran_num,
                            Foreign-born AS foreign_born,
                            `Average Household Size` AS average_household_size,
                            `State Code` AS state_code,
                            Race AS race,
                            Count AS count
            FROM demograhpic_table
            ORDER BY state, city
        """)
    
    # write table to parquet files
    city_demographic_table.write.parquet(
        path=os.path.join(output_data, 'demographic'),
        mode='overwrite'
    )

def process_airport_data(spark, input_data, output_data):
    # read data file
    df = spark.read.csv(input_data, header=True)
    
    # filter by country
    df = df.where(df['iso_country'] == 'US')
    
    # extract columns for table
    df.createOrReplaceTempView("airport_table")
    city_airport_table = spark.sql(
        """ SELECT DISTINCT ident,
                            type,
                            name,
                            elevation_ft,
                            iso_region,
                            municipality,
                            gps_code,
                            iata_code,
                            local_code,
                            coordinates
            FROM airport_table
            ORDER BY municipality
        """)
    
    # write table to parquet files
    city_airport_table.write.parquet(
        path=os.path.join(output_data, 'airport'),
        mode='overwrite'
    )

def main():
    spark = create_spark_session()
    input_data_prefix = "s3a://dend-yuhou/input_data/"
    output_data = "s3a://dend-yuhou/output_data/"
    
    process_i94_data(spark, input_data_prefix, output_data)
    print('Finish i94 dataset.')
    process_temperature_data(spark, os.path.join(input_data_prefix, 'GlobalLandTemperaturesByCity.csv'), output_data)
    print('Finish temperature dataset.')
    process_demographic_data(spark, os.path.join(input_data_prefix, 'us-cities-demographics.csv'), output_data)
    print('Finish demographic dataset.')
    process_airport_data(spark, os.path.join(input_data_prefix, 'airport-codes_csv.csv'), output_data)
    print('Finish airport dataset.')
    
if __name__ == "__main__":
    main()