import configparser
import os
from pyspark.sql import SparkSession



# reads the dl.cfg file to get the credentials required to access to S3
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - reads all the song data from S3 into a dataframe
    - extracts columns to create songs and artists table
    - writes songs and artists tables to parquet files in S3
    """
    # get filepath to song data file
    song_data = input_data+"song_data/*/*/*/*.json"
    
    # read song data files
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration
                               FROM song_data
                               WHERE song_id IS NOT NULL""")
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table
     .write
     .mode('overwrite')
     .partitionBy("year", "artist_id")
     .parquet(output_data+'songs/')
    )

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT artist_id, 
                                                 artist_name as name, 
                                                 artist_location as location, 
                                                 artist_latitude as latitude, 
                                                 artist_longitude as longitude
                                 FROM song_data
                                 WHERE artist_id IS NOT NULL""") 
    
    # write artists table to parquet files
    (artists_table
     .write
     .mode('overwrite')
     .parquet(output_data+'artists/')
    )


def process_log_data(spark, input_data, output_data):


def main():
    """
    - creates a spark session
    - passes the session and input and output paths to a function to process song data
    - passes the session and input and output paths to a function to process log data
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-project-4/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
