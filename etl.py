import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofweek, hour, weekofyear, to_timestamp



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
    """
    - reads all the log data from S3 into a dataframe
    - extracts columns to create users, time tables
    - creats songplays table by joining song data and log data
    - writes users, time and songplays tables to parquet files in S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data")
    df = spark.sql("""SELECT * 
                      From log_data
                      Where page='NextSong'
                      """)

    # extract columns for users table    
    df.createOrReplaceTempView("log_data")
    users_table = spark.sql("""SELECT DISTINCT userId as user_id, 
                                  firstName as first_name, 
                                  lastName as last_name, 
                                  gender, 
                                  level
                            FROM log_data
                            WHERE userId IS NOT NULL""")
    
    # write users table to parquet files
    (users_table
     .write
     .mode('overwrite')
     .parquet(output_data+'users/')
    )

    # create timestamp column from original timestamp column
    df = df.withColumn("ts", to_timestamp(df.ts/1000))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data")
    time_table = spark.sql("""SELECT DISTINCT ts as start_time, 
                                     hour(ts) as hour, 
                                     day(ts) as day,
                                     weekofyear(ts) as week,
                                     month(ts) as month,
                                     year(ts) as year,
                                     dayofweek(ts) as weekday
                            
                              FROM log_data
                              WHERE ts IS NOT NULL""")
    
    # write time table to parquet files partitioned by year and month
    (time_table
     .write
     .mode('overwrite')
     .partitionBy("year", "month")
     .parquet(output_data+'time/')
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs/")
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT l.ts as start_time, 
                                      l.userId as user_id, 
                                      l.level, 
                                      s.song_id, 
                                      s.artist_id, 
                                      l.sessionId as session_id, 
                                      l.location, 
                                      l.userAgent,
                                      month(ts) as month,
                                      year(ts) as year
                                FROM song_data s
                                JOIN log_data l ON (l.song=s.title)
                                WHERE l.page='NextSong'""")
    songplays_table= songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    # write songplays table to parquet files partitioned by year and month
    (songplays_table
     .write
     .mode('overwrite')
     .partitionBy("year", "month")
     .parquet(output_data+'songplays/')
    )

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
