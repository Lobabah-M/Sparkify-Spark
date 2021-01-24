# Description
This project was created for the purpose of analyzing data for the music streaming app ***Sparkify***. In particular, the analysis team is interested in understanding what songs the users are listening to. Their user base has grown and they want to transfer their data warehouse to a data lake. This is done by building an ETL pipeline in python to transfer data from JSON files on **S3**, process the data using **spark** and load the data back into S3 in **parquet files** containing a set of dimensional tables. 

# How to Run the Project on EMR Using AWS Console
1. Create an AWS **IAM user** with **AdministratorAccess**
2. Copy the access key and secret into the **dl.cfg** file 
3. Upload **etl.py** to an S3 bucket        
4. Create an EMR cluster
5. When the cluster is in *waiting* status, go to the **steps** tab to submit the *spark* job
   
   Step type: Spark Application
   
   Deploy mode: Cluster
   
   Application Location: S3 URI to etl.py
   
   Action on failure: continue 
   
   
6. Delete the cluster


# File Description 
## Repository
* **dl.cfg** - contains the AWS credentials needed to access the S3 buckets

* **etl.py** - Loads data from S3, creates dataframes that represent the dimensional tables and writes them to parquet files in S3

* **README.md** - contains a description of the project


## S3

* **Song data**: *s3://udacity-dend/song_data* - contains metadata about a song and the artist of that song in JSON format.

* **Log data**: *s3://udacity-dend/log_data* - contains simulated app activity logs from an imaginary music streaming app based on configuration settings.



# Database Schema and ETL Pipline

Data is loaded from json files in S3 to dataframes and transformed using pyspark SQL to a set of dimensional tables then loaded back into S3. Each table is stored in a seperate folder. Songs table folders are partitioned by year and then artist. Time table files are partitioned by year and month. 

### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Data is transfered into this table by *joining* the two staging table using songId.

### Dimension Tables
* **users** - users in the app (user_id, first_name, last_name, gender, level)
    
* **songs** - songs in music database (song_id, title, artist_id, year, duration)
  
* **artists** - artists in music database (artist_id, name, location, lattitude, longitude)
  
* **time** - timestamps of records in songplays broken down into specific units
  (start_time, hour, day, week, month, year, weekday)


