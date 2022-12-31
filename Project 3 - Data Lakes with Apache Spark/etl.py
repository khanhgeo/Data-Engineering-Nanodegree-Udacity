import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructType, StructField, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a new Spark session or gets an existing one
    """
    print("\nCreating Spark session...\n")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def create_song_df(spark, input_data):
    """
    Creates song dataframe from data source files
    """
    print("\nCreating song dataframe...\n")
    
    # define song schema
    song_schema = StructType([
        StructField('artist_id', StringType(), False),
        StructField('artist_latitude', StringType(), True),
        StructField('artist_longitude', StringType(), True),
        StructField('artist_location', StringType(), True),
        StructField('artist_name', StringType(), False),
        StructField('song_id', StringType(), False),
        StructField('title', StringType(), False),
        StructField('duration', DoubleType(), True),
        StructField('year', IntegerType(), True)
    ])
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data', '*', '*', '*', '*.json')
    
    # read song data file
    song_df = spark.read.json(song_data, schema = song_schema)
    
    return song_df


def create_log_df(spark, input_data):
    """
    Creates log dataframe from data source log files
    """
    print("\nCreating log dataframe...\n")
    
    # define log schema
    log_schema = StructType([
        StructField('artist', StringType(), False),
        StructField('auth', StringType(), True),
        StructField('firstName', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('itemInSession', LongType(), True),
        StructField('lastName', StringType(), True),
        StructField('length', DoubleType(), True),
        StructField('level', StringType(), True),
        StructField('location', StringType(), True),
        StructField('method', StringType(), True),
        StructField('page', StringType(), False),
        StructField('registration', DoubleType(), True),
        StructField('sessionId', LongType(), True),
        StructField('song', StringType(), False),
        StructField('status', LongType(), True),
        StructField('ts', LongType(), False),
        StructField('userAgent', StringType(), True),
        StructField('userId', StringType(), False),
    ])
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data', '*.json')

    # read log data file
    log_df = spark.read.json(log_data, schema = log_schema)
    
    # filter by actions for song plays
    log_df = log_df.where(log_df['page'] == 'NextSong')
    
    return log_df


def process_data(spark, song_df, log_df, output_data):
    """
    1. Creates analytical tables
    2. Writes new tables into S3
    """

    # extract columns to create songs table
    songs_table = song_df.select('song_id','title','artist_id','year','duration') \
                         .dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    print("\nWriting songs table into S3...\n")
    songs_table.write.parquet(os.path.join(output_data, 'songs'), mode = 'overwrite', partitionBy = ['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = song_df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude') \
                           .withColumnRenamed('artist_name','artist') \
                           .withColumnRenamed('artist_location','location') \
                           .withColumnRenamed('artist_latitude','latitude') \
                           .withColumnRenamed('artist_longitude','longitude') \
                           .dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    print("\nWriting artists table into S3...\n")
    artists_table.write.parquet(os.path.join(output_data, 'artists'), mode = 'overwrite')

    # extract columns for users table    
    users_table = log_df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                        .withColumnRenamed('userId', 'user_id') \
                        .withColumnRenamed('firstName', 'first_name') \
                        .withColumnRenamed('lastName', 'last_name') \
                        .dropDuplicates(['user_id'])
    
    # write users table to parquet files
    print("\nWriting users table into S3...\n")
    users_table.write.parquet(os.path.join(output_data, 'users'), mode = 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    log_df = log_df.withColumn('start_time', get_timestamp(log_df['ts']))
    
    # extract columns to create time table
    time_table = log_df.select('start_time',
                           hour(col('start_time')).alias('hour'),
                           dayofmonth(col('start_time')).alias('day'),
                           weekofyear(col('start_time')).alias('week'),
                           month(col('start_time')).alias('month'),
                           year(col('start_time')).alias('year'), 
                           dayofweek(col('start_time')).alias('weekday')) \
                       .dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    print("\nWriting time table into S3...\n")
    time_table.write.parquet(os.path.join(output_data, 'time'), mode = 'overwrite', partitionBy = ['year', 'month'])

    # join log and song dataframes to use for songplays table
    songplay_df = log_df.join(song_df, (log_df['song'] == song_df['title']) & (log_df['artist'] == song_df['artist_name'])) \
                        .withColumn('songplay_id', monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplay_df.select('songplay_id', 'start_time', col('userId').alias('user_id'), \
                                         'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'), \
                                         'location', col('userAgent').alias('user_agent'), \
                                          year(col('start_time')).alias('year'), \
                                          month(col('start_time')).alias('month')) \
                                 .dropDuplicates(['start_time'])

    # write songplays table to parquet files partitioned by year and month
    print("\nWriting songplays table into S3...\n")
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), mode = 'overwrite', partitionBy = ['year', 'month'])


def main():
    """
    1. Creates a new Spark session or gets an existing one
    2. Creates analytical tables from song and log json data files
    3. Writes new tables to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://capstonefinalproject/"
    
    song_df = create_song_df(spark, input_data)
    log_df = create_log_df(spark, input_data)
    
    process_data(spark, song_df, log_df, output_data)
    print("\nETL process completed\n")


if __name__ == "__main__":
    main()
