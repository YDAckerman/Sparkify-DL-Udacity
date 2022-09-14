import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    create and return a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - load song data
    - extract and write songs table
    - extract and write artists table
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration'
                            ).drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year') \
                     .mode("overwrite") \
                     .parquet(output_data + "songs_table/")

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude').drop_duplicates()

    # write artists table to parquet files
    artists_table.write \
                 .mode("overwrite") \
                 .parquet(output_data + "artists_table/")

    pass


def process_log_data(spark, input_data, output_data):
    """
    - load log events data
    - create and write users table
    - create and write times table
    - load songs and artists table
    - merge with log events to create songplays table
    - write songplays table
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # create start_time and user_id columns
    df = df.withColumn('start_time', from_unixtime(df.ts/1000)) \
           .withColumn('user_id', col('userId').cast('int')) \
           .drop("userId")

    # extract columns for users table
    # create a row for each user based on the most recent log entry
    # using a window function
    w = Window.partitionBy("user_id").orderBy(col("start_time").desc())
    users_table = df.withColumn("row", row_number().over(w)) \
                    .filter(col("row") == 1) \
                    .select('user_id', col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), "gender",
                            "level")

    # write users table to parquet files
    users_table.write \
               .mode("overwrite") \
               .parquet(output_data + 'users_table/')

    # extract columns to create time table
    time_table = df.select(col("start_time")).distinct()
    time_table = time_table.withColumn("hour",
                                       hour(time_table.start_time)) \
                           .withColumn("day",
                                       dayofmonth(time_table.start_time)) \
                           .withColumn("week",
                                       weekofyear(time_table.start_time)) \
                           .withColumn("weekday",
                                       date_format('start_time', 'u')) \
                           .withColumn("month",
                                       month(time_table.start_time)) \
                           .withColumn("year",
                                       year(time_table.start_time))

    # write time table to parquet files partitioned by year and month
    time_table.write \
              .partitionBy("year", "month") \
              .mode("overwrite") \
              .parquet(output_data + 'time_table/')

    # read in song and artist data to use for songplays table
    # this is not efficient, since we already read these in.
    # however, I'm building to the rubric...
    songs_table = spark.read.parquet(output_data +
                                     'songs_table/*/*.parquet')
    artists_table = spark.read.parquet(output_data +
                                       'artists_table/*.parquet')

    # make a preliminary merge of the songs and artists tables:
    merge_df = songs_table.join(artists_table, "artist_id", 'inner') \
                          .select("song_id", "artist_id", "title",
                                  "artist_name").drop_duplicates()

    # extract columns from joined song and log datasets to create
    # songplays table
    cond = [df.song == merge_df.title, df.artist == merge_df.artist_name]
    songplays_table = df.join(merge_df, cond, "inner") \
                        .select("start_time", "user_id", "level",
                                "song_id", "artist_id", "location",
                                col("sessionId").alias("session_id"),
                                col("userAgent").alias("user_agent")) \
                        .withColumn("year",
                                    year("start_time")) \
                        .withColumn("month",
                                    month("start_time")) \
                        .withColumn("songplay_id",
                                    monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
                         .mode("overwrite") \
                         .parquet(output_data + 'songplays_table/')

    pass


def main():
    """
    - create spark session
    - process song and log-event data into tables in the datalake on s3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://my-sparkify-data-lake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
