import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_songs;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE STAGING TABLES
staging_songs_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     double precision,
    artist_longitude    double precision,
    artist_location     varchar,
    artist_name         varchar,
    song_id             varchar,
    title               text,
    duration            double precision,
    year                int )
diststyle all;     
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar,
    auth            varchar,
    firstName       varchar,
    gender          varchar,
    iteminSection   int,
    lastName        varchar,
    length          float,
    level           varchar,
    location        varchar,
    method          varchar,
    page            varchar,
    registration    float,
    sectionId       int,
    song            text,
    status          int,
    ts              bigint,
    userAgent       text,
    userId          int)
diststyle all; 
""")

# CREATE DIM AND FACT TABLES
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id     int PRIMARY KEY sortkey,
    first_name  varchar NOT NULL,
    last_name   varchar NOT NULL,
    gender      varchar,
    level       varchar )
diststyle all; 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     varchar PRIMARY KEY sortkey,
    title       varchar NOT NULL,
    artist_id   varchar,
    year        int,
    duration    double precision NOT NULL )
diststyle all;  
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   varchar PRIMARY KEY sortkey,
    name        varchar NOT NULL,
    location    varchar,
    lattitude   double precision,
    longitude   double precision )
diststyle all;  
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  timestamp PRIMARY KEY sortkey,
    hour        int,
    day         int,
    week        int,
    month       int,
    year        int,
    weekday     int )
diststyle all; 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey distkey,
    start_time  timestamp NOT NULL REFERENCES time(start_time),
    user_id     int NOT NULL REFERENCES users(user_id),
    level       varchar,
    song_id     varchar REFERENCES songs(song_id),
    artist_id   varchar REFERENCES artists(artist_id),
    session_id  int,
    location    varchar,
    user_agent  text ); 
""")


# COPY DATA TO STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}' 
    REGION 'us-west-2'
    FORMAT AS JSON '{}';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'  
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# INSERT DATA TO FINAL DIM AND FACT TABLES
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT  userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
                        EXTRACT(hour from start_time),
                        EXTRACT(day from start_time),
                        EXTRACT(week from start_time),
                        EXTRACT(month from start_time),
                        EXTRACT(year from start_time),
                        EXTRACT(weekday from start_time)
        FROM staging_events
        WHERE ts IS NOT NULL
""")

songplay_table_insert = ("""
    INSERT INTO songplays (--songplay_id,
                            start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT timestamp 'epoch' + se.ts / 1000 * interval '1 second' AS start_time,
                        se.userId AS user_id,
                        se.level AS level, 
                        ss.song_id AS song_id,
                        ss.artist_id AS artist_id,
                        se.sectionId AS session_id,
                        se.location AS location,
                        se.userAgent AS user_agent
        FROM staging_songs ss
        JOIN staging_events se 
            ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong'                     
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
