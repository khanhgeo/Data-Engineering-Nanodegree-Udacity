import os
import glob
import psycopg2
import pandas as pd
import configparser
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Open song files data
    - Insert data to songs and artists dimension tables
    """    
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = [x for x in df[['song_id','title', 'artist_id', 'year', 'duration']].values[0].tolist()]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [x for x in df[['artist_id','artist_name', 'artist_location', 'artist_latitude', 
                                  'artist_longitude']].values[0].tolist()]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Open log files data
    - Filter playing song data, extract time data the insert data to time and users dimension tables
    """ 
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime then extract time data
    df['start_time'] = pd.to_datetime(df['ts'], unit='ms')
    df['hour']=df['start_time'].dt.hour
    df['day'] = df['start_time'].dt.day
    df['week'] = df['start_time'].dt.week
    df['month'] = df['start_time'].dt.month
    df['year'] = df['start_time'].dt.year
    df['weekday'] = df['start_time'].dt.day_name()
    
    # insert time data records
    time_df = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # extract data and songplay table
    df = df[['start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'song', 'artist', 'length']]
    df['song_id'] = None
    df['artist_id'] = None

    # insert songplay records
    
    for index, row in df.iterrows():        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = row[['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']]
        songplay_data['song_id'] = songid
        songplay_data['artist_id'] = artistid
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Get all JSON files from directory
    - Get the total number of files in directory then iterate over files and process
    """   
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects to the sparkifydb 
    - Process all the song and log data files then insert data to all designed tables
    """    
    # connect to db
    config = configparser.ConfigParser()
    config.read('db.cfg')
    host = config['db']['HOST']
    dbname = config['db']['DB_NAME']
    user = config['db']['DB_USER']
    password = config['db']['DB_PASSWORD']
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['db'].values()))
    cur = conn.cursor()

    # process data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # create database erd
    graph = create_schema_graph(metadata=MetaData(f'postgresql://{user}:{password}@{host}/{dbname}'))
    graph.write_png('sparkifydb_erd.png')

    conn.close()


if __name__ == "__main__":
    main()