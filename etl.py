import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import pdb 
import re
import json
import datetime

def process_song_file(cur, filepath):
    # open song file
    """Convert song json file to a DataFrame and extract the song data into the tables"""
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 
                         'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 
                           'artist_location', 'artist_latitude', 
                           'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    """Convert log json file to a DataFrame and extract the log data    into the tables.
    Filters data by "NextSong" page
    Transforms the timestamp column to a datetime before extraction
    """
    try:
        df = pd.read_json(filepath)
    except ValueError as e:
        file2 = open(filepath, 'r')
        contents = file2.read()
        pattern = re.compile("{\"artist\".*}")
        patternmatches = pattern.findall(contents)
        listofdicts =[]
        for match in patternmatches: 
            listofdicts.append(json.loads(match))
        df = pd.DataFrame.from_dict(listofdicts, orient='columns')

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df['ts'].values.tolist(), t.dt.hour.values.tolist(),
                 t.dt.day.values.tolist(), t.dt.week.values.tolist(), 
                 t.dt.month.values.tolist(), t.dt.year.values.tolist(), 
                 t.dt.weekday.values.tolist())
    column_labels = ('start_time', 'hour', 'day', 'week', 'month',                          'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    if str(time_df['start_time'].dtype) in ['int64','float64']:
        starttime_dates = []
        for time2 in time_df['start_time']:   
            seconds = datetime.datetime.fromtimestamp(time2/1000)
            starttime_dates.append(seconds)
        time_df['start_time'] = starttime_dates
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except Exception as e:
            print()
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 
                  'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    if str(df['ts'].dtype) in ['int64','float64']:
        starttime_dates = []
        for time2 in df['ts']:   
            seconds = datetime.datetime.fromtimestamp(time2/1000)
            starttime_dates.append(seconds)
        df['ts'] = starttime_dates
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
        songplay_data = (row.ts, row.userId, row.level, songid, 
                         artistid, row.sessionId, row.location, 
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    """Iterate trhough files in filepath and process them using a given function"""
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
    """Execute our ETL pipeline by connecting to the DB and processing data files"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    pdb.set_trace()
    main()