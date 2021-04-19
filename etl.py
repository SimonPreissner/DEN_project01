import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

try: 
    from tqdm import tqdm
except ModuleNotFoundError:
    print("unable to import tqdm; no nice progress bars for now...")


def process_song_file(cur, filepath):
    """
    Opens and reads data about songs and inserts the information into the 
    tables 'songs' and 'artists'.
    """
    # open song file
    df = pd.DataFrame(pd.read_json(filepath, typ='series'))

    # insert song record
    song_data = list(df[0][['song_id', 
                            'title', 
                            'artist_id', 
                            'year', 
                            'duration']].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[0][['artist_id', 
                           'artist_name', 
                           'artist_location', 
                           'artist_latitude', 
                           'artist_longitude']].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Opens and reads data about songplays and inserts the information into 
    the tables 'time', 'users' and 'songplays'. Data for 'time' is extracted 
    from the value of 'ts' (timestamp). Inserting into 'songplays' utilizes 
    data from the 'artists' table, so please execute process_song_file() 
    before this function.  
    """
    # open log file
    df = pd.DataFrame(pd.read_json(filepath, lines=True))

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    # 'ts' is read as int -> needs to be converted to 3-decimal float -> /1000
    t = pd.Series([datetime.fromtimestamp(ts/1000) for ts in df['ts']])
    
    # insert time data records
    # extract tuples from the datetime objects, then re-arrange them by 'columns'
    time_data = [*zip(*[(v.timestamp()*1000, 
                         v.hour, 
                         v.day, 
                         v.week, 
                         v.month, 
                         v.year, 
                         v.weekday()) for v in t])]
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results[0].replace('(','').replace(')','').split(',')
            #print(songid, artistid) this will print out the one matching tuple.
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, 
                         row.ts*1000, # re-converting to comply with 'ts' from the log file
                         row.userId, 
                         row.level, 
                         songid, 
                         artistid, 
                         row.sessionId, 
                         row.location, 
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    List all files under a directory path and execute a specified ETL function.  
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
    try: 
        for i, datafile in tqdm(enumerate(all_files, 1)):
            func(cur, datafile)
            conn.commit()
    except ModuleNotFoundError as e:
        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()