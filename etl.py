import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


# Function to process song files and insert the data into the specific data moddel.
def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)  # Opens the song file

    #Insert artist record
        
    artist_data_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data_values = artist_data_df.values
    first_record_df = artist_data_df.values[0]
    artist_data = first_record_df.tolist()

    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as error:
        print(error)

    # Insert song record
    song_data_df = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data_values = song_data_df.values
    first_record_df = song_data_values[0]
    song_data = first_record_df.tolist()

    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as error:
        print(error)

# Function that processes the log file and insert data into the data model
def process_log_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)  # Open the log file

    df = df[df.page == 'NextSong'] # Filter by NextSong action

    # Let's convert the timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # Inserting the time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as error:
            print(error)

    # Load the users table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates().dropna()

    # Insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as error:
            print(error)

    # Insert songplay records
    for index, row in df.iterrows():

        # Gets songid and artist id from song and artists tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

# Function to read all the files in the filepath 

def process_data(cur, conn, filepath, func):
    all_files = []  # Get all files matching extension from directory
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get the total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterating and processing the files
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

# Main function
def main():

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=franchise password='420GLOCKzone.'")
    cur = conn.cursor()

    process_data(cur, conn, filepath='./data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='./data/log_data', func=process_log_file)

    conn.close()



if __name__ == "__main__":
    main()

        
    
