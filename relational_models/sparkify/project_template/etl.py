import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
        - Process single-record song data (data/song_data) .json files
        - Parses record for necessary data to populate the 'songs' and 'artists' tables
        - Data quality assumptions:\
            All records are complete (no NaN or empty values); Records containing empty or NaN values will be ignored.
            All records are unique (no duplicate records); Duplicate records will be ignored.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # Only execute if there are no empty values
    if not df.isna().any().any():
        # Only one record per file
        # insert song record
        # Retrieve values for song_id, title, artits_id, year, and duration columns
        song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
        cur.execute(song_table_insert, song_data)
        # insert artist record
        artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        - Process multi-record log data (data/log_data) .json files
        - Parses records for necessary data and populates the 'time' and 'users' tables
        - Retrieves song_id and artist_id from 'songs' and 'artists' tables respectively; combines with log data to populate the 'songplays' table
        - Data quality assumptions:
            All records are complete (no NaN or empty values); Records containing empty or NaN values will be ignored.
            All records are unique (no duplicate records); Duplicate records will be ignored.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query("page == 'NextSong'")
    # Drop any NaN or empty values and reset the index
    df = df.dropna().reset_index(drop=True)

    # convert timestamp column to datetime
    # t = df.ts.apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0)
    t = pd.to_datetime(df["ts"], unit="ms")
    # insert time data records
    # Create a unified iterator across the time iterables produced by the dt attribute
    time_data = list(zip(t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = ("timestamp", "hour", "day", "week_of_year", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None
        # insert songplay record
        songplay_data = (t[index], int(df["userId"][index]), df["level"][index], song_id, artist_id, int(df["sessionId"][index]), df["location"][index], df["userAgent"][index])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        - Retrieves a list of absolute paths to all JSON files under 'filepath'
        - Processes all identified files using 'func'
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=admin password=admin")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
