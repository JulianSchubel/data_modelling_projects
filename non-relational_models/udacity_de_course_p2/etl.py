import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

# checking current working directory
print(os.getcwd())

# path to event_data subfolder
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# get total number of rows 
# print(len(full_data_rows_list))
# list event data rows
# print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))

# connect to casssandra instance
cluster = Cluster(["127.0.0.1"])

# establish a session
session = cluster.connect()

query = """
    CREATE KEYSPACE IF NOT EXISTS project2 
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1}
"""
try:
    session.execute(query)
except Excpetion as e:
    print(e)

session.set_keyspace('project2')

# Query 1: song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
# Drop table if it exists
session.execute("DROP TABLE IF EXISTS session_song_plays")
# session_id as partition key distributes data by session, which is appropriate given we are querying for a session and subsequently data about the session (item_in_session)
# item_in_session as clustering column completes the primary key and orders data by item_in_session
query = """
    CREATE TABLE IF NOT EXISTS session_song_plays (
        session_id INT,
        item_in_session INT,
        song TEXT,
        artist_name TEXT,
        length DOUBLE,
        PRIMARY KEY (session_id, item_in_session))
"""
try:
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO session_song_plays (session_id, item_in_session, song, artist_name, length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[9], line[0], float(line[5])))

query = "SELECT artist_name, song, length FROM session_song_plays WHERE session_id = 338 AND item_in_session = 4"
result = session.execute(query)

for row in result:
    print(row.artist_name, row.song, row.length)

# Query 2: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
# Drop table if it exists
session.execute("DROP TABLE IF EXISTS user_sessions")
try:
    # user_id and session_id columns as partition key distributes the data by user sessions, which given querying based on user_id and session_id is efficient
    # adding the item_in_session column as clustering column a) completes the primary key and b) satisfies ordering by item_in_session requirement on disk. 
    query = """
        CREATE TABLE IF NOT EXISTS user_sessions (
            user_id INT,
            session_id INT,
            item_in_session INT,
            first_name TEXT,
            last_name TEXT,
            song TEXT,
            artist_name TEXT,
            PRIMARY KEY ((user_id, session_id), item_in_session)
        )
    """
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_sessions (user_id, session_id, item_in_session, first_name, last_name, song, artist_name) "
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[1], line[4], line[9], line[0]))

query = "SELECT artist_name, song, first_name, last_name FROM user_sessions WHERE user_id = 10 AND session_id = 182"
result = session.execute(query)

for row in result:
    print(row.artist_name, row.song, row.first_name, row.last_name)

# Query 3: Every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# Drop table if it exists
session.execute("DROP TABLE IF EXISTS user_song_listens")
try:
    # song column as partition key distrubutes data by song, the main subject of the query
    # adding a user_id clustering_key completes the primary_key
    query = """
        CREATE TABLE IF NOT EXISTS user_song_listens (
            song TEXT,
            user_id INT,
            first_name TEXT,
            last_name TEXT,
            artist_name TEXT,
            PRIMARY KEY (song, user_id)
        )
    """
    session.execute(query)
except Exception as e:
    print(e)

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO user_song_listens (song, user_id, first_name, last_name, artist_name) "
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4], line[0]))

query = "SELECT first_name, last_name, song, artist_name FROM user_song_listens WHERE song = 'All Hands Against His Own'"
result = session.execute(query)

for row in result:
    print(row.first_name, row.last_name, row.song, row.artist_name)

tables = [i.name for i in session.execute("DESCRIBE TABLES")]
print(tables)
#for t in tables:
#    session.execute( f"DROP TABLE {t}")

session.shutdown()
cluster.shutdown()
