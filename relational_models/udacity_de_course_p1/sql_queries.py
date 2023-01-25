# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# Fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL NOT NULL, start_time TIMESTAMP NOT NULL, user_id INT NOT NULL, level VARCHAR NOT NULL, song_id VARCHAR, artist_id VARCHAR, session_id INT, location VARCHAR, user_agent VARCHAR, PRIMARY KEY (songplay_id), UNIQUE (start_time, user_id, level, user_agent));
""")

# Dimension tables
# All data excluding personal (with the exception of artists) and location data is considered mandatory as the minimum data quality requirement for dimension tables.

# users in the app
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (user_id INT NOT NULL, first_name VARCHAR, last_name VARCHAR, gender CHAR(1), level VARCHAR NOT NULL, PRIMARY KEY (user_id));
""")

# songs in music database
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR NOT NULL, title VARCHAR NOT NULL, artist_id VARCHAR NOT NULL, year INT NOT NULL, duration NUMERIC NOT NULL, PRIMARY KEY (song_id));
""")

# artists in music database
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR NOT NULL, name VARCHAR NOT NULL, location VARCHAR, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, PRIMARY KEY (artist_id));
""")

# timestamps of records in songplays broken down into specific units
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP NOT NULL, hour INT, day INT, week INT, month INT, year INT, weekday INT, PRIMARY KEY (start_time));
""")

# INSERT RECORDS
# Fact table
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time, user_id, level, user_agent)
    DO NOTHING
""")

# Dimension tables
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE
        SET level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id
    FROM artists a
    JOIN songs s ON s.artist_id = a.artist_id
    WHERE s.title LIKE %s AND a.name LIKE %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
