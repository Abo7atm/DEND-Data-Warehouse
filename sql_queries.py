# TODO:
# 1) Sort keys and dist keys
# 2) Forign keys -- DONE

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# READING VALUES FROM CONFIG FILE
# S3 CONFIG
log_data = config.get('S3', 'LOG_DATA')
song_data = config.get('S3', 'SONG_DATA')
json_path = config.get('S3', 'LOG_JSONPATH')
# IAM CONFIG
iam_role = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# files_fields = [
#     { # structure of files that go into events table
#         "artist": "Explosions In The Sky",
#         "auth": "Logged In",
#         "firstName": "Layla",
#         "gender": "F",
#         "itemInSession": 87,
#         "lastName": "Griffin",
#         "length": 220.3424,
#         "level": "paid",
#         "location": "Lake Havasu City-Kingman, AZ",
#         "method": "PUT",
#         "page": "NextSong",
#         "registration": 1541057188796.0,
#         "sessionId": 984,
#         "song": "So Long_ Lonesome",
#         "status": 200,
#         "ts": 1543449470796,
#         "userAgent": "\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.125 Safari\/537.36\"",
#         "userId": "24"
#     },
#     { # structure of files that go into songs table
#         "num_songs": 1,
#         "artist_id": "ARJIE2Y1187B994AB7",
#         "artist_latitude": null,
#         "artist_longitude": null,
#         "artist_location": "",
#         "artist_name": "Line Renaud",
#         "song_id": "SOUPIRU12A6D4FA1E1",
#         "title": "Der Kleine Dompfaff",
#         "duration": 152.92036,
#         "year": 0
#     },
# ]
#     
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist          TEXT,
        auth            TEXT,
        firstName       TEXT,
        gender          TEXT,
        itemInSession   INT,
        lastName        TEXT,
        length          NUMERIC,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    NUMERIC,
        sessionId       INT,
        song            TEXT,
        status          INT,
        ts              TIMESTAMP,
        userAgent       TEXT,
        userID          INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artists_id          TEXT,
        artist_latitude     NUMERIC,
        artist_longitude    NUMERIC,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            NUMERIC,
        year                INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time  TIMESTAMP SORTKEY,
        user_id     INT,
        level       TEXT,
        song_id     TEXT DISTKEY,
        artist_id   TEXT,
        session_id  INT,
        location    TEXT,
        user_agent  TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id     INT PRIMARY KEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     TEXT PRIMARY KEY DISTKEY,
        title       TEXT,
        artist_id   TEXT,
        year        INT,
        duration    NUMERIC
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT PRIMARY KEY,
        name        TEXT,
        location    TEXT,
        latitude    NUMERIC,
        longitude   NUMERIC
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP PRIMARY KEY SORTKEY,
        hour        INT,
        day         INT,
        week        INT,
        month       INT,
        year        INT,
        weekday     INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON {}
    TIMEFORMAT AS 'epochmillisecs';
""").format(log_data, iam_role, json_path)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(song_data, iam_role)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, 
        song_id, artist_id, session_id,
        location, user_agent
    )
    SELECT DISTINCT L.ts AS start_time,
           L.userId AS user_id,
           L.level, S.song_id, S.artists_id,
           L.sessionId AS session_id,
           L.location,
           L.userAgent AS user_agent
    FROM staging_events AS L
    JOIN staging_songs AS S
    ON  L.song   = S.title
    AND L.artist = S.artist_name
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id, first_name, last_name,
        gender, level
    )
    SELECT DISTINCT userId AS user_id,
           firstName AS first_name,
           lastName AS last_name,
           gender, level
    FROM staging_events
    WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id, title, artist_id,
        year, duration
    )
    SELECT DISTINCT song_id, title, artists_id,
    year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id, name, location,
        latitude, longitude
    )
    SELECT DISTINCT artists_id, artist_name AS name, 
           artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artists_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time, hour, day,
        week, month, year,
        weekday
    )
    SELECT ts,
          EXTRACT(hr FROM ts) AS hour,
          EXTRACT(d FROM ts) AS day,
          EXTRACT(w FROM ts) AS week,
          EXTRACT(mon from ts) AS month,
          EXTRACT(y from ts) AS year,
          EXTRACT(weekday from ts) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

