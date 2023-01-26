import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
REGION = config.get("CLUSTER", "REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
    event_id           INT IDENTITY(0, 1)    PRIMARY KEY,
    artist_name        VARCHAR(255),
    auth               VARCHAR(255),
    user_first_name    VARCHAR(255),
    user_gender        CHAR,
    item_in_session    INT,
    user_last_name     VARCHAR(255),
    song_length        FLOAT,
    user_level         VARCHAR(255),
    location           VARCHAR(255),
    method             VARCHAR(255),
    page               VARCHAR(255),
    registration       FLOAT,
    session_id         INT,
    song_title         VARCHAR(255),
    status             INT,
    ts                 VARCHAR(50),
    user_agent         TEXT,
    user_id            INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs
(
    num_songs           INT,
    artist_id           VARCHAR(18),
    artist_latitude     FLOAT,
    artist_longitude    FLOAT,
    artist_location     VARCHAR(255),
    artist_name         VARCHAR(255),
    song_id             VARCHAR(18)     PRIMARY KEY,
    title               VARCHAR(255),
    duration            FLOAT,
    year                INT             SORTKEY
)
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
    songplay_id    INT IDENTITY(0, 1)    PRIMARY KEY    DISTKEY,
    start_time     TIMESTAMP,
    user_id        INT,
    level          VARCHAR(255),
    song_id        VARCHAR(18),
    artist_id      VARCHAR(18),
    session_id     INT,
    location       VARCHAR(255),
    user_agent     TEXT
)
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id       INT           PRIMARY KEY,
    first_name    VARCHAR(255),
    last_name     VARCHAR(255),
    gender        CHAR,
    level         VARCHAR(255)
) DISTSTYLE all
""")

song_table_create = ("""
CREATE TABLE songs
(
    song_id      VARCHAR(18)    PRIMARY KEY,
    title        VARCHAR(255),
    artist_id    VARCHAR(18)    DISTKEY,
    year         INT             SORTKEY,
    duration     FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists
(
    artist_id    VARCHAR(18)    PRIMARY KEY    DISTKEY,
    name         VARCHAR(255),
    location     VARCHAR(255),
    latitude     FLOAT,
    longitude    FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time
(
    start_time    TIMESTAMP    PRIMARY KEY    DISTKEY    SORTKEY,
    hour          INT,
    day           INT,
    week          INT,
    month         INT,
    year          INT,
    weekday       INT
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {table} from {bucket}
credentials 'aws_iam_role={role}'
JSON {path} region '{r}'
""").format(
    table='staging_events', 
    bucket=LOG_DATA,
    role=IAM_ROLE,
    path=LOG_PATH,
    r=REGION   
)

staging_songs_copy = ("""
COPY {table} from {bucket}
credentials 'aws_iam_role={role}'
JSON 'auto' region '{r}'
""").format(
    table='staging_songs', 
    bucket=SONG_DATA,
    role=IAM_ROLE,
    r=REGION
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + e.ts/1000 * interval '1 second' AS start_time,
       e.user_id,
       e.user_level,
       s.song_id,
       s.artist_id,
       e.session_id,
       e.location,
       e.user_agent
FROM staging_events e
LEFT JOIN staging_songs s
ON e.song_title = s.title
AND e.artist_name = s.artist_name
AND ABS(e.song_length - s.duration) < 2
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT(user_id),
       user_first_name,
       user_last_name,
       user_gender,
       user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, 
       title, 
       artist_id, 
       year, 
       duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, 
       artist_name      AS name,
       artist_location  AS location,
       artist_latitude  AS latitude,
       artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
       EXTRACT(hour FROM start_time),
       EXTRACT(day FROM start_time),
       EXTRACT(week FROM start_time),
       EXTRACT(month FROM start_time),
       EXTRACT(year FROM start_time),
       EXTRACT(weekday FROM start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
