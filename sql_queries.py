import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist            varchar,                     
        auth              varchar,
        firstName         varchar,           
        gender            char(1),
        itemInSession     int,
        lastName          varchar,
        length            real,
        level             varchar,
        location          varchar,            
        method            varchar,
        page              varchar,
        registration      real,
        sessionid         int,
        song              varchar, 
        status            int,
        ts                bigint,
        userAgent         varchar,
        userId            varchar
);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        artist_id            varchar,
        artist_location      varchar,
        artist_latitude      real,
        artist_longitude     real,
        artist_name          varchar,
        duration             real,
        num_songs            int,
        song_id              varchar,
        title                varchar,
        year                 int
);
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id      int            IDENTITY(0,1)      PRIMARY KEY,
        start_time       timestamp      NOT NULL, 
        user_id          varchar        NOT NULL,
        level            varchar, 
        song_id          varchar,
        artist_id        varchar,
        session_id       int            NOT NULL,
        location         varchar,
        user_agent       varchar
);
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id        varchar            PRIMARY KEY,
        first_name     varchar,
        last_name      varchar,
        gender         char(1),
        level          varchar
);
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id        varchar        NOT NULL     PRIMARY KEY,
        title          varchar,
        artist_id      varchar        NOT NULL,
        year           varchar,
        duration       real
);
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id       varchar        NOT NULL     PRIMARY KEY,
        name            varchar        NOT NULL,
        location        varchar,
        latitude        real,
        longitude       real
);
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time    timestamp    NOT NULL    PRIMARY KEY,
        hour          smallint     NOT NULL,
        day           smallint     NOT NULL,
        week          smallint     NOT NULL,
        month         smallint     NOT NULL,
        year          smallint     NOT NULL,
        weekday       smallint     NOT NULL
);
""")

# STAGING TABLES

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
    JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN']) 

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' AS start_time,
        e.userId AS user_id,
        s.song_id AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location AS location,
        e.userAgent AS user_agent
    FROM staging_events e 
    JOIN staging_songs s 
    ON (e.song = s.title);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name as name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day, 
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(dow FROM start_time) AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
