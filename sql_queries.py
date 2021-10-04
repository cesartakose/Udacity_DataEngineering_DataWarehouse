import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events_table (artist text,
                                        auth varchar,
                                        firstName varchar,
                                        gender varchar,
                                        itemInSession int,
                                        lastName varchar,
                                        length float,
                                        level varchar,
                                        location text,
                                        method varchar,
                                        page varchar,
                                        registration varchar,
                                        sessionId int,
                                        song text,
                                        status int,
                                        ts timestamp,
                                        userAgent varchar,
                                        userId int
                                        );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table (num_songs int,
                                        artist_id varchar,
                                        artist_latitude float,
                                        artist_longitude float,
                                        artist_location text,
                                        artist_name text,
                                        song_id varchar,
                                        title text,
                                        duration float,
                                        year int    
                                        );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY,
                                start_time timestamp REFERENCES time(start_time),
                                user_id varchar REFERENCES users(user_id),
                                level varchar NOT NULL,
                                song_id varchar REFERENCES songs(song_id),
                                artist_id varchar REFERENCES artists(artist_id),
                                session_id int NOT NULL,
                                location text,
                                user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE users (user_id varchar PRIMARY KEY,
                            first_name varchar NOT NULL,
                            last_name varchar NOT NULL,
                            gender varchar,
                            level varchar NOT NULL)
""")

song_table_create = ("""
    CREATE TABLE songs (song_id varchar PRIMARY KEY,
                            title text NOT NULL,
                            artist_id varchar REFERENCES artists(artist_id),
                            year int,
                            duration float NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE artists (artist_id varchar PRIMARY KEY,
                                name text NOT NULL,
                                text varchar,
                                latitude float,
                                longitude float)
""")

time_table_create = ("""
    CREATE TABLE time (start_time timestamp PRIMARY KEY,
                            hour int,
                            day int,
                            week int,
                            month int,
                            year int,
                            weekday int)
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM {}
        iam_role {}
        region 'us-west-2'
        format as json {}
        timeformat 'epochmillisecs';
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs_table FROM {}
        iam_role {}
        region 'us-west-2'
        json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays
        (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
        )
        SELECT DISTINCT
            e.ts,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
        FROM staging_events_table e
        JOIN staging_songs_table s 
        ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
        WHERE e.page = 'NextSong'
        AND ts IS NOT NULL
""")

user_table_insert = ("""
    INSERT INTO users
        (
        user_id, 
        first_name,
        last_name, 
        gender,
        level
        )
        SELECT DISTINCT 
            userId, 
            firstName,
            lastName, 
            gender,
            level

        FROM staging_events_table e1

    WHERE e1.userId IS NOT NULL
        AND ts = (SELECT max(ts) FROM staging_events_table e2 WHERE e1.userId = e2.userId)
        AND e1.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs
        (
        song_id,
        title,
        artist_id,
        year,
        duration
        )
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs_table
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists
        (
        artist_id,
        name,
        location,
        latitude,
        longitude
        )
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs_table
        WHERE artist_id IS NOT NULL
""")


time_table_insert = ("""
    INSERT INTO time
        (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
        )
        SELECT DISTINCT
            ts,
            date_part(h,ts),
            date_part(d,ts),
            date_part(w,ts),
            date_part(mon,ts),
            date_part(y,ts),
            date_part(dow,ts)
        FROM staging_events_table
        WHERE ts IS NOT NULL
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
