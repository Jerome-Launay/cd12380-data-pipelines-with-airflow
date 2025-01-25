class SqlQueries:

    songplay_table_insert = ("""
        CREATE temp table stage (like songplays); 

        INSERT INTO stage 
        SELECT DISTINCT
            md5(se.sessionid || se.ts) AS songplay_id,
            se.ts AS start_time,
            se.userId AS user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId AS session_id,
            se.location,
            se.userAgent AS user_agent
        FROM staging_events se
        JOIN staging_songs ss ON ss.title = se.song AND ss.artist_name = se.artist
        WHERE se.page = 'NextSong';

        MERGE INTO songplays
        USING stage on (stage.songplay_id = songplays.songplay_id)
        WHEN MATCHED THEN 
        UPDATE SET songplay_id = stage.songplay_id , start_time= stage.start_time, user_id = stage.user_id,
                level = stage.level, song_id = stage.song_id, artist_id = stage.artist_id, session_id = stage.session_id,
                location = stage.location, user_agent = stage.user_agent
        WHEN NOT MATCHED THEN
        INSERT (songplay_id , start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        VALUES (stage.songplay_id , stage.start_time, stage.user_id, stage.level, stage.song_id, stage.artist_id, stage.session_id, stage.location, stage.user_agent);

        DROP TABLE stage;
    """)


    user_table_insert = ("""
        INSERT INTO users
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO songs
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        INSERT INTO artists
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        INSERT INTO times
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events
        (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        CHAR(1),
        itemInSession INTEGER,
        lastName      VARCHAR,
        length        FLOAT,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  FLOAT,
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INTEGER
        );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs
        (
        num_songs        INTEGER,
        artist_id        VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         FLOAT,
        year             INTEGER
        );
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs
        (
        song_id   VARCHAR PRIMARY KEY SORTKEY,
        title     VARCHAR,
        artist_id VARCHAR NOT NULL,
        year      INT,
        duration  FLOAT
        );
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users
        (
        user_id    INTEGER PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     CHAR(1),
        level      VARCHAR
        );
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists
        (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name      VARCHAR,
        location  VARCHAR,
        latitude  FLOAT,
        longitude FLOAT
        );
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS times
        (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour       INTEGER NOT NULL,
        day        INTEGER NOT NULL,
        week       INTEGER NOT NULL,
        month      INTEGER NOT NULL,
        year       INTEGER NOT NULL,
        weekday    INTEGER
        );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays
        (
        songplay_id TEXT PRIMARY KEY SORTKEY UNIQUE,
        start_time  TIMESTAMP,
        user_id     INTEGER NOT NULL,
        level       VARCHAR,
        song_id     VARCHAR NOT NULL,
        artist_id   VARCHAR NOT NULL,
        session_id  INTEGER NOT NULL,
        location    VARCHAR,
        user_agent  VARCHAR
        );
    """)