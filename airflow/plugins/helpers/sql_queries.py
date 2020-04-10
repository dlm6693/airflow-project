class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.ts) songplay_id,
                TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second' AS start_time,
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM staging_events events
            JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE events.page = 'NextSong'
    """)

    user_table_insert = ("""
        SELECT DISTINCT
            userid AS user_id, 
            firstname AS first_name,
            lastname AS last_name, 
            gender,
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT DISTINCT
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT DISTINCT
            artist_id, 
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude, 
            artist_longitude AS longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT 
            start_time, 
            EXTRACT(hour FROM start_time) AS hour, 
            EXTRACT(day FROM start_time) AS day, 
            EXTRACT(week FROM start_time) AS week, 
            EXTRACT(month FROM start_time), 
            EXTRACT(year FROM start_time), 
            EXTRACT(dayofweek FROM start_time)
        FROM songplays
    """)