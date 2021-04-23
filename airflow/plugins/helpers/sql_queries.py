class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
            FROM (
                SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong'
            ) events
            LEFT JOIN staging_songs songs
                ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)








    sales_table_insert = ("""
        SELECT distinct 
            staging_v_Fours.o_Janvier AS v_fours_janvier_prev, 
            staging_v_Fours.r_Janvier AS v_fours_janvier_reel,
            staging_ca_Fours.o_Janvier AS ca_fours_janvier_prev, 
            staging_ca_Fours.r_Janvier AS ca_fours_janvier_reel, 
            staging_mb_Fours.o_Janvier AS mb_fours_janvier_prev, 
            staging_mb_Fours.r_Janvier AS mb_fours_janvier_reel, 
        FROM staging_v_Fours 
        JOIN staging_mb_fours ON staging_v_Fours.villes = staging_mb_fours.villes 
            AND staging_v_fours.annee = staging_mb_fours.annee
        JOIN staging_ca_fours ON staging_v_fours.villes = staging_ca_fours.villes 
            AND staging_v_fours.annee = staging_ca_fours.annee
    """)


	
	



    turnover_oven_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    

    

    
    
    
    
    
    
    
    