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







    # item = v_fours ; mois = janvier
    # 0 = v_fours ; 1 = janvier
    sales_table_insert = ("""
        SELECT  
            staging_v_{0}.o_Janvier AS v_{0}_janvier_prev, 
            staging_v_{0}.r_Janvier AS v_{0}_janvier_reel,
            staging_ca_{0}.o_Janvier AS ca_{0}_janvier_prev, 
            staging_ca_{0}.r_Janvier AS ca_{0}_janvier_reel, 
            staging_mb_{0}.o_Janvier AS mb_{0}_janvier_prev, 
            staging_mb_{0}.r_Janvier AS mb_{0}_janvier_reel,
            ville.id_ville,
            temps.id_temps,
            famille_produit.id_famille_produit,
            magasin.id_magasin
        FROM staging_v_{0} 
        JOIN staging_mb_{0} 
            ON staging_v_{0}.villes = staging_mb_{0}.villes 
            AND staging_v_{0}.annee = staging_mb_{0}.annee
        JOIN staging_ca_{0} 
            ON staging_v_{0}.villes = staging_ca_{0}.villes 
            AND staging_v_{0}.annee = staging_ca_{0}.annee
        JOIN ville ON staging_v_{0}.villes = ville.lib_ville
        JOIN temps 
            ON staging_v_{0}.annee = temps.annee
            AND temps.mois = '{1}'
        JOIN famille_produit
        JOIN magasin
    """) # to be finished!!!

    sales_table_insert.format('fours', 'janvier')
    sales_table_insert.format('hifi', 'fevrier')
    sales_table_insert.format('magneto', 'mars')


	
	



    turnover_oven_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)
    
    

    

    
    
    
    
    
    
    
    