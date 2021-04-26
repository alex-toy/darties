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




    # item = v_fours ; mois = janvier
    # 0 = v_fours ; 1 = janvier
    sales_table_insert = ("""
        SELECT  
            staging_v_{0}.o_janvier AS v_{0}_janvier_prev, 
            staging_v_{0}.r_janvier AS v_{0}_janvier_reel,
            staging_ca_{0}.o_janvier AS ca_{0}_janvier_prev, 
            staging_ca_{0}.r_janvier AS ca_{0}_janvier_reel, 
            staging_mb_{0}.o_janvier AS mb_{0}_janvier_prev, 
            staging_mb_{0}.r_janvier AS mb_{0}_janvier_reel,
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
        JOIN famille_produit ON ???
        JOIN magasin ON ???
    """) # to be finished!!!

    sales_table_insert.format('fours', 'janvier')
    sales_table_insert.format('hifi', 'fevrier')
    sales_table_insert.format('magneto', 'mars')


	

    time_table_insert = ("""
              (annee, semestre, trimestre, mois, lib_mois)
        VALUES(2020,         1,         1,    1, 'janvier'),
        VALUES(annee, semestre, trimestre, mois, lib_mois),
        VALUES(annee, semestre, trimestre, mois, lib_mois);
    """)
    
    




    
    
    
    
    
    
    
    