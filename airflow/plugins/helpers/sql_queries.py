class SqlQueries:

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
        JOIN ville 
            ON staging_v_{0}.villes = ville.lib_ville
        JOIN temps 
            ON staging_v_{0}.annee = temps.annee
            AND temps.mois = '{1}'
        JOIN famille_produit 
            ON famille_produit.lib_famille_produit = '{0}'
        JOIN magasin ON magasin.???
    """) # to be finished!!!

    sales_table_insert.format('fours', 'janvier')
    sales_table_insert.format('hifi', 'fevrier')
    sales_table_insert.format('magneto', 'mars')


	


    
    




    
    
    
    
    
    
    
    