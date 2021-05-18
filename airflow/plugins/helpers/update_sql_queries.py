class UpdateSqlQueries:


    update_ca_fours_query = ("""
        (lib_magasin, id_enseigne) 
        SELECT  
            staging_magasin.lib_magasin,
            staging_magasin.id_enseigne
        
        FROM staging_magasin;
    """)


    update_v_fours_table = ("""
        (lib_magasin, id_enseigne) 
        SELECT  
            staging_magasin.lib_magasin,
            staging_magasin.id_enseigne
        
        FROM staging_magasin;
    """)


    



    







    sales_table_insert = ("""
        (id_ville, id_temps, id_famille_produit, id_magasin, vente_objectif, vente_reel, CA_reel, CA_objectif, marge_reel, marge_objectif) 
        SELECT  
            ville.id_ville,
            temps.id_temps,
            famille_produit.id_famille_produit,
            magasin.id_magasin,
            staging_v_{0}.o_{1},
            staging_v_{0}.r_{1},
            staging_ca_{0}.o_{1},
            staging_ca_{0}.r_{1},
            staging_mb_{0}.o_{1},
            staging_mb_{0}.r_{1}
            
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
            AND temps.lib_mois = '{1}'
        
        JOIN famille_produit 
            ON famille_produit.lib_famille_produit = '{0}'
        
        JOIN magasin 
            ON magasin.lib_magasin = ville.lib_ville
    """) 



    ville_table_insert = ("""
        (lib_ville, lib_continent, lib_pays, lib_departement, lib_reg_anc, lib_reg_nouv) 
        SELECT  
            staging_cities.lib_ville,
            staging_cities.lib_continent,
            staging_cities.lib_pays,
            staging_cities.lib_departement,
            staging_mapping.regions,
            staging_cities.lib_reg_nouv
        
        FROM staging_cities 
        
        JOIN staging_mapping
            ON staging_cities.lib_departement = staging_mapping.departements;
    """)


    devise_table_insert = ("""
        (lib_devise, lib_pays) 
        SELECT  
            DISTINCT staging_currency.currency_names,
            staging_currency.country_names
        
        FROM staging_currency;
    """)


    cours_table_insert = ("""
        (id_devise, mois, annee, cours) 
        SELECT  
            devise.id_devise,
            staging_currency.mois,
            staging_currency.annee,
            staging_currency.currency_values
        
        FROM staging_currency 
        
        JOIN devise
            ON devise.lib_devise = staging_currency.currency_names;
    """)


    magasin_table_insert = ("""
        (lib_magasin, id_enseigne) 
        SELECT  
            staging_magasin.lib_magasin,
            staging_magasin.id_enseigne
        
        FROM staging_magasin;
    """)


    
	
    
    
    
    
    