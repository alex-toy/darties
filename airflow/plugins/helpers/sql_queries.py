class SqlQueries:

    # item = v_fours ; mois = janvier
    # 0 = v_fours ; 1 = janvier
    sales_table_insert = ("""
        SELECT  
            ville.id_ville,
            temps.id_temps,
            famille_produit.id_famille_produit,
            magasin.id_magasin
            staging_v_{0}.o_janvier,
            staging_v_{0}.r_janvier,
            staging_ca_{0}.o_janvier,
            staging_ca_{0}.r_janvier,
            staging_mb_{0}.o_janvier,
            staging_mb_{0}.r_janvier,
            
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
        
        JOIN staging_magasin 
            ON staging_magasin.villes = ville.lib_ville
    """) 

    #sales_table_insert.format('fours', 'janvier')
    #sales_table_insert.format('hifi', 'fevrier')
    #sales_table_insert.format('magneto', 'mars')



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
        (lib_devise) 
        SELECT  
            DISTINCT staging_currency.currency_names
        
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


    
	
    
    
    
    
    
    