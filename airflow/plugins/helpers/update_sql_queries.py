class UpdateSqlQueries:


    update_query = ("""
        UPDATE sales
        SET {0} = (
            SELECT {1}.r_mois
            
            FROM {1}
            JOIN ville ON 
                {1}.villes = ville.lib_ville
            JOIN temps ON 
                {1}.annee = temps.annee AND
                {1}.lib_mois = temps.lib_mois
            JOIN famille_produit ON 
                famille_produit.id_famille_produit = (
                    SELECT sales.id_famille_produit
                    FROM sales
                    JOIN famille_produit ON sales.id_famille_produit = famille_produit.id_famille_produit
                    WHERE famille_produit.lib_famille_produit = '{2}'
                )
                
            WHERE 
                ville.id_ville = sales.id_ville AND
                {1}.villes = ville.lib_ville AND
                
                temps.id_temps = sales.id_temps AND
                {1}.lib_mois = temps.lib_mois AND
                {1}.annee = temps.annee AND 

                famille_produit.lib_famille_produit = '{2}'
        );
    """)

    


    update_ca_fours_query = ("""
        UPDATE sales
        SET CA_reel = (
            SELECT staging_monthly_ca_fours.r_mois
            
            FROM staging_monthly_ca_fours
            JOIN ville ON 
                staging_monthly_ca_fours.villes = ville.lib_ville
            JOIN temps ON 
                staging_monthly_ca_fours.annee = temps.annee AND
                staging_monthly_ca_fours.lib_mois = temps.lib_mois
            
            WHERE 
                staging_monthly_ca_fours.villes = sales.id_ville AND
                staging_monthly_ca_fours.annee = sales.id_temps AND
                staging_monthly_ca_fours.lib_mois = sales.id_temps
        );
    """)









    








