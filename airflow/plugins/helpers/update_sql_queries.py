class UpdateSqlQueries:


    update_query = ("""
        UPDATE sales
        SET sales.{} = (
            SELECT {}.r_mois
            
            FROM {}
            JOIN ville ON 
                {}.villes = ville.lib_ville
            JOIN temps ON 
                {}.annee = temps.annee AND
                {}.lib_mois = temps.lib_mois
            
            WHERE 
                {}.villes = sales.id_ville AND
                {}.annee = sales.id_temps AND
                {}.lib_mois = sales.id_temps AND
        );
    """)

    


    update_ca_fours_query = ("""
        UPDATE sales
        SET sales.CA_reel = (
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
                staging_monthly_ca_fours.lib_mois = sales.id_temps AND
        );
    """)









    








