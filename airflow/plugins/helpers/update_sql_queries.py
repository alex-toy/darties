class UpdateSqlQueries:


    update_query = ("""
        UPDATE sales 
        SET {0} = r_mois
        
        FROM {1}
        JOIN ville ON 
            {1}.villes = ville.lib_ville
        JOIN temps ON 
            {1}.annee = temps.annee AND
            {1}.lib_mois = temps.lib_mois
        JOIN famille_produit ON 
            famille_produit.lib_famille_produit = '{2}'
        
        WHERE 
            sales.id_ville = ville.id_ville AND
            sales.id_temps = temps.id_temps AND
            sales.id_famille_produit = famille_produit.id_famille_produit;
    """)


    test = ("""
        UPDATE target
        
        SET 
            target.num_hit_ratio = updates.num_hit_ratio,
            target.plan_id = updates.plan_id
        
        FROM updates 
        
        WHERE 
            target.name = updates.name AND 
            target.title = updates.title AND 
            target.age = updates.age
    """)


    


    update_queryold = ("""
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
                famille_produit.lib_famille_produit = '{2}'
                
            WHERE 
                sales.id_ville = ville.id_ville AND
                {1}.villes = ville.lib_ville AND
                
                sales.id_temps = temps.id_temps AND
                {1}.lib_mois = temps.lib_mois AND
                {1}.annee = temps.annee AND 

                sales.id_famille_produit = {3}
        )
        
        WHERE 
            sales.id_famille_produit = {3};
    """)


    


    
