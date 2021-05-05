CREATE TABLE IF NOT EXISTS public.staging_darties (
	Villes varchar(256),
	annee int4,
	Enseignes varchar(256),
	Publicité decimal(16,8),
	REGION varchar(256),
	Emplacemen varchar(256),
	Nb_Caisses int4,
	Population int4,
	Taux_Ouvri decimal(16,8),
	Taux_Cadre decimal(16,8),
	Taux_Inact decimal(16,8),
	Moins_25an decimal(16,8),
	Les_25_35a decimal(16,8),
	Plus_35ans decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_V_Fours (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_V_Hifi (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_V_Magneto (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_CA_Fours (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_CA_Hifi (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_CA_Magneto (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_MB_Fours (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_MB_Hifi (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_MB_Magneto (
	Villes varchar(256),
	annee int4,
	O_Janvier decimal(16,8),
	R_Janvier decimal(16,8),
	O_Fevrier decimal(16,8),
	R_Fevrier decimal(16,8),
	O_Mars decimal(16,8),
	R_Mars decimal(16,8),
	O_Avril decimal(16,8),
	R_Avril decimal(16,8),
	O_Mai decimal(16,8),
	R_Mai decimal(16,8),
	O_Juin decimal(16,8),
	R_Juin decimal(16,8),
	O_Juillet decimal(16,8),
	R_Juillet decimal(16,8),
	O_Aout decimal(16,8),
	R_Aout decimal(16,8),
	O_Septembre decimal(16,8),
	R_Septembre decimal(16,8),
	O_Octobre decimal(16,8),
	R_Octobre decimal(16,8),
	O_Novembre decimal(16,8),
	R_Novembre decimal(16,8),
	O_Decembre decimal(16,8),
	R_Decembre decimal(16,8)
);


CREATE TABLE IF NOT EXISTS public.staging_currency (
	country_names varchar(30),
	currency_names varchar(30),
	currency_values decimal(16,8),
	annee int4,
	mois int4
);


CREATE TABLE IF NOT EXISTS public.staging_cities (
	lib_ville varchar(30),
	lib_departement varchar(30),
	lib_reg_nouv varchar(30),
	lib_continent varchar(30),
	lib_pays varchar(30)
);


CREATE TABLE IF NOT EXISTS public.staging_mapping (
	departements varchar(30),
	regions varchar(30)
);


CREATE TABLE IF NOT EXISTS public.staging_profil (
	id_profil int4 NOT NULL,
	lib_profil varchar(20),
	type_zone varchar(20),
	id_zone int4,
	annee int4,
	CONSTRAINT profil_pkey PRIMARY KEY (id_profil)
);


CREATE TABLE IF NOT EXISTS public.staging_utilisateur (
	reg_com varchar(30),
	villes varchar(30),
	nom varchar(15),
	prenom varchar(15),
	"login" varchar(15),
	pwd varchar(25),
	mail varchar(30),
	id_profil int4 NOT NULL,
	annee int4
);


CREATE TABLE IF NOT EXISTS public.staging_enseigne (
	id_enseigne int4 NOT NULL,
	lib_enseigne varchar(30),
	CONSTRAINT enseigne_pkey PRIMARY KEY (id_enseigne)
);


CREATE TABLE IF NOT EXISTS public.staging_magasin (
	lib_magasin varchar(30),
	id_enseigne int4,
	annee int4,
	villes varchar(30)
);




-- Dimension and fact tables

CREATE TABLE IF NOT EXISTS public.sales (
	id_ville int4 NOT NULL,
	id_temps int4 NOT NULL,
	id_famille_produit int4 NOT NULL,
	id_magasin int4 NOT NULL,
	vente_objectif int4,
	vente_reel int4,
	CA_reel int4,
	CA_objectif int4,
	marge_reel decimal(16,8),
	marge_objectif decimal(16,8),
	CONSTRAINT sales_pkey PRIMARY KEY (id_ville, id_temps, id_famille_produit, id_magasin)
);


CREATE TABLE IF NOT EXISTS public.ville (
	id_ville bigint identity(1, 1),
	lib_ville varchar(30),
	lib_continent varchar(30),
	lib_pays varchar(30),
	lib_departement varchar(30),
	lib_reg_anc varchar(30),
	lib_reg_nouv varchar(30),
	--lib_reg_com varchar(30),
	CONSTRAINT ville_pkey PRIMARY KEY (id_ville)
);



CREATE TABLE IF NOT EXISTS public.famille_produit (
	id_famille_produit int4 NOT NULL,
	lib_famille_produit varchar(30),
	CONSTRAINT famille_produit_pkey PRIMARY KEY (id_famille_produit)
);



CREATE TABLE IF NOT EXISTS public.temps (
	id_temps bigint identity(1, 1),
	annee int4,
	semestre int4,
	trimestre int4,
	mois int4,
	lib_mois varchar(9),
	CONSTRAINT temps_pkey PRIMARY KEY (id_temps)
);



CREATE TABLE IF NOT EXISTS public.parametre (
	cpt_mois int4,
	cpt_erreur int4
);



CREATE TABLE IF NOT EXISTS public.cours (
	id_devise int4 NOT NULL,
	mois varchar(2),
	annee int4,
	cours decimal(16,8),
	CONSTRAINT cours_pkey PRIMARY KEY (id_devise)
);



CREATE TABLE IF NOT EXISTS public.devise (
	id_devise bigint identity(1, 1),
	lib_devise varchar(20),
	lib_pays varchar(30),
	CONSTRAINT devise_pkey PRIMARY KEY (id_devise)
);



CREATE TABLE IF NOT EXISTS public.utilisateur (
	id_utilisateur int4 NOT NULL,
	nom varchar(15),
	prenom varchar(15),
	_login varchar(15),
	mdp varchar(25),
	mail varchar(30),
	id_profil int4 NOT NULL,
	CONSTRAINT utilisateur_pkey PRIMARY KEY (id_utilisateur)
);



