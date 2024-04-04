with source as (
      select _FILE_NAME , *  from `plateforme-data-demo`.`0_raw_pdl`.`base_sirene_v3`
),
renamed as (
    SELECT 
        _FILE_NAME as lb_fichier_source,
        SIREN as cd_siren,
        NIC as cd_nic,
        SIRET as cd_siret,
        INSEE_COM as cd_insee,
        Denomination_usuelle_de_l_etablissement as lb_denomination,
        concat(Enseigne_de_l_etablissement_1, ',', Enseigne_de_l_etablissement_2, ',', Enseigne_de_l_etablissement_3)  as lb_enseignes,
        safe_cast(Date_de_creation_de_l_etablissement as date format 'YYYY-MM-DD' ) as dt_creation,
        safe_cast(Date_de_la_derniere_mise_a_jour_de_l_etablissement as date format 'YYYY-MM-DD' ) as dt_update,
        Categorie_de_l_entreprise as lb_categorie,
        replace(Annee_de_la_categorie_de_l_entreprise, '.0', '') as cd_annee_categorisation,
        case 
            when Caractere_employeur_de_l_etablissement = "Oui" then True
            when Caractere_employeur_de_l_etablissement = "Non" then False
            else False
        end as bl_est_employeur,
        Tranche_de_l_effectif_de_l_etablissement as lb_tranche_effectif,
        replace(Annee_de_la_tranche_d_effectif_de_l_etablissement, '.0', '') as cd_annee_tranche_effectif,
        case 
            when Etablissement_siege = "Oui" then True 
            when Etablissement_siege = "Non" then False
            else False 
        end as bl_siege,
        Activite_principale_de_l_etablissement as cd_activite,
        Section_de_l_etablissement as lb_section,
        Sous_section_de_l_etablissement as lb_sous_section,
        Code_postal_de_l_etablissement as cd_postal,
        Commune_de_l_etablissement as lb_commune,
        Premiere_ligne_de_l_adressage as lb_proprietaire_adresse,
        Adresse_de_l_etablissement as lb_adresse,
        Territoire as lb_territoire,
        split(Geolocalisation_de_l_etablissement, ', ')[0] as lb_geolocalisation_x,
        split(Geolocalisation_de_l_etablissement, ', ')[1] as lb_geolocalisation_y,
        --## Sigle_de_l_unite_legale as cd_unite_legale,
        --## Denomination_de_l_unite_legale as lb_unite_legale,
        --## case 
        --##     when Caractere_employeur_de_l_unite_legale == "Oui" then True
        --##     when Caractere_employeur_de_l_unite_legale == "Non" then False
        --##     else False 
        --## end as bl,
        --## Tranche_de_l_effectif_de_l_unite_legale as lb_tranche_effectif_unite_legale,
        --## concat(Denomination_usuelle_de_l_unite_legale_1, ',' ,Denomination_usuelle_de_l_unite_legale_2, ',', Denomination_usuelle_de_l_unite_legale_3, ',' as lb_denominations_unite_legale,
        --## Categorie_juridique_de_l_unite_legale as cd_categorie_juridique_ul,
        --## Libelle_categorie_juridique as lb_categorie_juridique_ul,
        --## Civilite_de_la_personne_physique as lb_civilite,
        --## Prenom_usuel_de_la_personne_physique as lb_prenom,
        --## Pseudonyme_de_la_personne_physique as lb_pseudo,
        --## Nom_de_la_personne_physique as lb_nom,
        --## Nom_d_usage_de_la_personne_physique as lb_nom_usage,
        --## case 
        --##     when Economie_sociale_et_solidaire_unite_legale == "Oui" then True
        --##     when Economie_sociale_et_solidaire_unite_legale == "Non" then False
        --##     else False
        --## end as bl_economie_sociale_solidaire_ul,
        PLUI as cd_plui,
    FROM source
)

select * from renamed