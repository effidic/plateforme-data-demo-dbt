with source AS (
      SELECT /* _FILE_NAME , */ *  from {{ source('paralympics', 'summer_para') }}
),
renamed AS (
    SELECT 
        /* _FILE_NAME AS lb_fichier_source, */
        Year as dt_annee,
        Host_country as lb_pays_hote,
        Country as lb_pays,
        Country_code as cd_pays,
        Gold as nb_m_or,
        Silver as nb_m_silver,
        Bronze as nb_m_bronze,
        M_total as nb_total_medailles,
        Men as nb_m_homme,
        Women as nb_m_femme,
        P_total as nb_total_p
    FROM source
)
SELECT * from renamed
