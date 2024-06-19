with source AS (
      SELECT /* _FILE_NAME , */ *  from {{ source('olympics', 'olympics_recent') }}
),
renamed AS (
    SELECT 
        /* _FILE_NAME AS lb_fichier_source, */
        Year as dt_annee,
        Country as lb_pays,
        Gold as nb_m_or,
        Silver as nb_m_silver,
        Bronze as nb_m_bronze,
        Games_Type as lb_saison,
        Total as nb_total_p
    FROM source
)
SELECT * from renamed
