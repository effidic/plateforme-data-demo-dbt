with source AS (
      SELECT /*_FILE_NAME ,*/ *  from {{ source('climate', 'climate') }}
),
renamed AS (
    SELECT 
        /*_FILE_NAME AS lb_fichier_source,*/
        ID as cd_id,
        Categorie as lb_cat,
        Total_kg as nb_kg,
        Agriculture as nb_agri,
        Production_biocarburant as nb_biocarb,
        Transformation_alimentaire as nb_transfo,
        Emballage as nb_emballage,
        Transport as nb_transport,
        Vente as nb_vente
    FROM source
)
SELECT * from renamed