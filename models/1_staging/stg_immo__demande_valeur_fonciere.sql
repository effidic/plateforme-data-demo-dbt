with source AS (
      SELECT _FILE_NAME , *  from {{ source('immobilier', 'demande_valeur_fonciere') }}
),
renamed AS (
    SELECT 
        _FILE_NAME AS lb_fichier_source,
        id_parcelle AS id_parcelle,
        id_mutation AS cd_id_mutation,
        safe_cast( date_mutation AS date format 'YYYY-MM-DD' ) AS dt_mutation,
        numero_disposition AS nb_disposition,
        nature_mutation AS lb_nature_mutation,
        safe_cast(valeur_fonciere AS decimal) AS mt_valeur_fonciere,
        adresse_numero AS cd_adresse_numero,
        adresse_suffixe AS lb_adresse_suffixe,
        adresse_nom_voie AS lb_adresse_nom_voie,
        adresse_code_voie AS cd_adresse_voie,
        code_postal AS cd_postal,
        code_commune AS cd_commune,
        nom_commune AS lb_commune,
        code_departement AS cd_departement,
        ancien_code_commune AS cd_ancien_commune,
        ancien_nom_commune AS lb_ancien_commune,
        ancien_id_parcelle AS cd_ancien_parcelle,
        numero_volume AS cd_volume,
        lot1_numero AS cd_lot1,
        safe_cast(lot1_surface_carrez AS decimal) AS nb_lot1_surface_carrez,
        lot2_numero AS cd_lot2,
        safe_cast(lot2_surface_carrez AS decimal) AS nb_lot2_surface_carrez,
        lot3_numero AS cd_lot3,
        safe_cast(lot3_surface_carrez AS decimal) AS nb_lot3_surface_carrez,
        lot4_numero AS cd_lot4,
        safe_cast(lot4_surface_carrez AS decimal) AS nb_lot4_surface_carrez,
        lot5_numero AS cd_lot5,
        safe_cast(lot5_surface_carrez AS decimal) AS nb_lot5_surface_carrez,
        safe_cast(nombre_lots AS int) AS lb_nombre_lots,
        code_type_local AS cd_type_local,
        type_local AS lb_type_local,
        code_nature_culture AS cd_nature_culture,
        nature_culture AS lb_nature_culture,
        code_nature_culture_speciale AS cd_nature_culture_speciale,
        nature_culture_speciale AS lb_nature_culture_speciale,
        safe_cast(surface_reelle_bati AS decimal) AS nb_surface_reelle_bati,
        safe_cast(nombre_pieces_principales AS int) AS lb_nombre_pieces_principales,
        safe_cast(surface_terrain AS decimal) AS nb_surface_terrain,
        safe_cast(longitude AS decimal) AS cd_longitude,
        safe_cast(latitude AS decimal) AS cd_latitude,
        null as test,
        CASE 
            WHEN mod(safe_cast(right(id_mutation, 6) AS int), 7) = 1 THEN 'simple' 
            ELSE 'exclusif'
        END as cd_type_mandat
    FROM source
)
SELECT * from renamed
