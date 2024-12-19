with source AS (
      SELECT _FILE_NAME , *  from `plateforme-data-demo`.`0_raw_immo`.`mandats`
),
renamed AS (
    SELECT 
        _FILE_NAME AS lb_fichier_source,
        type AS cd_type_mandat,
        safe_cast(split(replace(replace(intervalle, '€', ''), ' ', ''), 'à')[0] as decimal) AS mt_borne_inf_interval,
        safe_cast(split(replace(replace(intervalle, '€', ''), ' ', ''), 'à')[1] as decimal) AS mt_borne_sup_interval,
        safe_cast(replace(frais_flat, '€', '') AS decimal) AS mt_frais_plat,
        safe_cast(replace(frais_pourcentage, '%', '') AS decimal) AS mt_frais_pourcentage,
        safe_cast(replace(stationnement, '€', '') AS decimal) AS mt_frais_stationnement,
        safe_cast(replace(complement, '€', '') AS decimal) AS mt_complement,
        'dummy' as dummy_column_polytech
    FROM source
)
SELECT * from renamed