with source AS (
      SELECT _FILE_NAME , *  from {{ source('olympics', 'dictionnaire') }}
),
renamed AS (
    SELECT 
        _FILE_NAME AS lb_fichier_source,
        Pays as lb_pays,
        Code as cd_pays,
        safe_cast(Population as int) as nb_population,
        safe_cast(Pib_par_habitant as decimal) as mt_pib_par_habitant
    FROM source
)
SELECT * from renamed
