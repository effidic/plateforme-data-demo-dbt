with source AS (
      SELECT _FILE_NAME , *  from `plateforme-data-demo`.`0_raw_olympics`.`summer`
),
renamed AS (
    SELECT 
        _FILE_NAME AS lb_fichier_source,
        Annee as dt_annee,
        Ville as lb_ville,
        Sport as lb_sport,
        Discipline as lb_discipline,
        Athlete as lb_athlete,
        Pays as cd_pays,
        Genre as lb_genre,
        Evenement as lb_evenement,
        Medaille as lb_medaille
    FROM source
)
SELECT * from renamed