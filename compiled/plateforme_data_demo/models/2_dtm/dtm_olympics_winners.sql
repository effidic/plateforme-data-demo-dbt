with all_olympics AS (
    SELECT
        olympics_summer.*,
        
    FROM `plateforme-data-demo`.`1_stage_dbt`.`stg_olympics__summer` olympics_summer
    UNION ALL 
    SELECT
        olympics_winter.*,
    FROM `plateforme-data-demo`.`1_stage_dbt`.`stg_olympics__winter`AS olympics_winter
)
SELECT 
    all_olympics.lb_fichier_source as lb_saison,
    all_olympics.dt_annee,
    all_olympics.lb_ville,
    all_olympics.lb_sport,
    all_olympics.lb_discipline,
    all_olympics.lb_athlete,
    all_olympics.cd_pays,
    olympics_dict.lb_pays,
    all_olympics.lb_genre,
    all_olympics.lb_evenement,
    all_olympics.lb_medaille,
    CASE 
        WHEN lb_medaille = 'Gold' THEN 5
        WHEN lb_medaille = 'Silver' THEN 3
        WHEN lb_medaille = 'Bronze' THEN 1
        ELSE 0
    END AS nb_score_medaille
FROM all_olympics
LEFT JOIN `plateforme-data-demo`.`1_stage_dbt`.`stg_olympics__dictionnaire` AS olympics_dict
ON olympics_dict.cd_pays = all_olympics.cd_pays