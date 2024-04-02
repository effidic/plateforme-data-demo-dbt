{%- set P_TABLE_1= 'stg_olympics__summer'-%}
{%- set P_TABLE_2= 'stg_olympics__winter'-%}

with all_olympics AS (
    SELECT
        olympics_summer.*,
        
    FROM {{ ref(P_TABLE_1) }} olympics_summer
    UNION ALL 
    SELECT
        olympics_winter.*,
    FROM {{ ref(P_TABLE_1) }}AS olympics_winter
)
SELECT 
    REPLACE(ARRAY_REVERSE(SPLIT(all_olympics.lb_fichier_source, '/'))[SAFE_OFFSET(0)], '.csv', '') as lb_saison,
    all_olympics.dt_annee,
    all_olympics.lb_ville,
    all_olympics.lb_sport,
    all_olympics.lb_discipline,
    SPLIT(all_olympics.lb_athlete, ', ')[SAFE_OFFSET(0)] AS lb_athlete_name,
    SPLIT(all_olympics.lb_athlete, ', ')[SAFE_OFFSET(1)] AS lb_athlete_surname,
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
LEFT JOIN {{ ref('stg_olympics__dictionnaire') }} AS olympics_dict
ON olympics_dict.cd_pays = all_olympics.cd_pays