{%- set P_TABLE_1= 'stg_paralympics__summer'-%}
{%- set P_TABLE_2= 'stg_paralympics__winter'-%}
{%- set P_TABLE_3= 'stg_olympics__summer'-%}
{%- set P_TABLE_4= 'stg_olympics__winter'-%}

with all_paralympics AS (
    SELECT
        paralympics_summer.*,
        
    FROM {{ ref(P_TABLE_1) }} paralympics_summer
    UNION ALL 
    SELECT
        paralympics_winter.*,
    FROM {{ ref(P_TABLE_2) }}AS paralympics_winter
),
all_olympics AS (
    SELECT
        olympics_summer.*,
        
    FROM {{ ref(P_TABLE_3) }} olympics_summer
    UNION ALL 
    SELECT
        olympics_winter.*,
    FROM {{ ref(P_TABLE_4) }}AS olympics_winter
)
SELECT 
    all_paralympics.lb_pays,
    all_paralympics.nb_total_medailles,
    all_olympics.lb_medaille
FROM all_paralympics
INNER JOIN all_olympics ON all_olympics.cd_pays = all_paralympics.cd_pays
ORDER BY all_paralympics.cd_pays
