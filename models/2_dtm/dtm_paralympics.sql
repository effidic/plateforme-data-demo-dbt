{%- set P_TABLE_1= 'stg_paralympics__summer' -%}
{%- set P_TABLE_2= 'stg_paralympics__winter' -%}

WITH all_paralympics AS (
    SELECT
        "summer" as lb_saison,
        "paralympique" as lb_compet,
        dt_annee,
        nb_m_or,
        nb_m_silver,
        nb_m_bronze,
        paralympics_summer.cd_pays,
        paralympics_summer.lb_pays,
        olympics_dict.nb_population,
        nb_total_medailles
    FROM {{ ref(P_TABLE_1) }} AS paralympics_summer
    LEFT JOIN {{ ref('stg_olympics__dictionnaire') }} AS olympics_dict
    ON olympics_dict.cd_pays = paralympics_summer.cd_pays
    UNION ALL 
    SELECT
        "winter" as lb_saison,
        "paralympique" as lb_compet,
        dt_annee,
        nb_m_or,
        nb_m_silver,
        nb_m_bronze,
        paralympics_winter.cd_pays,
        paralympics_winter.lb_pays,
        olympics_dict.nb_population,
        nb_total_medailles
    FROM {{ ref(P_TABLE_2) }} AS paralympics_winter
    LEFT JOIN {{ ref('stg_olympics__dictionnaire') }} AS olympics_dict
    ON olympics_dict.cd_pays = paralympics_winter.cd_pays
)

SELECT * FROM all_paralympics