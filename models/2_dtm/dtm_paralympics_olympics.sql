{%- set P_TABLE_1= 'stg_paralympics__summer' -%}
{%- set P_TABLE_2= 'stg_paralympics__winter' -%}
{%- set P_TABLE_3= 'stg_olympics__summer' -%}
{%- set P_TABLE_4= 'stg_olympics__winter' -%}

WITH all_paralympics AS (
    SELECT
        "summer" as lb_saison,
        "paralympique" as lb_compet,
        dt_annee,
        nb_m_or,
        nb_m_silver,
        nb_m_bronze,
        cd_pays,
        lb_pays,
        nb_total_medailles
    FROM {{ ref(P_TABLE_1) }}
    UNION ALL 
    SELECT
        "winter" as lb_saison,
        "paralympique" as lb_compet,
        dt_annee,
        nb_m_or,
        nb_m_silver,
        nb_m_bronze,
        cd_pays,
        lb_pays,
        nb_total_medailles
    FROM {{ ref(P_TABLE_2) }}
),
all_olympics AS (
    SELECT
        "summer" as lb_saison,
        "olympique" as lb_compet,
        dt_annee,
        cd_pays,
        lb_medaille
    FROM {{ ref(P_TABLE_3) }}
    UNION ALL 
    SELECT
        "winter" as lb_saison,
        "olympique" as lb_compet,
        dt_annee,
        cd_pays,
        lb_medaille
    FROM {{ ref(P_TABLE_4) }}
),
paralympics_count AS (
    SELECT
        lb_saison,
        all_paralympics.dt_annee,
        all_paralympics.nb_m_or,
        all_paralympics.nb_m_bronze,
        all_paralympics.nb_m_silver,
        lb_compet,
        cd_pays,
        lb_pays,
        SUM(nb_total_medailles) AS total_medailles_paralympics
    FROM all_paralympics
    GROUP BY cd_pays, lb_pays, 
        all_paralympics.dt_annee,
        all_paralympics.nb_m_or,
        all_paralympics.nb_m_bronze,
        all_paralympics.nb_m_silver,
        lb_saison, lb_compet
),
olympics_count AS (
    SELECT
        dt_annee,
        lb_saison,
        cd_pays,
        lb_compet,
        lb_medaille,
        COUNT(lb_medaille) AS total_medailles_olympics
    FROM all_olympics
    GROUP BY cd_pays, dt_annee, lb_saison, lb_compet, lb_medaille
)
SELECT 
    paralympics_count.lb_saison,
    CAST(paralympics_count.dt_annee as integer) as dt_annee,
    paralympics_count.cd_pays,
    paralympics_count.lb_compet,
    paralympics_count.nb_m_or,
    paralympics_count.nb_m_silver,
    paralympics_count.nb_m_bronze
    FROM paralympics_count
UNION ALL
SELECT * FROM (SELECT
lb_saison,CAST(dt_annee as integer) as dt_annee, cd_pays, lb_compet,lb_medaille, total_medailles_olympics

FROM olympics_count)
PIVOT(sum(total_medailles_olympics) FOR lb_medaille IN ('Gold','Silver','Bronze'))

-- ORDER BY paralympics_count.lb_pays
