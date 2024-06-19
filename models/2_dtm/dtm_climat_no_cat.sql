{%- set P_TABLE_1= 'stg_climat' -%}
{%- set P_TABLE_2= 'dtm_climat_categories' -%}

WITH climat_no_cat AS (
    SELECT
        c.*,
        d.categorie_id
    FROM {{ ref(P_TABLE_1) }} c
    LEFT JOIN {{ ref(P_TABLE_2) }} d
    ON c.lb_cat = d.lb_cat
)

SELECT
    cd_id,
    lb_prod,
    categorie_id,
    nb_kg,
    nb_agri,
    nb_biocarb,
    nb_transfo,
    nb_emballage,
    nb_transport,
    nb_vente
    
FROM climat_no_cat
