{%- set P_TABLE_1= 'stg_climat' -%}
{%- set P_TABLE_2= 'dtm_climat_categories' -%}

WITH produits_avec_id_categorie AS (
    SELECT
        DISTINCT
        climat.lb_prod,
        categories.categorie_id
    FROM {{ ref(P_TABLE_1) }} AS climat
    LEFT JOIN {{ ref(P_TABLE_2) }} AS categories
    ON climat.lb_cat = categories.lb_cat
)

SELECT
    MD5(lb_prod) AS produit_id,
    lb_prod AS nom_produit,
    categorie_id
FROM produits_avec_id_categorie
