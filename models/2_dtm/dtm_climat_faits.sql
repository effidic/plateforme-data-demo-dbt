{%- set P_TABLE_1= 'stg_climat' -%}
{%- set P_TABLE_2= 'dtm_climat_produits' -%}

WITH climat_avec_id_produit AS (
    SELECT
        climat.cd_id,
        produits.produit_id,
        climat.nb_kg,
        climat.nb_agri,
        climat.nb_biocarb,
        climat.nb_transfo,
        climat.nb_emballage,
        climat.nb_transport,
        climat.nb_vente
    FROM {{ ref(P_TABLE_1) }} AS climat
    LEFT JOIN {{ ref(P_TABLE_2) }} AS produits
    ON climat.lb_prod = produits.nom_produit
)

SELECT
    cd_id,
    produit_id,
    nb_kg,
    nb_agri,
    nb_biocarb,
    nb_transfo,
    nb_emballage,
    nb_transport,
    nb_vente
FROM climat_avec_id_produit
