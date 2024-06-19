{%- set P_TABLE_1= 'stg_climat' -%}

WITH categories AS (
    SELECT DISTINCT
        lb_cat
    FROM {{ ref(P_TABLE_1) }}
)
SELECT
    MD5(lb_cat) AS categorie_id,
    lb_cat
FROM categories
