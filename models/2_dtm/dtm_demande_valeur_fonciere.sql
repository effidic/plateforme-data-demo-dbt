{%- set P_TABLE= 'stg_immo__demande_valeur_fonciere'-%}

SELECT
    valeur_fonciere.*,
    mt_valeur_fonciere AS mt_valeur_fonciere_ht,
FROM {{ ref(P_TABLE) }} valeur_fonciere
LEFT JOIN {{ ref('stg_immo__mandats') }} AS mandats
    ON mandats.cd_type_mandat = valeur_fonciere.cd_type_mandat
WHERE valeur_fonciere.mt_valeur_fonciere >= mandats.mt_borne_inf_interval
    AND valeur_fonciere.mt_valeur_fonciere <= mandats.mt_borne_sup_interval