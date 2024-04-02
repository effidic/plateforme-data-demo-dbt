{%- set P_TABLE= 'stg_immo__demande_valeur_fonciere'-%}

SELECT
    valeur_fonciere.*,
    mt_valeur_fonciere AS mt_valeur_fonciere_ht,
    mt_valeur_fonciere + mt_frais_plat + (mt_valeur_fonciere * mt_frais_pourcentage) AS mt_valeur_fonciere_ttc,
    safe_divide(mt_valeur_fonciere, nb_lot1_surface_carrez) AS mt_metre_carre_lot_1,
    safe_divide(mt_valeur_fonciere, nb_surface_reelle_bati) AS mt_metre_carre_bati,
    safe_divide(mt_valeur_fonciere, nb_surface_terrain) AS mt_metre_carre_terrain
FROM {{ ref(P_TABLE) }} valeur_fonciere
LEFT JOIN {{ ref('stg_immo__mandats') }} AS mandats
ON mandats.cd_type_mandat = valeur_fonciere.cd_type_mandat
WHERE valeur_fonciere.mt_valeur_fonciere >= mandats.mt_borne_inf_interval
AND valeur_fonciere.mt_valeur_fonciere <= mandats.mt_borne_sup_interval