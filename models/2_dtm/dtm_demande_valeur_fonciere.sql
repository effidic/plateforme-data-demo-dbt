{%- set P_TABLE= 'stg_immo__demande_valeur_fonciere'-%}

SELECT
    valeur_fonciere.*,
    mt_valeur_fonciere AS mt_valeur_fonciere_ht,
    mt_valeur_fonciere + mt_frais_plat + (mt_valeur_fonciere * mt_frais_pourcentage) AS mt_valeur_fonciere_ttc,
    safe_divide(nb_lot1_surface_carrez, mt_valeur_fonciere) AS mt_metre_carre_1,
    -- nb_lot2_surface_carrez / mt_valeur_fonciere AS mt_metre_carre_2,
    -- nb_lot3_surface_carrez / mt_valeur_fonciere AS mt_metre_carre_3,
    -- nb_lot4_surface_carrez / mt_valeur_fonciere AS mt_metre_carre_4,
    -- nb_lot5_surface_carrez / mt_valeur_fonciere AS mt_metre_carre_5,
    safe_divide(nb_surface_reelle_bati, mt_valeur_fonciere) AS mt_metre_carre_reel  
FROM {{ ref(P_TABLE) }} valeur_fonciere
LEFT JOIN {{ ref('stg_immo__mandats') }} AS mandats
ON mandats.cd_type_mandat = valeur_fonciere.cd_type_mandat
WHERE valeur_fonciere.mt_valeur_fonciere >= mandats.mt_borne_inf_interval
AND valeur_fonciere.mt_valeur_fonciere <= mandats.mt_borne_sup_interval