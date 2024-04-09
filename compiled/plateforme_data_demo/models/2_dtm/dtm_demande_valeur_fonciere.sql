SELECT
    valeur_fonciere.* EXCEPT (lb_nature_culture,cd_nature_culture_speciale,lb_nature_culture_speciale),
    mt_valeur_fonciere AS mt_valeur_fonciere_hors_commission,
    null AS mt_valeur_fonciere_totale,
    null AS mt_metre_carre_lot_1,
    null AS mt_metre_carre_bati,
    null AS mt_metre_carre_terrain,
    null AS lb_nature_culture,
    null AS cd_nature_culture_speciale,
    null AS lb_nature_culture_speciale
FROM `plateforme-data-demo`.`1_stage_dbt`.`stg_immo__demande_valeur_fonciere` valeur_fonciere
LEFT JOIN `plateforme-data-demo`.`1_stage_dbt`.`stg_immo__mandats` AS mandats
    ON mandats.cd_type_mandat = valeur_fonciere.cd_type_mandat
WHERE valeur_fonciere.mt_valeur_fonciere >= mandats.mt_borne_inf_interval
    AND valeur_fonciere.mt_valeur_fonciere <= mandats.mt_borne_sup_interval