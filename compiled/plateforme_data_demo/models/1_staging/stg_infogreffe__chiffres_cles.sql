with source as (
      select _FILE_NAME , *  from `plateforme-data-demo`.`0_raw_infogreffe`.`chiffres_cles`
),
renamed as (
    select
        _FILE_NAME as lb_fichier_source,
        denomination as lb_denomination,
        siren as cd_siren,
        nic as cd_nic,
        forme_juridique as lb_forme_juridique,
        code_ape as cd_ape,
        libelle_ape as lb_ape,
        adresse as lb_adresse,
        code_postal as cd_postal,
        ville as lb_ville,
        num_dept as cd_departement,
        departement as lb_departement,
        region as lb_region,
        code_greffe as cd_greffe,
        greffe as lb_greffe,
        safe_cast( date_immatriculation as date format 'YYYY-MM-DD' ) as dt_immatriculation,
        safe_cast( date_radiation as date format 'YYYY-MM-DD' ) as dt_radiation,
        statut as lb_statut,
        geolocalisation as cd_geolocalisation,
        safe_cast( date_de_publication as date format 'YYYY-MM-DD' ) as dt_publication,
        millesime_1 as cd_annee_1,
        safe_cast( date_de_cloture_exercice_1 as date format 'YYYY-MM-DD' ) as dt_cloture_exercice_1,
        safe_cast( duree_1 as int ) as nb_mois_periode_1,
        safe_cast(ca_1 as decimal) as mt_ca_1,
        safe_cast(resultat_1 as decimal) as mt_resultat_1,
        effectif_1 as lb_effectif_1,
        millesime_2 as cd_annee_2,
        safe_cast( date_de_cloture_exercice_2 as date format 'YYYY-MM-DD' ) as dt_cloture_exercice_2,
        safe_cast( duree_2 as int ) as nb_mois_periode_2,
        safe_cast(ca_2 as decimal) as mt_ca_2,
        safe_cast(resultat_2 as decimal) as mt_resultat_2,
        effectif_2 as lb_effectif_2,
        millesime_3 as cd_annee_3,
        safe_cast( date_de_cloture_exercice_3 as date format 'YYYY-MM-DD' ) as dt_cloture_exercice_3,
        safe_cast( duree_3 as int ) as nb_mois_periode_3,
        safe_cast(ca_3 as decimal) as mt_ca_3,
        safe_cast(resultat_3 as decimal) as mt_resultat_3,
        effectif_3 as lb_effectif_3,
        id as cd_id,
        fiche_identite as lb_fiche_identite,
        tranche_ca_millesime_1 as lb_tranche_ca_millesime_1,
        tranche_ca_millesime_2 as lb_tranche_ca_millesime_2,
        tranche_ca_millesime_3 as lb_tranche_ca_millesime_3
    from source
)
, normalized as ( 
    select 
        lb_fichier_source,
        lb_denomination,
        cd_siren,
        cd_nic,
        lb_forme_juridique,
        cd_ape,
        lb_ape,
        lb_adresse,
        cd_postal,
        lb_ville,
        cd_departement,
        lb_departement,
        lb_region,
        cd_greffe,
        lb_greffe,
        dt_immatriculation,
        dt_radiation,
        lb_statut,
        cd_geolocalisation,
        dt_publication,
        cd_id,
        lb_fiche_identite, 
        cd_annee_1 as cd_annee,
        dt_cloture_exercice_1 as dt_cloture_exercice,
        nb_mois_periode_1 as nb_mois_periode,
        mt_ca_1 as mt_ca,
        mt_resultat_1 as mt_resultat,
        lb_effectif_1 as lb_effectif,
        lb_tranche_ca_millesime_1 as lb_tranche_ca_millesime 
from renamed
UNION ALL 
    select 
        lb_fichier_source,
        lb_denomination,
        cd_siren,
        cd_nic,
        lb_forme_juridique,
        cd_ape,
        lb_ape,
        lb_adresse,
        cd_postal,
        lb_ville,
        cd_departement,
        lb_departement,
        lb_region,
        cd_greffe,
        lb_greffe,
        dt_immatriculation,
        dt_radiation,
        lb_statut,
        cd_geolocalisation,
        dt_publication,
        cd_id,
        lb_fiche_identite, 
        cd_annee_2 as cd_annee,
        dt_cloture_exercice_2 as dt_cloture_exercice,
        nb_mois_periode_2 as nb_mois_periode,
        mt_ca_2 as mt_ca,
        mt_resultat_2 as mt_resultat,
        lb_effectif_2 as lb_effectif,
        lb_tranche_ca_millesime_2 as lb_tranche_ca_millesime 
from renamed
UNION ALL 
    select 
        lb_fichier_source,
        lb_denomination,
        cd_siren,
        cd_nic,
        lb_forme_juridique,
        cd_ape,
        lb_ape,
        lb_adresse,
        cd_postal,
        lb_ville,
        cd_departement,
        lb_departement,
        lb_region,
        cd_greffe,
        lb_greffe,
        dt_immatriculation,
        dt_radiation,
        lb_statut,
        cd_geolocalisation,
        dt_publication,
        cd_id,
        lb_fiche_identite, 
        cd_annee_3 as cd_annee,
        dt_cloture_exercice_3 as dt_cloture_exercice,
        nb_mois_periode_3 as nb_mois_periode,
        mt_ca_3 as mt_ca,
        mt_resultat_3 as mt_resultat,
        lb_effectif_3 as lb_effectif,
        lb_tranche_ca_millesime_3 as lb_tranche_ca_millesime 
from renamed
        )
select * from normalized