with source as (
      select _FILE_NAME , *  from `plateforme-data-demo`.`0_raw_pdl`.`observatoire_communication`
),
renamed as (
    SELECT 
        _FILE_NAME as lb_fichier_source,
        ifnull(Commune , 'NA' ) as lb_commune,
        epci as lb_epci,
        Departement as lb_departement,
        Region as lb_region,
        sup_id as cd_sup_id,
        id as cd_id,
        generation as lb_generation,
        Technologie as lb_technologie,
        code_departement as cd_departement,
        code_insee as cd_insee,
        date_de_mise_a_jour as dt_de_mise_a_jour,
        sta_nm_anfr as lb_sta_nm_anfr,
        nat_id as lb_nat_id,
        sup_nm_haut as lb_sup_nm_haut,
        tpo_id as lb_tpo_id,
        adr_lb_lieu as lb_adr_lieu,
        adr_lb_add1 as lb_adr_add1,
        adr_lb_add2 as lb_adr_add2,
        adr_lb_add3 as lb_adr_add3,
        Code_postal as lb_code_postal,
        INSEE as lb_insee,
        coord as lb_coord,
        recordid as cd_recordid,
        adm_lb_lnom as lb_adm_lb_lnom,
        statut as cd_statut,
        Date_de_mise_en_service as dt_de_mise_en_service,
        localisation as cd_localisation
    FROM source
)

select * from renamed