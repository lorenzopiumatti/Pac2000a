ALTER TABLE boom.tin_data_item_in ALTER COLUMN min_order TYPE numeric(9, 2) USING min_order::numeric(9, 2);

ALTER TABLE boom.tin_data_item_in ALTER COLUMN max_order TYPE numeric(9, 2) USING max_order::numeric(9, 2);

DROP VIEW vin_data_assortment_hd_out;

drop view vin_data_item_pd_out;

ALTER TABLE boom.tmd_orderable_assortments ALTER COLUMN min_order TYPE numeric(9, 2) USING min_order::numeric(9, 2);

ALTER TABLE boom.tmd_orderable_assortments ALTER COLUMN max_order TYPE numeric(9, 2) USING max_order::numeric(9, 2);

ALTER TABLE boom.tmd_orderable_assortments_var ALTER COLUMN min_order TYPE numeric(9, 2) USING min_order::numeric(9, 2);

ALTER TABLE boom.tmd_orderable_assortments_var ALTER COLUMN max_order TYPE numeric(9, 2) USING max_order::numeric(9, 2);

CREATE OR REPLACE VIEW boom.vin_data_assortment_hd_out
AS SELECT ti.id AS item_id,
    ti.item,
    tn.id AS network_id,
    tn.network_code AS destinatario,
    ( SELECT "substring"(min(tfil.features_value::text), 1, 1) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'MAG_RIF'::text)) AND tfil.item_id = ti.id) AS magazzino,
        CASE
            WHEN instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) <> 0 THEN substr(ti.item::text, 1, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) - 1)
            ELSE ti.item::text
        END AS articolo,
    substr(ti.item::text, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) + 1) AS diff,
    (ti.item::text || '_'::text) || tn.network_code::text AS item_pdv,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '000000'::character varying
            ELSE ttp.third_party
        END AS supplier_code,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '00'::text
            WHEN ttp.supplier_type_pc = 2 THEN substr(toa.operational_agreement::text, length(toa.operational_agreement::text) - 1, 10)
            ELSE substr(toa.operational_agreement::text, 2, 10)
        END AS differ_supplier,
    ( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'assortment_status_pc'::text AND vpt.boom_parameter::text = toav.assortment_status_pc::text) AS cod_stato_articolo,
    ( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'delivery_status_pc'::text AND vpt.boom_parameter::text = toav.delivery_status_pc::text) AS cod_stato_articolo_mag,
    tsic.supplier_item_code AS codi_art_forn_ord,
    tsic.description_item AS descr_art_forn_ord,
    ''::text AS codi_art_forn_fatt,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN 'IC'::text
            ELSE 'IF'::text
        END AS unita_misura_ord_forn,
    toav.min_order AS qta_min_order,
    toav.max_order AS qta_max_order,
    tlu.coefficient AS imballo_di_acquisto,
    ( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'assortment_status_pc'::text AND vpt.boom_parameter::text = toav.assortment_status_pc::text) AS codice_stato_articolo_fornitore,
    'L'::text AS tipo_listino,
    '000000'::text AS numero_listino_acquisti,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '01'::text
            ELSE substr(toa.operational_agreement::text, length(toa.operational_agreement::text) - 1, 10)
        END AS differenziatore_listino_acquisti,
    '01'::text AS progressivo_listino_acquisti,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tppv.start_date
        END AS data_inizio_val_listino,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tppv.end_date
        END AS data_fine_val_listino,
    tppv.price * COALESCE(substr(NULLIF((( SELECT tfil.features_value
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_14'::text))::text))) AND tfil.features_value::text ~~ ('S%'::text COLLATE case_like)))::text, ''::text), 2, 10)::numeric, 1::numeric) AS costo
   FROM tmd_items ti
     JOIN tpa_languages tl ON tl.is_default = 1::numeric
     JOIN ttr_items ti2 ON ti2.item_id = ti.id AND ti2.language_id = tl.id
     JOIN tmd_networks tn ON tn.network_node_type_pc = 4
     JOIN tmd_item_logistics til ON til.item_id = ti.id
     JOIN tmd_logistic_units tlu ON tlu.item_logistic_id = til.id AND tlu.logistic_unit_pc = 41
     JOIN tmd_orderable_assortments toav ON toav.item_id = ti.id AND tn.id = toav.network_id AND toav.main_supplier = 0::numeric
     JOIN tmd_operational_agreements toa ON toa.id = toav.operational_agreement_id
     JOIN tmd_third_parties ttp ON ttp.id = toa.third_party_id
     LEFT JOIN vmd_purchase_prices_v tppv ON tppv.item_logistic_id = til.id AND tppv.operational_agreement_id = toa.id AND tppv.cost_type_pc = 1 AND tn.id = tppv.network_id
     LEFT JOIN tmd_supplier_item_codes tsic ON tsic.item_logistic_id = til.id AND tsic.third_party_id = ttp.id;


CREATE OR REPLACE VIEW boom.vin_data_item_pd_out
AS SELECT ti.id AS item_id,
    ti.item,
    tn.id AS network_id,
    tn.network_code AS destinatario,
    ( SELECT "substring"(min(tfil.features_value::text), 1, 1) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'MAG_RIF'::text)) AND tfil.item_id = ti.id) AS magazzino,
        CASE
            WHEN instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) <> 0 THEN substr(ti.item::text, 1, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) - 1)
            ELSE ti.item::text
        END AS articolo,
    substr(ti.item::text, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) + 1) AS diff,
    (ti.item::text || '_'::text) || tn.network_code::text AS item_pdv,
    ti2.description AS descrizione,
    ( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'item_type_pc'::text AND vpt.boom_parameter::text = ti.item_type_pc::text) AS tipo_art,
    substr(ts.structure_code::text, 1, 2) AS merc_area,
    substr(ts.structure_code::text, 3, 2) AS merc_sett,
    substr(ts.structure_code::text, 5, 2) AS merc_gruppo,
    substr(ts.structure_code::text, 7, 2) AS merc_segmento,
    '00001'::text AS merc_prog,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'DAT_CRE'::text)) AND tfil.item_id = ti.id) AS data_nascita,
    lpad(round(tv.vat_value * 10::numeric)::text, 3, '0'::text) AS iva_alfanumerica,
    COALESCE(tv.law) AS legge,
    COALESCE(tv.paragraph, ''::character varying) AS comma,
        CASE
            WHEN ti.is_multi_vat = 1::numeric THEN 'S'::text
            ELSE 'N'::text
        END AS flag_multi_iva,
        CASE
            WHEN ti.brand_pc = 1 THEN 'N'::text
            ELSE 'S'::text
        END AS marchio,
    tis.weight_unit_measure_pc AS unita_misura_fisc,
    fn_tpa_parameters_getdescription(tis.unit_measure_ph, tis.weight_unit_measure_pc, 'IT'::character varying) AS desc_unita_misura_fisc,
        CASE
            WHEN tis.weight_unit_measure_pc = 10 THEN 0::numeric(9,3)
            ELSE tis.val_weight
        END AS peso_netto,
        CASE
            WHEN tis.weight_unit_measure_pc = 10 THEN 0::numeric(9,3)
            ELSE til.medium_weight
        END AS peso_lordo,
    COALESCE(( SELECT min('S'::text) AS text
           FROM tmd_sale_codes_var tsc
          WHERE tn.id = tsc.network_id AND tis.id = tsc.item_sale_id AND tsc.code_type_pc = 10 AND tsc.end_date >= CURRENT_DATE
         LIMIT 1), 'N'::text) AS flag_da_pesare,
    '0000000'::text AS vuoto,
    ti.item_category_pc AS cod_reparto_cassa,
    COALESCE(ti.supplier_expiry_days, 0::numeric(5,0)) AS durata_max_bilance,
    '01'::text AS rep_commerciale,
    fn_tpa_parameters_getdescription(ti.stock_unit_ph, ti.stock_unit_pc, 'IT'::character varying) AS desc_unita_misura_prezzo,
    substr(COALESCE(( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_14'::text))::text))) AND tfil.features_value::text ~~ ('S%'::text COLLATE case_like)), 'U'::character varying::text), 1, 1) AS tipo_rapp_cv,
    COALESCE(substr(NULLIF(( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_14'::text))::text))) AND tfil.features_value::text ~~ ('S%'::text COLLATE case_like)), ''::text), 2, 10)::numeric, 1::numeric) AS rapp_cv,
    COALESCE((( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_12'::text))::text)))))::numeric, 0::numeric) AS calo_peso,
    '001'::text AS centro_costo,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '000000'::character varying
            ELSE ttp.third_party
        END AS supplier_code,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '00'::text
            WHEN ttp.supplier_type_pc = 2 THEN substr(toa.operational_agreement::text, length(toa.operational_agreement::text) - 1, 10)
            ELSE substr(toa.operational_agreement::text, 2, 10)
        END AS differ_supplier,
    upper(( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'assortment_status_pc'::text AND vpt.boom_parameter::text = toav.assortment_status_pc::text)) AS cod_stato_articolo,
    fn_tpa_parameters_getdescription(64::bigint, toav.assortment_status_pc, 'IT'::character varying) AS desc_cod_stato_articolo,
    tis2.description AS descr_pos,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_3'::text))::text)))) AS stagionalita,
    COALESCE(0::numeric, 0::numeric(5,0)) AS gg_scad_art,
    upper(( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'delivery_status_pc'::text AND vpt.boom_parameter::text = toav.delivery_status_pc::text)) AS cod_stato_articolo_mag,
    fn_tpa_parameters_getdescription(66::bigint, toav.delivery_status_pc, 'IT'::character varying) AS desc_cod_stato_articolo_mag,
    tsic_forn.supplier_item_code AS codi_art_forn_ord,
    tsic_forn.description_item AS descr_art_forn_ord,
    COALESCE(tsic_fact.supplier_item_code, tsic_forn.supplier_item_code)::text AS codi_art_forn_fatt,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN 'IC'::text
            ELSE 'IF'::text
        END AS unita_misura_ord_forn,
    toav.min_order AS qta_min_order,
    toav.max_order AS qta_max_order,
    tlu.coefficient AS imballo_di_acquisto,
    upper(( SELECT min(vpt.external_parameter::text) AS min
           FROM vpa_parameter_trans vpt
          WHERE vpt.table_field::text = 'assortment_status_pc'::text AND vpt.boom_parameter::text = toav.assortment_status_pc::text)) AS codice_stato_articolo_fornitore,
    'L'::text AS tipo_listino,
    '000000'::text AS numero_listino_acquisti,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN '01'::text
            ELSE substr(toa.operational_agreement::text, length(toa.operational_agreement::text) - 1, 10)
        END AS differenziatore_listino_acquisti,
    '01'::text AS progressivo_listino_acquisti,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tppv.start_date
        END AS data_inizio_val_listino,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tppv.end_date
        END AS data_fine_val_listino,
    tppv.price * COALESCE(substr(NULLIF((( SELECT tfil.features_value
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_14'::text))::text))) AND tfil.features_value::text ~~ ('S%'::text COLLATE case_like)))::text, ''::text), 2, 10)::numeric, 1::numeric) AS costo,
        CASE
            WHEN ttp.third_party::text = '867'::text THEN 'E'::text
            ELSE 'Q'::text
        END AS tipo_listino_pubblico,
    '000001'::text AS numero_listino_pubblico,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tspv.start_date
        END AS data_decorrenza_pubblico,
        CASE
            WHEN ti.is_local = 0::numeric THEN NULL::date
            ELSE tspv.end_date
        END AS data_scadenza_listino_pubblico,
    tspv.price AS prezzo_pubblico,
    substr(( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_1'::text AND vpt2.type_flow = 10::numeric))::text)))), 2, 5) AS cod_mag_riordino,
    'EUR'::text AS divisa,
    '00'::text AS altezza,
    '00'::text AS larghezza,
    '00'::text AS lunghezza,
    'P'::text AS unita_mov_vendita,
    '00000'::text AS nr_imb_unita_mov_vendita,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'CONSI'::text)) AND tfil.item_id = ti.id) AS flag_consigliato,
        CASE
            WHEN ti.brand_pc = 1 THEN '$'::text
            ELSE ( SELECT min(vp_brand.str_val_1::text) AS min
               FROM vpa_parameters vp_brand
              WHERE vp_brand.is_default = 1::numeric AND vp_brand.parameter_header = ti.under_brand_ph AND vp_brand.parameter_code = ti.under_brand_pc)
        END AS tipo_articolo_a_marchio,
    ( SELECT substr(ti.item::text, 1, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) - 1) AS substr
           FROM tmd_items
          WHERE tmd_items.id = ti.parent_item_id) AS variante_articolo_padre,
    ( SELECT substr(ti.item::text, instr(ti.item::text COLLATE case_like, '-'::text, 1, 1) + 1) AS substr
           FROM tmd_items
          WHERE tmd_items.id = ti.parent_item_id) AS diff_variante_articolo_padre,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_5'::text))::text)))) AS cod_riclassificazione_1,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_6'::text))::text)))) AS cod_riclassificazione_2,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.item_id = ti.id AND tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = ((( SELECT vpt2.boom_parameter
                           FROM vpa_parameter_trans vpt2
                          WHERE vpt2.table_field::text = 'attribute_code_7'::text))::text)))) AS cod_riclassificazione_3,
    COALESCE(( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'LK_STD_PR'::text)) AND tfil.item_id = ti.id), 'N'::text) AS prz_pub_bloc,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'E_COMM_MIN'::text)) AND tfil.item_id = ti.id) AS e_comm_min,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'E_COMM_CNF'::text)) AND tfil.item_id = ti.id) AS e_comm_cnf,
    ( SELECT min(tfil.features_value::text) AS min
           FROM tmd_feature_item_links tfil
          WHERE tfil.specific_feature_id = (( SELECT tsf.id
                   FROM tpa_specific_features tsf
                  WHERE tsf.specific_feature::text = 'E_COMM_PZ'::text)) AND tfil.item_id = ti.id) AS e_comm_pz
   FROM tmd_items ti
     JOIN tpa_languages tl ON tl.is_default = 1::numeric
     JOIN ttr_items ti2 ON ti2.item_id = ti.id AND ti2.language_id = tl.id
     JOIN tmd_vat tv ON tv.id = ti.sale_vat_id
     JOIN tmd_merchandise_structures tms ON tms.is_default = 1::numeric
     JOIN tmd_structures ts ON ts.merchandise_structure_id = tms.id
     JOIN tmd_structure_item_links tsil ON tsil.item_id = ti.id AND tsil.structure_id = ts.id
     JOIN tmd_item_sales tis ON ti.id = tis.item_id
     JOIN ttr_item_sales tis2 ON tis2.item_sale_id = tis.id AND tis2.language_id = tl.id
     JOIN tmd_networks tn ON tn.network_node_type_pc = 4
     JOIN tmd_item_logistics til ON til.item_id = ti.id
     JOIN tmd_logistic_units tlu ON tlu.item_logistic_id = til.id AND tlu.logistic_unit_pc = 41
     LEFT JOIN tmd_orderable_assortments toav ON toav.item_id = ti.id AND tn.id = toav.network_id AND toav.main_supplier = 1::numeric
     LEFT JOIN tmd_operational_agreements toa ON toa.id = toav.operational_agreement_id
     LEFT JOIN tmd_third_parties ttp ON ttp.id = toa.third_party_id
     LEFT JOIN vmd_sale_prices_v tspv ON tspv.item_sale_id = tis.id AND tspv.sale_price_type_pc = 1 AND tn.id = tspv.network_id
     LEFT JOIN vmd_purchase_prices_v tppv ON tppv.item_logistic_id = til.id AND tppv.operational_agreement_id = toa.id AND tppv.cost_type_pc = 1 AND tn.id = tppv.network_id
     LEFT JOIN tmd_supplier_item_codes tsic_forn ON tsic_forn.item_logistic_id = til.id AND tsic_forn.third_party_id = ttp.id AND tsic_forn.code_type_pc = 1
     LEFT JOIN tmd_supplier_item_codes tsic_fact ON tsic_fact.item_logistic_id = til.id AND tsic_fact.third_party_id = ttp.id AND tsic_fact.code_type_pc = 2;
