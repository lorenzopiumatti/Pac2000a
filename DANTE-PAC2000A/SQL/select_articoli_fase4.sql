select nextval('tin_data_item_out_id_seq'::regclass) as "id", 
    row_number() over (order by cod_art) as "row_number", 
    vpio.*, 
    COALESCE(:user, 'PH') AS last_user 
  from vin_populate_item_out vpio
where item_id = :par
 where cod_art = any ((select array(select item 
                                    from tmd_items ti 
                                    where is_updated = 1 
                                    and UPPER(last_user) = COALESCE(:user, UPPER(last_user)) 
                                    and :par = 2 
                                    union 
                                    select ti.item 
                                    from tmd_item_sales tis 
                                    inner join tmd_items ti on ti.id = tis.item_id 
                                    where tis.is_updated = 1 
                                    and UPPER(tis.last_user) = COALESCE(:user, UPPER(tis.last_user)) 
                                    and :par = 2 
                                    union 
                                    select ti.item 
                                    from tmd_logistic_units tlu 
                                    inner join tmd_item_logistics til on til.id = tlu.item_logistic_id 
                                    inner join tmd_items ti on ti.id = til.item_id 
                                    where tlu.is_updated = 1 
                                    and tlu.logistic_unit_pc = '41' 
                                    and UPPER(tlu.last_user) = COALESCE(:user, UPPER(tlu.last_user)) 
                                    and :par = 2 
                                    union 
                                    select ti.item 
                                    from tmd_sale_codes_var tscv 
                                    inner join tmd_item_sales tis on tis.id = tscv.item_sale_id 
                                    inner join tmd_items ti on ti.id = tis.item_id 
                                    inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
                                                                    tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
                                                                    tp.num_val_5 = 1 
                                    where tscv.is_updated = 1 
                                    and UPPER(tscv.last_user) = COALESCE(:user, UPPER(tscv.last_user)) 
                                    and :par = 2 
                                    and current_date between tscv.start_date and tscv.end_date 
                                    union 
                                    select ti.item 
                                    from tmd_items ti 
                                    inner join tmd_kit_components tkc on tkc.item_id=ti.id 
                                    where :par = 2 
                                    and tkc.is_updated=1 
                                    and UPPER(tkc.last_user) = COALESCE(:user, UPPER(tkc.last_user)) 
                                    union 
                                    select ti.item 
                                    from tmd_sale_prices_var tspv2 
                                    inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
                                    inner join tmd_items ti on ti.id = tis.item_id 
                                    inner join vpa_parameters vp on vp.parameter_header =tspv2.sale_price_type_ph and vp.is_default =1 and vp.parameter_code = tspv2.sale_price_type_pc and vp.num_val_1=1 
                                    where :par = 2 
                                    and tspv2.is_sent = 0 
                                    and UPPER(tspv2.last_user) = COALESCE(:user, UPPER(tspv2.last_user)) 
                                    and current_date between tspv2.start_date and tspv2.end_date 
                                    union 
                                    select ti.item 
                                    from tmd_structure_item_links_var lnk 
                                    inner join tmd_items ti on ti.id=lnk.item_id 
                                    inner join tmd_structures ts on ts.id = lnk.structure_id 
                                    inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
                                    where :par = 2 
                                    and lnk.is_updated=1 
                                    and UPPER(lnk.last_user) = COALESCE(:user, UPPER(lnk.last_user)) 
                                    and current_date between lnk.start_date and lnk.end_date 
                                    union 
                                    select ti.item 
                                    from tmd_feature_item_links tfil 
                                    inner join tmd_items ti on ti.id = tfil.item_id 
                                    inner join tpa_specific_features tsf on tsf.id = tfil.specific_feature_id and tsf.used_scales = 1 and tsf.is_active = 1 
                                    where :par = 2 
                                    and tfil.is_updated = 1 
                                    and UPPER(tfil.last_user) = COALESCE(:user, UPPER(tfil.last_user)) 
                                    union 
                                    select td.item 
                                    from tin_data_item_in td 
                                    inner join tmd_items ti on ti.item = td.item 
                                    where :par = 1 
                                    and processing_step3 = 1 
                                    and processing_step4 = 0 
                                    and td.transaction_code=#TRANSACTION# 
                                    union 
                                    select ti.item 
                                    from tmd_sale_codes_var tscv inner join tmd_item_sales tis on tscv.item_sale_id = tis.id 
                                    inner join tmd_items ti on ti.id = tis.item_id 
                                    inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
                                                                    tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
                                                                    tp.num_val_5 = 1 
                                    where tscv.is_updated = 1 
                                    and tscv.last_user = 'user_batch' 
                                    and :par = 1 
                                    and current_date between tscv.start_date and tscv.end_date 
                                    union 
                                    select ti.item 
                                    from tmd_sale_prices_var tspv2 
                                    inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
                                    inner join tmd_items ti on ti.id = tis.item_id 
                                    where :par = 1 
                                    and tspv2.is_sent = 0 
                                    and tspv2.last_user = 'user_batch' 
                                    and current_date between tspv2.start_date and tspv2.end_date 
                                    and exists (select 1 
                                                                    from vpa_parameters vp 
                                                                    where vp.parameter_header =tspv2.sale_price_type_ph 
                                                                      and vp.is_default =1 
                                                                      and vp.parameter_code = tspv2.sale_price_type_pc 
                                                                      and vp.num_val_1=1 ) 
                                    union 
                                    select ti.item 
                                    from tmd_structure_item_links_var lnk 
                                    inner join tmd_items ti on ti.id = lnk.item_id 
                                    inner join tmd_structures ts on ts.id = lnk.structure_id 
                                    inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
                                    where :par = 1 
                                    and lnk.is_updated=1 
                                    and lnk.last_user = 'user_batch' 
                                    and current_date between lnk.start_date and lnk.end_date 
  ))::varchar[] )


---------------------------------------------------------------------
--------------------------------------------------------------
  vin_populate_item_out
----------------------------------------------------------------
------------------------------------------------------------------
-- boom.vin_populate_item_out source

CREATE OR REPLACE VIEW boom.vin_populate_item_out
AS SELECT DISTINCT ti.is_multi_vat AS item_type,
    ti.item_category_pc AS reparto,
    ti.item AS cod_art,
    ti.id AS item_id,
    tis2.description AS descr_var_sale_long,
    tspv.price AS sale_price,
    tspv.start_date AS price_val_date_begin,
    tspv.end_date AS price_val_date_end,
    tsc.sale_code AS art_barcode,
    tsc.is_label,
    tv.vat AS code_sale_vat,
    tis2.description AS descr_cash,
    tn.network_cash AS shop_code,
    ti.stock_unit_pc AS stock_unit,
    til.unit_measure_pc AS measure_unit,
    tlu.pieces AS pieces_pack,
    '0'::text AS flag_treat,
    NULL::text AS date_treat,
    ti.transaction_code AS trans_num,
    now() AS date_ins,
    now() AS date_mod,
    'tsitemout'::text AS prog_last_mod,
    'file'::text || to_char(now(), 'yyyymmddhh24miss'::text) AS file_name,
        CASE
            WHEN tsc.is_updated = 1::numeric THEN 'I'::text
            WHEN tsc.end_date < CURRENT_DATE THEN 'D'::text
            ELSE 'U'::text
        END AS type_op,
    ((((( SELECT vfr.attributo
           FROM vmd_feature_replicator vfr
          WHERE vfr.item_id = ti.id AND vfr.network_id = tn.id)) || ';TSACSATTR18='::text) || COALESCE((( SELECT
                CASE tfil.features_value
                    WHEN 'S'::text THEN 1
                    WHEN 'N'::text THEN 0
                    ELSE NULL::integer
                END AS "case"
           FROM tmd_feature_item_links tfil
             JOIN tpa_specific_features tsf ON tsf.id = tfil.specific_feature_id AND tsf.specific_feature::text = 'F_V_PESO_C'::text
          WHERE tfil.item_id = ti.id))::bigint, ( SELECT count(DISTINCT 1) AS count
           FROM tmd_sale_codes_var tscv2
          WHERE tscv2.item_sale_id = tis.id AND CURRENT_DATE >= tscv2.start_date AND CURRENT_DATE <= tscv2.end_date AND tscv2.code_type_pc = 10 AND (tscv2.bilance_department_pc::numeric = ANY (((( SELECT ARRAY( SELECT vp.parameter_code
                           FROM vpa_parameters vp
                          WHERE vp.parameter_header = 85 AND vp.is_default = 1::numeric AND vp.num_val_3 = 1::numeric) AS "array"))::numeric[])))))) || ';TSACSATTR26='::text) || (( SELECT count(DISTINCT 1) AS count
           FROM tmd_sale_codes_var tscv2
          WHERE tscv2.item_sale_id = tis.id AND CURRENT_DATE >= tscv2.start_date AND CURRENT_DATE <= tscv2.end_date AND tscv2.code_type_pc = 10)) AS features,
    "substring"(ts.structure_code::text, 1, 2) AS struct_lv1,
    "substring"(ts.structure_code::text, 3, 2) AS struct_lv2,
    "substring"(ts.structure_code::text, 5, 2) AS struct_lv3,
    "substring"(ts.structure_code::text, 7, 2) AS struct_lv4,
    "substring"(ts.structure_code::text, 9, 2) AS struct_lv5,
    "substring"(ts.structure_code::text, 11, 2) AS struct_lv6,
    "substring"(ts.structure_code::text, 13, 2) AS struct_lv7,
    "substring"(ts.structure_code::text, 15, 2) AS struct_lv8,
    "substring"(ts.structure_code::text, 17, 2) AS struct_lv9,
    tsc.id AS "tmd_sale_codes_var.id",
    tlu.id AS "tmd_logistic_units.id",
    til.id AS "tmd_item_logistics.id",
    tis.id AS "tmd_item_sales.id",
    tspv.id AS id_sale_price,
    tsilv.id AS structure_id,
    ( SELECT string_agg(tkc.id::character varying::text, ','::text) AS string_agg
           FROM tmd_kit_components tkc
          WHERE ti.id = tkc.item_id) AS comp_id,
        CASE
            WHEN tsc.code_type_pc <> 10 THEN ''::text
            ELSE ( SELECT
                    CASE
                        WHEN ti_old_plu_item.item::text = ti.item::text THEN ''::text
                        ELSE ((((ti_old_plu_item.item::text || '§'::text) || tsc.bilance_department_pc::text) || tsc.plu_code::text) || '§'::text) || ti_old_plu_item.description::text
                    END AS "case"
               FROM tmd_sale_codes_var tscv3
                 JOIN tmd_item_sales tis3 ON tis3.id = tscv3.item_sale_id
                 JOIN vmd_items ti_old_plu_item ON ti_old_plu_item.id = tis3.item_id
              WHERE tscv3.sale_code::text = tsc.sale_code::text AND tscv3.network_id = tsc.network_id AND tscv3.code_type_pc = 10 AND (tsc.start_date - 1) >= tscv3.start_date AND (tsc.start_date - 1) <= tscv3.end_date AND (CURRENT_DATE - 1) >= tscv3.start_date AND (CURRENT_DATE - 1) <= tscv3.end_date AND tscv3.is_updated = 1::numeric)
        END AS old_plu_data
   FROM tmd_items ti
     JOIN tmd_item_sales tis ON tis.item_id = ti.id
     JOIN ttr_item_sales tis2 ON tis2.item_sale_id = tis.id AND tis2.language_id = (( SELECT tpa_languages.id
           FROM tpa_languages
          WHERE tpa_languages.is_default = 1::numeric))
     JOIN tmd_structure_item_links_var tsilv ON tsilv.item_id = tis.item_id AND CURRENT_DATE >= tsilv.start_date AND CURRENT_DATE <= tsilv.end_date
     JOIN tmd_structures ts ON ts.id = tsilv.structure_id
     JOIN tmd_merchandise_structures tms ON tms.id = ts.merchandise_structure_id AND tms.is_default = 1::numeric
     JOIN tmd_sale_prices_var tspv ON tspv.item_sale_id = tis.id AND CURRENT_DATE >= tspv.start_date AND CURRENT_DATE <= tspv.end_date AND (tspv.sale_price_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT 1
                   FROM vpa_parameters vp
                  WHERE vp.parameter_header = tspv.sale_price_type_ph AND vp.is_default = 1::numeric AND vp.parameter_code = tspv.sale_price_type_pc AND vp.num_val_1 = 1::numeric) AS "array"))::numeric[])))
     JOIN vpa_networks_desc vnd ON vnd.network_id = tspv.network_id AND vnd.is_stock_management = 1::numeric
     JOIN tmd_networks tn ON tn.id = vnd.child_network_id
     JOIN tmd_sale_codes_var tsc ON tsc.item_sale_id = tis.id AND CURRENT_DATE >= tsc.start_date AND CURRENT_DATE <= tsc.end_date AND (tsc.code_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT tp.parameter_code
                   FROM tpa_parameters tp,
                    tpa_parameter_headers tph
                  WHERE tph.id = tp.parameter_header_id AND tph.parameter_header = tsc.code_type_ph AND tp.num_val_5 = 1::numeric) AS "array"))::numeric[])))
     LEFT JOIN tmd_item_logistics til ON til.item_id = ti.id
     LEFT JOIN tmd_logistic_units tlu ON tlu.item_logistic_id = til.id AND tlu.logistic_unit_pc = '41'::bigint
     JOIN tmd_vat tv ON tv.id = tspv.vat_id
     JOIN tmd_saleable_assortments tsa ON tsa.item_sale_id = tis.id AND tsa.network_id = tn.id AND tsa.status_pc = 1;