
   SELECT  item ,count( *) ----transaction_code, processing_item, item, id -- Aggiunto id per l'update finale
        FROM tin_data_item_in td
        WHERE processing_step3 = 0 and processing_step4 = 0
          AND processing_step2 = 1
          AND processing_step1 = 1
--          AND ((transaction_code_param is not null and td.transaction_code = transaction_code_param ) or (transaction_code_param is null ))
          AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
group by item;

select * from tmd_items order by creation_date desc

select * from tmd_saleable_assortments tsa order by  1 desc;

select * from tmd_items where id = 12590

select * from tck_logs  order by 1 desc;

select * from tpa_lock_manage tlm   order by 1 desc;

VACUUM full tin_data_item_out;

 SELECT   --source_type_pc,file_name, 
 count(*),item----count(distinct item )  -- Aggiunto id per l'update finale
        FROM tin_data_item_in td
        WHERE processing_step3 = 5 and processing_step4 = 5
          AND processing_step2 = 1 and   exists (select 1 from tmd_items a where a.item = td.item	 )
          AND processing_step1 = 1
---          AND ((transaction_code_param is not null and td.transaction_code = transaction_code_param ) or (transaction_code_param is null ))
          AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
   group by ---- source_type_pc,file_name,
   item
          
   select * from tin_data_item_in
WHERE 1=1
  AND processing_step3 = 1
  AND processing_step4 = 0
  AND processing_step2 = 1
  -----AND item > '205542-01'
  ---AND attribute_value_1 IS NOT NULL
  AND processing_step1 = 1
  AND transaction_code IN (1877072, 1877073, 1877071, 1877074)
  order by item, number_line 

   
UPDATE tin_data_item_in
SET processing_step4 = 8
WHERE 1=1
  AND processing_step3 = 1 ---and attribute_code_1 = 'INGRED'
  AND processing_step4 = 0
  AND processing_step2 = 1
  -----AND item > '205542-01'
  -----AND attribute_value_1 IS NOT NULL
  AND processing_step1 = 1
  AND transaction_code IN (1877072, 1877073, 1877071, 1877074);

order by item 
   
update tin_data_item_in set  processing_step4 = 0
where item in (
 SELECT  distinct item 
        FROM tin_data_item_in td
        WHERE 1=1 and processing_step3 = 1 and processing_step4 = 8 
          AND processing_step2 = 1	
          AND processing_step1 = 1    and transaction_code in (1877072,
1877073,
1877071,
1877074 )
------  and not exists (select 1 from tmd_sale_codes_var b where b.sale_code = td.sale_code)
          limit 10
        )
        and processing_step3 = 1 and  processing_step4 = 8
        AND processing_step2 = 1	
        AND processing_step1 = 1   
        and transaction_code in (1877072,
1877073,
1877071,
1877074 )
   


         select count(*) from tin_data_item_in where processing_step3 = 0 and  processing_step4 = 0 
        
 select distinct transaction_code  from tin_data_item_in where processing_step3 = 1 and  processing_step4 = 8 
AND processing_step2 = 1           AND processing_step1 = 1 

select now(), current_timestamp, current_Date  

 select * from tin_data_item_in where update_date >= current_Date  
 and item = '13042-01' order by item , number_line


select * from tmd_items where item = '100000-01'

select * from tmd_structure_item_links_var  where item_id in ( select id from tmd_items where item = '518279-03')

select * from tmd_sale_codes_var ticv where item_sale_id in (select id from tmd_item_Sales where item_id = 21520 ) 

        
 vmd_features_item_links_cash
 
        select count(*) from tin_data_item_out tdio where tsacadcre >= current_date 
        
          
          
          select * from tmd_supplier_item_codes where item_id in ( select id from tmd_items where item = '15019-01')
          

          select  *  from tpa_lock_manage tlm 



=select * from tm

        select * from tck_logs order by 1 desc
        
        select * from  tin_data_item_in where update_date > now() - INTERVAL '90 minutes' 
        
        select substr(error_message,1,30),count(*) from tin_data_item_in where update_date > CURRENT_TIMESTAMP - INTERVAL '240 minutes'
        group by substr(error_message,1,30)
        
        update tin_data_item_in tdii set processing_step3 = 0, processing_puchase_price = 0 ,  processing_assortment = 0
        where  item in 
        (select item from tmd_items where creation_date >= current_date)
        and processing_step1 = 1 and  processing_step2 = 1 and  processing_step3 = 2 --- and item = '102459-01'
        and  processing_puchase_price = 2 and  processing_assortment = 2 and  processing_sale_price = 1 and processing_item = 1
        
        order by item , number_line
        
        select  processing_step1, processing_step2, processing_step3 , processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature, count(distinct item )
        from tin_data_item_in tdii where update_date >= now() - INTERVAL '240 minutes' 
        group by processing_step1, processing_step2, processing_step3, processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature
        
        SELECT * tin
        
      select item,count(*) from tin_data_item_in    where update_date > CURRENT_TIMESTAMP - INTERVAL '240 minutes'
      group by item
   
      having count(*)  > 1
     tmd_purchase_prices_var
      
     select * from tmd_feature_item_links
     

            -
            
            
            
select * from tmd_sale_codes tsc 
where code_type_pc not in (10,4) and plu_code is not null and last_user in ('fn_initial_load_item','AGGIORNO')


boom.tin_data_item_in


update tmd_sale_codes_var tsc  set plu_code= null, bilance_department_pc= null, bilance_code= null
where code_type_pc not in (10,4) and plu_code is not null and last_user in ('fn_initial_load_item','AGGIORNO')

select * from tin_data_item_out tdio where tsacacexr = '34948-15'  order by 1 desc;

SELECT array_to_json(array_agg(row_to_json(t))) FROM (SELECT item as sz_code,
array_to_json(array_agg
    (row_to_json(v))
)::json::text  as j_data
FROM
vmd_features_item_links_cash as v
WHERE
item = '679113-01'
 AND
(code_feature in ('REP_BIL','INGRED','ALLERG','VV_ART','CODETI','MAG_RIF','CONSI','BRAND','STAG','EAN','REP','ASS','DAYS','DESCR_ETI','PRODOTTO','VARIETA','ORIGINE','CALIBRO','CATEGORIA', 'IMBALLO', 'COD_RIC_2', 'ESCL_OMNIB', 'ELABEL_CODE')
    OR (description_value_feature = '2061190' AND code_feature IN ('TASTO_BIL','PLU','REP_BIL_COD', 'REP_BIL_DESCR','PMAN', 'TARA')
    )
)
 GROUP BY item) t
 
select * from tin_data_item_in where tsacacexr = '34948-15' order by 1 desc

where processing_step3 = 1 and processing_step4 = 0

--1732890
select * from tin_data_item_out tdio  
order by 1 desc

select * from tmd_networks tn  where is_stock_management = 1

select * from tpa_lock_manage tlm   order by 1 desc;

select * from tmd_feature_item_links tfil where specific_feature_id in  (16,36)  and network_id is not null;

select * from tmd_feature_item_links tfil where item_id in (select id from tmd_items where item = '205939-01' ) ----'205939-01')

select * FROM vmd_feature_replicator vfr
          WHERE vfr.item_id = 43470

          
          select * from tmd_networks tn 


          select *  from (
          select ti.item, netw.id, netw.network_code, netw.description ,
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id is null ) count_is_null,
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 3) count_3, 
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 7) count_7, 
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 10) count_10, 
          (select count(*)from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 12) count_12, 
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 14) count_14, 
          (select count(*) from tmd_feature_item_links tfil where tfil.item_id = ti.id  and tfil.network_id = 16) count_16,
           ((((( SELECT vfr.attributo
           FROM vmd_feature_replicator vfr
          WHERE vfr.item_id = ti.id AND vfr.network_id = netw.id)) || ';TSACSATTR18='::text) || COALESCE((( SELECT
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
          WHERE tscv2.item_sale_id = tis.id AND CURRENT_DATE >= tscv2.start_date AND CURRENT_DATE <= tscv2.end_date AND tscv2.code_type_pc = 10)) AS features
          from tmd_items ti , tmd_item_Sales tis , (select  id, network_code, description   from tmd_networks where is_stock_management = 1 ) netw
          where ti.item_type_pc <>3  and tis.item_id = ti.id
          ) a where  a.item= '100003-01' ----a.features is null and 

          
          create table save_tmd_feature_item_links_il as  
          select * from 
          
          update tmd_feature_item_links set network_id = null
          
          
          
          select * from application_config
          
          
          where specific_feature_id = 16 and network_id = 3 

          
          select id from tmd_networks where is_stock_management = 1
          
          select * from tmd_sale_codes_var tsc where item_sale_id in ( select id from tmd_item_sales where item_id =43470 )
          
          select * from tpa_specific_features tsf 

          
          select * from tpa_lock_manage tlm 




select * FROM tmd_purchase_prices_var ts where item_id in ( select id from tmd_items where item = '569964-01')

select * from tck_logs order by 1 desc

select * from tmd_orderable_assortments_var  
where item_id in ( select id from tmd_items where item = '569964-01')


select * from tmd_feature_item_links tfil where item_id in ( select id from tmd_items where item = '100003-01')

select * from tmd_items 
where item >=  '218297-01'
order by item 

SELECT (( SELECT count(DISTINCT ti.id) AS count
           FROM tmd_items ti
             JOIN tmd_item_sales tis ON tis.item_id = ti.id
             JOIN tmd_structure_item_links_var tsilv ON tsilv.item_id = tis.item_id AND CURRENT_DATE >= tsilv.start_date AND CURRENT_DATE <= tsilv.end_date
             JOIN tmd_structures ts ON ts.id = tsilv.structure_id
             JOIN tmd_merchandise_structures tms ON tms.id = ts.merchandise_structure_id AND tms.is_default = 1::numeric
             JOIN tmd_sale_prices_var tspv ON tspv.item_sale_id = tis.id AND CURRENT_DATE >= tspv.start_date AND CURRENT_DATE <= tspv.end_date AND (tspv.sale_price_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT 1
                           FROM vpa_parameters vp
                          WHERE vp.parameter_header = tspv.sale_price_type_ph AND vp.is_default = 1::numeric AND vp.parameter_code = tspv.sale_price_type_pc AND vp.num_val_1 = 1::numeric) AS "array"))::numeric[])))
             JOIN tmd_networks tn ON tn.id = tspv.network_id
             JOIN tmd_sale_codes_var tsc ON tsc.item_sale_id = tis.id AND CURRENT_DATE >= tsc.start_date AND CURRENT_DATE <= tsc.end_date AND (tsc.code_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT tp.parameter_code
                           FROM tpa_parameters tp,
                            tpa_parameter_headers tph
                          WHERE tph.id = tp.parameter_header_id AND tph.parameter_header = tsc.code_type_ph AND tp.num_val_5 = 1::numeric) AS "array"))::numeric[])))
             JOIN tmd_vat tv ON tv.id = tspv.vat_id
             JOIN tmd_saleable_assortments tsa ON tsa.item_sale_id = tis.id AND tsa.network_id = tn.id AND tsa.status_pc = 1
          WHERE ti.item::text = ANY (((( SELECT ARRAY( SELECT ti_1.item
                           FROM tmd_items ti_1
                          WHERE ti_1.is_updated = 1::numeric AND ti_1.last_user::text <> 'user_batch'::text
                        UNION
                         SELECT ti_1.item
                           FROM tmd_item_sales tis_1
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                          WHERE tis_1.is_updated = 1::numeric AND tis_1.last_user::text <> 'user_batch'::text
                        UNION
                         SELECT ti_1.item
                           FROM tmd_sale_codes_var tscv
                             JOIN tmd_item_sales tis_1 ON tis_1.id = tscv.item_sale_id
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                             JOIN tpa_parameters tp ON tp.parameter_code = tscv.code_type_pc AND tp.parameter_header_id = (( SELECT tph.id
                                   FROM tpa_parameter_headers tph
                                  WHERE tph.parameter_header = tscv.code_type_ph)) AND tp.num_val_5 = 1::numeric
                          WHERE tscv.is_updated = 1::numeric AND tscv.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= tscv.start_date AND CURRENT_DATE <= tscv.end_date
                        UNION
                         SELECT ti_1.item
                           FROM tmd_sale_prices_var tspv2
                             JOIN tmd_item_sales tis_1 ON tis_1.id = tspv2.item_sale_id
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                          WHERE tspv2.is_sent = 0::numeric AND tspv2.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= tspv2.start_date AND CURRENT_DATE <= tspv2.end_date AND (EXISTS ( SELECT 1
                                   FROM vpa_parameters vp
                                  WHERE vp.parameter_header = tspv2.sale_price_type_ph AND vp.is_default = 1::numeric AND vp.parameter_code = tspv2.sale_price_type_pc AND vp.num_val_1 = 1::numeric))
                        UNION
                         SELECT ti_1.item
                           FROM tmd_structure_item_links_var lnk
                             JOIN tmd_items ti_1 ON ti_1.id = lnk.item_id
                             JOIN tmd_structures ts_1 ON ts_1.id = lnk.structure_id
                             JOIN tmd_merchandise_structures tms_1 ON tms_1.id = ts_1.merchandise_structure_id AND tms_1.is_default = 1::numeric
                          WHERE lnk.is_updated = 1::numeric AND lnk.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= lnk.start_date AND CURRENT_DATE <= lnk.end_date
                        UNION
                         SELECT ti_1.item
                           FROM tmd_feature_item_links tfil
                             JOIN tmd_items ti_1 ON ti_1.id = tfil.item_id
                             JOIN tpa_specific_features tsf ON tsf.id = tfil.specific_feature_id AND tsf.used_scales = 1::numeric AND tsf.is_active = 1::numeric
                          WHERE tfil.is_updated = 1::numeric AND tfil.last_user::text <> 'user_batch'::text) AS "array"))::text[])))) + (( SELECT count(*) AS count
           FROM tin_trace_sale_code_boss
          WHERE tin_trace_sale_code_boss.processing_status = 0 AND tin_trace_sale_code_boss.end_date < CURRENT_DATE)) AS variations_out,
    ( SELECT count(*) AS count
           FROM tin_trace_in tti
          WHERE tti.date_elab = CURRENT_DATE) AS variations_in;

select * from tin_data_item_out

explain
select count(*) from tin_data_item_out tdio   where tsacatrt = 0

order by 1 desc 

where tsacadcre >= current_date --and tsacacexr = '205939-01'
---and tsacatrt = 0
group by tsacasite

select count(*) from tin_data_item_out tdio   where ( tsacadcre >= now() - INTERVAL '90 minutes'  or tsacadmaj >= now() - INTERVAL '90 minutes') 

select *  FROM tpa_specific_features tsf 
 
SELECT tfil.item_id,
            (tp.str_val_1::text || '='::text) || tfil.features_value::text AS stringa,
            vna.network_id
           FROM tpa_specific_features tsf
             JOIN tpa_parameters tp ON tp.parameter_header_id = (( SELECT tpa_parameter_headers.id
                   FROM tpa_parameter_headers
                  WHERE tpa_parameter_headers.parameter_header = 67)) AND tp.num_val_5 = 1::numeric 
                  AND tsf.specific_feature::text = tp.str_val_2::text
             JOIN tmd_feature_item_links tfil ON tfil.specific_feature_id = tsf.id and tfil.item_id in( select id from tmd_items where item = '100003-01')
             JOIN vpa_networks_asc vna ON vna.parent_network_id =
                CASE
                    WHEN COALESCE(tfil.network_id, 0::bigint) = 0 THEN 1::bigint
                    ELSE tfil.network_id
                END
             JOIN tmd_networks tn ON tn.id = vna.network_id AND tn.network_node_type_pc = 4 

select * from tmd_networks where network_node_type_pc = 4
             
select *  FROM tpa_specific_features tsf
             JOIN tpa_parameters tp ON tp.parameter_header_id = (( SELECT tpa_parameter_headers.id
                   FROM tpa_parameter_headers
                  WHERE tpa_parameter_headers.parameter_header = 67)) AND tp.num_val_5 = 1::numeric AND tsf.specific_feature::text = tp.str_val_2::text
                  JOIN tmd_feature_item_links tfil ON tfil.specific_feature_id = tsf.id and tfil.item_id = 43470

select * from vpa_networks_asc;

select * from tmd_sale_codes_var tscv where sale_code = '8032589010103'

select * from tmd_feature_item_links tfml 
where item_id in ( select id from tmd_items where item = '205939-01')


select * from vpa_networks_desc vnd 

 SELECT *
           FROM vmd_feature_replicator vfr
          WHERE vfr.item_id = 43470
          
          AND vfr.network_id = tn.id)) || ';TSACSATTR18='::text) 
          
          

          select * from swdist.application_config
          
          
          select * from tsm_stocks ts  order by 1 desc; 

          
vmd_features_item_links_cash

select count(distinct tdio.tsacacexr ) from tin_data_item_out tdio  where   tsacatrt = 0

tsacatrt <> 0  order by 1 desc

2025-06-17 09:09:17.500	111161


select tsacasite, count(distinct tdio.tsacacexr ) from tin_data_item_out tdio  where tsacatrt = 0 
group by tsacasite

select 'item',count(*)
    from tmd_items ti 
    where is_updated = 1 
    and UPPER(last_user) = COALESCE(NULL, UPPER(last_user)) 
    and 2 = 2 
    union 
    select 'tmd_item_sales',count(*)
    from tmd_item_sales tis 
    inner join tmd_items ti on ti.id = tis.item_id 
    where tis.is_updated = 1 
    and UPPER(tis.last_user) = COALESCE(NULL, UPPER(tis.last_user)) 
    and 2 = 2 
    union 
    select 'tmd_logistic_units',count(*)
    from tmd_logistic_units tlu 
    inner join tmd_item_logistics til on til.id = tlu.item_logistic_id 
    inner join tmd_items ti on ti.id = til.item_id 
    where tlu.is_updated = 1 
    and tlu.logistic_unit_pc = '41' 
    and UPPER(tlu.last_user) = COALESCE(NULL, UPPER(tlu.last_user)) 
    and 2 = 2 
    union 
    select 'tmd_sale_codes_var',count(*)
    from tmd_sale_codes_var tscv 
    inner join tmd_item_sales tis on tis.id = tscv.item_sale_id 
    inner join tmd_items ti on ti.id = tis.item_id 
    inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
                                    tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
                                    tp.num_val_5 = 1 
    where tscv.is_updated = 1 
    and UPPER(tscv.last_user) = COALESCE(NULL, UPPER(tscv.last_user)) 
    and 2 = 2 
    and current_date between tscv.start_date and tscv.end_date 
    union 
    select 'tmd_kit_components',count(*)
    from tmd_items ti 
    inner join tmd_kit_components tkc on tkc.item_id=ti.id 
    where 2 = 2 
    and tkc.is_updated=1 
    and UPPER(tkc.last_user) = COALESCE(NULL, UPPER(tkc.last_user)) 
    union 
    select 'tmd_sale_prices_var',count(*)
    from tmd_sale_prices_var tspv2 
    inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
    inner join tmd_items ti on ti.id = tis.item_id 
    inner join vpa_parameters vp on vp.parameter_header =tspv2.sale_price_type_ph and vp.is_default =1 and vp.parameter_code = tspv2.sale_price_type_pc and vp.num_val_1=1 
    where 2 = 2 
    and tspv2.is_sent = 0 
    and UPPER(tspv2.last_user) = COALESCE(NULL, UPPER(tspv2.last_user)) 
    and current_date between tspv2.start_date and tspv2.end_date 
    union 
    select 'tmd_structure_item_links_var',count(*)
    from tmd_structure_item_links_var lnk 
    inner join tmd_items ti on ti.id=lnk.item_id 
    inner join tmd_structures ts on ts.id = lnk.structure_id 
    inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
    where 2 = 2 
    and lnk.is_updated=1 
    and UPPER(lnk.last_user) = COALESCE(NULL, UPPER(lnk.last_user)) 
    and current_date between lnk.start_date and lnk.end_date 
    union 
    select 'tmd_feature_item_links',count(*)
    from tmd_feature_item_links tfil 
    inner join tmd_items ti on ti.id = tfil.item_id 
    inner join tpa_specific_features tsf on tsf.id = tfil.specific_feature_id and tsf.used_scales = 1 and tsf.is_active = 1 
    where 2 = 2 
    and tfil.is_updated = 1 
    and UPPER(tfil.last_user) = COALESCE(NULL, UPPER(tfil.last_user))     
    union 
    select 'tin_data_item_in',count(*)
    from tin_data_item_in td 
    inner join tmd_items ti on ti.item = td.item 
    where 2 = 1 
    and processing_step3 = 1 
    and processing_step4 = 0 
  ---  and td.transaction_code=#TRANSACTION# 
    union 
    select 'tmd_sale_codes_var',count(*)
    from tmd_sale_codes_var tscv inner join tmd_item_sales tis on tscv.item_sale_id = tis.id 
    inner join tmd_items ti on ti.id = tis.item_id 
    inner join tpa_parameters tp on tp.parameter_code = tscv.code_type_pc and 
                                    tp.parameter_header_id = (select id from tpa_parameter_headers tph where tph.parameter_header = tscv.code_type_ph) and 
                                    tp.num_val_5 = 1 
    where tscv.is_updated = 1 
    and tscv.last_user = 'user_batch' 
    and 2 = 1 
    and current_date between tscv.start_date and tscv.end_date 
    union 
    select 'tmd_sale_prices_var',count(*)
    from tmd_sale_prices_var tspv2 
    inner join tmd_item_sales tis on tis.id = tspv2.item_sale_id 
    inner join tmd_items ti on ti.id = tis.item_id 
    where 2 = 1 
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
    select 'tmd_structure_item_links_var',count(*)
    from tmd_structure_item_links_var lnk 
    inner join tmd_items ti on ti.id = lnk.item_id 
    inner join tmd_structures ts on ts.id = lnk.structure_id 
    inner join tmd_merchandise_structures tms on tms.id = ts.merchandise_structure_id and tms.is_default =1 
    where 2 = 1 
    and lnk.is_updated=1 
    and lnk.last_user = 'user_batch' 
    and current_date between lnk.start_date and lnk.end_date 
    
    
    select  ti.*,inventory_id
from     tsm_inventories ti,
          tsm_inventory_steps ts,
          tmd_networks tn
where     ti.id = ts.inventory_id
and     ts.inventory_step_pc = 2
and     ts.step_status = 0
and     ti.network_id = tn.id
and     tn.network_cash  = '7953'

select * from  tsm_inventories ti  order by 1 desc;

select * from  tsm_inventories_detail tid  where inventory_id = 235  order by 1 desc;  

select * from tmd_sale_codes tsc where item_sale_id in ( select id from tmd_item_sales where item_id in (select tid.item_id   from  tsm_inventories_detail tid  where inventory_id = 235  order by 1 desc) )
and current_date between start_date and end_date and code_type_pc = 5

select * from  tsm_inventory_inputs tii    order by 1 desc;

select * from  tsm_inventory_perimeter tip     order by 1 desc;



select * from  tin_data_terminal_detail tdtd    order by 1 desc ;



select * FROM tmd_purchase_prices_var ts where item_id in ( select id from tmd_items where item = '569964-01')

select * from tck_logs order by 1 desc

select * from tmd_orderable_assortments_var  
where item_id in ( select id from tmd_items where item = '569964-01')


select * from tmd_feature_item_links tfil where item_id in ( select id from tmd_items where item = '100003-01')

select * from tmd_items 
where item >=  '218297-01'
order by item 

SELECT (( SELECT count(DISTINCT ti.id) AS count
           FROM tmd_items ti
             JOIN tmd_item_sales tis ON tis.item_id = ti.id
             JOIN tmd_structure_item_links_var tsilv ON tsilv.item_id = tis.item_id AND CURRENT_DATE >= tsilv.start_date AND CURRENT_DATE <= tsilv.end_date
             JOIN tmd_structures ts ON ts.id = tsilv.structure_id
             JOIN tmd_merchandise_structures tms ON tms.id = ts.merchandise_structure_id AND tms.is_default = 1::numeric
             JOIN tmd_sale_prices_var tspv ON tspv.item_sale_id = tis.id AND CURRENT_DATE >= tspv.start_date AND CURRENT_DATE <= tspv.end_date AND (tspv.sale_price_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT 1
                           FROM vpa_parameters vp
                          WHERE vp.parameter_header = tspv.sale_price_type_ph AND vp.is_default = 1::numeric AND vp.parameter_code = tspv.sale_price_type_pc AND vp.num_val_1 = 1::numeric) AS "array"))::numeric[])))
             JOIN tmd_networks tn ON tn.id = tspv.network_id
             JOIN tmd_sale_codes_var tsc ON tsc.item_sale_id = tis.id AND CURRENT_DATE >= tsc.start_date AND CURRENT_DATE <= tsc.end_date AND (tsc.code_type_pc::numeric = ANY (((( SELECT ARRAY( SELECT tp.parameter_code
                           FROM tpa_parameters tp,
                            tpa_parameter_headers tph
                          WHERE tph.id = tp.parameter_header_id AND tph.parameter_header = tsc.code_type_ph AND tp.num_val_5 = 1::numeric) AS "array"))::numeric[])))
             JOIN tmd_vat tv ON tv.id = tspv.vat_id
             JOIN tmd_saleable_assortments tsa ON tsa.item_sale_id = tis.id AND tsa.network_id = tn.id AND tsa.status_pc = 1
          WHERE ti.item::text = ANY (((( SELECT ARRAY( SELECT ti_1.item
                           FROM tmd_items ti_1
                          WHERE ti_1.is_updated = 1::numeric AND ti_1.last_user::text <> 'user_batch'::text
                        UNION
                         SELECT ti_1.item
                           FROM tmd_item_sales tis_1
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                          WHERE tis_1.is_updated = 1::numeric AND tis_1.last_user::text <> 'user_batch'::text
                        UNION
                         SELECT ti_1.item
                           FROM tmd_sale_codes_var tscv
                             JOIN tmd_item_sales tis_1 ON tis_1.id = tscv.item_sale_id
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                             JOIN tpa_parameters tp ON tp.parameter_code = tscv.code_type_pc AND tp.parameter_header_id = (( SELECT tph.id
                                   FROM tpa_parameter_headers tph
                                  WHERE tph.parameter_header = tscv.code_type_ph)) AND tp.num_val_5 = 1::numeric
                          WHERE tscv.is_updated = 1::numeric AND tscv.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= tscv.start_date AND CURRENT_DATE <= tscv.end_date
                        UNION
                         SELECT ti_1.item
                           FROM tmd_sale_prices_var tspv2
                             JOIN tmd_item_sales tis_1 ON tis_1.id = tspv2.item_sale_id
                             JOIN tmd_items ti_1 ON ti_1.id = tis_1.item_id
                          WHERE tspv2.is_sent = 0::numeric AND tspv2.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= tspv2.start_date AND CURRENT_DATE <= tspv2.end_date AND (EXISTS ( SELECT 1
                                   FROM vpa_parameters vp
                                  WHERE vp.parameter_header = tspv2.sale_price_type_ph AND vp.is_default = 1::numeric AND vp.parameter_code = tspv2.sale_price_type_pc AND vp.num_val_1 = 1::numeric))
                        UNION
                         SELECT ti_1.item
                           FROM tmd_structure_item_links_var lnk
                             JOIN tmd_items ti_1 ON ti_1.id = lnk.item_id
                             JOIN tmd_structures ts_1 ON ts_1.id = lnk.structure_id
                             JOIN tmd_merchandise_structures tms_1 ON tms_1.id = ts_1.merchandise_structure_id AND tms_1.is_default = 1::numeric
                          WHERE lnk.is_updated = 1::numeric AND lnk.last_user::text <> 'user_batch'::text AND CURRENT_DATE >= lnk.start_date AND CURRENT_DATE <= lnk.end_date
                        UNION
                         SELECT ti_1.item
                           FROM tmd_feature_item_links tfil
                             JOIN tmd_items ti_1 ON ti_1.id = tfil.item_id
                             JOIN tpa_specific_features tsf ON tsf.id = tfil.specific_feature_id AND tsf.used_scales = 1::numeric AND tsf.is_active = 1::numeric
                          WHERE tfil.is_updated = 1::numeric AND tfil.last_user::text <> 'user_batch'::text) AS "array"))::text[])))) + (( SELECT count(*) AS count
           FROM tin_trace_sale_code_boss
          WHERE tin_trace_sale_code_boss.processing_status = 0 AND tin_trace_sale_code_boss.end_date < CURRENT_DATE)) AS variations_out,
    ( SELECT count(*) AS count
           FROM tin_trace_in tti
          WHERE tti.date_elab = CURRENT_DATE) AS variations_in;

select * from tin_data_item_out

explain
select count(*) from tin_data_item_out tdio   where tsacatrt = 0

order by 1 desc 

where tsacadcre >= current_date --and tsacacexr = '205939-01'
---and tsacatrt = 0
group by tsacasite

select count(*) from tin_data_item_out tdio   where ( tsacadcre >= now() - INTERVAL '90 minutes'  or tsacadmaj >= now() - INTERVAL '90 minutes') 

select *  FROM tpa_specific_features tsf 
 
SELECT tfil.item_id,
            (tp.str_val_1::text || '='::text) || tfil.features_value::text AS stringa,
            vna.network_id
           FROM tpa_specific_features tsf
             JOIN tpa_parameters tp ON tp.parameter_header_id = (( SELECT tpa_parameter_headers.id
                   FROM tpa_parameter_headers
                  WHERE tpa_parameter_headers.parameter_header = 67)) AND tp.num_val_5 = 1::numeric 
                  AND tsf.specific_feature::text = tp.str_val_2::text
             JOIN tmd_feature_item_links tfil ON tfil.specific_feature_id = tsf.id and tfil.item_id in( select id from tmd_items where item = '100003-01')
             JOIN vpa_networks_asc vna ON vna.parent_network_id =
                CASE
                    WHEN COALESCE(tfil.network_id, 0::bigint) = 0 THEN 1::bigint
                    ELSE tfil.network_id
                END
             JOIN tmd_networks tn ON tn.id = vna.network_id AND tn.network_node_type_pc = 4 

select * from tmd_networks where network_node_type_pc = 4
             
select *  FROM tpa_specific_features tsf
             JOIN tpa_parameters tp ON tp.parameter_header_id = (( SELECT tpa_parameter_headers.id
                   FROM tpa_parameter_headers
                  WHERE tpa_parameter_headers.parameter_header = 67)) AND tp.num_val_5 = 1::numeric AND tsf.specific_feature::text = tp.str_val_2::text
                  JOIN tmd_feature_item_links tfil ON tfil.specific_feature_id = tsf.id and tfil.item_id = 43470

select * from vpa_networks_asc;

select * from tmd_sale_codes_var tscv where sale_code = '8032589010103'

select * from tmd_feature_item_links tfml 
where item_id in ( select id from tmd_items where item = '205939-01')


select * from vpa_networks_desc vnd 

 SELECT *
           FROM vmd_feature_replicator vfr
          WHERE vfr.item_id = 43470
          
          AND vfr.network_id = tn.id)) || ';TSACSATTR18='::text) 
          
          

          select * from swdist.application_config
          
          
          select * from tsm_stocks ts  order by 1 desc; 

          