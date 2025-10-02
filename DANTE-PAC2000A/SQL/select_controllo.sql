----------------------------------------------------
---------------------------------------------------  controllo + barcode attivi per prodotto 

select item_sale_id, is_label , network_id, count(sale_code)  from tmd_sale_codes_var tscv 
where is_label = 1 
group by item_sale_id,is_label, network_id
having count(*) > 1

select item_sale_id, is_label , network_id, count(sale_code)  from tmd_sale_codes tscv 
where is_label = 1 
group by item_sale_id,is_label, network_id
having count(*) > 1

----------------------------------------------------
---------------------------------------------------  controllo + aricoli  attivi per barcode  

select sale_code, count(item_Sale_id) from tmd_sale_codes_var tscv  where is_label = 1
group by sale_code
having count(item_Sale_id) > 1

select sale_code, count(item_Sale_id) from tmd_sale_codes tscv  where is_label = 0
group by sale_code
having count(item_Sale_id) > 1

----------------------------------------------------
---------------------------------------------------  controllo + forn ass attivi per prodotto 

select item_id, operational_agreement_id,count(*)---network_id,count(*)
from tmd_orderable_assortments_var toav 
where main_supplier = 1 
group by item_id, operational_agreement_id---,network_id 
having count(*)  >1 

select item_id, operational_agreement_id,count(*)---network_id,count(*)
from tmd_orderable_assortments toav 
where main_supplier = 1 
group by item_id, operational_agreement_id---,network_id 
having count(*)  >1 

-----------------------------------------------------
--------------------------------------------------------   controllo data fine < data inizio  
 
select 'tmd_orderable_assortments_var' tab, count(*) conta from tmd_orderable_assortments_var tppv    where end_date < start_date 
union
select 'tmd_orderable_assortments' tab, count(*) conta from tmd_orderable_assortments tppv    where end_date < start_date 
union
select 'tmd_purchase_prices_var' tab, count(*) conta from tmd_purchase_prices_var tppv    where end_date < start_date 
union
select 'tmd_purchase_prices' tab, count(*) conta from tmd_purchase_prices tspv  where end_date < start_date 
union
select 'tmd_sale_prices_var' tab, count(*) conta from tmd_sale_prices_var tspv  where end_date < start_date 
union
select 'tmd_sale_prices' tab, count(*) conta from tmd_sale_prices tspv  where end_date < start_date 
union
select 'tmd_sale_codes_var' tab, count(*) conta from tmd_sale_codes_var tspv  where end_date < start_date 
union
select 'tmd_sale_codes' tab, count(*) conta from tmd_sale_codes tspv  where end_date < start_date 















SELECT count(distinct item) , processing_step1, processing_step2, processing_step3
 from tin_data_item_in tdii where creation_date >= current_date-6
and file_name like 'SPH%' collate CASE_LIKE
group by processing_step1, processing_step2, processing_step3

select * from tpa_lock_manage tlm 


select count(distinct tsacacexr) from boom.tin_data_item_out where tsacadcre >= current_date-2


select   count(distinct item ), processing_step1, processing_step2, processing_step3 , substr(error_message,1,30)
        from tin_data_item_in tdii where creation_date >= current_date-5  
        group by processing_step1, processing_step2, processing_step3, substr(error_message,1,30)
        
        
select count(distinct item ),count(*), file_name, processing_step1, processing_step2, processing_step3 ,processing_step4, processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature
from tin_data_item_in tdii where creation_date >= current_date-5  ---and file_name like 'SPH%' collate CASE_LIKE
and  processing_step1 =1 and processing_step2 = 1 and processing_step3 = 0 
group by file_name, processing_step1, processing_step2, processing_step3,processing_step4, processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature
   


select count(*) from tmd_items where update_date >= current_date - 2


SELECT tin_data_item_in.id,
    tin_data_item_in.transaction_code,
    ltrim("substring"(tin_data_item_in.buffer_str::text, 1, 6), '0'::text) AS dest,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 382, 2) = '99'::text THEN 'R'::text
            ELSE "substring"(tin_data_item_in.buffer_str::text, 7, 1)
        END AS div_mag_rif,
    "substring"(tin_data_item_in.buffer_str::text, 8, 7) AS cod_art,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 15, 1) = '0'::text THEN "substring"(tin_data_item_in.buffer_str::text, 16, 2)
            ELSE "substring"(tin_data_item_in.buffer_str::text, 15, 3)
        END AS differ,
    "substring"(tin_data_item_in.buffer_str::text, 18, 35) AS long_desc,
    "substring"(tin_data_item_in.buffer_str::text, 53, 1) AS art_type,
    "substring"(tin_data_item_in.buffer_str::text, 58, 2) AS merch_l1,
    "substring"(tin_data_item_in.buffer_str::text, 64, 2) AS merch_l2,
    "substring"(tin_data_item_in.buffer_str::text, 70, 2) AS merch_l3,
    "substring"(tin_data_item_in.buffer_str::text, 76, 2) AS merch_l4,
    "substring"(tin_data_item_in.buffer_str::text, 78, 5) AS merch_l5,
    "substring"(tin_data_item_in.buffer_str::text, 83, 8) AS date_cre_art,
    "substring"(tin_data_item_in.buffer_str::text, 91, 3) AS vat_code,
    "substring"(tin_data_item_in.buffer_str::text, 94, 3) AS vat_exception,
    "substring"(tin_data_item_in.buffer_str::text, 97, 2) AS vat_section,
    "substring"(tin_data_item_in.buffer_str::text, 99, 1) AS multiple_vat_flag,
    "substring"(tin_data_item_in.buffer_str::text, 100, 1) AS brand_flag,
    "substring"(tin_data_item_in.buffer_str::text, 101, 2) AS sale_unit,
    "substring"(tin_data_item_in.buffer_str::text, 103, 9) AS net_weigth,
    "substring"(tin_data_item_in.buffer_str::text, 112, 9) AS gross_weigth,
    "substring"(tin_data_item_in.buffer_str::text, 121, 1) AS article_weigth_flag,
    "substring"(tin_data_item_in.buffer_str::text, 122, 7) AS packaging_associated,
    "substring"(tin_data_item_in.buffer_str::text, 129, 3) AS muv_code,
    "substring"(tin_data_item_in.buffer_str::text, 132, 5) AS expiration_days,
    "substring"(tin_data_item_in.buffer_str::text, 137, 2) AS commercial_department,
    "substring"(tin_data_item_in.buffer_str::text, 139, 2) AS unit_of_measure,
    "substring"(tin_data_item_in.buffer_str::text, 141, 1) AS cv_ratio_type,
    "substring"(tin_data_item_in.buffer_str::text, 142, 7) AS cv_ratio,
    "substring"(tin_data_item_in.buffer_str::text, 149, 5) AS shop_weigth_loss,
    "substring"(tin_data_item_in.buffer_str::text, 154, 3) AS cost_center,
    "substring"(tin_data_item_in.buffer_str::text, 157, 6) AS supplier_code,
    "substring"(tin_data_item_in.buffer_str::text, 163, 2) AS differ_supplier,
    "substring"(tin_data_item_in.buffer_str::text, 165, 1) AS assortment_status,
    "substring"(tin_data_item_in.buffer_str::text, 166, 20) AS receipt_destination,
    "substring"(tin_data_item_in.buffer_str::text, 186, 1) AS seasonality,
    "substring"(tin_data_item_in.buffer_str::text, 187, 5) AS expiration_days2,
    "substring"(tin_data_item_in.buffer_str::text, 192, 1) AS warehouse_assortment_status,
    "substring"(tin_data_item_in.buffer_str::text, 193, 6) AS order_supplier_code,
    "substring"(tin_data_item_in.buffer_str::text, 199, 2) AS diff_order_supplier_code,
    "substring"(tin_data_item_in.buffer_str::text, 201, 15) AS item_code_supplier,
    "substring"(tin_data_item_in.buffer_str::text, 216, 35) AS desc_item_code_supplier,
    "substring"(tin_data_item_in.buffer_str::text, 251, 6) AS invoice_supplier_code,
    "substring"(tin_data_item_in.buffer_str::text, 257, 2) AS diff_invoice_supplier_code,
    "substring"(tin_data_item_in.buffer_str::text, 259, 15) AS item_code_supplier_invoice,
    "substring"(tin_data_item_in.buffer_str::text, 274, 2) AS unit_of_measure_supplier_order,
    "substring"(tin_data_item_in.buffer_str::text, 276, 9) AS min_qty_supplier_order,
    "substring"(tin_data_item_in.buffer_str::text, 285, 9) AS max_qty_supplier_order,
    "substring"(tin_data_item_in.buffer_str::text, 294, 5) AS supplier_packaging,
    "substring"(tin_data_item_in.buffer_str::text, 299, 1) AS status_article_supplier,
    "substring"(tin_data_item_in.buffer_str::text, 300, 1) AS purchast_list_type,
    "substring"(tin_data_item_in.buffer_str::text, 301, 6) AS list_number,
    "substring"(tin_data_item_in.buffer_str::text, 307, 2) AS diff_list,
    "substring"(tin_data_item_in.buffer_str::text, 309, 2) AS list_sequence,
    "substring"(tin_data_item_in.buffer_str::text, 311, 8) AS start_date_list,
    "substring"(tin_data_item_in.buffer_str::text, 319, 8) AS end_date_list,
    "substring"(tin_data_item_in.buffer_str::text, 327, 11) AS net_cost,
    "substring"(tin_data_item_in.buffer_str::text, 340, 1) AS public_list_type,
    "substring"(tin_data_item_in.buffer_str::text, 341, 6) AS public_list_number,
    "substring"(tin_data_item_in.buffer_str::text, 347, 8) AS start_date_pub_list,
    "substring"(tin_data_item_in.buffer_str::text, 355, 8) AS end_date_pub_list,
    "substring"(tin_data_item_in.buffer_str::text, 363, 1) AS cv_ratio_pub_type,
    "substring"(tin_data_item_in.buffer_str::text, 364, 7) AS cv_ratio_pub,
    "substring"(tin_data_item_in.buffer_str::text, 371, 11) AS public_price,
    "substring"(tin_data_item_in.buffer_str::text, 382, 2) AS virtual_warehouse_cod,
    "substring"(tin_data_item_in.buffer_str::text, 384, 8) AS send_date,
    "substring"(tin_data_item_in.buffer_str::text, 392, 6) AS send_time,
    "substring"(tin_data_item_in.buffer_str::text, 398, 10) AS send_user,
    ''::text AS currency,
    btrim("substring"(tin_data_item_in.buffer_str::text, 413, 2)) AS packaging_component_heigth,
    btrim("substring"(tin_data_item_in.buffer_str::text, 415, 2)) AS packaging_component_width,
    btrim("substring"(tin_data_item_in.buffer_str::text, 417, 2)) AS packaging_component_length,
    "substring"(tin_data_item_in.buffer_str::text, 419, 1) AS mov_sale_unit,
    btrim("substring"(tin_data_item_in.buffer_str::text, 420, 5)) AS packaging_sale_unit,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 444, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS recommended_flag,
    "substring"(tin_data_item_in.buffer_str::text, 428, 1) AS brand_class,
    "substring"(tin_data_item_in.buffer_str::text, 429, 5) AS reclass_cod1,
    "substring"(tin_data_item_in.buffer_str::text, 434, 5) AS reclass_cod2,
    "substring"(tin_data_item_in.buffer_str::text, 439, 5) AS reclass_cod3,
    "substring"(tin_data_item_in.buffer_str::text, 466, 1) AS seral_number_pub_block,
    ''::text AS ecommerce_hg_min,
    ''::text AS ecommerce_hg_packaging,
    ''::text AS ecommerce_hg_piece,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 454, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS articolo_web,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 459, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS articolo_detraibile,
    "substring"(tin_data_item_in.buffer_str::text, 464, 2) AS tipo_spesa,
    "substring"(tin_data_item_in.buffer_str::text, 467, 8) AS data_ultima_vendita,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 475, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS controllo_antitaccheggio,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 480, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS qta_vendita_obbligatoria,
        CASE
            WHEN "substring"(tin_data_item_in.buffer_str::text, 485, 5) = 'True'::text THEN 'S'::text
            ELSE 'N'::text
        END AS articolo_celiachia
   FROM tin_data_item_in
  WHERE tin_data_item_in.source_type_pc = 110 AND tin_data_item_in.processing_step1 = 1::numeric AND tin_data_item_in.processing_step2 = 8::numeric; 





