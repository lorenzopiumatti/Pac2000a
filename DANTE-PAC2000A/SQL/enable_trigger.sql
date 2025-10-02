------------------
SELECT 'ALTER TABLE ' || c.relname || ' ENABLE TRIGGER ' || tg.tgname || ';'
FROM pg_trigger tg
JOIN pg_class c ON tg.tgrelid = c.oid
JOIN pg_proc p ON tg.tgfoid = p.oid
WHERE p.proname in  ('fn_ref_anag_table','fn_trace_items_ext_file_out');

-----------------------------------------

ALTER TABLE tmd_orderable_assortments ENABLE TRIGGER tmd_orderable_assortments_ref_anag_ins;
ALTER TABLE tmd_orderable_assortments ENABLE TRIGGER tmd_orderable_assortments_ref_anag_upd;
ALTER TABLE tmd_purchase_prices ENABLE TRIGGER tmd_purchase_prices_ref_anag_ins;
ALTER TABLE tmd_purchase_prices ENABLE TRIGGER tmd_purchase_prices_ref_anag_upd;
ALTER TABLE tmd_sale_codes ENABLE TRIGGER tmd_sale_codes_ref_anag_ins;
ALTER TABLE tmd_sale_codes ENABLE TRIGGER tmd_sale_codes_ref_anag_upd;
ALTER TABLE tmd_sale_prices ENABLE TRIGGER tmd_sale_prices_ref_anag_ins;
ALTER TABLE tmd_items ENABLE TRIGGER tmd_items_tr3;
ALTER TABLE tmd_item_sales ENABLE TRIGGER tmd_item_sales_tr2;
ALTER TABLE tmd_sale_prices_var ENABLE TRIGGER tmd_sale_prices_var_tr2;
ALTER TABLE tmd_item_logistics ENABLE TRIGGER tmd_item_logistics_tr2;
ALTER TABLE tmd_logistic_unit_codes ENABLE TRIGGER tmd_logistic_unit_codes_tr2;
ALTER TABLE tmd_logistic_units ENABLE TRIGGER tmd_logistic_units_tr2;
ALTER TABLE tmd_kit_components ENABLE TRIGGER tmd_kit_components_tr3;
ALTER TABLE tmd_orderable_assortments_var ENABLE TRIGGER tmd_orderable_assortments_var_tr2;
ALTER TABLE tmd_sale_codes_var ENABLE TRIGGER tmd_sale_codes_var_tr4;
ALTER TABLE tmd_purchase_prices_var ENABLE TRIGGER tmd_purchase_prices_var_tr2;
ALTER TABLE tmd_saleable_assortments ENABLE TRIGGER tmd_saleable_assortments_tr2;
ALTER TABLE tmd_structure_item_links_var ENABLE TRIGGER tmd_structure_item_links_var_tr2;
ALTER TABLE tmd_supplier_item_codes ENABLE TRIGGER tmd_supplier_item_codes_tr2;
ALTER TABLE tmd_items ENABLE TRIGGER tmd_items_tr2;
ALTER TABLE tmd_item_sales ENABLE TRIGGER tmd_item_sales_tr1;
ALTER TABLE tmd_sale_prices_var ENABLE TRIGGER tmd_sale_prices_var_tr1;
ALTER TABLE tmd_item_logistics ENABLE TRIGGER tmd_item_logistics_tr1;
ALTER TABLE tmd_logistic_unit_codes ENABLE TRIGGER tmd_logistic_unit_codes_tr1;
ALTER TABLE tmd_logistic_units ENABLE TRIGGER tmd_logistic_units_tr1;
ALTER TABLE tmd_kit_components ENABLE TRIGGER tmd_kit_components_tr2;
ALTER TABLE tmd_orderable_assortments_var ENABLE TRIGGER tmd_orderable_assortments_var_tr1;
ALTER TABLE tmd_sale_codes_var ENABLE TRIGGER tmd_sale_codes_var_tr3;
ALTER TABLE tmd_purchase_prices_var ENABLE TRIGGER tmd_purchase_prices_var_tr1;
ALTER TABLE tmd_saleable_assortments ENABLE TRIGGER tmd_saleable_assortments_tr1;
ALTER TABLE tmd_structure_item_links_var ENABLE TRIGGER tmd_structure_item_links_var_tr1;
ALTER TABLE tmd_supplier_item_codes ENABLE TRIGGER tmd_supplier_item_codes_tr1;
ALTER TABLE tmd_feature_item_links ENABLE TRIGGER tmd_feature_item_links_tr3;
ALTER TABLE tmd_feature_item_links ENABLE TRIGGER tmd_feature_item_links_tr2;
ALTER TABLE tmd_sale_prices ENABLE TRIGGER tmd_sale_prices_ref_anag_upd;

-------------------------------------------------

update tmd_sale_prices tsd set last_user = 'user_batch'
where tsd.item_sale_id in (
SELECT tia.item_sale_id frOM   tmd_item_anag tia  
WHERE  tia.sale_price IS NULL 
  AND  EXISTS (
       SELECT 1 
       FROM tmd_sale_prices tsp 
       WHERE tsp.network_id = tia.network_id  
        AND tsp.item_sale_id = tia.item_sale_id
        and current_DAte between tsp.start_date and tsp.end_date
  			)
  		)
  
update tmd_purchase_prices tsd set last_user = 'user_batch'
where tsd.item_id in (
  SELECT item_id frOM   tmd_item_anag tia  
WHERE  tia.purchase_price  IS NULL 
  AND  EXISTS (
       SELECT 1 
       FROM tmd_purchase_prices   tsp 
       WHERE tsp.network_id = tia.network_id  
        AND tsp.item_id = tia.item_id
        and current_DAte between tsp.start_date and tsp.end_date
  ) )
  
  
  




