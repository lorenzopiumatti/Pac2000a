
SELECT 'ALTER TABLE ' || c.relname || ' DISABLE TRIGGER ' || tg.tgname || ';'
FROM pg_trigger tg
JOIN pg_class c ON tg.tgrelid = c.oid
JOIN pg_proc p ON tg.tgfoid = p.oid
WHERE p.proname in  ('fn_ref_anag_table','fn_trace_items_ext_file_out');

--------------------------------------------------

ALTER TABLE tmd_orderable_assortments DISABLE TRIGGER tmd_orderable_assortments_ref_anag_ins;
ALTER TABLE tmd_orderable_assortments DISABLE TRIGGER tmd_orderable_assortments_ref_anag_upd;
ALTER TABLE tmd_purchase_prices DISABLE TRIGGER tmd_purchase_prices_ref_anag_ins;
ALTER TABLE tmd_purchase_prices DISABLE TRIGGER tmd_purchase_prices_ref_anag_upd;
ALTER TABLE tmd_sale_codes DISABLE TRIGGER tmd_sale_codes_ref_anag_ins;
ALTER TABLE tmd_sale_codes DISABLE TRIGGER tmd_sale_codes_ref_anag_upd;
ALTER TABLE tmd_sale_prices DISABLE TRIGGER tmd_sale_prices_ref_anag_ins;
ALTER TABLE tmd_items DISABLE TRIGGER tmd_items_tr3;
ALTER TABLE tmd_item_sales DISABLE TRIGGER tmd_item_sales_tr2;
ALTER TABLE tmd_sale_prices_var DISABLE TRIGGER tmd_sale_prices_var_tr2;
ALTER TABLE tmd_item_logistics DISABLE TRIGGER tmd_item_logistics_tr2;
ALTER TABLE tmd_logistic_unit_codes DISABLE TRIGGER tmd_logistic_unit_codes_tr2;
ALTER TABLE tmd_logistic_units DISABLE TRIGGER tmd_logistic_units_tr2;
ALTER TABLE tmd_kit_components DISABLE TRIGGER tmd_kit_components_tr3;
ALTER TABLE tmd_orderable_assortments_var DISABLE TRIGGER tmd_orderable_assortments_var_tr2;
ALTER TABLE tmd_sale_codes_var DISABLE TRIGGER tmd_sale_codes_var_tr4;
ALTER TABLE tmd_purchase_prices_var DISABLE TRIGGER tmd_purchase_prices_var_tr2;
ALTER TABLE tmd_saleable_assortments DISABLE TRIGGER tmd_saleable_assortments_tr2;
ALTER TABLE tmd_structure_item_links_var DISABLE TRIGGER tmd_structure_item_links_var_tr2;
ALTER TABLE tmd_supplier_item_codes DISABLE TRIGGER tmd_supplier_item_codes_tr2;
ALTER TABLE tmd_items DISABLE TRIGGER tmd_items_tr2;
ALTER TABLE tmd_item_sales DISABLE TRIGGER tmd_item_sales_tr1;
ALTER TABLE tmd_sale_prices_var DISABLE TRIGGER tmd_sale_prices_var_tr1;
ALTER TABLE tmd_item_logistics DISABLE TRIGGER tmd_item_logistics_tr1;
ALTER TABLE tmd_logistic_unit_codes DISABLE TRIGGER tmd_logistic_unit_codes_tr1;
ALTER TABLE tmd_logistic_units DISABLE TRIGGER tmd_logistic_units_tr1;
ALTER TABLE tmd_kit_components DISABLE TRIGGER tmd_kit_components_tr2;
ALTER TABLE tmd_orderable_assortments_var DISABLE TRIGGER tmd_orderable_assortments_var_tr1;
ALTER TABLE tmd_sale_codes_var DISABLE TRIGGER tmd_sale_codes_var_tr3;
ALTER TABLE tmd_purchase_prices_var DISABLE TRIGGER tmd_purchase_prices_var_tr1;
ALTER TABLE tmd_saleable_assortments DISABLE TRIGGER tmd_saleable_assortments_tr1;
ALTER TABLE tmd_structure_item_links_var DISABLE TRIGGER tmd_structure_item_links_var_tr1;
ALTER TABLE tmd_supplier_item_codes DISABLE TRIGGER tmd_supplier_item_codes_tr1;
ALTER TABLE tmd_feature_item_links DISABLE TRIGGER tmd_feature_item_links_tr3;
ALTER TABLE tmd_feature_item_links DISABLE TRIGGER tmd_feature_item_links_tr2;
ALTER TABLE tmd_sale_prices DISABLE TRIGGER tmd_sale_prices_ref_anag_upd;

------------------------------------------------------------------------------------------------------------