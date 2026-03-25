-- DROP FUNCTION boom.fn_enrich_tin_data_item_in(int4, int4);

CREATE OR REPLACE FUNCTION boom.fn_enrich_tin_data_item_in(limit_filter integer, source_type_pc_filter integer)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
processed_rows bigint := 0;
BEGIN
INSERT INTO boom.tin_data_item_in(
    id,
    transaction_code,
    network,
    item,
    sale_vat,
    structure_code,
    item_category_pc,
    pieces_pack,
    sale_price,
    unit_sale_price_pc,
    start_date_sale,
    end_date_sale,
    item_sale,
    description_item,
    item_type_pc,
    description_sale,
    stock_unit_pc,
    brand_pc,
    supplier_expiry_days,
    purchase_vat,
    is_local,
    weight_unit_measure_sale_pc,
    val_weight_sale,
    unit_measure_pc,
    medium_weight,
    supplier_code,
    operational_agreement,
    main_supplier,
    min_order,
    max_order,
    assortment_status_pc,
    delivery_status_pc,
    supplier_item_1,
    supplier_code_type_1,
    description_supplier_item_1,
    supplier_item_2,
    supplier_code_type_2,
    purchase_price,
    unit_purchase_price_pc,
    start_date_purchase,
    end_date_purchase,
    attribute_value_1,
    attribute_value_2,
    attribute_value_3,
    attribute_value_4,
    attribute_value_5,
    attribute_value_6,
    attribute_value_7,
    attribute_value_8,
    attribute_value_9,
    attribute_value_10,
    attribute_value_11,
    attribute_value_12,
    attribute_value_13,
    attribute_value_14,
    attribute_value_15,
    is_multi_vat,
    processing_step2,
    processing_step3,
    processing_step4,
    processing_item,
    processing_sale_code,
    processing_assortment,
    processing_puchase_price,
    processing_sale_price,
    processing_feature,
    start_date_structure,
    sale_code,
    sale_code_type_pc,
    start_date_sale_code,
    end_date_sale_code,
    end_date_structure,
    start_date_assortment,
    end_date_assortment,
    error_message,
    item_logistic,
    item_logistic_type,
    multiple_reorder,
    pieces_sale,
    supplier_is_active_1,
    attribute_code_1,
    attribute_code_2,
    attribute_code_3,
    attribute_code_4,
    attribute_code_5,
    attribute_code_6,
    attribute_code_7,
    attribute_code_8,
    attribute_code_9,
    attribute_code_10,
    attribute_code_11,
    attribute_code_12,
    attribute_code_13,
    attribute_code_14,
    attribute_code_15,
    under_brand_pc -- <--- AGGIUNTO
)
WITH
    param_trans AS (
        SELECT
            table_field COLLATE "C" as table_field,
            external_parameter COLLATE "C" as external_parameter,
            boom_parameter as code,
            boom_parameter as boom_parameter_text,
            type_flow,
            flow
        FROM boom.vpa_parameter_trans
        WHERE table_field COLLATE "C" IN ('unit_sale_price_pc','item_type_pc','stock_unit_pc','brand_pc','weight_unit_measure_sale_pc','unit_measure_pc','assortment_status_pc','delivery_status_pc','unit_purchase_price_pc','item_logistic_type','multiple_reorder','pieces_sale','attribute_code_1','attribute_code_2','attribute_code_3','attribute_code_4','attribute_code_5','attribute_code_6','attribute_code_7','attribute_code_8','attribute_code_9','attribute_code_10','attribute_code_11','attribute_code_12','attribute_code_13','attribute_code_14','attribute_code_15','central_supplier')
    ),
    vat_lookup AS (
        SELECT vat::int as vat_code, vat_value, law COLLATE "C" as law, paragraph COLLATE "C" as paragraph
        FROM boom.tmd_vat
    ),
    item_links AS (
        SELECT DISTINCT ON (ti.item COLLATE "C") ti.item COLLATE "C" as item, lnk.start_date, lnk.end_date
        FROM boom.tmd_structure_item_links_var lnk JOIN boom.tmd_items ti ON ti.id = lnk.item_id
        WHERE lnk.end_date >= current_date ORDER BY ti.item COLLATE "C", lnk.start_date
    ),
    item_assortments AS (
        SELECT DISTINCT ON (ti.item COLLATE "C") ti.item COLLATE "C" as item, ts.start_date, ts.end_date
        FROM boom.tmd_orderable_assortments_var ts JOIN boom.tmd_items ti ON ti.id = ts.item_id
        WHERE ts.end_date >= current_date ORDER BY ti.item COLLATE "C", ts.start_date
    ),
    -- Tabella 12: lookup brand_pc per posizione (N → posizione 2, S → posizione 1)
    brand_tabella_12 AS (
        SELECT
            (SELECT parameter_code::int FROM boom.vpa_parameters WHERE parameter_header = 12 AND parameter_code = 1 AND is_default = 1 LIMIT 1) AS code_pos1,
            (SELECT parameter_code::int FROM boom.vpa_parameters WHERE parameter_header = 12 AND parameter_code = 2 AND is_default = 1 LIMIT 1) AS code_pos2
    ),
    -- FIX: Rimosso COLLATE da num_val_1 (numerico)
    under_brand_lookup AS (
        SELECT 
            num_val_1 as b_flag, 
            str_val_1 COLLATE "C" as b_class, 
            parameter_code::int8 as brand_id
        FROM boom.vpa_parameters
        WHERE parameter_header_id = 94 AND language_id = 1
    ),
    base_data AS (
        SELECT v.* FROM boom.vin_data_item_in v LEFT JOIN boom.tin_data_item_in t ON v.id = t.id
        WHERE t.processing_step2 = 0 AND t.source_type_pc = source_type_pc_filter
        AND (v.min_qty_supplier_order::numeric)/100 <= (substring(v.max_qty_supplier_order::text, 2)::numeric)/100.0
        LIMIT limit_filter
    )
SELECT
    v.id, v.transaction_code, v.dest, ltrim(v.cod_art COLLATE "C", '0') || '-' || lpad(v.differ::text, 2, '0'),
    COALESCE(vat_sale.vat_code, 0)::text, v.merch_l1 || v.merch_l2 || v.merch_l3 || v.merch_l4,
    NULLIF(v.muv_code COLLATE "C", '')::int, NULLIF(v.supplier_packaging COLLATE "C", '')::int,
    (v.public_price::numeric(15,5))/100, COALESCE(NULLIF(p_unit_sale_price.code, '')::int, 1),
    to_date(NULLIF(v.start_date_pub_list COLLATE "C", ''), 'YYYYMMDD'), to_date(NULLIF(v.end_date_pub_list COLLATE "C", '20991231'), 'YYYYMMDD'),
    1, TRIM(BOTH '"' FROM REPLACE(TRIM(COALESCE(v.long_desc, '')), E'\"', '''')), COALESCE(NULLIF(p_item_type.code, '')::int, 0), TRIM(v.receipt_destination),
    COALESCE(NULLIF(p_stock_unit.code, '')::int, 1), COALESCE(CASE WHEN UPPER(TRIM(COALESCE(v.brand_class, ''))) = 'N' THEN tbl12.code_pos2 ELSE tbl12.code_pos1 END, 0),
    NULLIF(v.expiration_days COLLATE "C", '')::int, COALESCE(vat_purchase.vat_code, 0)::text, 0,
    COALESCE(NULLIF(p_weight_unit_measure_sale.code, '')::int, 1),
    CASE WHEN COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) = 0 AND COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) = 0 THEN 1 WHEN COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) <> 0 AND COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) = 0 THEN 1 ELSE COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) / 10000 END,
    COALESCE(NULLIF(p_unit_measure.code, '')::int, 1),
    CASE WHEN COALESCE(NULLIF(p_stock_unit.code, '')::int, 1) = 1 THEN 1 ELSE CASE WHEN COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) = 0 AND COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) = 0 THEN 1 WHEN COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) = 0 AND COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) <> 0 THEN COALESCE(NULLIF(v.net_weigth, '')::numeric, 0) / 10000 ELSE CASE WHEN v.cv_ratio_type = 'S' AND NULLIF(LTRIM(v.cv_ratio, '0'), '') IS NOT NULL AND COALESCE(NULLIF(v.cv_ratio, '')::NUMERIC, 0) <> 0 THEN (COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) / 10000) / (COALESCE(NULLIF(v.cv_ratio, '')::NUMERIC, 1) / 1000) ELSE COALESCE(NULLIF(v.gross_weigth, '')::numeric, 0) / 10000 END END END,
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN p_central_supplier.code::text ELSE ltrim(v.supplier_code, '0') END,
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN (COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) ELSE ltrim(v.supplier_code, '0') || '-' || COALESCE(v.differ_supplier COLLATE "C", '') END,
    CASE WHEN v.supplier_code = v.order_supplier_code THEN 1 ELSE 0 END::numeric(1),
    (v.min_qty_supplier_order::numeric)/100, LEAST(max_qty_supplier_order::numeric/100.0, 999999.999),
    COALESCE(NULLIF(p_assortment_status.code, '')::int, 0)::numeric(1), COALESCE(NULLIF(p_delivery_status.code, '')::int, 0)::numeric(1),
    TRIM(v.item_code_supplier), 1, v.desc_item_code_supplier, NULL, NULL,
    CASE WHEN v.cv_ratio_type = 'S' AND COALESCE(NULLIF(v.cv_ratio, '')::NUMERIC, 0) <> 0 THEN ROUND ( COALESCE(NULLIF(v.net_cost, '')::numeric, 0)/100 / (COALESCE(NULLIF(v.cv_ratio, '')::NUMERIC, 1)/1000) , 2 ) ELSE ROUND (COALESCE(NULLIF(v.net_cost, '')::numeric, 0)/100 , 2 ) END,
    COALESCE(NULLIF(p_unit_sale_price_pc.code, '')::int, 1), to_date(NULLIF(v.start_date_list COLLATE "C", ''), 'YYYYMMDD'), to_date(NULLIF(v.end_date_list COLLATE "C", '20991231'), 'YYYYMMDD'),
    COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", ''), NULLIF(TRIM(v.date_cre_art), ''), NULLIF(TRIM(v.seasonality), ''), NULLIF(TRIM(v.recommended_flag), ''), NULLIF(TRIM(v.reclass_cod1), ''), NULLIF(TRIM(v.reclass_cod2), ''), NULLIF(TRIM(v.reclass_cod3), ''), NULLIF(TRIM(v.ecommerce_hg_min), ''), NULLIF(TRIM(v.ecommerce_hg_packaging), ''), NULLIF(TRIM(v.ecommerce_hg_piece), ''), NULLIF(TRIM(v.article_weigth_flag), ''),
    CASE WHEN NULLIF(v.shop_weigth_loss, '') IS NULL THEN NULL WHEN COALESCE(NULLIF(v.shop_weigth_loss, '')::NUMERIC, 0) = 0 THEN NULL ELSE TO_CHAR(COALESCE(NULLIF(v.shop_weigth_loss, '')::NUMERIC, 0)/1000, 'FM999999990.0') END,
    TRIM(v.article_weigth_flag), v.cv_ratio_pub_type || TRIM(TO_CHAR( CAST(v.cv_ratio_pub AS INT)/1000 , '9999.0')), NULL,
    CASE v.multiple_vat_flag COLLATE "C" WHEN 'S' THEN 1 WHEN 'N' THEN 0 ELSE 0 END,
    1, 0, 0, 0, 9, 0, 0, 0, 0, COALESCE(lnk.start_date, current_date),
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN (SELECT barcode || (CASE WHEN mod(total, 10) = 0 THEN '0' ELSE (10 - mod(total, 10))::text END) FROM (SELECT '79' || substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 2 for 2) || substring(lpad(replace(ltrim(v.cod_art COLLATE "C", '0') || '-' || lpad(v.differ::text, 2, '0'), '-', ''), 8, '0') from 1 for 8) as barcode) b CROSS JOIN LATERAL (SELECT sum((substring(barcode from i for 1)::int) * (CASE WHEN i % 2 = 1 THEN 1 ELSE 3 END)) as total FROM generate_series(1, 12) s(i)) s) ELSE NULL END,
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN 12 ELSE NULL END,
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN current_date ELSE NULL END,
    CASE WHEN substring((COALESCE(v.div_mag_rif COLLATE "C", '') || COALESCE(v.virtual_warehouse_cod COLLATE "C", '')) COLLATE "C" from 1 for 1) = 'M' THEN to_date('20991231', 'YYYYMMDD') ELSE NULL END,
    COALESCE(lnk.end_date, to_date('20991231', 'YYYYMMDD')), COALESCE(assort.start_date, current_date), COALESCE(assort.end_date, to_date('20991231', 'YYYYMMDD')), NULL,
    1, COALESCE(NULLIF(p_item_logistic_type.code, '')::int, 1), COALESCE(NULLIF(p_multiple_reorder.code, '')::int, 1)::numeric(9,3), COALESCE(NULLIF(p_pieces_sale.code, '')::int, 1)::numeric(9), 1,
    p_attribute_code_1.boom_parameter_text, p_attribute_code_2.boom_parameter_text, p_attribute_code_3.boom_parameter_text, p_attribute_code_4.boom_parameter_text, p_attribute_code_5.boom_parameter_text, p_attribute_code_6.boom_parameter_text, p_attribute_code_7.boom_parameter_text, p_attribute_code_8.boom_parameter_text, p_attribute_code_9.boom_parameter_text, p_attribute_code_10.boom_parameter_text, p_attribute_code_11.boom_parameter_text, p_attribute_code_12.boom_parameter_text, p_attribute_code_13.boom_parameter_text, p_attribute_code_14.boom_parameter_text, p_attribute_code_15.boom_parameter_text,
    ubl.brand_id as under_brand_pc -- <--- VALORIZZAZIONE
FROM base_data v
    LEFT JOIN param_trans p_unit_sale_price ON p_unit_sale_price.table_field = 'unit_sale_price_pc' AND p_unit_sale_price.external_parameter = v.unit_of_measure COLLATE "C"
    LEFT JOIN param_trans p_item_type ON p_item_type.table_field = 'item_type_pc' AND p_item_type.external_parameter = v.art_type COLLATE "C"
    LEFT JOIN param_trans p_stock_unit ON p_stock_unit.table_field = 'stock_unit_pc' AND p_stock_unit.external_parameter = v.unit_of_measure COLLATE "C"
    CROSS JOIN brand_tabella_12 tbl12
    LEFT JOIN param_trans p_weight_unit_measure_sale ON p_weight_unit_measure_sale.table_field = 'weight_unit_measure_sale_pc' AND p_weight_unit_measure_sale.external_parameter = trim(v.sale_unit COLLATE "C")
    LEFT JOIN param_trans p_unit_measure ON p_unit_measure.table_field = 'unit_measure_pc' AND p_unit_measure.external_parameter = v.unit_of_measure COLLATE "C"
    LEFT JOIN param_trans p_assortment_status ON p_assortment_status.table_field = 'assortment_status_pc' AND p_assortment_status.external_parameter = v.status_article_supplier COLLATE "C"
    LEFT JOIN param_trans p_delivery_status ON p_delivery_status.table_field = 'delivery_status_pc' AND p_delivery_status.external_parameter = v.warehouse_assortment_status COLLATE "C"
    LEFT JOIN param_trans p_unit_sale_price_pc ON p_unit_sale_price_pc.table_field = 'unit_sale_price_pc' AND p_unit_sale_price_pc.external_parameter = v.unit_of_measure COLLATE "C"
    LEFT JOIN vat_lookup vat_sale ON vat_sale.vat_value = v.vat_code::numeric/10 AND vat_sale.law = trim(v.vat_exception COLLATE "C") AND ((trim(v.vat_section COLLATE "C") COLLATE "C" = '' AND vat_sale.paragraph IS NULL) OR (trim(v.vat_section COLLATE "C") COLLATE "C" <> '' AND vat_sale.paragraph = trim(v.vat_section COLLATE "C")))
    LEFT JOIN vat_lookup vat_purchase ON vat_purchase.vat_value = v.vat_code::numeric/10 AND vat_purchase.law = trim(v.vat_exception COLLATE "C") AND ((trim(v.vat_section COLLATE "C") COLLATE "C" = '' AND vat_purchase.paragraph IS NULL) OR (trim(v.vat_section COLLATE "C") COLLATE "C" <> '' AND vat_purchase.paragraph = trim(v.vat_section COLLATE "C")))
    LEFT JOIN item_links lnk ON lnk.item = ltrim((v.cod_art || '-' || v.differ) COLLATE "C", '0')
    LEFT JOIN item_assortments assort ON assort.item = ltrim((v.cod_art || '-' || v.differ) COLLATE "C", '0')
    LEFT JOIN param_trans p_item_logistic_type ON p_item_logistic_type.table_field = 'item_logistic_type' AND p_item_logistic_type.type_flow = 10
    LEFT JOIN param_trans p_multiple_reorder ON p_multiple_reorder.table_field = 'multiple_reorder' AND p_multiple_reorder.flow = 1
    LEFT JOIN param_trans p_pieces_sale ON p_pieces_sale.table_field = 'pieces_sale' AND p_pieces_sale.flow = 1
    LEFT JOIN param_trans p_attribute_code_1 ON p_attribute_code_1.table_field = 'attribute_code_1' AND p_attribute_code_1.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_2 ON p_attribute_code_2.table_field = 'attribute_code_2' AND p_attribute_code_2.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_3 ON p_attribute_code_3.table_field = 'attribute_code_3' AND p_attribute_code_3.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_4 ON p_attribute_code_4.table_field = 'attribute_code_4' AND p_attribute_code_4.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_5 ON p_attribute_code_5.table_field = 'attribute_code_5' AND p_attribute_code_5.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_6 ON p_attribute_code_6.table_field = 'attribute_code_6' AND p_attribute_code_6.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_7 ON p_attribute_code_7.table_field = 'attribute_code_7' AND p_attribute_code_7.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_8 ON p_attribute_code_8.table_field = 'attribute_code_8' AND p_attribute_code_8.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_9 ON p_attribute_code_9.table_field = 'attribute_code_9' AND p_attribute_code_9.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_10 ON p_attribute_code_10.table_field = 'attribute_code_10' AND p_attribute_code_10.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_11 ON p_attribute_code_11.table_field = 'attribute_code_11' AND p_attribute_code_11.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_12 ON p_attribute_code_12.table_field = 'attribute_code_12' AND p_attribute_code_12.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_13 ON p_attribute_code_13.table_field = 'attribute_code_13' AND p_attribute_code_13.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_14 ON p_attribute_code_14.table_field = 'attribute_code_14' AND p_attribute_code_14.type_flow = 10
    LEFT JOIN param_trans p_attribute_code_15 ON p_attribute_code_15.table_field = 'attribute_code_15' AND p_attribute_code_15.type_flow = 10
    LEFT JOIN param_trans p_central_supplier ON p_central_supplier.table_field = 'central_supplier'
    -- JOIN UNDER BRAND (usa brand_pc da tabella 12: N→pos2, S→pos1)
    LEFT JOIN under_brand_lookup ubl 
        ON ubl.b_flag = COALESCE(CASE WHEN UPPER(TRIM(COALESCE(v.brand_class, ''))) = 'N' THEN tbl12.code_pos2 ELSE tbl12.code_pos1 END, 0)
        AND ubl.b_class = v.brand_class COLLATE "C"

ON conflict (id) do UPDATE SET
    transaction_code = excluded.transaction_code, network = excluded.network, item = excluded.item, sale_vat = excluded.sale_vat, structure_code = excluded.structure_code, item_category_pc = excluded.item_category_pc, pieces_pack = excluded.pieces_pack, sale_price = excluded.sale_price, unit_sale_price_pc = excluded.unit_sale_price_pc, start_date_sale = excluded.start_date_sale, end_date_sale = excluded.end_date_sale, item_sale = excluded.item_sale, description_item = TRIM(excluded.description_item), item_type_pc = excluded.item_type_pc, description_sale = TRIM(excluded.description_sale), stock_unit_pc = excluded.stock_unit_pc, brand_pc = excluded.brand_pc, supplier_expiry_days = excluded.supplier_expiry_days, purchase_vat = excluded.purchase_vat, is_local = excluded.is_local, weight_unit_measure_sale_pc = excluded.weight_unit_measure_sale_pc, val_weight_sale = excluded.val_weight_sale, unit_measure_pc = excluded.unit_measure_pc, medium_weight = excluded.medium_weight, supplier_code = excluded.supplier_code, operational_agreement = excluded.operational_agreement, main_supplier = excluded.main_supplier, min_order = excluded.min_order, max_order = excluded.max_order, assortment_status_pc = excluded.assortment_status_pc, delivery_status_pc = excluded.delivery_status_pc, supplier_item_1 = TRIM(excluded.supplier_item_1), supplier_code_type_1 = excluded.supplier_code_type_1, description_supplier_item_1 = excluded.description_supplier_item_1, supplier_item_2 = excluded.supplier_item_2, supplier_code_type_2 = excluded.supplier_code_type_2, purchase_price = excluded.purchase_price, unit_purchase_price_pc = excluded.unit_purchase_price_pc, start_date_purchase = excluded.start_date_purchase, end_date_purchase = excluded.end_date_purchase, attribute_value_1 = excluded.attribute_value_1, attribute_value_2 = excluded.attribute_value_2, attribute_value_3 = excluded.attribute_value_3, attribute_value_4 = excluded.attribute_value_4, attribute_value_5 = excluded.attribute_value_5, attribute_value_6 = excluded.attribute_value_6, attribute_value_7 = excluded.attribute_value_7, attribute_value_8 = excluded.attribute_value_8, attribute_value_9 = excluded.attribute_value_9, attribute_value_10 = excluded.attribute_value_10, attribute_value_11 = excluded.attribute_value_11, attribute_value_12 = excluded.attribute_value_12, attribute_value_13 = excluded.attribute_value_13, attribute_value_14 = excluded.attribute_value_14, attribute_value_15 = excluded.attribute_value_15, is_multi_vat = excluded.is_multi_vat, processing_step2 = excluded.processing_step2, processing_step3 = excluded.processing_step3, processing_step4 = excluded.processing_step4, processing_item = excluded.processing_item, processing_sale_code = excluded.processing_sale_code, processing_assortment = excluded.processing_assortment, processing_puchase_price = excluded.processing_puchase_price, processing_sale_price = excluded.processing_sale_price, processing_feature = excluded.processing_feature, start_date_structure = excluded.start_date_structure, sale_code = excluded.sale_code, sale_code_type_pc = excluded.sale_code_type_pc, start_date_sale_code = excluded.start_date_sale_code, end_date_sale_code = excluded.end_date_sale_code, end_date_structure = excluded.end_date_structure, start_date_assortment = excluded.start_date_assortment, end_date_assortment = excluded.end_date_assortment, error_message = excluded.error_message, item_logistic = excluded.item_logistic, item_logistic_type = excluded.item_logistic_type, multiple_reorder = excluded.multiple_reorder, pieces_sale = excluded.pieces_sale, supplier_is_active_1 = excluded.supplier_is_active_1, attribute_code_1 = excluded.attribute_code_1, attribute_code_2 = excluded.attribute_code_2, attribute_code_3 = excluded.attribute_code_3, attribute_code_4 = excluded.attribute_code_4, attribute_code_5 = excluded.attribute_code_5, attribute_code_6 = excluded.attribute_code_6, attribute_code_7 = excluded.attribute_code_7, attribute_code_8 = excluded.attribute_code_8, attribute_code_9 = excluded.attribute_code_9, attribute_code_10 = excluded.attribute_code_10, attribute_code_11 = excluded.attribute_code_11, attribute_code_12 = excluded.attribute_code_12, attribute_code_13 = excluded.attribute_code_13, attribute_code_14 = excluded.attribute_code_14, attribute_code_15 = excluded.attribute_code_15,
    under_brand_pc = excluded.under_brand_pc;

GET DIAGNOSTICS processed_rows = ROW_COUNT;
RETURN processed_rows;
END;
$function$
;
