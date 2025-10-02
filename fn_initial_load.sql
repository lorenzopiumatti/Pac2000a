-- DROP FUNCTION boom.fn_initial_load(int8, varchar);

CREATE OR REPLACE FUNCTION boom.fn_initial_load(p_scrivi_log bigint, p_item_inp character varying)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- v. 01102025 lp
    -- ERRORE_ELABORAZIONE EXCEPTION; -- Puoi decommentare se hai una definizione globale per questa eccezione

    -- Variabili per i parametri di input della funzione
    p_transaction_code INTEGER;
    p_user TEXT DEFAULT 'user_batch'; -- Utente di default, puoi cambiarlo
    p_network_ids INTEGER[3]; -- Array di ID dei network per il loop

    -- Variabili per i dati estratti dal cursore
    r_transaction_row RECORD; -- Variabile per il FETCH dal cur_transactions
    r_item_data RECORD;       -- Variabile per il FETCH dal cur_items
    r_temp_item RECORD;       -- Nuovo record per iterare sulla tabella temporanea

    -- Variabili di default estratte all'inizio
    w_clocks TIMESTAMP;
    w_ecr_id INTEGER;
    w_central_dep TEXT;
    w_cash_dep TEXT;
    w_cash_dep_feature_id INTEGER;
    w_cash_dep_description TEXT;
    w_vat_id INTEGER;
    w_brand_pc INTEGER;
    w_supplier_id INTEGER;
    w_operational_agreement_id INTEGER;
    w_logistic_unit_measure INTEGER;
    w_consi TEXT;
    w_consi_id INTEGER;
    w_consi_description TEXT;
    w_mag_rif TEXT;
    w_mag_rif_id INTEGER;
    w_mag_rif_description TEXT;
    w_f_prz TEXT;
    w_f_prz_id INTEGER;
    w_f_prz_id_description TEXT;
    w_stag TEXT;
    w_stag_id INTEGER;
    w_stag_description TEXT;
    w_f_v_peso_c TEXT;
    w_f_v_peso_c_id INTEGER;
    w_f_v_peso_c_description TEXT;
    w_price_unit INTEGER;
	w_plu_code TEXT;
	w_bilance_department_pc INTEGER;
	w_bilance_code TEXT;

    -- Variabili di lavoro per gli ID generati
    w_item_code TEXT;
    w_param_id INTEGER;
	v_structure_item_id INTEGER;
    w_new_item_code_val INTEGER;
    w_item_id INTEGER;
    w_item_sale_id INTEGER;
    w_item_logistic_id INTEGER;
    w_logistic_unit_id INTEGER;
    w_orderable_assortments_id INTEGER;
    w_purchase_prices_id INTEGER;
    w_sale_prices_var_id INTEGER;
    w_esito_processing_item  INTEGER;
    w_esito_processing_sales_code  INTEGER;
    w_esito_processing_assortment  INTEGER;
    w_esito_processing_puchase_price  INTEGER;
    w_esito_processing_sale_price  INTEGER;
    w_esito_processing_features  INTEGER;
    w_existing_supplier_item_code_id INTEGER;
    w_f_scrivi_log INTEGER;
    -- Variabili per il logging
    w_log_text TEXT;
    w_log_err  TEXT;
    w_log_return INTEGER;
    w_count_total INTEGER;
    w_network_id INTEGER;

    -- Variabili per controllare l'esistenza dei record prima di INSERT/UPDATE
    v_item_exists BOOLEAN;
    v_item_sale_exists BOOLEAN;
    v_item_logistic_exists BOOLEAN;
    v_logistic_unit_1_exists BOOLEAN;
    v_logistic_unit_41_exists BOOLEAN;
    v_structure_item_link_exists BOOLEAN;
    v_structure_item_link_exists_1 BOOLEAN;
    v_sale_code_exists BOOLEAN;
    v_saleable_assortment_exists BOOLEAN;
    v_orderable_assortment_exists BOOLEAN;
    v_orderable_assortment_exists_same_record BOOLEAN;
    v_purchase_price_exists BOOLEAN;
    v_sale_price_exists BOOLEAN;
    v_feature_item_link_exists BOOLEAN;
    v_ttr_item_exists BOOLEAN;
    v_ttr_item_sale_exists BOOLEAN;
    current_feature_id INTEGER;
    update_attribute_id INTEGER;
    current_attribute_value TEXT;
    current_network_value INTEGER;
    w_iva_acq_item INTEGER;
    current_attribute_code TEXT;
    current_feature_description TEXT; -- Aggiunto per popolare str_val
	v_update_type TEXT;

    -- Variabile per la dimensione del blocco
    block_size CONSTANT INTEGER := 100;
    -- Variabile per contare le righe nel blocco corrente
    rows_in_current_block INTEGER;
    rows_in_TOTAL INTEGER;

    -- Cursore 1: Transazioni da elaborare
    cur_transactions CURSOR (transaction_code_param INTEGER) FOR
   SELECT distinct a.item ----transaction_code, processing_item, item, id -- Aggiunto id per l'update finale
     from  (
        select td.*        
	FROM tin_data_item_in td
        WHERE processing_step3 = 0 and processing_step4 = 0
          AND processing_step2 = 1
          AND processing_step1 = 1
	     and ((item = p_item_inp and p_item_inp is not null) OR (p_item_inp is null))
          AND ((transaction_code_param is not null and td.transaction_code = transaction_code_param ) or (transaction_code_param is null ))
          AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
   ORDER BY processing_item, td.transaction_code, CASE WHEN td.processing_sale_code = 0 AND processing_item != 0 THEN td.id ELSE 1 END DESC, td.id ASC)a;



    -- Cursore 2: Articoli da elaborare
    cur_items CURSOR (----p_transaction_code INTEGER, p_processing_item INTEGER,
                      p_item TEXT ) FOR
        SELECT
            td.*,
            COALESCE(tn.id, -1) AS network_id,
            COALESCE(ti.id, -1) AS item_id,
            CASE
                WHEN ti.id IS NOT NULL AND (COALESCE(ti.item_type_pc, 0) != COALESCE(td.item_type_pc, 0) OR COALESCE(ti.item_category_pc, 0) != COALESCE(td.item_category_pc, 0) OR COALESCE(ti.stock_unit_pc, 0) != COALESCE(td.stock_unit_pc, 0) OR COALESCE(ti.brand_pc, 0) != COALESCE(td.brand_pc, 0) OR COALESCE(ti.supplier_expiry_days, 0) != COALESCE(td.supplier_expiry_days, 0) OR COALESCE(ti.depot_expiry_days, 0) != COALESCE(td.depot_expiry_days, 0) OR COALESCE(ti.shop_expiry_days, 0) != COALESCE(td.shop_expiry_days, 0) OR COALESCE(ti.consumer_expiry_days, 0) != COALESCE(td.consumer_expiry_days, 0) OR COALESCE(ti.short_description, '#NULL#') != COALESCE(td.description_item, '#NULL#') OR COALESCE(ti.description, '#NULL#') != COALESCE(td.description_item, '#NULL#') OR COALESCE(ti.under_brand_pc, 0) != COALESCE(td.under_brand_pc, 0) OR COALESCE(ti.is_multi_vat, 0) != COALESCE(td.is_multi_vat, 0))
                THEN 1
                WHEN ti.id IS NULL THEN 1
                ELSE -1
            END AS "update_item",
            COALESCE(tis.id, -1) AS sale_id,
            CASE
                WHEN tis.id IS NOT NULL AND (COALESCE(tis.val_length, 0) != COALESCE(td.val_length_sale, 0) OR COALESCE(tis.val_width, 0) != COALESCE(td.val_length_sale, 0) OR COALESCE(tis.val_height, 0) != COALESCE(td.val_Width_Sale, 0) OR COALESCE(tis.val_weight, 0) != COALESCE(td.val_Weight_Sale, 0) OR COALESCE(tis.size_unit_measure_pc, 0) != COALESCE(td.size_Unit_Measure_Sale_Pc, 0) OR COALESCE(tis.weight_unit_measure_pc, 0) != COALESCE(td.weight_Unit_Measure_Sale_Pc, 0) OR COALESCE(tis.pieces, 0) != COALESCE(td.pieces_sale, 0) OR COALESCE(tis.description, '#NULL#') != COALESCE(td.description_sale, '#NULL#'))
                THEN 1
                WHEN tis.id IS NULL THEN 1
                ELSE -1
            END AS "update_sale",
            COALESCE(til.id, -1) AS logistic_id,
            CASE
                WHEN til.id IS NOT NULL AND (COALESCE(til.unit_measure_pc, 0) != COALESCE(td.unit_Measure_Pc, 0) OR COALESCE(til.medium_weight, 0) != COALESCE(td.medium_Weight, 0))
                THEN 1
                WHEN til.id IS NULL THEN 1
                ELSE -1
            END AS "update_logistic",
            COALESCE(tlu1.id, -1) AS logistic_unit_id_1,
            CASE
                WHEN tlu1.id IS NOT NULL AND (COALESCE(tlu1.size_unit_measure_pc, 0) != COALESCE(td.size_Unit_Measure_sale_Pc, 0) OR COALESCE(tlu1.weight_unit_measure_pc, 0) != COALESCE(td.weight_Unit_Measure_Sale_Pc, 0))
                THEN 1
                WHEN tlu1.id IS NULL THEN 1
                ELSE -1
            END AS "update_logistic_unit_1",
            COALESCE(tlu41.id, -1) AS logistic_unit_id_41,
            CASE
                WHEN tlu41.id IS NOT NULL AND (COALESCE(tlu41.size_unit_measure_pc, 0) != COALESCE(td.size_Unit_Measure_Sale_Pc, 0) OR COALESCE(tlu41.weight_unit_measure_pc, 0) != COALESCE(td.weight_Unit_Measure_Sale_Pc, 0) OR COALESCE(tlu41.coefficient, 0) != COALESCE(td.pieces_pack, 0) OR COALESCE(tlu41.val_Height, 0) != COALESCE(td.val_Height_Pack, 0) OR COALESCE(tlu41.val_Length, 0) != COALESCE(td.val_Length_Pack, 0) OR COALESCE(tlu41.val_Width, 0) != COALESCE(td.val_Width_Pack, 0))
                THEN 1
                WHEN tlu41.id IS NULL THEN 1
                ELSE -1
            END AS "update_logistic_unit_41",
            COALESCE(ts.id, ts_ecr_def.id) AS structure_id,
            COALESCE(tsil.id, -1) AS structure_link_id,
            COALESCE(tsff1.id, -1) AS feature_id1,
            COALESCE(tsff2.id, -1) AS feature_id2,
            COALESCE(tsff3.id, -1) AS feature_id3,
            COALESCE(tsff4.id, -1) AS feature_id4,
            COALESCE(tsff5.id, -1) AS feature_id5,
            COALESCE(tsff6.id, -1) AS feature_id6,
            COALESCE(tsff7.id, -1) AS feature_id7,
            COALESCE(tsff8.id, -1) AS feature_id8,
            COALESCE(tsff9.id, -1) AS feature_id9,
            COALESCE(tsff10.id, -1) AS feature_id10,
            COALESCE(tsff11.id, -1) AS feature_id11,
            COALESCE(tsff12.id, -1) AS feature_id12,
            COALESCE(tsff13.id, -1) AS feature_id13,
            COALESCE(tsff14.id, -1) AS feature_id14,
            COALESCE(tsff15.id, -1) AS feature_id15,
            COALESCE(tfe1.id, -1) AS attribute1_id,
            CASE
                WHEN tfe1.id IS NOT NULL AND (COALESCE(tfe1.features_value, '#NULL#') != COALESCE(td.attribute_Value_1, '#NULL#'))
                THEN 1
                WHEN tfe1.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute1",
            COALESCE(tfe2.id, -1) AS attribute2_id,
            CASE
                WHEN tfe2.id IS NOT NULL AND (COALESCE(tfe2.features_value, '#NULL#') != COALESCE(td.attribute_Value_2, '#NULL#'))
                THEN 1
                WHEN tfe2.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute2",
            COALESCE(tfe3.id, -1) AS attribute3_id,
            CASE
                WHEN tfe3.id IS NOT NULL AND (COALESCE(tfe3.features_value, '#NULL#') != COALESCE(td.attribute_Value_3, '#NULL#'))
                THEN 1
                WHEN tfe3.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute3",
            COALESCE(tfe4.id, -1) AS attribute4_id,
            CASE
                WHEN tfe4.id IS NOT NULL AND (COALESCE(tfe4.features_value, '#NULL#') != COALESCE(td.attribute_Value_4, '#NULL#'))
                THEN 1
                WHEN tfe4.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute4",
            COALESCE(tfe5.id, -1) AS attribute5_id,
            CASE
                WHEN tfe5.id IS NOT NULL AND (COALESCE(tfe5.features_value, '#NULL#') != COALESCE(td.attribute_Value_5, '#NULL#'))
                THEN 1
                WHEN tfe5.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute5",
            COALESCE(tfe6.id, -1) AS attribute6_id,
            CASE
                WHEN tfe6.id IS NOT NULL AND (COALESCE(tfe6.features_value, '#NULL#') != COALESCE(td.attribute_Value_6, '#NULL#'))
                THEN 1
                WHEN tfe6.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute6",
            COALESCE(tfe7.id, -1) AS attribute7_id,
            CASE
                WHEN tfe7.id IS NOT NULL AND (COALESCE(tfe7.features_value, '#NULL#') != COALESCE(td.attribute_Value_7, '#NULL#'))
                THEN 1
                WHEN tfe7.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute7",
            COALESCE(tfe8.id, -1) AS attribute8_id,
            CASE
                WHEN tfe8.id IS NOT NULL AND (COALESCE(tfe8.features_value, '#NULL#') != COALESCE(td.attribute_Value_8, '#NULL#'))
                THEN 1
                WHEN tfe8.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute8",
            COALESCE(tfe9.id, -1) AS attribute9_id,
            CASE
                WHEN tfe9.id IS NOT NULL AND (COALESCE(tfe9.features_value, '#NULL#') != COALESCE(td.attribute_Value_9, '#NULL#'))
                THEN 1
                WHEN tfe9.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute9",
            COALESCE(tfe10.id, -1) AS attribute10_id,
            CASE
                WHEN tfe10.id IS NOT NULL AND (COALESCE(tfe10.features_value, '#NULL#') != COALESCE(td.attribute_Value_10, '#NULL#'))
                THEN 1
                WHEN tfe10.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute10",
            COALESCE(tfe11.id, -1) AS attribute11_id,
            CASE
                WHEN tfe11.id IS NOT NULL AND (COALESCE(tfe11.features_value, '#NULL#') != COALESCE(td.attribute_Value_11, '#NULL#'))
                THEN 1
                WHEN tfe11.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute11",
            COALESCE(tfe12.id, -1) AS attribute12_id,
            CASE
                WHEN tfe12.id IS NOT NULL AND (COALESCE(tfe12.features_value, '#NULL#') != COALESCE(td.attribute_Value_12, '#NULL#'))
                THEN 1
                WHEN tfe12.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute12",
            COALESCE(tfe13.id, -1) AS attribute13_id,
            CASE
                WHEN tfe13.id IS NOT NULL AND (COALESCE(tfe13.features_value, '#NULL#') != COALESCE(td.attribute_Value_13, '#NULL#'))
                THEN 1
                WHEN tfe13.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute13",
            COALESCE(tfe14.id, -1) AS attribute14_id,
            CASE
                WHEN tfe14.id IS NOT NULL AND (COALESCE(tfe14.features_value, '#NULL#') != COALESCE(td.attribute_Value_14, '#NULL#'))
                THEN 1
                WHEN tfe14.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute14",
            COALESCE(tfe15.id, -1) AS attribute15_id,
            CASE
                WHEN tfe15.id IS NOT NULL AND (COALESCE(tfe15.features_value, '#NULL#') != COALESCE(td.attribute_Value_15, '#NULL#'))
                THEN 1
                WHEN tfe15.id IS NULL THEN 1
                ELSE -1
            END AS "update_attribute15",
            COALESCE(tsil.structure_id, -1) AS current_structure_code_id,
            COALESCE((SELECT MAX(tsil2.end_Date) FROM tmd_structure_item_links_var tsil2 WHERE tsil2.item_id = ti.id AND tsil2.structure_id = ts.id AND tsil2.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_structure,
            COALESCE(ticv.id, -1) AS item_code_id,
            CASE
                WHEN ticv.id IS NOT NULL AND (COALESCE(ticv.code_type_pc, 0) != COALESCE(td.item_code_type, 0) OR COALESCE(ticv.item_code, '#NULL#') != COALESCE(td.item_code, '#NULL#') OR COALESCE(ticv.end_date, '2099-12-31') != COALESCE(td.end_date_item_code, '2099-12-31'))
                THEN 1
                WHEN ticv.id IS NULL THEN 1
                ELSE -1
            END AS "update_item_code",
            COALESCE(ticv.item_code, '-1') AS current_item_code,
            COALESCE((SELECT MAX(ticv2.end_Date) FROM tmd_item_codes_var ticv2 WHERE ti.id = ticv.item_id AND td.item_code_type = ticv.code_type_pc AND ticv2.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_item_code,
            COALESCE(toav.id, -1) AS assortment_id1,
            CASE
                WHEN toav.id IS NOT NULL AND (td.min_order <> toav.min_order OR td.max_order <> toav.max_order OR td.multiple_reorder <> toav.multiple_reorder OR toav.assortment_status_pc <> td.assortment_status_pc OR toav.delivery_status_pc <> td.delivery_status_pc OR td.end_date_assortment <> toav.end_date) AND td.start_date_assortment = toav.start_date
                THEN 1
                WHEN toav.id IS NULL THEN 1
                ELSE -1
            END AS "update_assortment",
            COALESCE((SELECT MAX(toav2.end_Date) FROM tmd_orderable_assortments_var toav2 WHERE toav2.item_id = ti.id AND toav2.item_logistic_id = til.id AND toav2.logistic_unit_id = tlu_ass.id AND tn.id = toav2.network_id AND toa.id = toav2.operational_agreement_id AND toav2.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_assortment,
            COALESCE(ttp.id, -1) AS third_party_id,
            COALESCE(toa.id, -1) AS operational_agreement_id,
            COALESCE(tsic1.id, -1) AS supplier_item_code1_id,
            CASE
                WHEN tsic1.id IS NOT NULL AND (COALESCE(tsic1.code_type_pc, 0) != COALESCE(td.supplier_code_type_1, 0) OR COALESCE(tsic1.supplier_item_code, '#NULL#') != COALESCE(td.supplier_item_1, '#NULL#') OR COALESCE(tsic1.description_item, '#NULL#') != COALESCE(td.description_supplier_item_1, '#NULL#') OR COALESCE(tsic1.is_active, 0) != COALESCE(td.supplier_is_active_1, 0))
                THEN 1
                WHEN tsic1.id IS NULL THEN 1
                ELSE -1
            END AS "update_supplier_item_code1",
            COALESCE(tsic2.id, -1) AS supplier_item_code2_id,
            CASE
                WHEN tsic2.id IS NOT NULL AND (COALESCE(tsic2.code_type_pc, 0) != COALESCE(td.supplier_code_type_2, 0) OR COALESCE(tsic2.supplier_item_code, '#NULL#') != COALESCE(td.supplier_item_2, '#NULL#') OR COALESCE(tsic2.description_item, '#NULL#') != COALESCE(td.description_supplier_item_2, '#NULL#') OR COALESCE(tsic2.is_active, 0) != COALESCE(td.supplier_is_active_2, 0))
                THEN 1
                WHEN tsic2.id IS NULL THEN 1
                ELSE -1
            END AS "update_supplier_item_code2",
            COALESCE((SELECT tscv3.ID FROM tmd_sale_codes_var tscv3 WHERE td.sale_code != tscv3.sale_code AND tscv3.item_sale_id = tis.id AND td.sale_code_type_pc = tscv3.code_type_pc AND tscv3.code_type_pc = 12 AND tscv3.network_id IS NULL AND td.START_date_sale_code <= tscv3.end_date LIMIT 1), -1) AS close_barcode_id,
            COALESCE(tscv.id, -1) AS sale_code_id,
            CASE
                WHEN tscv.id IS NOT NULL AND (tscv.end_date <> td.end_date_sale_code)
                THEN 1
                WHEN tscv.id IS NULL THEN 1
                ELSE -1
            END AS "update_sale_code",
            COALESCE((SELECT MAX(tscv4.end_Date) FROM tmd_sale_codes_var tscv4 WHERE td.sale_code = tscv4.sale_code AND td.sale_code_type_pc = tscv4.code_type_pc AND (tscv4.network_id IS NULL OR tscv4.network_id = tn.id) AND tscv4.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_sale_code,
            COALESCE(tppv.id, -1) AS purchase_price_id,
            COALESCE(tppv.price, -1) AS current_purchase_price,
            CASE
                WHEN tppv.id IS NOT NULL AND (tppv.price <> td.purchase_price OR tppv.unit_price_pc <> td.unit_purchase_price_pc OR tppv.vat_id <> tv_acq.id OR tppv.end_date <> td.end_date_purchase)
                THEN 1
                WHEN tppv.id IS NULL THEN 1
                ELSE -1
            END AS "update_purchase_price",
            COALESCE((SELECT MAX(tppv2.end_Date) FROM tmd_purchase_prices_var tppv2 WHERE tppv2.item_id = ti.id AND tppv2.operational_agreement_id = toa.id AND tppv2.network_id = tn.id AND tppv2.unit_price_pc = td.unit_purchase_price_pc AND tppv2.cost_type_pc = 1 AND tppv2.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_purchase_price,
            COALESCE(tspw.id, -1) AS sale_price_id,
            COALESCE(tspw.price, -1) AS current_sale_price,
            CASE
                WHEN tspw.id IS NOT NULL AND (tspw.price <> td.sale_price OR tspw.unit_price_pc <> td.unit_sale_price_pc OR tspw.vat_id <> tv_sale.id OR tspw.end_date <> td.end_date_sale)
                THEN 1
                WHEN tspw.id IS NULL THEN 1
                ELSE -1
            END AS "update_sale_price",
            COALESCE((SELECT MAX(tspw2.end_Date) FROM tmd_sale_prices_var tspw2 WHERE tis.id = tspw2.item_sale_id AND tspw2.sale_price_type_pc = 1 AND tn.id = tspw2.network_id AND tspw2.start_Date <= CURRENT_DATE), '2001-01-01'::DATE) AS last_date_sale_price,
            COALESCE(tv_acq.id, -1) AS purchase_vat_id,
            COALESCE(tv_sale.id, -1) AS sale_vat_id,
            COALESCE((SELECT CASE WHEN tspw3.price = td.sale_price THEN -1 ELSE tspw3.id END FROM tmd_sale_prices_var tspw3 WHERE tis.id = tspw3.item_sale_id AND tspw3.sale_price_type_pc = 1 AND tn.id = tspw3.network_id AND (td.start_date_purchase BETWEEN tspw3.start_Date AND tspw3.end_date OR td.end_date_purchase BETWEEN tspw3.start_Date AND tspw3.end_date) AND tspw3.start_Date >= CURRENT_DATE LIMIT 1), -2) AS sale_price_future,
            COALESCE((SELECT CASE WHEN tppv3.price = td.purchase_price THEN -1 ELSE tppv3.id END FROM tmd_purchase_prices_var tppv3 WHERE tppv3.item_id = ti.id AND tppv3.operational_agreement_id = toa.id AND tppv3.network_id = tn.id AND tppv3.unit_price_pc = td.unit_purchase_price_pc AND tppv3.cost_type_pc = 1 AND (td.start_date_purchase BETWEEN tppv3.start_Date AND tppv3.end_date OR td.end_date_purchase BETWEEN tppv3.start_Date AND tppv3.end_date) AND tppv3.start_Date >= CURRENT_DATE LIMIT 1), -2) AS purchase_price_future,
            COALESCE((SELECT tscv5.ID FROM tmd_sale_codes_var tscv5, vpa_parameters vp WHERE tscv5.sale_code = td.sale_code AND tscv5.item_sale_id != tis.id AND tscv5.code_type_pc = td.sale_code_type_pc AND tscv5.code_type_pc = 12 AND tscv5.network_id IS NULL AND td.START_date_sale_code <= tscv5.end_date AND vp.parameter_header = 15 AND vp.parameter_code = tscv5.code_type_pc AND vp.num_val_4 = 1 AND vp.is_default = 1 AND EXISTS (SELECT 1 FROM vpa_parameters vp WHERE vp.parameter_header = 86 AND parameter_code = 2 AND num_val_1 = 1 AND is_default = 1) LIMIT 1), -1) AS close_barcode_id,
            td.is_label AS is_label_sale_code,
            COALESCE(ti.purchase_vat_id, -1) AS master_data_purchase_vat_id,
            COALESCE(ti.sale_vat_id, -1) AS master_data_sale_vat_id,
            CASE WHEN (td.attribute_value_15 = 'S' OR lock_prz_std.cnt_lock > 0) THEN 1 ELSE 0 END AS lock_prz_std,
            CASE WHEN (td.item_category_pc <> ti.item_category_pc) THEN 1 ELSE 0 END AS upd_item_category_pc,
            CASE WHEN (td.sale_vat <> tv_sale.vat) THEN 1 ELSE 0 END AS upd_sale_vat,
            CASE WHEN (td.brand_pc <> ti.brand_pc) THEN 1 ELSE 0 END AS upd_brand_pc,
            CASE WHEN ts_rep.id IS NULL THEN '1' ELSE '0' END AS use_default_rep,
            ts_rep_def.structure_code AS default_rep,
            td.transaction_code,
            processing_item,
            processing_sale_code,
            processing_assortment,
            processing_puchase_price,
            processing_sale_price,
            processing_feature,
            tlu_ass.item_logistic_id ,
            coalesce(tsic1.code_type_pc ,  coalesce(td.supplier_code_type_1,-1) ,-1) supplier_code_type_1,
            coalesce(tsic1.supplier_item_code , coalesce(td.supplier_item_1 ,'-1'), '-1') supplier_item_code_1,
            coalesce(tsic2.code_type_pc ,  coalesce(td.supplier_code_type_2 ,-1), -1) supplier_code_type_2,
            coalesce(tsic2.supplier_item_code , coalesce(td.supplier_item_2 ,'-1'), '-1') supplier_item_code_2,
            td.supplier_is_active_1, td.supplier_is_active_2
        FROM tin_data_item_in td
        LEFT JOIN tmd_networks tn ON tn.network_code = td.network
        LEFT JOIN vmd_items ti ON td.item = ti.item
        LEFT JOIN vmd_item_sales tis ON ti.id = tis.item_id AND td.item_sale = tis.item_sale
        LEFT JOIN tmd_item_logistics til ON ti.id = til.item_id AND td.item_logistic = til.item_logistic
        LEFT JOIN tmd_logistic_units tlu1 ON tlu1.item_logistic_id = til.id AND tlu1.logistic_unit_pc = 1
        LEFT JOIN tmd_logistic_units tlu41 ON tlu41.item_logistic_id = til.id AND tlu41.logistic_unit_pc = 41
        LEFT JOIN tmd_logistic_units tlu_ass ON tlu_ass.item_logistic_id = til.id AND tlu_ass.logistic_unit_pc = td.item_logistic_type
        LEFT JOIN vmd_structures ts ON ts.structure_code = td.structure_code
        LEFT JOIN LATERAL(SELECT tsil.* FROM tmd_structure_item_links_var tsil, vmd_structures ts2 WHERE tsil.item_id = ti.id AND CURRENT_DATE BETWEEN tsil.start_Date AND tsil.end_date AND ts2.id = tsil.structure_id) tsil ON 1 = 1
        LEFT JOIN tpa_specific_features tsff1 ON td.attribute_code_1 = tsff1.specific_feature
        LEFT JOIN tpa_specific_features tsff2 ON td.attribute_code_2 = tsff2.specific_feature
        LEFT JOIN tpa_specific_features tsff3 ON td.attribute_code_3 = tsff3.specific_feature
        LEFT JOIN tpa_specific_features tsff4 ON td.attribute_code_4 = tsff4.specific_feature
        LEFT JOIN tpa_specific_features tsff5 ON td.attribute_code_5 = tsff5.specific_feature
        LEFT JOIN tpa_specific_features tsff6 ON td.attribute_code_6 = tsff6.specific_feature
        LEFT JOIN tpa_specific_features tsff7 ON td.attribute_code_7 = tsff7.specific_feature
        LEFT JOIN tpa_specific_features tsff8 ON td.attribute_code_8 = tsff8.specific_feature
        LEFT JOIN tpa_specific_features tsff9 ON td.attribute_code_9 = tsff9.specific_feature
        LEFT JOIN tpa_specific_features tsff10 ON td.attribute_code_10 = tsff10.specific_feature
        LEFT JOIN tpa_specific_features tsff11 ON td.attribute_code_11 = tsff11.specific_feature
        LEFT JOIN tpa_specific_features tsff12 ON td.attribute_code_12 = tsff12.specific_feature
        LEFT JOIN tpa_specific_features tsff13 ON td.attribute_code_13 = tsff13.specific_feature
        LEFT JOIN tpa_specific_features tsff14 ON td.attribute_code_14 = tsff14.specific_feature
        LEFT JOIN tpa_specific_features tsff15 ON td.attribute_code_15 = tsff15.specific_feature
        LEFT JOIN vmd_specific_feature_item_linked tfe1 ON td.attribute_code_1 = tfe1.specific_feature AND tfe1.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe2 ON td.attribute_code_2 = tfe2.specific_feature AND tfe2.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe3 ON td.attribute_code_3 = tfe3.specific_feature AND tfe3.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe4 ON td.attribute_code_4 = tfe4.specific_feature AND tfe4.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe5 ON td.attribute_code_5 = tfe5.specific_feature AND tfe5.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe6 ON td.attribute_code_6 = tfe6.specific_feature AND tfe6.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe7 ON td.attribute_code_7 = tfe7.specific_feature AND tfe7.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe8 ON td.attribute_code_8 = tfe8.specific_feature AND tfe8.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe9 ON td.attribute_code_9 = tfe9.specific_feature AND tfe9.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe10 ON td.attribute_code_10 = tfe10.specific_feature AND tfe10.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe11 ON td.attribute_code_11 = tfe11.specific_feature AND tfe11.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe12 ON td.attribute_code_12 = tfe12.specific_feature AND tfe12.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe13 ON td.attribute_code_13 = tfe13.specific_feature AND tfe13.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe14 ON td.attribute_code_14 = tfe14.specific_feature AND tfe14.item_id = ti.id
        LEFT JOIN vmd_specific_feature_item_linked tfe15 ON td.attribute_code_15 = tfe15.specific_feature AND tfe15.item_id = ti.id
        LEFT JOIN tmd_item_codes_var ticv ON ti.id = ticv.item_id AND td.item_code_type = ticv.code_type_pc AND CURRENT_DATE BETWEEN ticv.start_Date AND ticv.end_date
        LEFT JOIN tmd_third_parties ttp ON ttp.third_party = td.supplier_code
        LEFT JOIN tmd_operational_agreements toa ON toa.third_party_id = ttp.id AND toa.operational_agreement = td.operational_agreement
        LEFT JOIN tmd_orderable_assortments_var toav ON toav.item_id = ti.id AND toav.item_logistic_id = til.id AND toav.logistic_unit_id = tlu_ass.id AND tn.id = toav.network_id AND toa.id = toav.operational_agreement_id AND CURRENT_DATE BETWEEN toav.start_Date AND toav.end_date
        LEFT JOIN tmd_supplier_item_codes tsic1 ON tsic1.item_id = ti.id AND tsic1.item_logistic_id = til.id AND tsic1.third_party_id = ttp.id AND td.supplier_code_type_1 = tsic1.code_type_pc AND td.supplier_item_1 = tsic1.supplier_item_code
        LEFT JOIN tmd_supplier_item_codes tsic2 ON tsic2.item_id = ti.id AND tsic2.item_logistic_id = til.id AND tsic2.third_party_id = ttp.id AND td.supplier_code_type_2 = tsic2.code_type_pc AND td.supplier_item_2 = tsic2.supplier_item_code
        LEFT JOIN tmd_purchase_prices_var tppv ON tppv.item_id = ti.id AND tppv.operational_agreement_id = toa.id AND tppv.network_id = tn.id AND tppv.cost_type_pc = 1 AND CURRENT_DATE BETWEEN tppv.start_Date AND tppv.end_date
        LEFT JOIN tmd_sale_codes_var tscv ON td.sale_code = tscv.sale_code AND tscv.item_sale_id = tis.id AND td.sale_code_type_pc = tscv.code_type_pc AND (tscv.network_id IS NULL OR tscv.network_id = tn.id) AND CURRENT_DATE BETWEEN tscv.start_date AND tscv.end_date
        LEFT JOIN tmd_sale_prices_var tspw ON tis.id = tspw.item_sale_id AND tn.id = tspw.network_id AND CURRENT_DATE BETWEEN tspw.start_Date AND tspw.end_date AND tspw.sale_price_type_pc = 1
        LEFT JOIN tmd_vat tv_acq ON tv_acq.vat = td.purchase_vat
        LEFT JOIN tmd_vat tv_sale ON tv_sale.vat = td.sale_vat
        LEFT JOIN LATERAL(SELECT COUNT(*) AS cnt_lock FROM tpa_specific_features tsf, tmd_feature_item_links tfil WHERE specific_feature = 'LK_STD_PR' AND tfil.specific_feature_id = tsf.id AND tfil.item_id = ti.id AND UPPER(tfil.features_value) = 'S' LIMIT 1) lock_prz_std ON 1 = 1
        LEFT JOIN vpa_parameter_trans vptecr ON vptecr.table_field = 'default_ecr'
        LEFT JOIN vpa_parameter_trans vptdep ON vptdep.table_field = 'default_dep'
        LEFT JOIN tmd_merchandise_structures tms_rep ON tms_rep.merchandise_structure = 'REPARTO'
        LEFT JOIN tmd_structures ts_rep ON ts_rep.structure_code = td.item_category_pc::VARCHAR AND ts_rep.merchandise_structure_id = tms_rep.id
        LEFT JOIN tmd_structures ts_rep_def ON ts_rep_def.structure_code = vptdep.boom_parameter AND ts_rep_def.merchandise_structure_id = tms_rep.id
        LEFT JOIN vmd_structures ts_ecr_def ON ts_ecr_def.structure_code = vptecr.boom_parameter
        WHERE processing_step3 = 0
          AND processing_step2 = 1
          AND processing_step1 = 1
          AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
         --- AND td.transaction_code = p_transaction_code
          ---AND td.processing_item = p_processing_item
          AND td.item = p_item
        ORDER BY processing_item, td.transaction_code, CASE WHEN td.processing_sale_code = 0 AND processing_item != 0 THEN td.id ELSE 1 END ASC, td.id ASC;

BEGIN
    -- Imposta i parametri di input della funzione (questi dovranno essere passati quando chiami la funzione)
    p_transaction_code := NULL; -- Sostituisci con il codice transazione desiderato, o lascialo NULL per tutti.
    -- p_network_ids := ARRAY[1, 2, 3]; -- Esempio: Sostituisci con gli ID dei network pertinenti
    w_f_scrivi_log := p_scrivi_log; -- Imposta a 0 per disabilitare il log, a 1 per abilitarlo.
	rows_in_total := 0;
    w_clocks :=  clock_timestamp();

    w_log_text := 'INIZIO PROCEDURA : '||TO_CHAR(w_clocks, 'YYYY-MM-DD HH24:MI:SS.US');
    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);

    ---
    w_log_text := 'ESTRAGGO BRAND';
    IF w_f_scrivi_log = 1 THEN
        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
    END IF;

    SELECT (SELECT PARAMETER_CODE FROM VPA_PARAMETERS VP2 WHERE PARAMETER_HEADER = 12 AND IS_DEFAULT = 1 AND STR_VAL_1 = VP.STR_VAL_1)
    INTO W_BRAND_PC
    FROM VPA_PARAMETERS VP
    WHERE PARAMETER_HEADER = 96
      AND IS_DEFAULT = 1
      AND DESCRIPTION = 'BRAND';


     SELECT count(distinct item) ----transaction_code, processing_item, item, id -- Aggiunto id per l'update finale
	 into w_count_total
        FROM tin_data_item_in td
        WHERE processing_step3 = 0 and processing_step4 = 0
          AND processing_step2 = 1
          AND processing_step1 = 1
          AND ((p_transaction_code is not null and td.transaction_code = p_transaction_code ) or (p_transaction_code is null ))
          AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0);


    -- Crea una tabella temporanea per memorizzare i dati del blocco
    -- TEMPORARY TABLE è automaticamente droppata alla fine della sessione
    CREATE TEMPORARY TABLE temp_transaction_items (
     ---   transaction_code INTEGER,
      ---  processing_item INTEGER,
        item TEXT
      ---  id BIGINT -- Aggiungi l'ID della riga di tin_data_item_in per l'update finale
    ) ON COMMIT DROP; -- ON COMMIT DROP garantisce che la tabella venga eliminata alla fine della transazione

    ---
    w_log_text := 'Loop sul primo cursore (transazioni) per recuperare blocchi';
    IF w_f_scrivi_log = 1 THEN
        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
    END IF;

    -- Apri il primo cursore
    OPEN cur_transactions(p_transaction_code);

    LOOP
        -- Resetta il contatore delle righe recuperate per questo blocco
        rows_in_current_block := 0;

        w_log_text := 'LOOP INTERNO PER RECUPERARE UN BLOCCO DI 100 ARTICOLI';
        IF w_f_scrivi_log = 1 THEN
             w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
        END IF;

        -- Loop interno per recuperare un blocco di 100 articoli (o meno se finiscono)
        FOR i IN 1..block_size LOOP
            FETCH cur_transactions INTO r_transaction_row; -- Recupera la riga dal cursore principale

            -- Se non ci sono più righe nel cursore principale, esci dal loop di recupero
            IF NOT FOUND THEN
                EXIT;
            END IF;

            -- Inserisci la riga recuperata nella tabella temporanea
            INSERT INTO temp_transaction_items (item) ----(transaction_code, processing_item, item, id)
            VALUES (r_transaction_row.item);
-----			VALUES (r_transaction_row.transaction_code, r_transaction_row.processing_item, r_transaction_row.item, r_transaction_row.id);

            rows_in_current_block := rows_in_current_block + 1; -- Incrementa il contatore
			rows_in_total := rows_in_total+  1;
        END LOOP;

        -- Se nessuna riga è stata recuperata in questo tentativo, significa che non ci sono più dati
        IF rows_in_current_block = 0 THEN
            EXIT; -- Esci dal loop principale
        END IF;

       RAISE NOTICE 'ELABORAZIONE DI UN BLOCCO DI % ARTICOLI DALLA TABELLA TEMPORANEA.', rows_in_current_block;
        w_log_text := w_clocks||'- ELABORAZIONE DI UN BLOCCO DI ' || ROWS_IN_CURRENT_BLOCK || ' ARTICOLI DALLA TABELLA TEMPORANEA. RECORD NR. : '|| rows_in_total||' / '|| w_count_total;
        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);

        -- Itera sulla tabella temporanea per elaborare ogni articolo
        FOR r_temp_item IN SELECT item FROM temp_transaction_items LOOP
            -- Questo BEGIN...END con EXCEPTION gestisce gli errori per ogni singolo r_item_data
            -- all'interno del cur_items loop
            FOR r_item_data IN cur_items(r_temp_item.item) LOOP
        		BEGIN
					-- r_item_data è un record o una struttura che contiene i dati originali
					-- Inizializza le variabili con i valori originali da r_item_data
					w_esito_processing_item          := r_item_data.processing_item;
					w_esito_processing_sales_code    := r_item_data.processing_sale_code;
					w_esito_processing_assortment    := r_item_data.processing_assortment;
					w_esito_processing_puchase_price := r_item_data.processing_puchase_price;
					w_esito_processing_sale_price    := r_item_data.processing_sale_price;
					w_esito_processing_features      := r_item_data.processing_feature;

                    w_log_text := null;
                    w_log_err := null;

					-- Applica la logica condizionale a ciascuna variabile:
					-- Se il valore originale è 1, assegna 2; altrimenti, mantiene il valore originale.
					-- 1. Per w_esito_processing_item
/* 					IF w_esito_processing_item = 1 THEN
					    w_esito_processing_item := 2;
					END IF;

					-- 2. Per w_esito_processing_sales_code
					IF w_esito_processing_sales_code = 1 THEN
					    w_esito_processing_sales_code := 2;
					END IF;

					-- 3. Per w_esito_processing_assortment
					IF w_esito_processing_assortment = 1 THEN
					    w_esito_processing_assortment := 2;
					END IF;

					-- 4. Per w_esito_processing_puchase_price
					IF w_esito_processing_puchase_price = 1 THEN
					    w_esito_processing_puchase_price := 2;
					END IF;

					-- 5. Per w_esito_processing_sale_price
					IF w_esito_processing_sale_price = 1 THEN
					    w_esito_processing_sale_price := 2;
					END IF;

					-- 6. Per w_esito_processing_features
					IF w_esito_processing_features = 1 THEN
					    w_esito_processing_features := 2;
					END IF; */

                    w_log_text := 'ESTRAGGO TMD_STRUCTURES';
	        	    IF w_f_scrivi_log = 1 THEN
	        	         w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	        	    END IF;

	        	    SELECT VP.STR_VAL_1,
	        	           (SELECT ID FROM tpa_specific_features tsf WHERE specific_feature = 'REP'),
	        	           (SELECT str_val
	        	            FROM tpa_features_models tfm
	        	            WHERE specific_feature_id = (SELECT ID FROM tpa_specific_features tsf_inner WHERE specific_feature = 'REP')
	        	              AND FEATURES_VALUE = r_item_data.item_category_pc::VARCHAR)
	        	    INTO W_CASH_DEP, W_CASH_DEP_FEATURE_ID, W_CASH_DEP_DESCRIPTION
	        	    FROM VPA_PARAMETERS VP
	        	    WHERE PARAMETER_HEADER = 96
	        	      AND VP.DESCRIPTION = 'CASH_DEP'
	        	      AND IS_DEFAULT = 1;

                    w_log_text := 'ESTRAGGO IVA';
	        	    IF w_f_scrivi_log = 1 THEN
	        	         w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	        	    END IF;

	        	    IF r_item_data.is_multi_vat = 0 THEN
	        	         SELECT TV.ID
	        	         INTO W_VAT_ID
	        	         FROM TMD_VAT TV, VPA_PARAMETERS VP
	        	         WHERE PARAMETER_HEADER = 96
	        	           AND VP.DESCRIPTION = 'VAT_VALUE'
	        	           AND IS_DEFAULT = 1
	        	           AND VAT_VALUE::VARCHAR = STR_VAL_1::VARCHAR;
	        	    ELSE
	        	         SELECT TV.ID
	        	         INTO W_VAT_ID
	        	         FROM TMD_VAT TV, VPA_PARAMETERS VP
	        	         WHERE PARAMETER_HEADER = 96
	        	           AND VP.DESCRIPTION = 'MULTI_VAT_CODE'
	        	           AND IS_DEFAULT = 1
	        	           AND VAT::VARCHAR = STR_VAL_1::VARCHAR;
	        	    END IF;

                    w_log_text := 'ESTRAGGO UNITA DI MISURA LOGISTICA';
	        	    IF w_f_scrivi_log = 1 THEN
	        	        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	        	    END IF;

	        	    SELECT PARAMETER_CODE
	        	    INTO W_LOGISTIC_UNIT_MEASURE
	        	    FROM VPA_PARAMETERS VP
	        	    WHERE PARAMETER_HEADER = 30
	        	      AND IS_DEFAULT = 1
	        	      AND STR_VAL_1 = (SELECT DESCRIPTION
	        	                           FROM VPA_PARAMETERS VP_inner
	        	                           WHERE PARAMETER_HEADER = 10
	        	                             AND IS_DEFAULT = 1
	        	                             AND PARAMETER_CODE = r_item_data.stock_unit_pc);

                    w_log_text := 'ESTRAGGO UNITA DI MISURA PREZZO';
	        	    IF w_f_scrivi_log = 1 THEN
	        	        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	        	    END IF;

	        	    SELECT PARAMETER_CODE
	        	    INTO W_PRICE_UNIT
	        	    FROM VPA_PARAMETERS VP
	        	    WHERE PARAMETER_HEADER = 31
	        	      AND IS_DEFAULT = 1
	        	      AND STR_VAL_1 = (SELECT DESCRIPTION
	        	                           FROM VPA_PARAMETERS VP_inner
	        	                           WHERE PARAMETER_HEADER = 10
	        	                             AND IS_DEFAULT = 1
	        	                             AND PARAMETER_CODE = r_item_data.stock_unit_pc);

	        		-- INIZIALIZZO IL CODICE ARTICOLO PER TUTTI I TIPI RECORD
	        		w_item_code := COALESCE(r_item_data.item,'-1');
	        	    w_clocks :=  clock_timestamp();

                    w_log_text := '#####INIZIO ARTICOLO : '||TO_CHAR(w_clocks, 'YYYY-MM-DD HH24:MI:SS.US')||
                                  ' TIPOLOGIA : '|| coalesce(r_item_data.SOURCE_TYPE_PC,'-1') ||' - '|| substr (r_item_data.file_name,1,4)  ||
                                  '- ARTICOLO : '|| coalesce(w_item_code,'-1') ||' - '||coalesce(r_item_data.description_item,'-1') ||
                                  '- FORNITORE : '|| coalesce(r_item_data.supplier_code,'-1') ||
                                  '- NR.LINEA : '|| r_item_data.number_line ;
                   	IF w_f_scrivi_log = 1 THEN
                   		 w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	        	 	END IF;
                    ----

                    -- INIZIALIZZO LE VARIABILI DA UTILIZZARE NEL RESTO DEL PROGRAMMA, SE NON SONO VALORIZZATI PASSO I VALORI DELLA GENERAZIONE ARTICOLO
                    IF COALESCE(r_item_data.ITEM_LOGISTIC_ID,-1) <> -1 THEN
                        W_ITEM_LOGISTIC_ID := r_item_data.ITEM_LOGISTIC_ID ;
                    END IF;

                    IF COALESCE(r_item_data.LOGISTIC_UNIT_ID_1,-1) <> -1 THEN
                        W_LOGISTIC_UNIT_ID := r_item_data.LOGISTIC_UNIT_ID_1 ;
                    END IF;

                    IF COALESCE(r_item_data.sale_id,-1)  <> -1 THEN
                        W_ITEM_SALE_ID := r_item_data.sale_id ;
                    END IF;

                    IF COALESCE(r_item_data.ITEM_ID,-1) <> -1 THEN
                        W_ITEM_ID := r_item_data.ITEM_ID ;
                    END IF;

	        	    IF r_item_data.processing_item = 0 THEN


                        IF r_item_data.update_item = 1 THEN

                            w_log_text := 'GESTIONE CODICE ARTICOLO';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            IF r_item_data.item IS NULL THEN
                                w_esito_processing_item := 2;
                                w_log_text := 'ERRORE DI ELABORAZIONE !! CODICE ARTICOLO NON VALORIZZATO !!';
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
								RAISE EXCEPTION 'Errore specifico rilevato: %', w_log_text
								        USING ERRCODE = 'P0001';  -- Codice personalizzato (prefisso 'P')

                            ELSE
                                w_item_code := r_item_data.item;
                            END IF;

                            w_log_text := 'GESTIONE TMD_ITEMS';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_items WHERE item = w_item_code FOR UPDATE) INTO v_item_exists;

                            IF v_item_exists THEN
                                w_log_text := 'AGGIORNO RIGA TMD_ITEMS per item ' || w_item_code;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE tmd_items
                                SET
                                    item_type_pc = r_item_data.item_type_pc,
                                    item_category_pc = r_item_data.item_category_pc,
                                    stock_unit_pc = r_item_data.stock_unit_pc,
                                    brand_pc = W_BRAND_PC,
                                    supplier_expiry_days = 99999,
                                    depot_expiry_days = 0,
                                    shop_expiry_days = 0,
                                    consumer_expiry_days = 0,
                                    purchase_vat_id = COALESCE(r_item_data.purchase_vat_id, W_VAT_ID),
                                    sale_vat_id = COALESCE(r_item_data.sale_vat_id, W_VAT_ID),
                                    is_local = 0,
                                    is_updated = 1,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code,
                                    is_multi_vat = r_item_data.is_multi_vat
                                WHERE item = w_item_code
                                RETURNING ID INTO W_ITEM_ID;
                            ELSE
                                w_log_text := 'INSERISCO RIGA TMD_ITEMS per item ' || w_item_code;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_items (
                                    item,
                                    item_type_pc,
                                    item_category_pc,
                                    stock_unit_pc,
                                    brand_pc,
                                    supplier_expiry_days,
                                    depot_expiry_days,
                                    shop_expiry_days,
                                    consumer_expiry_days,
                                    purchase_vat_id,
                                    sale_vat_id,
                                    is_local,
                                    is_updated,
                                    last_user,
                                    transaction_code,
                                    is_multi_vat
                                ) VALUES (
                                    w_item_code,
                                    r_item_data.item_type_pc,
                                    r_item_data.item_category_pc,
                                    r_item_data.stock_unit_pc,
                                    W_BRAND_PC,
                                    99999,
                                    0,
                                    0,
                                    0,
                                    COALESCE(r_item_data.purchase_vat_id, W_VAT_ID),
                                    COALESCE(r_item_data.sale_vat_id, W_VAT_ID),
                                    0,
                                    1,
                                    p_user,
                                    r_item_data.transaction_code,
                                    r_item_data.is_multi_vat
                                ) RETURNING ID INTO W_ITEM_ID;
                            END IF;

                            w_log_text := 'GESTIONE TTR_ITEMS (descrizione articolo)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM ttr_items WHERE item_id = w_item_id AND language_id in (SELECT id FROM tpa_languages) FOR UPDATE) INTO v_ttr_item_exists;

                            IF v_ttr_item_exists THEN
                                w_log_text := 'AGGIORNO DESCRIZIONE ARTICOLO per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE ttr_items
                                SET short_description = r_item_data.description_item,
                                    description = r_item_data.description_item,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_id = w_item_id AND language_id IN (SELECT id FROM tpa_languages);
                            ELSE
                                w_log_text := 'INSERISCO DESCRIZIONE ARTICOLO per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO ttr_items (item_id, language_id, short_description, description, last_user, transaction_code)
                                SELECT w_item_id, id, r_item_data.description_item, r_item_data.description_item, p_user, r_item_data.transaction_code
                                FROM tpa_languages tl;
                            END IF;
                        END IF;


                        IF r_item_data.update_sale = 1 THEN
                            w_log_text := 'GESTIONE TMD_ITEM_SALES (variante di vendita)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_item_sales WHERE item_id = w_item_id AND item_sale = 1 FOR UPDATE) INTO v_item_sale_exists;

                            IF v_item_sale_exists THEN
                                w_log_text := 'AGGIORNO VARIANTE DI VENDITA per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE tmd_item_sales
                                SET
                                    val_weight = r_item_data.val_weight_sale,
                                    weight_unit_measure_pc = r_item_data.weight_Unit_Measure_Sale_Pc,
                                    pieces = 1,
                                    is_updated = 1,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_id = w_item_id AND item_sale = 1
                                RETURNING ID INTO W_ITEM_SALE_ID;
                            ELSE
                                w_log_text := 'INSERISCO VARIANTE DI VENDITA per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_item_sales (
                                    item_id,
                                    item_sale,
                                    val_weight,
                                    weight_unit_measure_pc,
                                    pieces,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    w_item_id,
                                    1,
                                    r_item_data.val_weight_sale,
                                    r_item_data.weight_Unit_Measure_Sale_Pc,
                                    1,
                                    1,
                                    p_user,
                                    r_item_data.transaction_code
                                ) RETURNING ID INTO W_ITEM_SALE_ID;
                            END IF;

                            w_log_text := 'GESTIONE TTR_ITEM_SALES (descrizione variante vendita)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM ttr_item_sales WHERE item_sale_id = W_ITEM_SALE_ID AND language_id in (SELECT id FROM tpa_languages ) FOR UPDATE) INTO v_ttr_item_sale_exists;

                            IF v_ttr_item_sale_exists THEN
                                w_log_text := 'AGGIORNO DESCRIZIONE VARIANTE VENDITA per item_sale_id ' || W_ITEM_SALE_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE ttr_item_sales
                                SET description = r_item_data.description_sale,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_sale_id = W_ITEM_SALE_ID AND language_id IN (SELECT id FROM tpa_languages);
                            ELSE
                                w_log_text := 'INSERISCO DESCRIZIONE VARIANTE VENDITA per item_sale_id ' || W_ITEM_SALE_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO ttr_item_sales (
                                    item_sale_id,
                                    language_id,
                                    description,
                                    last_user,
                                    transaction_code
                                ) SELECT W_ITEM_SALE_ID, id, r_item_data.description_sale, p_user, r_item_data.transaction_code
                                FROM tpa_languages tl;
                            END IF;
                        END IF;


                        IF r_item_data.update_logistic = 1 THEN
                            w_log_text := 'GESTIONE TMD_ITEM_LOGISTICS (variante logistica)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_item_logistics WHERE item_id = w_item_id AND item_logistic = 1 FOR UPDATE) INTO v_item_logistic_exists;

                            IF v_item_logistic_exists THEN
                                w_log_text := 'AGGIORNO VARIANTE LOGISTICA per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE tmd_item_logistics
                                SET
                                    unit_measure_pc = W_LOGISTIC_UNIT_MEASURE,
                                    medium_weight = 1,
                                    is_updated = 1,
                                    last_user = P_USER,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_id = w_item_id AND item_logistic = 1
                                RETURNING ID INTO W_ITEM_LOGISTIC_ID;

                            ELSE
                                w_log_text := 'INSERISCO VARIANTE LOGISTICA per item_id ' || w_item_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_item_logistics (
                                    item_logistic,
                                    item_id,
                                    unit_measure_pc,
                                    medium_weight,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    1,
                                    w_item_id,
                                    W_LOGISTIC_UNIT_MEASURE,
                                    1,
                                    1,
                                    P_USER,
                                    r_item_data.transaction_code
                                ) RETURNING ID INTO W_ITEM_LOGISTIC_ID;
                            END IF;
                        END IF;

                        IF r_item_data.update_logistic_unit_1 = 1 THEN

                            w_log_text := 'GESTIONE UNITA LOGISTICA 1';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_logistic_units WHERE item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_pc = 1 FOR UPDATE) INTO v_logistic_unit_1_exists;

                            IF v_logistic_unit_1_exists  THEN
                                w_log_text := 'AGGIORNO UNITA LOGISTICA 1 per item_logistic_id ' || W_ITEM_LOGISTIC_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE tmd_logistic_units
                                SET
                                    weight_unit_measure_pc = r_item_data.weight_unit_measure_sale_pc,
                                    val_height = r_item_data.val_Height_Pack,
                                    val_length = r_item_data.val_Length_Pack,
                                    val_width = r_item_data.val_Width_Pack,
                                    val_volume = 0.000,
                                    val_weight = NULL,
                                    pieces = 1,
                                    packs = 0,
                                    coefficient = 1,
                                    is_updated = 1,
                                    last_user = P_USER,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_pc = 1
                                RETURNING ID INTO W_LOGISTIC_UNIT_ID;
                            ELSE
                                w_log_text := 'INSERISCO UNITA LOGISTICA 1 per item_logistic_id ' || W_ITEM_LOGISTIC_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_logistic_units (
                                    item_logistic_id,
                                    logistic_unit_pc,
                                    weight_unit_measure_pc,
                                    val_height,
                                    val_length,
                                    val_width,
                                    val_volume,
                                    val_weight,
                                    pieces,
                                    packs,
                                    coefficient,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    W_ITEM_LOGISTIC_ID,                            -- item_logistic_id,
                                    1,                                             -- logistic_unit_pc,
                                    r_item_data.weight_unit_measure_sale_pc,       -- weight_unit_measure_pc,
                                    r_item_data.val_Height_Pack,                   -- val_height,
                                    r_item_data.val_Length_Pack,                   -- val_length,
                                    r_item_data.val_Width_Pack,                    -- val_width,
                                    0.000,                                         -- val_volume,
                                    NULL,                   -- val_weight,
                                    1,                                             -- pieces,
                                    0,                                             -- packs,
                                    1,                                             -- coefficient,
                                    1,                                             -- is_updated,
                                    P_USER,                                        -- last_user,
                                    r_item_data.transaction_code                   -- transaction_code
                                ) RETURNING ID INTO W_LOGISTIC_UNIT_ID;
                            END IF;
                        END IF;

                        IF r_item_data.update_logistic_unit_41 = 1 THEN
                            w_log_text := 'GESTIONE UNITA LOGISTICA 41';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_logistic_units WHERE item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_pc = 41 FOR UPDATE) INTO v_logistic_unit_41_exists;

                            IF v_logistic_unit_41_exists THEN
                                w_log_text := 'AGGIORNO UNITA LOGISTICA 41 per item_logistic_id ' || W_ITEM_LOGISTIC_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                UPDATE tmd_logistic_units
                                SET
                                    weight_unit_measure_pc = r_item_data.weight_Unit_Measure_Sale_Pc,
                                    val_height = r_item_data.val_Height_Pack,
                                    val_length = r_item_data.val_Length_Pack,
                                    val_width = r_item_data.val_Width_Pack,
                                    val_volume = 0.000,
                                    val_weight = r_item_data.pieces_pack,
                                    pieces = r_item_data.pieces_pack,
                                    packs = 1,
                                    coefficient = r_item_data.pieces_pack,
                                    is_updated = 1,
                                    last_user = P_USER,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_pc = 41;
                            ELSE
                                w_log_text := 'INSERISCO UNITA LOGISTICA 41 per item_logistic_id ' || W_ITEM_LOGISTIC_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_logistic_units (
                                    item_logistic_id,
                                    logistic_unit_pc,
                                    weight_unit_measure_pc,
                                    val_height,
                                    val_length,
                                    val_width,
                                    val_volume,
                                    val_weight,
                                    pieces,
                                    packs,
                                    coefficient,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    W_ITEM_LOGISTIC_ID,                             ----- item_logistic_id,
                                    41,                                             ----- logistic_unit_pc,
                                    r_item_data.weight_unit_measure_sale_pc,                                              ----- weight_unit_measure_pc,
                                    r_item_data.val_Height_Pack,                    ----- val_height,
                                    r_item_data.val_Length_Pack,                    ----- val_length,
                                    r_item_data.val_Width_Pack,                     ----- val_width,
                                    0.000,                                          ----- val_volume,
                                    r_item_data.pieces_pack,                    ----- val_weight,
                                    r_item_data.pieces_pack,                                              ----- pieces,
                                    1,                                              ----- packs,
                                    r_item_data.pieces_pack,                        ----- coefficient,
                                    1,                                              ----- is_updated,
                                    P_USER,                                         ----- last_user,
                                    r_item_data.transaction_code                    ----- transaction_code
                                );
                            END IF;
                        END IF;

                        w_log_text := 'GESTIONE LEGAME STRUTTURA ECR';
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;



                        SELECT EXISTS (SELECT 1 FROM tmd_structure_item_links_var WHERE structure_id = r_item_data.structure_id AND item_id = W_ITEM_ID AND CURRENT_DATE BETWEEN start_date AND end_date FOR UPDATE) INTO v_structure_item_link_exists;

                        IF v_structure_item_link_exists THEN
                            w_log_text := '1-AGGIORNO tmd_structure_item_links_var LEGAME STRUTTURA ECR per item_id ' || W_ITEM_ID;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            UPDATE tmd_structure_item_links_var
                            SET   end_date = CURRENT_DATE - INTERVAL '1 day', -- Close current link
                                  is_updated = 1,
                                  last_user = P_USER,
                                  transaction_code = r_item_data.transaction_code
                            WHERE structure_id = r_item_data.structure_id
                              AND item_id = W_ITEM_ID
                              AND CURRENT_DATE BETWEEN start_date AND end_date;

                            w_log_text := '1-Aggiorno TMD_STRUCTURE_ITEM_LINKS da TMD_STRUCTURE_ITEM_LINKS_VAR per item_id ' || W_ITEM_ID;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

--                                UPDATE TMD_STRUCTURE_ITEM_LINKS
--                                SET END_DATE = CURRENT_DATE - INTERVAL '1 day',
--                                    UPDATE_DATE = CURRENT_TIMESTAMP,
--                                    LAST_USER = P_USER,
--                                    TRANSACTION_CODE = r_item_data.transaction_code
--                                WHERE ITEM_ID = W_ITEM_ID
--                                AND STRUCTURE_ID = r_item_data.structure_id
--                                AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;

                                w_log_text := '1-NON ESISTE E INSERIMENTO TMD_STRUCTURE_ITEM_LINKS_VAR PER CURRENT DATE E 311299 per item_id -  ' || W_ITEM_ID || '  ESISTENZA : '||v_structure_item_link_exists_1
                                                || '  START_dATE : '||(CURRENT_DATE) || '  END_dATE : 31/12/2099' || '  STRUCTURE : '||r_item_data.structure_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_structure_item_links_var (
                                    structure_id,
                                    item_id,
                                    start_date,
                                    end_date,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    r_item_data.structure_id,
                                    W_ITEM_ID,
                                    CURRENT_DATE,
                                    TO_DATE('31/12/2099', 'DD/MM/YYYY'),
                                    1,
                                    P_USER,
                                    r_item_data.transaction_code
                                );

--                            w_log_text := '1-INSERIMENTO TMD_STRUCTURE_ITEM_LINKS da TMD_STRUCTURE_ITEM_LINKS_VAR per item_id ' || W_ITEM_ID;
--                                IF w_f_scrivi_log = 1 THEN
--                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                END IF;
--
--                                INSERT INTO TMD_STRUCTURE_ITEM_LINKS (ID, STRUCTURE_ID, ITEM_ID, START_DATE, END_DATE, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
--                                SELECT nextval('tmd_structure_item_links_id_seq'::regclass),
--                                       structure_id, item_id, start_date, end_date, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, last_user, transaction_code
--                                FROM TMD_STRUCTURE_ITEM_LINKS_VAR
--                                WHERE CURRENT_DATE BETWEEN START_DATE AND END_DATE
--                                  AND ITEM_ID = W_ITEM_ID
--                                  AND STRUCTURE_ID = r_item_data.structure_id;
                        ELSE
	        				w_log_text := '1-Verifico se esiste record per strutt merc principale  su  TMD_STRUCTURE_ITEM_LINKS_var per item_id ' || W_ITEM_ID;

						   sELECT tl.id
							into v_structure_item_id
							FROM boom.tmd_merchandise_structures tm , tmd_structures ts ,tmd_structure_item_links_var tl
							WHERE tm.is_default = 1
							and ts.merchandise_structure_id = tm.id
							and tl. structure_id = ts.id
							and current_date between tl.start_date and tl.end_date
							and tl.item_id = W_ITEM_ID;

	                        IF coalesce(v_structure_item_id,-1) <> -1 THEN
		        				w_log_text := '1-Aggiorno la data fine a ieri su   TMD_STRUCTURE_ITEM_LINKS_var per item_id ' || W_ITEM_ID;
								update tmd_structure_item_links_var
								set   end_date = CURRENT_DATE - INTERVAL '1 day', -- Close current link
	                                  is_updated = 1,
	                                  last_user = P_USER,
	                                  transaction_code = r_item_data.transaction_code
	                            WHERE id = v_structure_item_id;
							END IF;

                            w_log_text := '2-INSERISCO tmd_structure_item_links_var LEGAME STRUTTURA ECR per item_id ' || W_ITEM_ID;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            INSERT INTO tmd_structure_item_links_var (
                                structure_id,
                                item_id,
                                start_date,
                                end_date,
                                is_updated,
                                last_user,
                                transaction_code
                            ) VALUES (
                                r_item_data.structure_id,
                                W_ITEM_ID,
                                CURRENT_DATE,
                                TO_DATE('31/12/2099', 'DD/MM/YYYY'),
                                1,
                                P_USER,
                                r_item_data.transaction_code
                            );

--                            w_log_text := '2-INSERIMENTO TMD_STRUCTURE_ITEM_LINKS da TMD_STRUCTURE_ITEM_LINKS_VAR per item_id ' || W_ITEM_ID;
--                            IF w_f_scrivi_log = 1 THEN
--                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                            END IF;
--
--                            INSERT INTO TMD_STRUCTURE_ITEM_LINKS (ID, STRUCTURE_ID, ITEM_ID, START_DATE, END_DATE, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
--                            SELECT nextval('tmd_structure_item_links_id_seq'::regclass),
--                                   structure_id, item_id, start_date, end_date, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, last_user, transaction_code
--                            FROM TMD_STRUCTURE_ITEM_LINKS_VAR
--                            WHERE CURRENT_DATE BETWEEN START_DATE AND END_DATE
--                              AND ITEM_ID = W_ITEM_ID
--                              AND STRUCTURE_ID = r_item_data.structure_id;
                        END IF;

                        IF w_esito_processing_item <> 2 THEN
                            w_esito_processing_item = 1 ;
                        END IF;

                    END IF;


                    IF r_item_data.processing_feature = 0 THEN
                        w_log_text := 'GESTIONE TMD_FEATURE_ITEM_LINKS (attributi STD)';
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;

                        -- Determina il feature_id e il valore dell'attributo in base all'indice del loop
                        w_log_text := 'Determino feature_id e valore per ATTRIBUTO ' ;
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;

                        FOR i IN 1..15 LOOP -- Loop da 1 a 15

                            update_attribute_id := NULL;
                            SELECT INTO update_attribute_id
                                CASE i
                                    WHEN 1 THEN r_item_data.update_attribute1
                                    WHEN 2 THEN r_item_data.update_attribute2
                                    WHEN 3 THEN r_item_data.update_attribute3
                                    WHEN 4 THEN r_item_data.update_attribute4
                                    WHEN 5 THEN r_item_data.update_attribute5
                                    WHEN 6 THEN r_item_data.update_attribute6
                                    WHEN 7 THEN r_item_data.update_attribute7
                                    WHEN 8 THEN r_item_data.update_attribute8
                                    WHEN 9 THEN r_item_data.update_attribute9
                                    WHEN 10 THEN r_item_data.update_attribute10
                                    WHEN 11 THEN r_item_data.update_attribute11
                                    WHEN 12 THEN r_item_data.update_attribute12
                                    WHEN 13 THEN r_item_data.update_attribute13
                                    WHEN 14 THEN r_item_data.update_attribute14
                                    WHEN 15 THEN r_item_data.update_attribute15
                                END;

                            current_feature_id := NULL;
                            SELECT INTO current_feature_id
                                CASE i
                                    WHEN 1 THEN r_item_data.feature_id1
                                    WHEN 2 THEN r_item_data.feature_id2
                                    WHEN 3 THEN r_item_data.feature_id3
                                    WHEN 4 THEN r_item_data.feature_id4
                                    WHEN 5 THEN r_item_data.feature_id5
                                    WHEN 6 THEN r_item_data.feature_id6
                                    WHEN 7 THEN r_item_data.feature_id7
                                    WHEN 8 THEN r_item_data.feature_id8
                                    WHEN 9 THEN r_item_data.feature_id9
                                    WHEN 10 THEN r_item_data.feature_id10
                                    WHEN 11 THEN r_item_data.feature_id11
                                    WHEN 12 THEN r_item_data.feature_id12
                                    WHEN 13 THEN r_item_data.feature_id13
                                    WHEN 14 THEN r_item_data.feature_id14
                                    WHEN 15 THEN r_item_data.feature_id15
                                END;

                            current_attribute_code := NULL;
                            SELECT INTO current_attribute_code
                                CASE i
                                    WHEN 1 THEN r_item_data.attribute_code_1
                                    WHEN 2 THEN r_item_data.attribute_code_2
                                    WHEN 3 THEN r_item_data.attribute_code_3
                                    WHEN 4 THEN r_item_data.attribute_code_4
                                    WHEN 5 THEN r_item_data.attribute_code_5
                                    WHEN 6 THEN r_item_data.attribute_code_6
                                    WHEN 7 THEN r_item_data.attribute_code_7
                                    WHEN 8 THEN r_item_data.attribute_code_8
                                    WHEN 9 THEN r_item_data.attribute_code_9
                                    WHEN 10 THEN r_item_data.attribute_code_10
                                    WHEN 11 THEN r_item_data.attribute_code_11
                                    WHEN 12 THEN r_item_data.attribute_code_12
                                    WHEN 13 THEN r_item_data.attribute_code_13
                                    WHEN 14 THEN r_item_data.attribute_code_14
                                    WHEN 15 THEN r_item_data.attribute_code_15
                                END;

                            current_attribute_value := NULL;
                            SELECT INTO current_attribute_value
                                CASE i
                                    WHEN 1 THEN r_item_data.attribute_Value_1
                                    WHEN 2 THEN r_item_data.attribute_Value_2
                                    WHEN 3 THEN r_item_data.attribute_Value_3
                                    WHEN 4 THEN r_item_data.attribute_Value_4
                                    WHEN 5 THEN r_item_data.attribute_Value_5
                                    WHEN 6 THEN r_item_data.attribute_Value_6
                                    WHEN 7 THEN r_item_data.attribute_Value_7
                                    WHEN 8 THEN r_item_data.attribute_Value_8
                                    WHEN 9 THEN r_item_data.attribute_Value_9
                                    WHEN 10 THEN r_item_data.attribute_Value_10
                                    WHEN 11 THEN r_item_data.attribute_Value_11
                                    WHEN 12 THEN r_item_data.attribute_Value_12
                                    WHEN 13 THEN r_item_data.attribute_Value_13
                                    WHEN 14 THEN r_item_data.attribute_Value_14
                                    WHEN 15 THEN r_item_data.attribute_Value_15
                                END;

                            current_network_value := NULL;
                            IF current_feature_id in (2,3,4,5,11) THEN 
                                current_network_value := r_item_data.network_id ;
                            ELSE 
                                current_network_value := NULL; 
                            END IF;
        
                               -- CASE i
                                    -- WHEN 1 THEN null
                                    -- WHEN 2 THEN CASE WHEN r_item_data.network_id = -1 THEN NULL ELSE r_item_data.network_id END
                                    -- WHEN 3 THEN CASE WHEN r_item_data.network_id = -1 THEN NULL ELSE r_item_data.network_id END
                                    -- WHEN 4 THEN null
                                    -- WHEN 5 THEN null
                                    -- WHEN 6 THEN null
                                    -- WHEN 7 THEN null
                                    -- WHEN 8 THEN null
                                    -- WHEN 9 THEN null
                                    -- WHEN 10 THEN null
                                    -- WHEN 11 THEN CASE WHEN r_item_data.network_id = -1 THEN NULL ELSE r_item_data.network_id END
                                    -- WHEN 12 THEN null
                                    -- WHEN 13 THEN null
                                    -- WHEN 14 THEN null
                                    -- WHEN 15 THEN null
                                -- END;

                            -- Determina il feature_id e il valore dell'attributo in base all'indice del loop
                            w_log_text := 'Recupera la descrizione della feature se necessaria per str_val' ;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            -- Solo se l'attributo è presente (ID diverso da -1)
                            IF update_attribute_id <> -1 and current_feature_id <> -1 and coalesce(trim(current_attribute_value),'#NULL#') <> '#NULL#' THEN
                                w_log_text := 'Controllo esistenza per ATTRIBUTO ' || i || ' (feature_id: ' || current_feature_id || ')';
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                SELECT EXISTS (SELECT 1 FROM tmd_feature_item_links WHERE specific_feature_id = current_feature_id
                                             AND item_id = w_item_id FOR UPDATE) INTO v_feature_item_link_exists;

                                IF v_feature_item_link_exists THEN
                                    w_log_text := 'AGGIORNO ATTRIBUTO ' || i || ' per item_id ' || w_item_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    UPDATE tmd_feature_item_links
                                    SET features_value = (coalesce(trim(current_attribute_value),'')),
                                        dat_val = current_date,
                                        is_updated = 1,
                                        last_user = p_user,
                                        network_id = current_network_value,
                                        transaction_code = r_item_data.transaction_code
                                    WHERE specific_feature_id = current_feature_id AND item_id = w_item_id and features_value is null ;
                                ELSE
                                    w_log_text := 'INSERISCO ATTRIBUTO ' || i || ' per item_id ' || w_item_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    INSERT INTO tmd_feature_item_links (
                                        specific_feature_id,
                                        item_id,
                                        features_value,
                                        network_id,
                                        num_val,
                                        dat_val,
                                        is_updated,
                                        last_user,
                                        transaction_code
                                    ) VALUES (
                                        current_feature_id,
                                        w_item_id,
                                        (coalesce(trim(current_attribute_value),'')),
                                        current_network_value,
                                        NULL, -- Popula se hai un valore numerico
                                        current_date, -- Popula se hai un valore data
                                        1,
                                        p_user,
                                        r_item_data.transaction_code
                                    );
                                END IF;
                            ELSE
                                w_log_text := 'ATTRIBUTO ' || i || ' non valido (feature_id IS NULL o -1) o non valorizzato, salto l''elaborazione.';
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                            END IF;
                        END LOOP; -- Fine loop per gli attributi da 1 a 15

                        IF w_esito_processing_features <> 2 THEN
                            w_esito_processing_features := 1 ;
                        END IF;
                     END IF;

                    IF r_item_data.processing_sale_code = 0 THEN

                        IF r_item_data.update_sale_code = 1 THEN
                            w_log_text := 'GESTIONE BARCODE DI VENDITA (tmd_sale_codes_var)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

							IF W_ITEM_SALE_ID = -1 THEN
							     w_esito_processing_sales_code := 2 ;
							 	RAISE EXCEPTION 'Errore specifico rilevato: %', w_log_text
								        USING ERRCODE = 'P0001';  -- Codice personalizzato (prefisso 'P');

							END IF;

							v_sale_code_exists := FALSE;

                            SELECT EXISTS (SELECT 1 FROM tmd_sale_codes_var WHERE   item_sale_id = W_ITEM_SALE_ID 
                            AND code_type_pc = r_item_data.sale_code_type_pc 
                            AND (network_id IS NULL OR network_id = r_item_data.network_id) FOR UPDATE) INTO v_sale_code_exists;

                            IF v_sale_code_exists THEN

                                w_log_text := '1- AGGIORNO RIGA TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

 								w_log_text := '1- FORZO IL FLAG ETICHETTA A 0 SU  TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;

                                UPDATE tmd_sale_codes_var
                                SET is_label = 0 ,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_sale_id = W_ITEM_SALE_ID and code_type_pc = r_item_data.sale_code_type_pc;
                      

 							    w_log_text := '1- AGGIORNO RIGA TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

        --        				 UPDATE tmd_sale_codes_var set
        ---                            is_updated = 1,
         --                           is_label = 0 ,
         --                           last_user = p_user,
         ---                           transaction_code = r_item_data.transaction_code
         ----                       WHERE sale_code = r_item_data.sale_code and  item_sale_id = W_ITEM_SALE_ID AND code_type_pc = r_item_data.sale_code_type_pc AND (network_id IS NULL OR network_id = r_item_data.network_id) AND r_item_data.start_date_sale_code BETWEEN start_date AND end_date;

                                w_log_text := '1- INSERISCO RIGA TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                w_network_id := NULL;
                                w_plu_code := NULL;
                                w_bilance_department_pc := NULL;
                                w_bilance_code := NULL;

								IF r_item_data.sale_code_type_pc = 10 THEN
                                    w_network_id := r_item_data.network_id;
									w_plu_code := substr (r_item_data.sale_code,4,3);
									w_bilance_department_pc := substr (r_item_data.sale_code,3,1);
									w_bilance_code := substr (r_item_data.sale_code,4,3);
								END IF;

                                INSERT INTO tmd_sale_codes_var (
                                    item_sale_id,
                                    network_id,
                                    code_type_pc,
                                    sale_code,
                                    start_date,
                                    end_date,
                                    is_label,
 									plu_code,
									bilance_department_pc,
									bilance_code,
                                    tare,
                                    is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                    (select id from tmd_item_sales where item_id = w_item_id),
                                    w_network_id,
                                    r_item_data.sale_code_type_pc,
                                    r_item_data.sale_code,
                                    r_item_data.start_date_sale_code,
                                    r_item_data.end_date_sale_code,
                                    coalesce(r_item_data.is_label_sale_code,0),
									w_plu_code ,
									w_bilance_department_pc ,
									w_bilance_code ,
                                    r_item_data.tare,
                                    1,
                                    P_USER,
                                    r_item_data.transaction_code
                                );

                            ELSE
                              	w_log_text := '1- FORZO IL FLAG ETICHETTA A 0 SU  TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;

                                UPDATE tmd_sale_codes_var
                                SET is_label = 0 ,
                                    last_user = p_user,
                                    transaction_code = r_item_data.transaction_code
                                WHERE item_sale_id = W_ITEM_SALE_ID and code_type_pc = r_item_data.sale_code_type_pc;
                                -----
                                w_log_text := '1.1 - INSERISCO RIGA TMD_SALE_CODES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e code_type_pc ' || r_item_data.sale_code_type_pc;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                                ---
                                w_network_id := NULL;
                                w_plu_code := NULL;
                                w_bilance_department_pc := NULL;
                                w_bilance_code := NULL;

								IF r_item_data.sale_code_type_pc = 10 THEN
                                    w_network_id := r_item_data.network_id;
									w_plu_code := substr (r_item_data.sale_code,4,3);
									w_bilance_department_pc := substr (r_item_data.sale_code,3,1);
									w_bilance_code := substr (r_item_data.sale_code,4,3);
								END IF;
                                ------
                                INSERT INTO tmd_sale_codes_var (
                                    item_sale_id,
                                      network_id,
                                    code_type_pc,
                                    sale_code,
                                    start_date,
                                    end_date,
                                    is_label,
                       				plu_code,
									bilance_department_pc,
									bilance_code,
                                    tare,
                   					 is_updated,
                                    last_user,
                                    transaction_code
                                ) VALUES (
                                   (select id from tmd_item_sales where item_id = w_item_id) ,
                                   w_network_id,
                                    r_item_data.sale_code_type_pc,
                                    r_item_data.sale_code,
                                    r_item_data.start_date_sale_code,
                                    r_item_data.end_date_sale_code,
                                    coalesce(r_item_data.is_label_sale_code,0),
									w_plu_code ,
									w_bilance_department_pc ,
									w_bilance_code ,
                                    r_item_data.tare,
                                    1,
                                    P_USER,
                                    r_item_data.transaction_code
                                );

                            END IF;

--                            v_sale_code_exists := FALSE;
--
--                            SELECT EXISTS (SELECT 1 FROM tmd_sale_codes WHERE item_sale_id = W_ITEM_SALE_ID AND code_type_pc = r_item_data.sale_code_type_pc AND (network_id IS NULL OR network_id = r_item_data.network_id) FOR UPDATE) INTO v_sale_code_exists;
--
--                            IF v_sale_code_exists THEN
--                                w_log_text := '2-AGGIORNO RIGA TMD_SALE_CODES per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || COALESCE(r_item_data.network_id::TEXT, 'NULL');
--                                IF w_f_scrivi_log = 1 THEN
--                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                END IF;
--
--                                UPDATE tmd_sale_codes
--                                SET end_date = CURRENT_DATE - INTERVAL '1 day',
--                                    is_label = 0 ,
--                                    update_date = CURRENT_TIMESTAMP,
--                                    last_user = 'AGGIORNO',
--                                    transaction_code = r_item_data.transaction_code
--                                WHERE item_sale_id = W_ITEM_SALE_ID AND code_type_pc = r_item_data.sale_code_type_pc AND (network_id IS NULL OR network_id = r_item_data.network_id) AND CURRENT_DATE BETWEEN start_date AND end_date;
--
--                                w_log_text := '2-INSERISCO RIGA TMD_SALE_CODES per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || COALESCE(r_item_data.network_id::TEXT, 'NULL');
--                                IF w_f_scrivi_log = 1 THEN
--                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                END IF;
--
--                                INSERT INTO tmd_sale_codes (
--                                    id,
--                                    item_sale_id,
--                                    network_id,
--                                    code_type_pc,
--                                    sale_code,
--                                    start_date,
--                                    end_date,
--                                    is_label,
--                                    last_user,
--                                    transaction_code,
--                                    creation_date,
--                                    update_date
--                                ) VALUES (
--                                    nextval('tmd_sale_codes_id_seq'::regclass),
--                                    W_ITEM_SALE_ID,
--                                    r_item_data.network_id,
--                                    r_item_data.sale_code_type_pc,
--                                    r_item_data.sale_code,
--                                    r_item_data.start_date_sale_code,
--                                    r_item_data.end_date_sale_code,
--                                    coalesce(r_item_data.is_label_sale_code,0),
--                                    P_USER,
--                                    r_item_data.transaction_code,
--                                    CURRENT_TIMESTAMP,
--                                    CURRENT_TIMESTAMP
--                                );
--                            ELSE
--                                w_log_text := '2.2 - INSERISCO RIGA TMD_SALE_CODES per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || COALESCE(r_item_data.network_id::TEXT, 'NULL');
--                                IF w_f_scrivi_log = 1 THEN
--                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                END IF;
--
--                                INSERT INTO tmd_sale_codes (
--                                    id,
--                                    item_sale_id,
--                                    network_id,
--                                    code_type_pc,
--                                    sale_code,
--                                    start_date,
--                                    end_date,
--                                    is_label,
--                                    last_user,
--                                    transaction_code,
--                                    creation_date,
--                                    update_date
--                                ) VALUES (
--                                    nextval('tmd_sale_codes_id_seq'::regclass),
--                                    W_ITEM_SALE_ID,
--                                    r_item_data.network_id,
--                                    r_item_data.sale_code_type_pc,
--                                    r_item_data.sale_code,
--                                    r_item_data.start_date_sale_code,
--                                    r_item_data.end_date_sale_code,
--                                    coalesce(r_item_data.is_label_sale_code,0),
--                                    P_USER,
--                                    r_item_data.transaction_code,
--                                    CURRENT_TIMESTAMP,
--                                    CURRENT_TIMESTAMP
--                                );
--                            END IF;

                           -- Inserimento del codice a barre '7999' se non esiste e Mag_Rif = 'R99'
                            INSERT INTO tmd_sale_codes_var (item_sale_id, code_type_pc, sale_code, start_date, end_date, is_label, is_updated, transaction_code)
                            SELECT tis.id, 12, '7999'||substr(lpad(replace(ti.item collate "C"/*case_like*/,'-',''),8,'0'),1,8)||fn_get_check_digit('7999'||lpad(replace(ti.item collate "C"/*case_like*/,'-',''),8,'0')),
                                   CURRENT_DATE, TO_DATE('31122099','DDMMYYYY'), 0, 1, ti.transaction_code
                            FROM tmd_items ti
                            INNER JOIN tmd_item_sales tis ON ti.id = tis.item_id
                            WHERE ti.id = W_ITEM_ID
                            AND regexp_match(ti.item collate "C"/*case_like*/,'[A-Za-z]') IS NULL
                            AND NOT EXISTS (
                                SELECT 1 FROM tmd_sale_codes_var tscv
                                WHERE tscv.code_type_pc = 12 AND CURRENT_DATE BETWEEN tscv.start_date AND tscv.end_date AND tscv.item_sale_id = tis.id 
                            )
                            AND EXISTS (
                                SELECT 1 FROM tmd_feature_item_links tfil
                                WHERE tfil.item_id = ti.id
                                AND specific_feature_id = (SELECT id FROM tpa_specific_features tsf WHERE specific_feature = 'MAG_RIF')
                                AND features_value = 'R99'
                            );

--                           INSERT INTO tmd_sale_codes (item_sale_id, code_type_pc, sale_code, start_date, end_date, is_label, transaction_code)
--                            SELECT tis.id, 12, '7999'||substr(lpad(replace(ti.item collate "C"/*case_like*/,'-',''),8,'0'),1,8)||fn_get_check_digit('7999'||lpad(replace(ti.item collate "C"/*case_like*/,'-',''),8,'0')),
--                                   CURRENT_DATE, TO_DATE('31122099','DDMMYYYY'), 0, ti.transaction_code
--                            FROM tmd_items ti
--                            INNER JOIN tmd_item_sales tis ON ti.id = tis.item_id
--                            WHERE ti.id = W_ITEM_ID
--                            AND regexp_match(ti.item collate "C"/*case_like*/,'[A-Za-z]') IS NULL
--                            AND NOT EXISTS (
--                                SELECT 1 FROM tmd_sale_codes tscv
--                                WHERE tscv.code_type_pc = 12 AND CURRENT_DATE BETWEEN tscv.start_date AND tscv.end_date AND tscv.item_sale_id = tis.id
--                            )
--                            AND EXISTS (
--                                SELECT 1 FROM tmd_feature_item_links tfil
--                                WHERE tfil.item_id = ti.id
--                                AND specific_feature_id = (SELECT id FROM tpa_specific_features tsf WHERE specific_feature = 'MAG_RIF')
--                                AND features_value = 'R99'
--                            );
                        END IF; -- Chiusura IF r_item_data.update_sale_code = 1

                        IF w_esito_processing_sales_code <> 2 THEN
                            w_esito_processing_sales_code := 1 ;
                        END IF;

                    END IF; -- Chiusura IF r_item_data.processing_sale_code = 0

                    IF r_item_data.processing_assortment = 0 THEN
                        IF r_item_data.update_assortment = 1 THEN
                            w_log_text := '1 - GESTIONE ASSORTIMENTO FORNITORE CENTRALE (tmd_orderable_assortments_var)';
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;


                            w_log_text := '1 - VERIFICO SE ESISTE GIA UN RECORD CON ASSORTIMENTO FORNITORE CENTRALE (tmd_orderable_assortments_var)  '
                                            || ' logistic : '|| r_item_data.LOGISTIC_ID
                                            || ' logistic_unit_id1 : '|| r_item_data.LOGISTIC_UNIT_ID_1
                                            || ' operational_Agreement : '|| r_item_data.OPERATIONAL_AGREEMENT_ID
                                            || ' network  : '|| r_item_data.network_id
                                            || ' logistic : '|| W_ITEM_LOGISTIC_ID
                                            || ' logistic_unit_id1 : '|| W_LOGISTIC_UNIT_ID
                                            || ' ITEM_SALE_ID : '|| W_ITEM_SALE_ID;
                            IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            w_log_text := '1 - VERIFICO SU tmd_orderable_assortments_var SE E'' PRESENTE IL RECORD A PARITA'' DI DATI per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            IF r_item_data.third_party_id = -1 OR  r_item_data.OPERATIONAL_AGREEMENT_ID = -1 THEN
                                w_log_text := '1 - FORNITORE e/O ACCORDO NON PRESENTE IN ANAGRAFICA per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                w_log_err := w_log_text;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                                w_esito_processing_assortment := 2 ;


                            ELSE

                                SELECT EXISTS (SELECT 1 FROM tmd_orderable_assortments_var WHERE item_id = W_ITEM_ID
                                AND item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_id = W_LOGISTIC_UNIT_ID
                                AND operational_agreement_id = r_item_data.OPERATIONAL_AGREEMENT_ID AND network_id = r_item_data.network_id
                                AND r_item_data.start_date_assortment = start_date  FOR UPDATE) INTO v_orderable_assortment_exists;

                                IF v_orderable_assortment_exists then
                                    w_log_text := '1 - RECORD PRESENTE NON FACCIO NULLA per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                else
                                    w_log_text := '1 - RECORD NON PRESENTE A PARITA'' DI DATI per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    SELECT EXISTS (SELECT 1 FROM tmd_orderable_assortments_var WHERE item_id = W_ITEM_ID
                                    AND item_logistic_id = W_ITEM_LOGISTIC_ID AND logistic_unit_id = W_LOGISTIC_UNIT_ID
                                    AND operational_agreement_id = r_item_data.OPERATIONAL_AGREEMENT_ID AND network_id = r_item_data.network_id
                                    AND r_item_data.start_date_assortment BETWEEN start_date AND end_date FOR UPDATE) INTO v_orderable_assortment_exists;

                                    IF v_orderable_assortment_exists THEN
                                        w_log_text := '1 - AGGIORNO ASSORTIMENTO FORNITORE CENTRALE VAR 1 per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                        IF w_f_scrivi_log = 1 THEN
                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                        END IF;

                                        UPDATE tmd_orderable_assortments_var
                                            ----end_date = CURRENT_DATE - INTERVAL '1 day', -- Close current record
                                            SET end_date = CASE
                                                    -- Se la data di 'oggi meno 1 giorno' è >= della data di inizio (start_date)
                                                    WHEN CURRENT_DATE - INTERVAL '1 day' >= start_date 
                                                    -- Allora usa 'oggi meno 1 giorno' (il troncamento standard)
                                                    THEN CURRENT_DATE - INTERVAL '1 day'
                                                    -- Altrimenti (se 'oggi meno 1 giorno' è precedente a start_date)
                                                    ELSE start_date
                                            END,
                                            is_updated = 1,
											main_supplier = 0,
                                            last_user = P_USER,
                                            transaction_code = r_item_data.transaction_code
                                        WHERE item_id = W_ITEM_ID
                                          AND item_logistic_id = W_ITEM_LOGISTIC_ID
                                          AND logistic_unit_id = W_LOGISTIC_UNIT_ID
                                          AND operational_agreement_id = r_item_data.OPERATIONAL_AGREEMENT_ID
                                          AND network_id = r_item_data.network_id
                                          AND r_item_data.start_date_assortment BETWEEN start_date AND end_date;

--                                        w_log_text := '1 - AGGIORNO TMD_ORDERABLE_ASSORTMENTS per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
--                                        IF w_f_scrivi_log = 1 THEN
--                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                        END IF;
--
--                                        UPDATE TMD_ORDERABLE_ASSORTMENTS
--                                        SET END_DATE = CURRENT_DATE - INTERVAL '1 day',
--                                            UPDATE_DATE = CURRENT_TIMESTAMP,
--                                            LAST_USER = P_USER,
--                                            TRANSACTION_CODE = r_item_data.transaction_code
--                                        WHERE ITEM_ID = W_ITEM_ID AND ITEM_LOGISTIC_ID = W_ITEM_LOGISTIC_ID
--                                        AND LOGISTIC_UNIT_ID = W_LOGISTIC_UNIT_ID
--                                        AND OPERATIONAL_AGREEMENT_ID = r_item_data.OPERATIONAL_AGREEMENT_ID
--                                        AND NETWORK_ID = r_item_data.network_id AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;

                                        w_log_text := '1 - INSERISCO  ASSORTIMENTO FORNITORE CENTRALE VAR 1 per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                        IF w_f_scrivi_log = 1 THEN
                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                        END IF;

                                        SELECT nextval('tmd_orderable_assortments_id_seq'::regclass) INTO W_ORDERABLE_ASSORTMENTS_ID;

                                        INSERT INTO tmd_orderable_assortments_var (
                                            id, item_id, item_logistic_id, logistic_unit_id, operational_agreement_id, network_id,
                                            start_date, end_date, main_supplier, min_order, max_order, multiple_reorder,assortment_status_ph,delivery_status_ph,
                                            assortment_status_pc, delivery_status_pc, is_updated, last_user, transaction_code
                                        ) VALUES (
                                            W_ORDERABLE_ASSORTMENTS_ID, W_ITEM_ID, W_ITEM_LOGISTIC_ID, W_LOGISTIC_UNIT_ID, r_item_data.OPERATIONAL_AGREEMENT_ID,
                                            r_item_data.network_id, r_item_data.start_date_assortment, r_item_data.end_date_assortment , 1,
                                            r_item_data.min_order, r_item_data.max_order, r_item_data.multiple_reorder,64,66,
                                            r_item_data.assortment_status_pc, r_item_data.delivery_status_pc, 1, P_USER, r_item_data.transaction_code
                                        );

--                                        w_log_text := 'INSERISCO TMD_ORDERABLE_ASSORTMENTS da TMD_ORDERABLE_ASSORTMENTS_VAR 1 per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
--                                        IF w_f_scrivi_log = 1 THEN
--                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                        END IF;
--
--                                        INSERT INTO TMD_ORDERABLE_ASSORTMENTS (ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, ASSORTMENT_STATUS_PC, DELIVERY_STATUS_PH, DELIVERY_STATUS_PC, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
--                                        SELECT W_ORDERABLE_ASSORTMENTS_ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, assortment_status_pc, ASSORTMENT_STATUS_PC, delivery_status_pc, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, LAST_USER, TRANSACTION_CODE
--                                        FROM TMD_ORDERABLE_ASSORTMENTS_VAR
--                                        WHERE ITEM_ID = W_ITEM_ID
--                                          AND ID = W_ORDERABLE_ASSORTMENTS_ID
--                                          AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;

                                    ELSE

                                        w_log_text := 'INSERISCO ASSORTIMENTO FORNITORE CENTRALE VAR 2 per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                        IF w_f_scrivi_log = 1 THEN
                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                        END IF;

    									 UPDATE tmd_orderable_assortments_var
                                     ----end_date = CURRENT_DATE - INTERVAL '1 day', -- Close current record
                                                SET end_date = CASE
                                                    -- Se la data di 'oggi meno 1 giorno' è >= della data di inizio (start_date)
                                                    WHEN CURRENT_DATE - INTERVAL '1 day' >= start_date 
                                                    -- Allora usa 'oggi meno 1 giorno' (il troncamento standard)
                                                    THEN CURRENT_DATE - INTERVAL '1 day'
                                                    -- Altrimenti (se 'oggi meno 1 giorno' è precedente a start_date)
                                                    ELSE start_date
                                                END,
                                             is_updated = 1,
											main_supplier = 0,
                                            last_user = P_USER,
                                            transaction_code = r_item_data.transaction_code
                                        WHERE item_id = W_ITEM_ID
                                          AND item_logistic_id = W_ITEM_LOGISTIC_ID
                                          AND logistic_unit_id = W_LOGISTIC_UNIT_ID
                                   ----       AND operational_agreement_id = r_item_data.OPERATIONAL_AGREEMENT_ID
                                          AND network_id = r_item_data.network_id
                                          AND r_item_data.start_date_assortment BETWEEN start_date AND end_date;

                                        SELECT nextval('tmd_orderable_assortments_id_seq'::regclass) INTO W_ORDERABLE_ASSORTMENTS_ID;

                                        INSERT INTO tmd_orderable_assortments_var (
                                            id, item_id, item_logistic_id, logistic_unit_id, operational_agreement_id, network_id,
                                            start_date, end_date, main_supplier, min_order, max_order, multiple_reorder,assortment_status_ph,delivery_status_ph,
                                            assortment_status_pc, delivery_status_pc, is_updated, last_user, transaction_code
                                        ) VALUES (
                                            W_ORDERABLE_ASSORTMENTS_ID,  W_ITEM_ID, W_ITEM_LOGISTIC_ID, W_LOGISTIC_UNIT_ID,
                                            r_item_data.OPERATIONAL_AGREEMENT_ID,
                                            r_item_data.network_id, r_item_data.start_date_assortment, r_item_data.end_date_assortment, 1,
                                            r_item_data.min_order, r_item_data.max_order, r_item_data.multiple_reorder,64,66,
                                            r_item_data.assortment_status_pc, r_item_data.delivery_status_pc, 1, P_USER, r_item_data.transaction_code
                                        );

--                                        w_log_text := 'INSERISCO TMD_ORDERABLE_ASSORTMENTS da TMD_ORDERABLE_ASSORTMENTS_VAR 2 per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
--                                        IF w_f_scrivi_log = 1 THEN
--                                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                        END IF;
--
--                                        INSERT INTO TMD_ORDERABLE_ASSORTMENTS (ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, ASSORTMENT_STATUS_PC, DELIVERY_STATUS_PH, DELIVERY_STATUS_PC, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
--                                        SELECT W_ORDERABLE_ASSORTMENTS_ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, assortment_status_pc, DELIVERY_STATUS_PH, delivery_status_pc, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, LAST_USER, TRANSACTION_CODE
--                                        FROM TMD_ORDERABLE_ASSORTMENTS_VAR
--                                        WHERE ITEM_ID = W_ITEM_ID
--                                          AND ID = W_ORDERABLE_ASSORTMENTS_ID
--                                          AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;

                                    END IF;
                                END IF;
                            END IF;
                        END IF;

                        IF r_item_data.update_supplier_item_code1 = 1 AND w_esito_processing_assortment <> 2 THEN
                            IF COALESCE(r_item_data.third_party_id,-1) = -1 OR  COALESCE(r_item_data.OPERATIONAL_AGREEMENT_ID,-1) = -1 THEN
                                w_log_text := '1 - FORNITORE e/O ACCORDO NON PRESENTE IN ANAGRAFICA per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                w_log_err := w_log_text;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                                w_esito_processing_assortment := 2 ;
							END IF;
							-----
	                        IF w_esito_processing_assortment <> 2 AND r_item_data.supplier_code_type_1 <> '-1' AND  coalesce(r_item_data.supplier_item_code_1,'-1') <> '-1'
 							AND  coalesce(W_ITEM_LOGISTIC_ID,'-1') <> '-1' AND  coalesce(r_item_data.third_party_id,'-1') <> '-1' THEN
	                            w_log_text := 'GESTIONE CODICE ARTICOLO FORNITORE 1 (tmd_supplier_item_codes)';
	                            IF w_f_scrivi_log = 1 THEN
	                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                            END IF;

								 w_log_text := 'RICERCOR RIGA SU  TMD_SUPPLIER_ITEM_CODES per item_id - logistic_id ' || W_ITEM_ID ||' - '||W_ITEM_LOGISTIC_ID||
											 ' e fornitore ' || r_item_data.third_party_id || ' e SUPPL ITEM CODE : '||r_item_data.supplier_item_code_1;

	                            SELECT COALESCE(COUNT(id),0)
	                            INTO w_existing_supplier_item_code_id
	                            FROM tmd_supplier_item_codes
	                            WHERE item_id = W_ITEM_ID
	                              AND item_logistic_id = W_ITEM_LOGISTIC_ID
	                              AND third_party_id = r_item_data.third_party_id
	                              AND supplier_item_code = r_item_data.supplier_item_code_1; -- Aggiunto LIMIT 1 per performance e correttezza logica

	                           IF w_existing_supplier_item_code_id > 0 THEN
	                                w_log_text := 'TROVATA RIGA SU  TMD_SUPPLIER_ITEM_CODES per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
	                                IF w_f_scrivi_log = 1 THEN
	                                	w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                                END IF;


--	                                w_log_text := 'AGGIORNO RIGA TMD_SUPPLIER_ITEM_CODES per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
--	                                    IF w_f_scrivi_log = 1 THEN
--	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--	                                    END IF;
--
--	                                    UPDATE tmd_supplier_item_codes
--	                                    SET
--	                                        is_active = 0,
--	                                        is_updated = 1,
--	                                        update_date = CURRENT_TIMESTAMP,
--	                                        last_user = P_USER,
--	                                        transaction_code = r_item_data.transaction_code
--	                                    WHERE id = w_existing_supplier_item_code_id;
--
--	                                    w_log_text := '2 - INSERISCO NUOVA RIGA TMD_SUPPLIER_ITEM_CODES (aggiornamento) per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
--	                                    IF w_f_scrivi_log = 1 THEN
--	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--	                                    END IF;
--
--	                                    INSERT INTO tmd_supplier_item_codes (
--	                                        id,
--	                                        item_id,
--	                                        item_logistic_id,
--	                                        third_party_id,
--	                                        code_type_ph,
--	                                        code_type_pc,
--	                                        supplier_item_code,
--	                                        description_item,
--	                                        is_active,
--	                                        is_updated,
--	                                        creation_date,
--	                                        update_date,
--	                                        last_user,
--	                                        transaction_code
--	                                    ) VALUES (
--	                                        nextval('tmd_supplier_item_codes_id_seq'::regclass),
--	                                        W_ITEM_ID,
--	                                        W_ITEM_LOGISTIC_ID,
--	                                        r_item_data.third_party_id,
--	                                        16,
--	                                        r_item_data.supplier_code_type_1,
--	                                        r_item_data.supplier_item_code_1,
--	                                        r_item_data.description_item,
--	                                        r_item_data.supplier_is_active_1,
--	                                        1,
--	                                        CURRENT_TIMESTAMP,
--	                                        CURRENT_TIMESTAMP,
--	                                        P_USER,
--	                                        r_item_data.transaction_code
--	                                    );
	                                ELSE -- Se w_existing_supplier_item_code_id IS NULL (non trovato)
	                               		 w_log_text := 'INSERISCO RIGA  TMD_SUPPLIER_ITEM_CODES per item_id - logistic_id ' || W_ITEM_ID ||' - '||W_ITEM_LOGISTIC_ID||
											 ' e fornitore ' || r_item_data.third_party_id || ' e SUPPL ITEM CODE : '||r_item_data.supplier_item_code_1;

										  IF w_f_scrivi_log = 1 THEN
	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                                    END IF;

	                                    INSERT INTO tmd_supplier_item_codes (
	                                        id,
	                                        item_id,
	                                        item_logistic_id,
	                                        third_party_id,
	                                        code_type_ph,
	                                        code_type_pc,
	                                        supplier_item_code,
	                                        description_item,
	                                        is_active,
	                                        is_updated,
	                                        creation_date,
	                                        update_date,
	                                        last_user,
	                                        transaction_code
	                                    ) VALUES (
	                                        nextval('tmd_supplier_item_codes_id_seq'::regclass),
	                                        W_ITEM_ID,
	                                        W_ITEM_LOGISTIC_ID,
	                                        r_item_data.third_party_id,
	                                        16,
	                                        r_item_data.supplier_code_type_1,
	                                        r_item_data.supplier_item_code_1,
	                                        r_item_data.description_item,
	                                        r_item_data.supplier_is_active_1,
	                                        1,
	                                        CURRENT_TIMESTAMP,
	                                        CURRENT_TIMESTAMP,
	                                        P_USER,
	                                        r_item_data.transaction_code
	                                    );
	                            END IF;
	                        ELSE
							    w_log_text := 'DATI ASSORTIMENTO FORNITORE NON PRESENTI PER SUPPLIER ITEM CODE 1  !!';
                         	END IF;
                        END IF;

                        w_existing_supplier_item_code_id := NULL; -- Reset per il prossimo controllo

                        IF r_item_data.update_supplier_item_code2 = 1 AND w_esito_processing_assortment <> 2 THEN
                            IF COALESCE(r_item_data.third_party_id,-1) = -1 OR  COALESCE(r_item_data.OPERATIONAL_AGREEMENT_ID,-1) = -1 THEN
                                w_log_text := '1 - FORNITORE e/O ACCORDO NON PRESENTE IN ANAGRAFICA per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                w_log_err := w_log_text;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                                w_esito_processing_assortment = 2 ;

							END IF;
							-----

	                        IF w_esito_processing_assortment <> 2 AND r_item_data.supplier_code_type_2 <> '-1' AND coalesce(r_item_data.supplier_item_code_2,'-1') <> '-1'
							AND coalesce(W_ITEM_LOGISTIC_ID,'-1') <> '-1' AND  coalesce(r_item_data.third_party_id,'-1') <> '-1'THEN
	                            w_log_text := 'GESTIONE CODICE ARTICOLO FORNITORE 2 (tmd_supplier_item_codes)';
	                            IF w_f_scrivi_log = 1 THEN
	                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                            END IF;

	                            SELECT id
	                            INTO w_existing_supplier_item_code_id
	                            FROM tmd_supplier_item_codes
	                            WHERE item_id = W_ITEM_ID
	                              AND item_logistic_id = W_ITEM_LOGISTIC_ID
	                              AND third_party_id = r_item_data.third_party_id
	                              AND supplier_item_code = r_item_data.supplier_item_code_2
	                              LIMIT 1;

	                            IF w_existing_supplier_item_code_id IS NOT NULL THEN
	                                w_log_text := 'TROVATA RIGA SU  TMD_SUPPLIER_ITEM_CODES 2  per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
	                                IF w_f_scrivi_log = 1 THEN
	                                	w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                                END IF;

--	                                w_log_text := 'AGGIORNO RIGA TMD_SUPPLIER_ITEM_CODES per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
--	                                    IF w_f_scrivi_log = 1 THEN
--	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--	                                    END IF;
--
--	                                    UPDATE tmd_supplier_item_codes
--	                                    SET
--	                                        is_active = 0,
--	                                        is_updated = 1,
--	                                        update_date = CURRENT_TIMESTAMP,
--	                                        last_user = P_USER,
--	                                        transaction_code = r_item_data.transaction_code
--	                                    WHERE id = w_existing_supplier_item_code_id;
--
--	                                w_log_text := '1- INSERISCO NUOVA RIGA TMD_SUPPLIER_ITEM_CODES (aggiornamento) per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
--	                                    IF w_f_scrivi_log = 1 THEN
--	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--	                                    END IF;
--
--	                                    INSERT INTO tmd_supplier_item_codes (
--	                                        id,
--	                                        item_id,
--	                                        item_logistic_id,
--	                                        third_party_id,
--	                                        code_type_ph,
--	                                        code_type_pc,
--	                                        supplier_item_code,
--	                                        description_item,
--	                                        is_active,
--	                                        is_updated,
--	                                        creation_date,
--	                                        update_date,
--	                                        last_user,
--	                                        transaction_code
--	                                    ) VALUES (
--	                                        nextval('tmd_supplier_item_codes_id_seq'::regclass),
--	                                        W_ITEM_ID,
--	                                        W_ITEM_LOGISTIC_ID,
--	                                        r_item_data.third_party_id,
--	                                        16,
--	                                        r_item_data.supplier_code_type_2,
--	                                        r_item_data.supplier_item_code_2,
--	                                        r_item_data.description_item,
--	                                        r_item_data.supplier_is_active_2,
--	                                        1,
--	                                        CURRENT_TIMESTAMP,
--	                                        CURRENT_TIMESTAMP,
--	                                        P_USER,
--	                                        r_item_data.transaction_code
--	                                    );
	                                ELSE
	                                	w_log_text := 'INSERISCO RIGA TMD_SUPPLIER_ITEM_CODES (nuova) per item_id ' || W_ITEM_ID || ' e fornitore ' || r_item_data.third_party_id;
	                                    IF w_f_scrivi_log = 1 THEN
	                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
	                                    END IF;

	                                    INSERT INTO tmd_supplier_item_codes (
	                                        id,
	                                        item_id,
	                                        item_logistic_id,
	                                        third_party_id,
	                                        code_type_ph,
	                                        code_type_pc,
	                                        supplier_item_code,
	                                        description_item,
	                                        is_active,
	                                        is_updated,
	                                        creation_date,
	                                        update_date,
	                                        last_user,
	                                        transaction_code
	                                    ) VALUES (
	                                        nextval('tmd_supplier_item_codes_id_seq'::regclass),
	                                        W_ITEM_ID,
	                                        W_ITEM_LOGISTIC_ID,
	                                        r_item_data.third_party_id,
	                                        16,
	                                        r_item_data.supplier_code_type_2,
	                                        r_item_data.supplier_item_code_2,
	                                        r_item_data.description_item,
	                                        r_item_data.supplier_is_active_2,
	                                        1,
	                                        CURRENT_TIMESTAMP,
	                                        CURRENT_TIMESTAMP,
	                                        P_USER,
	                                        r_item_data.transaction_code
	                                    );
	                                END IF;
	                      	ELSE
							    w_log_text := 'DATI ASSORTIMENTO FORNITORE NON PRESENTI PER SUPPLIER ITEM CODE 2  !!';

                    		END IF;
                        END IF;
                        IF  w_esito_processing_assortment <> 2 THEN
                            w_esito_processing_assortment := 1 ;
                        END IF;
                END IF ; -- Chiusura IF r_item_data.processing_assortment = 0


                IF r_item_data.processing_puchase_price = 0 then
                    IF  r_item_data.update_purchase_price = 1 THEN
                        SELECT purchase_vat_id FROM tmd_items WHERE id = W_ITEM_ID INTO w_iva_acq_item;

                        w_log_text := 'GESTIONE PREZZO DI ACQUISTO (tmd_purchase_prices_var)';
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;

                        IF r_item_data.third_party_id = -1 OR  r_item_data.OPERATIONAL_AGREEMENT_ID = -1 THEN
                            w_log_text := '1 - FORNITORE e/O ACCORDO NON PRESENTE IN ANAGRAFICA per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                            w_log_err := w_log_text;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;
                            w_esito_processing_puchase_price = 2 ;
                        ELSE
                            w_log_text := '1 - VERIFICO SU tmd_purchase_prices_var SE E'' PRESENTE IL RECORD A PARITA'' DI DATI per item_id ' || W_ITEM_ID
                                                || ' e network ' || r_item_data.network_id|| ' e FORNITORE ' || r_item_data.OPERATIONAL_AGREEMENT_ID ;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT EXISTS (SELECT 1 FROM tmd_purchase_prices_var WHERE item_id = W_ITEM_ID
                                AND operational_agreement_id =  r_item_data.OPERATIONAL_AGREEMENT_ID
                                AND network_id = r_item_data.network_id AND cost_type_pc = 1
                                AND r_item_data.start_date_purchase = start_date  FOR UPDATE) INTO v_purchase_price_exists;

                            IF v_purchase_price_exists then
                                w_log_text := '1 - RECORD PRESENTE NON FACCIO NULLA per item_id ' || W_ITEM_ID
                                                || ' e network ' || r_item_data.network_id|| ' e FORNITORE ' || r_item_data.OPERATIONAL_AGREEMENT_ID ;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;
                                -- null; -- non necessario, basta non fare nulla
                            else
                                w_log_text := '1 - VERIFICO SU tmd_purchase_prices_var SE E'' PRESENTE IL RECORD IN DATA  per item_id ' || W_ITEM_ID
                                                || ' e network ' || r_item_data.network_id|| ' e FORNITORE ' || r_item_data.OPERATIONAL_AGREEMENT_ID ;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                SELECT EXISTS (SELECT 1 FROM tmd_purchase_prices_var WHERE item_id = W_ITEM_ID
                                AND operational_agreement_id =  r_item_data.OPERATIONAL_AGREEMENT_ID
                                AND network_id = r_item_data.network_id AND cost_type_pc = 1
                                AND  r_item_data.start_date_purchase BETWEEN start_date AND end_date FOR UPDATE) INTO v_purchase_price_exists;

                                IF v_purchase_price_exists THEN
                                    w_log_text := 'AGGIORNO PREZZO DI ACQUISTO tmd_purchase_prices_var per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    UPDATE tmd_purchase_prices_var
                                        SET end_date = CASE
                                                    -- Se la data di 'oggi meno 1 giorno' è >= della data di inizio (start_date)
                                                    WHEN CURRENT_DATE - INTERVAL '1 day' >= start_date 
                                                    -- Allora usa 'oggi meno 1 giorno' (il troncamento standard)
                                                    THEN CURRENT_DATE - INTERVAL '1 day'
                                                    -- Altrimenti (se 'oggi meno 1 giorno' è precedente a start_date)
                                                    ELSE start_date
                                        END,
                                          is_updated = 1,
                                        last_user = P_USER,
                                        transaction_code = r_item_data.transaction_code
                                    WHERE item_id = W_ITEM_ID
                                      AND operational_agreement_id =  r_item_data.OPERATIONAL_AGREEMENT_ID
                                      AND network_id = r_item_data.network_id
                                      AND cost_type_pc = 1
                                      AND  r_item_data.start_date_purchase BETWEEN start_date AND end_date;

--                                    w_log_text := 'AGGIORNO PREZZO DI ACQUISTO tmd_purchase_prices per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
--                                    IF w_f_scrivi_log = 1 THEN
--                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                                    END IF;
--
--                                    UPDATE tmd_purchase_prices
--                                    SET
--                                        end_date = CURRENT_DATE - INTERVAL '1 day',
--                                        last_user = P_USER,
--                                        transaction_code = r_item_data.transaction_code
--                                    WHERE item_id = W_ITEM_ID
--                                      AND operational_agreement_id =  r_item_data.OPERATIONAL_AGREEMENT_ID
--                                      AND network_id = r_item_data.network_id
--                                      AND cost_type_pc = 1
--                                      AND CURRENT_DATE BETWEEN start_date AND end_date;

                                    w_log_text := 'INSERIMENTO PREZZO DI ACQUISTO VAR per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    SELECT nextval('tmd_purchase_prices_id_seq'::regclass) INTO W_PURCHASE_PRICES_ID;

                                    INSERT INTO tmd_purchase_prices_var (id, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_pc, vat_id, start_date, end_date, cost_type_pc, is_updated, last_user, transaction_code, insert_type_pc)
                                    SELECT W_PURCHASE_PRICES_ID,
                                           W_ITEM_ID,
                                           (SELECT id FROM tmd_item_logistics WHERE item_id = W_ITEM_ID AND item_logistic = 1),
                                            r_item_data.OPERATIONAL_AGREEMENT_ID,
                                           r_item_data.network_id,
                                           r_item_data.purchase_price,
                                           r_item_data.unit_purchase_price_pc,
                                           COALESCE(NULLIF(r_item_data.purchase_vat_id, -1), w_iva_acq_item ),
                                            r_item_data.start_date_purchase,
                                           r_item_data.end_date_purchase,
                                           1,
                                           1,
                                           P_USER,
                                           r_item_data.transaction_code,
                                            1;
                                ELSE
                                    w_log_text := 'INSERISCO PREZZO DI ACQUISTO VAR per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                    IF w_f_scrivi_log = 1 THEN
                                        w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                    END IF;

                                    SELECT nextval('tmd_purchase_prices_id_seq'::regclass) INTO W_PURCHASE_PRICES_ID;

                                    INSERT INTO tmd_purchase_prices_var (id, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_pc, vat_id, start_date, end_date, cost_type_pc, is_updated, last_user, transaction_code,insert_type_pc)
                                    SELECT W_PURCHASE_PRICES_ID,
                                           W_ITEM_ID,
                                           (SELECT id FROM tmd_item_logistics WHERE item_id = W_ITEM_ID AND item_logistic = 1),
                                            r_item_data.OPERATIONAL_AGREEMENT_ID,
                                           r_item_data.network_id,
                                           r_item_data.purchase_price,
                                           r_item_data.unit_purchase_price_pc,
                                           COALESCE(NULLIF(r_item_data.purchase_vat_id, -1), w_iva_acq_item),
                                            r_item_data.start_date_purchase,
                                           r_item_data.end_date_purchase,
                                           1,
                                           1,
                                           P_USER,
                                           r_item_data.transaction_code,
                                           1;
                                END IF;

                                w_log_text := 'INSERISCO  TMD_PURCHASE_PRICES da TMD_PURCHASE_PRICES_VAR per item_id ' || W_ITEM_ID || ' e network ' || r_item_data.network_id;
                                IF w_f_scrivi_log = 1 THEN
                                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                                END IF;

                                INSERT INTO tmd_purchase_prices (id, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_ph, unit_price_pc, vat_id, start_date, end_date, insert_type_ph, insert_type_pc, cost_type_ph, cost_type_pc, promo_code, creation_date, update_date, last_user, transaction_code)
                                SELECT W_PURCHASE_PRICES_ID, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_ph, unit_price_pc, vat_id, start_date, end_date, insert_type_ph, insert_type_pc, cost_type_ph, cost_type_pc, promo_code, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, last_user, transaction_code
                                FROM tmd_purchase_prices_var
                                WHERE item_id = W_ITEM_ID
                                  AND ID = W_PURCHASE_PRICES_ID
                                  AND r_item_data.start_date_purchase BETWEEN START_DATE AND END_DATE;
                            END IF;
                        END IF;
                    END IF; -- Chiusura IF r_item_data.update_purchase_price = 1
                    IF w_esito_processing_puchase_price <> 2 THEN
                        w_esito_processing_puchase_price = 1 ;
                    END IF;

                END IF; -- Chiusura IF r_item_data.processing_puchase_price = 0

                IF r_item_data.processing_sale_price = 0 THEN
                    IF  r_item_data.update_sale_price = 1 THEN
                        w_log_text := 'GESTIONE PREZZO DI VENDITA (tmd_sale_prices_var)';
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;

                        SELECT EXISTS (SELECT 1 FROM tmd_sale_prices_var WHERE item_sale_id = W_ITEM_SALE_ID AND network_id = r_item_data.network_id AND sale_price_type_pc = 1 AND r_item_data.start_date_sale BETWEEN start_date AND end_date FOR UPDATE) INTO v_sale_price_exists;

                        IF v_sale_price_exists THEN
                            w_log_text := 'AGGIORNO PREZZO DI VENDITA VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            UPDATE tmd_sale_prices_var
                                    SET end_date = CASE
                                    -- Se la data di 'oggi meno 1 giorno' è >= della data di inizio (start_date)
                                    WHEN CURRENT_DATE - INTERVAL '1 day' >= start_date 
                                    -- Allora usa 'oggi meno 1 giorno' (il troncamento standard)
                                    THEN CURRENT_DATE - INTERVAL '1 day'
                                    -- Altrimenti (se 'oggi meno 1 giorno' è precedente a start_date)
                                    ELSE start_date
                                END,
                                 is_sent = 0,
                                last_user = P_USER,
                                transaction_code = r_item_data.transaction_code
                            WHERE item_sale_id = W_ITEM_SALE_ID
                              AND network_id = r_item_data.network_id
                              AND sale_price_type_pc = 1
                              AND r_item_data.start_date BETWEEN start_date AND end_date;

                            w_log_text := 'INSERISCO PREZZO DI VENDITA VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;
                            SELECT nextval('tmd_sale_prices_var_id_seq'::regclass) INTO W_SALE_PRICES_VAR_ID;

                            INSERT INTO tmd_sale_prices_var (id, item_sale_id, network_id, price, base_price,
                                    unit_price_pc, sale_price_type_pc, vat_id, start_date, end_date,
                                    is_sent, insert_type_pc, last_user, transaction_code)
                            SELECT W_SALE_PRICES_VAR_ID,
                                   W_ITEM_SALE_ID,
                                   r_item_data.network_id,
                                   r_item_data.sale_price,
                                   r_item_data.sale_price,
                                   r_item_data.unit_sale_price_pc,
                                   1,
                                   COALESCE(r_item_data.sale_vat_id, W_VAT_ID),
                                   r_item_data.start_date_sale,
                                   r_item_data.end_date_sale,
                                   0,
                                   1,
                                   P_USER,
                                   r_item_data.transaction_code;
                        ELSE
                            w_log_text := 'INSERISCO PREZZO DI VENDITA VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            SELECT nextval('tmd_sale_prices_var_id_seq'::regclass) INTO W_SALE_PRICES_VAR_ID;

                            INSERT INTO tmd_sale_prices_var (id, item_sale_id, network_id, price, base_price, unit_price_pc, sale_price_type_pc,
                                                                          vat_id, start_date, end_date, is_sent, insert_type_pc, last_user, transaction_code)
                            SELECT W_SALE_PRICES_VAR_ID,
                                   W_ITEM_SALE_ID,
                                   r_item_data.network_id,
                                   r_item_data.sale_price,
                                   r_item_data.sale_price,
                                   r_item_data.unit_sale_price_pc,
                                   1,
                                   COALESCE(r_item_data.sale_vat_id, W_VAT_ID),
                                   r_item_data.start_date_sale,
                                   r_item_data.end_date_sale,
                                   0,
                                   1,
                                   P_USER,
                                   r_item_data.transaction_code;
                        END IF;

--                         Also update tmd_sale_prices
--                        w_log_text := 'AGGIORNO TMD_SALE_PRICES da TMD_SALE_PRICES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
--                        IF w_f_scrivi_log = 1 THEN
--                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                        END IF;
--
--                        UPDATE tmd_sale_prices
--                        SET end_date = CURRENT_DATE - INTERVAL '1 day',
--                            update_date = CURRENT_TIMESTAMP,
--                            last_user = p_user,
--                            transaction_code = r_item_data.transaction_code
--                        WHERE item_sale_id = W_ITEM_SALE_ID AND network_id = r_item_data.network_id AND sale_price_type_pc = 1 AND CURRENT_DATE BETWEEN start_date AND end_date;
--
--                        w_log_text := 'INSERISCO TMD_SALE_PRICES da TMD_SALE_PRICES_VAR per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
--                        IF w_f_scrivi_log = 1 THEN
--                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--                        END IF;
--                        INSERT INTO tmd_sale_prices (id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code)
--                        SELECT W_SALE_PRICES_VAR_ID, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, last_user, transaction_code
--                        FROM tmd_sale_prices_var
--                        WHERE item_sale_id = W_ITEM_SALE_ID
--                          AND CURRENT_DATE BETWEEN start_date AND end_date
--                          AND ID = W_SALE_PRICES_VAR_ID;

                        w_log_text := 'GESTIONE ASSORTIMENTO VENDIBILE';
                        IF w_f_scrivi_log = 1 THEN
                            w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                        END IF;

                        SELECT EXISTS (SELECT 1 FROM tmd_saleable_assortments WHERE item_sale_id = W_ITEM_SALE_ID AND network_id = r_item_data.network_id FOR UPDATE) INTO v_saleable_assortment_exists;

                        IF v_saleable_assortment_exists THEN
                            w_log_text := 'AGGIORNO ASSORTIMENTO VENDIBILE per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            UPDATE tmd_saleable_assortments
                            SET
                                status_pc = 1,
                                last_user = p_user,
                                transaction_code = r_item_data.transaction_code
                            WHERE item_sale_id = W_ITEM_SALE_ID AND network_id = r_item_data.network_id;
                        ELSE
                            w_log_text := 'INSERISCO ASSORTIMENTO VENDIBILE per item_sale_id ' || W_ITEM_SALE_ID || ' e network ' || r_item_data.network_id;
                            IF w_f_scrivi_log = 1 THEN
                                w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                            END IF;

                            INSERT INTO tmd_saleable_assortments (
                                id,
                                item_sale_id,
                                network_id,
                                status_pc,
                                last_user,
                                transaction_code
                            ) VALUES (
                                nextval('tmd_saleable_assortments_id_seq'::regclass),
                                W_ITEM_SALE_ID,
                                r_item_data.network_id,
                                1,
                                p_user,
                                r_item_data.transaction_code
                            );
                        END IF;
                    END IF;
                    IF w_esito_processing_sale_price <> 2 THEN
                        w_esito_processing_sale_price = 1 ;
                    END IF;
                END IF;

                w_clocks :=  clock_timestamp();
                w_log_text := '#####FINE ARTICOLO : '||TO_CHAR(w_clocks, 'YYYY-MM-DD HH24:MI:SS.US') ||
                              ' TIPOLOGIA : '|| coalesce(r_item_data.SOURCE_TYPE_PC,'-1') ||' - '|| substr (r_item_data.file_name,1,4)  ||
                              '- ARTICOLO : '|| coalesce(w_item_code,'-1') ||' - '||coalesce(r_item_data.description_item,'-1') ||
                              '- FORNITORE : '|| coalesce(r_item_data.supplier_code,'-1') ||
                              '- NR.LINEA : '|| r_item_data.number_line ;
                IF w_f_scrivi_log = 1 THEN

                    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
                END IF;

                -- Update tin_data_item_in to mark as processed
                UPDATE tin_data_item_in
                SET update_date = w_clocks ,
                    last_user = 'fn_initial_load',
                    error_message = w_log_err,
                    processing_item = w_esito_processing_item,
                    processing_sale_code = w_esito_processing_sales_code,
                    processing_assortment = w_esito_processing_assortment,
                    processing_puchase_price = w_esito_processing_puchase_price,
                    processing_sale_price = w_esito_processing_sale_price,
                    processing_feature = w_esito_processing_features,
                    processing_step3 = CASE
                                        WHEN w_esito_processing_item =  2 OR
                                             w_esito_processing_sales_code = 2 OR
                                             w_esito_processing_assortment = 2 OR
                                             w_esito_processing_puchase_price = 2  OR
                                             w_esito_processing_sale_price = 2  OR
                                             w_esito_processing_features = 2
                                        THEN 2 -- Imposta a 2 se almeno una variabile non è 1
                                        ELSE 1 -- Altrimenti, imposta a 1 (se tutte sono 1)
                                    END
                WHERE transaction_code = r_item_data.transaction_code
                  AND id = r_item_data.id;	 -- Usa l'ID dalla tabella temporanea per aggiornare la riga corretta in tin_data_item_in

            EXCEPTION
               WHEN SQLSTATE 'P0001' THEN
					----------
	                -- Update tin_data_item_in to mark as processed with error
	                UPDATE tin_data_item_in
	                SET processing_item = w_esito_processing_item,
	                    processing_sale_code = w_esito_processing_sales_code,
	                    processing_assortment = w_esito_processing_assortment,
	                    processing_puchase_price = w_esito_processing_puchase_price,
	                    processing_sale_price = w_esito_processing_sale_price,
	                    processing_feature = w_esito_processing_features,
	                    processing_step3 = 2 ,
	                    update_date = w_clocks,
	                    last_user = 'fn_initial_load',
	                    error_message=  'ERRORE  ELABORAZIONE: ' || SQLERRM || ' - ' || SQLSTATE || ' - ' || w_log_text
	                WHERE transaction_code = r_item_data.transaction_code
	                  AND id = r_item_data.id; -- Utilizza r_item_data.id per l'UPDATE corretto
					----------
		     			IF w_f_scrivi_log = 1 THEN
	                        w_log_return := fn_log('ERROR', 'FN_INITIAL_LOAD', w_log_text, 0);

						END IF;

                WHEN OTHERS THEN
					 w_log_return := fn_log('ERROR', 'FN_INITIAL_LOAD', w_log_text, 0);
	                -- Pre-valorizzo w_log_text per l'UPDATE e il log di errore, prima dell'UPDATE stesso
	                w_log_text := '!!!ERRORE DURANTE ELABORAZIONE ARTICOLO: ' || SQLERRM || ' - ' || SQLSTATE ||
	                              ' TIPOLOGIA : '|| coalesce(r_item_data.SOURCE_TYPE_PC,'-1') ||' - '|| substr (r_item_data.file_name,1,4)  ||
	                              '- ARTICOLO : '|| coalesce(w_item_code,'-1') ||' - '||coalesce(r_item_data.description_item,'-1') ||
	                              '- FORNITORE : '|| coalesce(r_item_data.supplier_code,'-1') ||
	                              '- NR.LINEA : '|| r_item_data.number_line ;

	                -- Update tin_data_item_in to mark as processed with error
	                UPDATE tin_data_item_in
	                SET processing_item = w_esito_processing_item,
	                    processing_sale_code = w_esito_processing_sales_code,
	                    processing_assortment = w_esito_processing_assortment,
	                    processing_puchase_price = w_esito_processing_puchase_price,
	                    processing_sale_price = w_esito_processing_sale_price,
	                    processing_feature = w_esito_processing_features,
	                    processing_step3 = 2 ,
	                    update_date = w_clocks,
	                    last_user = 'fn_initial_load',
	                    error_message=  'ERRORE  ELABORAZIONE: ' || SQLERRM || ' - ' || SQLSTATE || ' - ' || w_log_text
	                WHERE transaction_code = r_item_data.transaction_code
	                  AND id = r_item_data.id; -- Utilizza r_item_data.id per l'UPDATE corretto

	                        w_log_return := fn_log('ERROR', 'FN_INITIAL_LOAD', w_log_text, 0);


	            END; -- Fine del blocco BEGIN/EXCEPTION per il singolo r_item_data
            END LOOP; -- Fine loop FOR r_item_data IN cur_items
        END LOOP; -- Fine loop FOR r_temp_item IN SELECT * FROM temp_transaction_items

        -- Svuota la tabella temporanea per il prossimo blocco
        TRUNCATE TABLE temp_transaction_items;

    END LOOP; -- Fine loop principale (per il FETCH dalla cur_transactions)

    -- Chiudi il primo cursore
    CLOSE cur_transactions;

    w_clocks :=  clock_timestamp();
    w_log_text := 'FINE PROCEDURA IMPIANTO: '||TO_CHAR(w_clocks, 'YYYY-MM-DD HH24:MI:SS.US');
    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);

--   w_clocks :=  clock_timestamp();
--    w_log_text := 'RICHIAMO PROCEDURA pr_last_operations FASE 3 : '||TO_CHAR(w_clocks, 'YYYY-MM-DD HH24:MI:SS.US');
--    w_log_return := fn_log('INFO', 'FN_INITIAL_LOAD', w_log_text, 0);
--   v_update_type := '3';
--	CALL boom.pr_last_operations(v_update_type);

   RETURN 1;

EXCEPTION
    WHEN OTHERS THEN
        w_log_text := 'ERRORE GLOBALE DURANTE L''ELABORAZIONE: ' || SQLERRM || ' - ' || SQLSTATE;
        w_log_return := fn_log('ERROR', 'FN_INITIAL_LOAD', w_log_text, 0);
       RAISE NOTICE USING MESSAGE = w_log_text;
        RETURN -1;
END;
$function$
;
