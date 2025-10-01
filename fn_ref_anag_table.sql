-- DROP FUNCTION boom.fn_ref_anag_table();

CREATE OR REPLACE FUNCTION boom.fn_ref_anag_table()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE
    rec             RECORD;
    w_log_text      text := '';
    w_process_name  text := 'fn_ref_anag_table';
    w_log_return    integer := 0;
BEGIN
    FOR rec IN SELECT * FROM new_table LOOP
        BEGIN
            IF UPPER(TG_TABLE_NAME::text) IN ('TMD_SALE_CODES','TMD_SALE_PRICES') THEN
                DELETE FROM TMD_ITEM_ANAG
                WHERE item_id IN (
                    SELECT tis.item_id
                    FROM tmd_item_sales tis
                    WHERE tis.id = rec.item_sale_id
                );

                INSERT INTO TMD_ITEM_ANAG (
                    item_id, margin, sale_code, code_type_ph, code_type_pc, is_label,
                    start_date_sale_code, end_date_sale_code, network_id, purchase_price,
                    start_date_purchase, end_date_purchase, purchase_unit_price_ph,
                    purchase_unit_price_pc, purchase_price_vat_id, purchase_cost_type_ph,
                    purchase_cost_type_pc, purchase_price_promo, sale_price_id, sale_price,
                    base_price, start_date_sale, end_date_sale, sale_unit_price_ph,
                    sale_unit_price_pc, sale_price_type_ph, sale_price_type_pc,
                    sale_price_promo, sale_price_vat_id, vat, is_sent, item_sale_id,
                    assortment_id, item_logistic_id, logistic_unit_id,
                    operational_agreement_id, assortment_start_date, assortment_end_date,
                    assortment_status_ph, assortment_status_pc, delivery_status_ph,
                    delivery_status_pc, old_sale_price, purchase_price_id
                )
                SELECT
                    ti.id,
                    round(fn_tmd_get_marg_perc(tsp.item_sale_id, tsp.vat_id, coalesce(tsp.network_id, tpp.network_id), 1::numeric, tpp.price, tsp.price), 3),
                    tsc.sale_code, tsc.code_type_ph, tsc.code_type_pc, tsc.is_label,
                    tsc.start_date, tsc.end_date, tn.id, tpp.price,
                    tpp.start_date, tpp.end_date, tpp.unit_price_ph, tpp.unit_price_pc,
                    tpp.vat_id, tpp.cost_type_ph, tpp.cost_type_pc, tpp.promo_code,
                    tsp.sale_price_id, tsp.price, tsp.base_price, tsp.start_date,
                    tsp.end_date, tsp.unit_price_ph, tsp.unit_price_pc,
                    tsp.sale_price_type_ph, tsp.sale_price_type_pc,
                    tsp.promo_code, tsp.vat_id, tv.vat::numeric, tsp.is_sent,
                    tis.id, tpp.assortment_id, tpp.item_logistic_id,
                    tpp.logistic_unit_id, tpp.operational_agreement_id,
                    tpp.assortment_start_date, tpp.assortment_end_date,
                    tpp.assortment_status_ph, tpp.assortment_status_pc,
                    tpp.delivery_status_ph, tpp.delivery_status_pc,
                    tsp.old_price, tpp.price_id
                FROM tmd_items ti
                INNER JOIN tmd_item_sales tis ON tis.item_id = ti.id AND tis.id = rec.item_sale_id
                INNER JOIN tmd_networks tn ON tn.is_stock_management = 1
                LEFT JOIN vmd_sale_prices_v tsp ON tsp.item_id = ti.id AND tsp.item_sale_id = tis.id AND tsp.network_id = tn.id
                LEFT JOIN tmd_vat tv ON tv.id = tsp.vat_id
                LEFT JOIN vmd_purchase_prices_v tpp ON tpp.item_id = ti.id AND tpp.network_id = tn.id
                LEFT JOIN tmd_sale_codes tsc ON tsc.item_sale_id = tis.id
                    AND tsc.is_label = 1
                    AND (
                        tsc.network_id = tn.id
                        OR (tsc.network_id IS NULL AND NOT EXISTS (
                            SELECT 1 FROM tmd_sale_codes tsc2
                            WHERE tsc2.item_sale_id = tsc.item_sale_id
                              AND tsc2.is_label = 1
                              AND tsc2.network_id = tn.id
                        ))
                    );

            ELSIF UPPER(TG_TABLE_NAME::text) IN ('TMD_PURCHASE_PRICES','TMD_ORDERABLE_ASSORTMENTS') THEN
                DELETE FROM TMD_ITEM_ANAG
                WHERE item_id = rec.item_id;

                INSERT INTO TMD_ITEM_ANAG (
                    item_id, margin, sale_code, code_type_ph, code_type_pc, is_label,
                    start_date_sale_code, end_date_sale_code, network_id, purchase_price,
                    start_date_purchase, end_date_purchase, purchase_unit_price_ph,
                    purchase_unit_price_pc, purchase_price_vat_id, purchase_cost_type_ph,
                    purchase_cost_type_pc, purchase_price_promo, sale_price_id, sale_price,
                    base_price, start_date_sale, end_date_sale, sale_unit_price_ph,
                    sale_unit_price_pc, sale_price_type_ph, sale_price_type_pc,
                    sale_price_promo, sale_price_vat_id, vat, is_sent, item_sale_id,
                    assortment_id, item_logistic_id, logistic_unit_id,
                    operational_agreement_id, assortment_start_date, assortment_end_date,
                    assortment_status_ph, assortment_status_pc, delivery_status_ph,
                    delivery_status_pc, old_sale_price, purchase_price_id
                )
                SELECT
                    ti.id,
                    round(fn_tmd_get_marg_perc(tsp.item_sale_id, tsp.vat_id, coalesce(tsp.network_id, tpp.network_id), 1::numeric, tpp.price, tsp.price), 3),
                    tsc.sale_code, tsc.code_type_ph, tsc.code_type_pc, tsc.is_label,
                    tsc.start_date, tsc.end_date, tn.id, tpp.price,
                    tpp.start_date, tpp.end_date, tpp.unit_price_ph, tpp.unit_price_pc,
                    tpp.vat_id, tpp.cost_type_ph, tpp.cost_type_pc, tpp.promo_code,
                    tsp.sale_price_id, tsp.price, tsp.base_price, tsp.start_date,
                    tsp.end_date, tsp.unit_price_ph, tsp.unit_price_pc,
                    tsp.sale_price_type_ph, tsp.sale_price_type_pc,
                    tsp.promo_code, tsp.vat_id, tv.vat::numeric, tsp.is_sent,
                    tis.id, tpp.assortment_id, tpp.item_logistic_id,
                    tpp.logistic_unit_id, tpp.operational_agreement_id,
                    tpp.assortment_start_date, tpp.assortment_end_date,
                    tpp.assortment_status_ph, tpp.assortment_status_pc,
                    tpp.delivery_status_ph, tpp.delivery_status_pc,
                    tsp.old_price, tpp.price_id
                FROM tmd_items ti
                INNER JOIN tmd_item_sales tis ON tis.item_id = ti.id
                INNER JOIN tmd_networks tn ON tn.is_stock_management = 1
                LEFT JOIN vmd_sale_prices_v tsp ON tsp.item_id = ti.id AND tsp.item_sale_id = tis.id AND tsp.network_id = tn.id
                LEFT JOIN tmd_vat tv ON tv.id = tsp.vat_id
                LEFT JOIN vmd_purchase_prices_v tpp ON tpp.item_id = ti.id AND tpp.network_id = tn.id
                LEFT JOIN tmd_sale_codes tsc ON tsc.item_sale_id = tis.id
                    AND tsc.is_label = 1
                    AND (
                        tsc.network_id = tn.id
                        OR (tsc.network_id IS NULL AND NOT EXISTS (
                            SELECT 1 FROM tmd_sale_codes tsc2
                            WHERE tsc2.item_sale_id = tsc.item_sale_id
                              AND tsc2.is_label = 1
                              AND tsc2.network_id = tn.id
                        ))
                    )
                WHERE ti.id = rec.item_id;
            END IF;

        EXCEPTION WHEN OTHERS THEN
           IF UPPER(TG_TABLE_NAME::text) IN ('TMD_SALE_CODES','TMD_SALE_PRICES') THEN
                w_log_text := format('Errore su item_sale_id=%s: %s - %s',
                                     COALESCE(rec.item_sale_id::text, 'NULL'),
                                     SQLSTATE, SQLERRM);
            ELSE
                w_log_text := format('Errore su item_id=%s: %s - %s',
                                     COALESCE(rec.item_id::text, 'NULL'),
                                     SQLSTATE, SQLERRM);
            END IF;
            PERFORM fn_log('ERROR', w_process_name, w_log_text, 0);
        END;
    END LOOP;

    RETURN NULL;
END;
$function$
;
