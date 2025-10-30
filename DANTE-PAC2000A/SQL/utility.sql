update tmd_sale_codes s set is_label = '0' where s.id in (
select a.minid from (
select min(tsc.id) minid, tsc.item_sale_id ,tsc.network_id , count(*) from tmd_sale_codes tsc where is_label = 1 ----item_sale_id = 34050 and is_label = 1
group by tsc.item_sale_id ,tsc.network_id 
having count(*) >1)a )

update tmd_sale_codes_var s set is_label = '0' where s.id in (
select a.minid from (
select min(tsc.id) minid, tsc.item_sale_id ,tsc.network_id , count(*) from tmd_sale_codes_var tsc where is_label = 1 ----item_sale_id = 34050 and is_label = 1
group by tsc.item_sale_id ,tsc.network_id 
having count(*) >1)a )


SELECT
    item,
    -- 1. Conteggio totale delle righe con processing_step3 = 8
    COUNT(*) AS total_rows_step3_eq_8,
    -- 2. Conteggio delle righe dove processing_step3 = 8 E (file_name contiene 'ph' o 'ih' o 'eh')
    COUNT(*) FILTER (
        WHERE file_name ILIKE '%ph%'  collate case_like
    ) AS count_with_ph,
     COUNT(*) FILTER (
        WHERE file_name ILIKE '%ih%' collate case_like 
    ) AS count_with_ih,
     COUNT(*) FILTER (
        WHERE file_name ILIKE '%eh%' collate case_like 
    ) AS count_with_eh
FROM
    tin_data_item_in
WHERE
    -- Filtro iniziale per includere solo i dati rilevanti per processing_step3 = 8
    processing_step3 = 8
GROUP BY
    item
-- La clausola HAVING è opzionale qui, ma la includo se vuoi filtrare SOLO gli item con più di un record totale.
-- Rimuovila se vuoi vedere tutti gli item che hanno almeno un record con step3=8.
HAVING
    COUNT(*) > 1;

----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************ 
   
UPDATE tin_data_item_in
SET 
    processing_step3 = 5,
    processing_step4 = 0,
    error_message = NULL,
    -- I campi 'processing_*' non uguali a 9 vengono resettati a 0
    processing_item = CASE WHEN processing_item <> 9 THEN 0 ELSE processing_item END,
    processing_sale_code = CASE WHEN processing_sale_code <> 9 THEN 0 ELSE processing_sale_code END,
    processing_assortment = CASE WHEN processing_assortment <> 9 THEN 0 ELSE processing_assortment END,
    processing_puchase_price = CASE WHEN processing_puchase_price <> 9 THEN 0 ELSE processing_puchase_price END,
    processing_sale_price = CASE WHEN processing_sale_price <> 9 THEN 0 ELSE processing_sale_price END,
    processing_feature = CASE WHEN processing_feature <> 9 THEN 0 ELSE processing_feature END
WHERE 
    -- 1. Filtri temporali e di stato applicati a TUTTI i record
    creation_date >= current_date - INTERVAL '10 day' 
    AND processing_step1 = 1 
    AND processing_step2 = 1
    -- Ho rimosso il filtro 'AND item = '1929-01'' perché ora il filtro è dinamico
    -- 2. Filtro basato sui 1000 ITEM ID selezionati
    AND item IN (
        SELECT item
        FROM tin_data_item_in
        WHERE 
            creation_date >= current_date - INTERVAL '10 day' 
            AND processing_step1 = 1 
            AND processing_step2 = 1
            -- Aggiungi qui il filtro se vuoi resettare solo gli articoli con errore 5
            AND processing_step3 = 8 
        GROUP BY item -- Raggruppa per ITEM per selezionare codici item distinti
        LIMIT 1000 -- Limita a un massimo di 1000 codici ITEM unici
    );

----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************ 
    
    UPDATE tin_data_item_in
SET 
    processing_step3 = 0,
    processing_step4 = 0,
    error_message = NULL,
    processing_item = CASE WHEN processing_item <> 9 THEN 0 ELSE processing_item END,
    processing_sale_code = CASE WHEN processing_sale_code <> 9 THEN 0 ELSE processing_sale_code END,
    processing_assortment = CASE WHEN processing_assortment <> 9 THEN 0 ELSE processing_assortment END,
    processing_puchase_price = CASE WHEN processing_puchase_price <> 9 THEN 0 ELSE processing_puchase_price END,
    processing_sale_price = CASE WHEN processing_sale_price <> 9 THEN 0 ELSE processing_sale_price END,
    processing_feature = CASE WHEN processing_feature <> 9 THEN 0 ELSE processing_feature END
WHERE 
    creation_date >= current_Date - INTERVAL '10 day' 
    AND processing_step1 = 1 
    AND processing_step2 = 1
-------    AND processing_step3 = 5  -- Filtra solo quelli che hanno l'errore che vuoi resettare
    AND item = '1929-01';      -- Filtra solo l'articolo specifico

----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************


select   count(distinct item ), processing_step1, processing_step2, processing_step3 , substr(error_message,1,30)
        from tin_data_item_in tdii where creation_date >= current_date-1  
        group by processing_step1, processing_step2, processing_step3, substr(error_message,1,30)
----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************


        select tsa.network_id, k.description , count(*) 
        from tmd_saleable_assortments tsa  ,tmd_networks k
        where k.id = tsa.network_id 
        group by tsa.network_id, k.description
        
----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************
        
update tin_data_item_in tdii set processing_step3 = 7 , processing_step4 = 7    ------ fornitori non presenti 
        where creation_date >= current_date-1 and 
       processing_step1 = 1 and  processing_step2 = 1 and processing_step3 = 2
       and error_message like '1 - FORNITORE e/O ACCORDO NON PRESENTE%' collate CASE_LIKE

 update tin_data_item_in tdii set processing_step3 = 7 , processing_step4 = 7  ------- record incompleti 
        where creation_date >= current_date-1 and 
       processing_step1 = 1 and  processing_step2 = 1 and processing_step3 = 2
       and error_message like 'ERRORE  ELABORAZIONE: null value in column "item_type_pc%' collate CASE_LIKE
       
----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************

select * from tmd_item_anag tia  where tia.network_id = 14  and  tia.sale_price is null 
and exists ( select 1 from tmd_sale_prices tsp where tsp.network_id = tia.network_id  and tsp.item_sale_id = tia.item_sale_id)  

----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************
DO
$$
DECLARE
    rec RECORD;
    v_network_id integer := 5;  -- Cambia se necessario
    v_totale integer;
    v_counter integer := 0;
BEGIN
    -- Calcolo totale record da aggiornare
    SELECT COUNT(*) INTO v_totale
    FROM tmd_item_anag tia
    WHERE tia.network_id = v_network_id
      AND tia.sale_price IS NULL 
      AND EXISTS (
          SELECT 1 
          FROM tmd_sale_prices tsp 
          WHERE tsp.network_id = tia.network_id  
            AND tsp.item_sale_id = tia.item_sale_id
            AND current_date BETWEEN start_date AND end_date
      );

    RAISE NOTICE 'Aggiornamento avviato per network_id=%, totale record da aggiornare=%', v_network_id, v_totale;

    FOR rec IN
        SELECT * 
        FROM tmd_item_anag tia  
        WHERE tia.network_id = v_network_id
          AND tia.sale_price IS NULL 
          AND EXISTS (
              SELECT 1 
              FROM tmd_sale_prices tsp 
              WHERE tsp.network_id = tia.network_id  
                AND tsp.item_sale_id = tia.item_sale_id
                AND current_date BETWEEN start_date AND end_date
          )
    LOOP
        v_counter := v_counter + 1;

        -- log con avanzamento
        RAISE NOTICE '(%/%): Aggiorno item_sale_id=%, network_id=%', 
                     v_counter, v_totale, rec.item_sale_id, rec.network_id;

        UPDATE tmd_sale_prices tsp  
        SET last_user = last_user  -- placeholder, qui metti l’aggiornamento vero
        WHERE tsp.item_sale_id = rec.item_sale_id
          AND tsp.network_id = rec.network_id;
    END LOOP;

    RAISE NOTICE 'Aggiornamento completato per network_id=%. Record aggiornati=%/%', v_network_id, v_counter, v_totale;
END;
$$;

commit;
----------***********************************************************************************************************************************************

DO
$$
DECLARE
    rec RECORD;
    v_network_id integer := 5;  -- Cambia se necessario
    v_totale integer;
    v_counter integer := 0;
BEGIN
    -- Calcolo totale record da aggiornare
    SELECT COUNT(*) INTO v_totale
    FROM tmd_item_anag tia
    WHERE tia.network_id = v_network_id
      AND tia.sale_code IS NULL 
      AND EXISTS (
          SELECT 1 
          FROM tmd_sale_codes tsc   
          WHERE tsc.network_id = tia.network_id  
            AND tsc.item_sale_id = tia.item_sale_id
            AND current_date BETWEEN start_date AND end_date
      );

    RAISE NOTICE 'Aggiornamento avviato per network_id=%, totale record da aggiornare=%', v_network_id, v_totale;

    FOR rec IN
        SELECT * 
        FROM tmd_item_anag tia  
        WHERE tia.network_id = v_network_id
          AND tia.sale_code IS NULL 
          AND EXISTS (
              SELECT 1 
              FROM tmd_sale_codes tsc   
              WHERE tsc.network_id = tia.network_id  
                AND tsc.item_sale_id = tia.item_sale_id
                AND current_date BETWEEN start_date AND end_date
          )
    LOOP
        v_counter := v_counter + 1;

        -- log con avanzamento
        RAISE NOTICE '(%/%): Aggiorno item_sale_id=%, network_id=%', 
                     v_counter, v_totale, rec.item_sale_id, rec.network_id;

        UPDATE tmd_sale_codes tsc  
        SET last_user = last_user  -- qui metti l’aggiornamento vero
        WHERE tsc.item_sale_id = rec.item_sale_id
          AND tsc.network_id = rec.network_id
          AND current_date BETWEEN tsc.start_date AND tsc.end_date;
    END LOOP;

    RAISE NOTICE 'Aggiornamento completato per network_id=%. Record aggiornati=%/%', v_network_id, v_counter, v_totale;
END;
$$;


commit;
----------************************************************************************************************************************************************
----------************************************************************************************************************************************************
----------************************************************************************************************************************************************

    insert into tin_Trace_in
        select  nextval('tin_trace_in_id_seq'::regclass),
file_name,
file_row,
ti.id,
network_id,
date_elab,
flg_new_item,
flg_upd_anag,
flg_new_structure,
flg_upd_structure,
flg_new_sale_code,
flg_upd_sale_code,
flg_new_sale_price,
flg_upd_sale_price,
flg_new_purchase_price,
flg_upd_purchase_price,
flg_new_assortment,
flg_upd_assortment,
flg_new_promo,
flg_upd_promo,
tin_Trace_in.creation_date,
tin_Trace_in.update_date,
tin_Trace_in.last_user,
tin_Trace_in.transaction_code,
flg_upd_item_category,
flg_upd_seasonality,
flg_upd_vat_id,
flg_upd_brand,
old_purchase_price,
old_sale_price
from tin_Trace_in , tmd_items ti
where tin_Trace_in.item_id  = 73958 

----------************************************************************************************************************************************************
----------************************************************************************************************************************************************
----------************************************************************************************************************************************************




select * from tmd_sale_prices tsp  where  update_date>= current_Date-20 and item_sale_id = 20671

update tmd_sale_prices tsp  SET last_user = 'AGG_PRZ.1' where network_id = 14 and  update_date>= current_Date-5 ----and item_sale_id = 20671

select * from tmd_item_anag tia  where tia.network_id = 14  and tia.item_sale_id = 20671
----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************

DO $$
DECLARE
    r RECORD;
    records_processed INTEGER := 0;
BEGIN
    RAISE NOTICE 'Inizio aggiornamento di TMD_SALE_PRICES con COMMIT per record.';

    -- Ciclo implicito con FOR
    FOR r IN SELECT id FROM tmd_sale_prices WHERE update_date >= CURRENT_DATE
    LOOP
        -- Esegui l'aggiornamento per il record corrente
        UPDATE tmd_sale_prices
        SET last_user = 'AGG_PRZ'
        WHERE id = r.id; -- Assumi che 'id' sia la chiave primaria di tmd_sale_prices

        -- Esegui il COMMIT per il singolo aggiornamento
        COMMIT;

        records_processed := records_processed + 1;

        -- (Opzionale) Puoi aggiungere un RAISE NOTICE ogni N record per il feedback
        IF records_processed % 100 = 0 THEN
            RAISE NOTICE 'Elaborati % record...', records_processed;
        END IF;

    END LOOP;

    RAISE NOTICE 'Fine aggiornamento. Aggiornati % record.', records_processedss; 

END;
$$ LANGUAGE plpgsql;
----------***********************************************************************************************************************************************
----------************************************************************************************************************************************************

 select processing_step3, processing_step4, substr(error_message,1,30), count(*) from tin_data_item_in where update_date >= current_timestamp - interval '35 minutes'
group by processing_step3, processing_step4, substr(error_message,1,30)


select   count(distinct item ), processing_step1, processing_step2, processing_step3 , substr(error_message,1,30)
        from tin_data_item_in tdii where update_date >= now() - INTERVAL '120 minutes' 
        group by processing_step1, processing_step2, processing_step3, substr(error_message,1,30)


 select * from tin_data_item_in ti   where update_date > CURRENT_TIMESTAMP - INTERVAL '60 minutes'
      and   processing_step1 = 1 and  processing_step2 = 1 and  processing_step3 = 2 and processing_step4 = 0 
      and error_message like '1 - FORNITORE%' collate CASE_LIKE and  exists (select 1 from  boom.tmd_third_parties where third_party = supplier_code )
      and  exists (select 1 from boom.tmd_operational_agreements  where operational_agreement = ti.operational_agreement )
    


  select substr(error_message,1,25) , count(*) from tin_data_item_in ti   where update_date > CURRENT_TIMESTAMP - INTERVAL '60 minutes'
      and   processing_step1 = 1 and  processing_step2 = 1 and  processing_step3 = 2 and processing_step4 = 0
group by substr(error_message,1,25)

     select substr(error_message,1,25) , count(*) from tin_data_item_in ti   where update_date > CURRENT_TIMESTAMP - INTERVAL '60 minutes'
      and   processing_step1 = 1 and  processing_step2 = 1 and  processing_step3 = 2 and processing_step4 = 0 and error_message like '1 - FORNITORE%' collate CASE_LIKE
group by substr(error_message,1,25)

   select * from tin_data_item_in ti   where update_date > CURRENT_TIMESTAMP - INTERVAL '60 minutes'
      and   processing_step1 = 1 and  processing_step2 = 1 and  processing_step3 = 1 and processing_step4 = 0
      and (  ( ti.item  is not null and not exists ( select 1 from tmd_items where item = ti.item)) or 
	         (ti.sale_code is not null and not exists ( select 1 from tmd_sale_codes_var where sale_code = ti.sale_code)))
      order  by item,number_line

     select  processing_step1, processing_step2, processing_step3 , processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature, count(distinct item )
        from tin_data_item_in tdii where update_date >= now() - INTERVAL '240 minutes' 
        group by processing_step1, processing_step2, processing_step3, processing_item ,  processing_sale_code,  processing_assortment, processing_puchase_price, processing_sale_price, processing_feature
   

 SELECT  *
        FROM tin_data_item_in td
        WHERE 1=1 and processing_step3 = 5 and processing_step4 = 5  
          AND processing_step2 = 1	
          AND processing_step1 = 1       and  not exists (select 1 from tmd_items a where a.item = td.item	 )

     SELECT item, max(description_item), count(*) numrec , count(distinct file_name) numfile 
     
 SELECT *
        FROM tin_data_item_in td
        WHERE 1=1  and 1=1 ----item = '78895-08' -- processing_step4 = 0
        and processing_step3 = 0
        and processing_step4 = 0
          AND processing_step2 = 1
          AND processing_step1 = 1
---and item = '70137-99'
order by number_line
 
        
        update tin_data_item_in td set processing_step3 = 5
        WHERE 1=1 and processing_step3 = 0--- and processing_step4 = 5  
          AND processing_step2 = 1	
          AND processing_step1 = 1 
        and   exists (select 1 from tmd_items a where a.item = td.item	 )



SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_item, processing_sale_code, processing_assortment, processing_puchase_price, processing_sale_price, processing_feature, item, description_item, item_type_pc, item_category_pc, stock_unit_pc, brand_pc, supplier_expiry_days, depot_expiry_days, shop_expiry_days, consumer_expiry_days, purchase_vat, sale_vat, is_local, structure_code, start_date_structure, end_date_structure, item_code, item_code_type, start_date_item_code, end_date_item_code, item_sale, description_sale, size_unit_measure_sale_pc, val_length_sale, val_width_sale, val_height_sale, weight_unit_measure_sale_pc, val_weight_sale, pieces_sale, sale_code, sale_code_type_pc, is_label, start_date_sale_code, end_date_sale_code, tare, is_variable_price, bilance_code, item_logistic, item_logistic_type, unit_measure_pc, medium_weight, pieces_pack, size_unit_measure_pack_pc, val_height_pack, val_length_pack, val_width_pack, weight_unit_measure_pack_pc, val_weight_pack, pieces_layer, size_unit_measure_layer_pc, val_height_layer, val_length_layer, val_width_layer, weight_unit_measure_layer_pc, val_weight_layer, pieces_pallet, size_unit_measure_pallet_pc, val_height_pallet, val_length_pallet, val_width_pallet, weight_unit_measure_pallet_pc, val_weight_pallet, supplier_code, operational_agreement, network, start_date_assortment, end_date_assortment, main_supplier, min_order, max_order, multiple_reorder, assortment_status_pc, delivery_status_pc, supplier_item_1, supplier_code_type_1, description_supplier_item_1, supplier_is_active_1, supplier_item_2, supplier_code_type_2, description_supplier_item_2, supplier_is_active_2, purchase_price, unit_purchase_price_pc, start_date_purchase, end_date_purchase, sale_price, unit_sale_price_pc, start_date_sale, end_date_sale, attribute_code_1, attribute_value_1, attribute_code_2, attribute_value_2, attribute_code_3, attribute_value_3, attribute_code_4, attribute_value_4, attribute_code_5, attribute_value_5, attribute_code_6, attribute_value_6, attribute_code_7, attribute_value_7, attribute_code_8, attribute_value_8, attribute_code_9, attribute_value_9, attribute_code_10, attribute_value_10, attribute_code_11, attribute_value_11, attribute_code_12, attribute_value_12, attribute_code_13, attribute_value_13, attribute_code_14, attribute_value_14, attribute_code_15, attribute_value_15, under_brand_pc, is_multi_vat
from  boom.tin_data_item_in where creation_date >= current_date ;
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_flag_kit, item, item_type_pc, component_item_1, item_sale_1, item_logistic_1, coefficient_1, percentage_1, component_item_2, item_sale_2, item_logistic_2, coefficient_2, percentage_2, component_item_3, item_sale_3, item_logistic_3, coefficient_3, percentage_3, component_item_4, item_sale_4, item_logistic_4, coefficient_4, percentage_4, component_item_5, item_sale_5, item_logistic_5, coefficient_5, percentage_5, component_item_6, item_sale_6, item_logistic_6, coefficient_6, percentage_6, component_item_7, item_sale_7, item_logistic_7, coefficient_7, percentage_7, component_item_8, item_sale_8, item_logistic_8, coefficient_8, percentage_8, component_item_9, item_sale_9, item_logistic_9, coefficient_9, percentage_9, component_item_10, item_sale_10, item_logistic_10, coefficient_10, percentage_10
FROM boom.tin_data_kit_in where creation_date >= current_date;
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_promotion, network, promo_code, promo_description, item, sale_price, start_date_sale, end_date_sale, promo_type, discount_perc, discount_value, quantity_m, quantity_n, mix_group, is_promo_fidelity, cost_type_pc, start_date_cost, end_date_cost, price_list_cost
FROM boom.tin_data_promotion_in where creation_date >= current_date;
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_ddt, network, third_party, operational_agreement, reception_type, reception_code, reception_date, document_number, document_date, order_num, carrier_third_party, str_comment, update_stock, reception_line, item, item_logistic, logistic_unit, quantity_pieces, quantity_packs, quantity_pallets, quantity_weight, quantity_volume, free_quantity, gross_price, net_price, net_net_price, vat, lot_code, expiry_date
FROM boom.tin_data_reception_in where creation_date >= current_date;
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_structure, processing_links_item_structure, structure_code_l0, structure_code_l1, structure_description_l1, level_description_l1, can_link_items_l1, structure_code_l2, structure_description_l2, level_description_l2, can_link_items_l2, structure_code_l3, structure_description_l3, level_description_l3, can_link_items_l3, structure_code_l4, structure_description_l4, level_description_l4, can_link_items_l4, structure_code_l5, structure_description_l5, level_description_l5, can_link_items_l5, structure_code_l6, structure_description_l6, level_description_l6, can_link_items_l6, item, start_date, end_date
FROM boom.tin_data_structure_in where creation_date >= current_date;
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_supplier, processing_client, third_party_types_pc, third_party, description, business_name, status_pc, vat_number, fiscal_code, language_code_pc, is_subject_to_vat, supplier_type_pc, is_local, operational_agreement, description_agreement, currency_pc, order_valorization_pc, reception_valorization_pc, can_return, can_manages_residue, day_manages_residue, can_article_not_order, service_agreement_model, network_code, start_date, end_date, order_type_pc, delivery_type_pc, sending_mode_pc, can_order_mon, can_order_tue, can_order_wed, can_order_thu, can_order_fri, can_order_sat, can_order_sun, preparation_time_mon, preparation_time_tue, preparation_time_wed, preparation_time_thu, preparation_time_fri, preparation_time_sat, preparation_time_sun, delivery_time_mon, delivery_time_tue, delivery_time_wed, delivery_time_thu, delivery_time_fri, delivery_time_sat, delivery_time_sun, address_type_pc, address_nation_pc, street_address, city, region, postcode, contact_role_pc, contact, telephone_number, mobile_number, fax, mail, contact_nation_pc
FROM boom.tin_data_third_party_in where creation_date >= current_date;
SELECT id, file_name, file_row, item_id, network_id, date_elab, flg_new_item, flg_upd_anag, flg_new_structure, flg_upd_structure, flg_new_sale_code, flg_upd_sale_code, flg_new_sale_price, flg_upd_sale_price, flg_new_purchase_price, flg_upd_purchase_price, flg_new_assortment, flg_upd_assortment, flg_new_promo, flg_upd_promo, creation_date, update_date, last_user, transaction_code, flg_upd_item_category, flg_upd_seasonality, flg_upd_vat_id, flg_upd_brand, old_purchase_price, old_sale_price
FROM boom.tin_trace_in where creation_date >= current_date;




"//Fase 1 acquisizione dati da AS400
var params = ['-jar', './acquisizione_file/acquisizione_file.jar', './acquisizione_file/config'];
var acquisizioneDatiAS400 = utils.execCommand('java', params);"

"//Fase 2 elaborazione interfaccia
params = ['-jar', './elaborazione_interfacce/elaborazione_interfacce.jar', './elaborazione_interfacce/config', '*'];
elaborazioneInterfaccePromise = utils.execCommand('java', params);
elaborazioneInterfaccePromise.on('exit', function (code)"

' ******       reception'

"//Fase 3 caricamento boom
params = ['-jar', './caricamento_boom/caricamento_boom.jar', './caricamento_boom/config', '*'];
caricamentoBoomPromise = utils.execCommand('java', params);
caricamentoBoomPromise.on('exit', function (code) {"

    "//Fase 3.5 caricamento tab out promo
    params = ['-jar', './caricamento_tab_out/caricamento_tab_out.jar', '-p', './caricamento_tab_out/config', '-t', 'promo'];
    caricamentoTabOutPromoPromise = utils.execCommand('java', params);
    caricamentoTabOutPromoPromise.on('exit', function (code) {"

"//Fase 3.5 caricamento tab out articoli
params = ['-jar', './caricamento_tab_out/caricamento_tab_out.jar', '-p', './caricamento_tab_out/config', '-t', 'item', '-s',  1,'-a', 'B'];
 caricamentoTabOutArticoliPromise = utils.execCommand('java', params);
caricamentoTabOutArticoliPromise.on('exit', function (code) {"

"//Fase 4 caricamento boss articoli
params = ['-jar', './caricamento_boss/caricamento_boss.jar', '-p', './caricamento_boss/config', '-t', 'item', '-u', 'user_batch'];
elaborazioneInterfaccePromise = utils.execCommand('java', params);
elaborazioneInterfaccePromise.on('exit', function (code) {
console.log(""Finito elaborazione interfacce"");"
    
    "//Fase 5 caricamento BOSS
    params = ['-jar', './caricamento_boss/caricamento_boss.jar', '-p', './caricamento_boss/config', '-t', 'promo'];
    caricamentoBossPromise = utils.execCommand('java', params);
    caricamentoBossPromise.on('exit', function (code) {
    console.log(""Caricamento boss"");"

java -jar acquisizione_file/acquisizione_file.jar acquisizione_file/config

java -jar elaborazione_interfacce/elaborazione_interfacce.jar /elaborazione_interfacce/config *

java -jar elaborazione_interfacce/elaborazione_interfacce.jar /elaborazione_interfacce/config reception

java -jar caricamento_boom/caricamento_boom.jar caricamento_boom/config *

--- step 0 - stoppare docker ofelia e tutte le schedulazioni di caricamento 
--- step 1 - caricare i file o sotto la cartella as400 o sotto la cartella s+ a seconda del file che arriva 



--- step 2 - carica le *in solo con il tracciato file  (pochi minuti) 
nohup java -jar acquisizione_file/acquisizione_file.jar acquisizione_file/config > output.log 2>&1 &


-- step 2.1 - spaccare a gruppi il blocco per i ph per evitare che vada in mem esaurita e lanciare lo step 3  quanto basta 
UPDATE tin_data_item_in
	SET processing_step2 = 5
	WHERE ctid IN (
		SELECT ctid
		FROM tin_data_item_in
		WHERE creation_date >= CURRENT_DATE
		  AND processing_step2 = 0 --- erano rimasti solo + record ph 
	);

	UPDATE tin_data_item_in
	SET processing_step2 = 0
	WHERE ctid IN (
		SELECT ctid
		FROM tin_data_item_in
		WHERE creation_date >= CURRENT_DATE
		  AND processing_step2 = 5
	);
	
--- step 3 - aggiornano le *in  con il tracciato file nei campi appositi ( dalle 	
nohup java -Xms512m -Xmx4g -jar ./elaborazione_interfacce/elaborazione_interfacce.jar ./elaborazione_interfacce/config "*" > output.log 2>&1 &




--- step 4 - creazione tabella salvataggio 
create table save_test_il_21052025_tin_data_item_in as 
select * from tin_data_item_in
where creation_date >= current_Date


--- step 5 - aggiorno record come da non elaborare  per step 3 e 4 
update tin_data_item_in set processing_step3 = 5 , processing_step4 = 5 
where creation_date >= current_Date

--- step 6 - aggiorno record come da  elaborare  per step 3 e 4  i primi 1000
UPDATE tin_data_item_in
SET processing_step3 = 0 , processing_step4 = 0
where creation_date >= current_Date 
and processing_step1 = 1 and  processing_step2 = 1 and item in 
													(
													select item from tin_data_item_in 
													where creation_date >= current_Date
													and processing_step3 = 5 and  processing_step4 = 5 
													and   source_type_pc = 110
													and item between  '104000-01' and '105100-01'
													)

----nohup java -Xms512m -Xmx4g -jar ./caricamento_boom/caricamento_boom.jar ./caricamento_boom/config "*" >  output.log 2>&1 &

--- step 7 - lancio lo STEP 3.5 per gli articoli  ( dalla in con stpe4 = 0 alla out ) 

['-jar', './caricamento_tab_out/caricamento_tab_out.jar', '-p', './caricamento_tab_out/config', '-t', 'item', '-s',  1,'-a', 'B'];

nohup  java -Xms512m -Xmx4g -jar ./caricamento_tab_out/caricamento_tab_out.jar '-p' ./caricamento_tab_out/config '-t' 'item'  '-s'   1 '-a' 'B' >  output.log 2>&1 &

nohup  java -Xmx5g -Xmx5g -jar ./caricamento_tab_out/caricamento_tab_out.jar '-p' ./caricamento_tab_out/config '-t'  'item'  '-s'   2  >  output.log 2>&1 &

nohup  java -Xmx5g -Xmx5g -jar ./caricamento_tab_out/caricamento_tab_out.jar '-p' ./caricamento_tab_out/config '-i' '100795-01' '-t' 'item'  '-s'   1  >  output.log 2>&1 &

nohup  java -Xmx5g -Xmx5g -jar ./caricamento_tab_out/caricamento_tab_out.jar '-p' ./caricamento_tab_out/config '-t'  'promo'   >  output.log 2>&1 &


--- ci mette 3-4 minuti per 1000 articoli 


--      public static final String ETICHETTE="3";
    -- public static final String PATH = "-p";
    -- public static final String TYPE = "-t";
    -- public static final String SOURCE = "-s";
    -- public static final String ACTION = "-a";
    -- public static final String USER = "-u";
    -- public static final String ITEM = "-i";
    -- public static String user = "NULL";
    -- public static String item = "";

    -- public static void main(String[] args) {
        -- HashMap<String, String> inParams = new HashMap<>();
        -- String configPath = null;
        -- String launchType = null;
        -- String queryPar;
        -- String executionType = null;
        -- Integer parallelTransactionCode = null;
        -- Integer mainTransactionCode=0;
        -- Integer fileHeaderId=0; */


--- step 8 - lancio lo STEP 4  per gli articoli   caricamento boss ( dalla out alla host staging ) 

nohup java -Xms512m -Xmx4g -jar ./caricamento_boss/caricamento_boss.jar '-p' ./caricamento_boss/config '-t' 'item' '-u' 'user_batch' > output.log 2>&1 &

nohup java -Xms1g -Xmx4g -jar ./caricamento_boss/caricamento_boss.jar '-p' ./caricamento_boss/config '-t' 'item'  > output.log 2>&1 &
                    
-- ha fatto 26000 articoli in 2hh quanto ci mette a farne 111.000
-- Se hai fatto 26.000 articoli in 2 ore (120 minuti), significa che stai producendo circa 216,67 articoli al minuto.
-- Per farne 111.000, mantenendo la stessa velocità, ci vorrebbero circa 512,31 minuti, ovvero circa 8 ore e 32,31 minuti.

    "//Fase 5 caricamento BOSS
    params = ['-jar', './caricamento_boss/caricamento_boss.jar', '-p', './caricamento_boss/config', '-t', 'promo'];
    caricamentoBossPromise = utils.execCommand('java', params);
    caricamentoBossPromise.on('exit', function (code) {
    console.log(""Caricamento boss"");"

nohup java -Xms1g -Xmx4g -jar ./caricamento_boss/caricamento_boss.jar '-p' ./caricamento_boss/config '-t' 'promo'  > output.log 2>&1 &

nohup java -Xms1g -Xmx4g -jar ./caricamento_boss/caricamento_boss.jar '-p' ./caricamento_boss/config '-t' 'labels'  > output.log 2>&1 &


----- ******************************************************************************
----- ******************************************************************************
----- ******************************************************************************
----- ******************************************************************************




2025-06-12 11:42:22.642 +0200	record: 36748     avvio 

2025-06-12 11:52:22.500 +0200	record: 35252

2025-06-12 12:15:48.335 +0200	record: 30664

2025-06-12 12:49:18.171 +0200	record: 16062

2025-06-12 13:11:18.171 +0200	record: 8121       fine  : 28627 record in 1h, 29min 



2025-06-12 14:39:18.171 +0200	record: 8121   avvio 

2025-06-12 14:43:18.171 +0200	record: 0   avvio 



INIZIO PROCEDURA : 2025-06-12 15:42:36.493961

FINE PROCEDURA IMPIANTO: 2025-06-12 15:55:40.765405 10.000 articoli 32338 record in 14
--- step 7 - lancio la prc di elaborazione per i record aggiornati 

nohup wget http://boom-ws:8080/acquisizione_completa_as400 -O - > output.log 2>&1 &


---- test1 = 4414 record e 1008 articoli - 234 kit 
--- ora inizio  : 13.26 
--- ora fine : 13.41
----> 15min per 1000 record 


---- test2 = 66792 record e 10076 articoli - 234 kit 
--- ora inizio  : 16.12 
--- ora fine : 18.56
---->  per 10076 record 


private static final String RECEPTION ="reception" ;
private static final String SUPPLIER_ASSORTMENT = "assortment";
private static final String ITEMS="item";
private static final String STRUC="structure";
private static final String EAN="sale_code";
private static final String SUPPL="third_party";
private static final String FEATURES="feature";
private static final String PROMO="promo";
private static final String KIT ="kit";

ps aux | grep acquisizione_file.jar

kill PID

anomalie  initial load 
   -file sph lo ha messo con un numeratore non corretto 113 al posto di 110 ( fatto upgrade sulla item_in )
   -la seconda fase deve essere rilanciata più volte perchè va in hype memory ( capire se leggere a blocchi i record per evitare di saturare la memoria java ) 
			Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
			at org.postgresql.core.OptimizedUTF8Encoder.charDecode(OptimizedUTF8Encoder.java:71)
			at org.postgresql.core.ByteOptimizedUTF8Encoder.decode(ByteOptimizedUTF8Encoder.java:27)
			at org.postgresql.core.Encoding.decode(Encoding.java:254)
			at org.postgresql.jdbc.PgResultSet.getString(PgResultSet.java:2064)
			at org.postgresql.jdbc.PgResultSet.getString(PgResultSet.java:2632)
			at com.tesisquare.boom.TSControlsCommon.getDataFromQuery(TSControlsCommon.java:59)
			at com.tesisquare.boom.item.TSItemsSettings.<init>(TSItemsSettings.java:137)
			at com.tesisquare.boom.TSManage.manageItems(TSManage.java:247)
			at com.tesisquare.boom.TSManage.getManagedObject(TSManage.java:154)
			at com.tesisquare.boom.TSManage.getControlledObject(TSManage.java:130)
			at com.tesisquare.boom.TSManage.elabObjects(TSManage.java:104)
			at com.tesisquare.boom.TSManage.<init>(TSManage.java:52)
			at com.tesisquare.boom.TSMain.main(TSMain.java:54)
    - no non c'è ora... al max puoi modificare nei vari file config della fase 4 i puntamenti alle tabelle da host_staging a elvispos
        in questo modo tutte le insert/update vengono fatte sull'altro schema
	- i kit non sono stati caricati correttamente ( tin_Data_kit_in)

   http://boom-base:10001/api/masterData/orderableAssortmentsVar 
   
   
   
   
   CALL labels.add_to_labels_list();
    SELECT
      *
    FROM labels.labels_list_view
    WHERE
      sz_code IN (
        SELECT
          sz_code
        FROM host_staging.control_variation_rows_check
        WHERE n0_id = ANY (ARRAY[1,2,3,4,5,............
        
        Items must send to scale (API body): []
        Items must send to eLabels (API body): [] 
        
        
   50000 articoli dalle 10.31 alle 
   
 /***********************************************************************************/
 
   
   
SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_promotion, network, promo_code, promo_description, item, sale_price, start_date_sale, end_date_sale, promo_type, discount_perc, discount_value, quantity_m, quantity_n, mix_group, is_promo_fidelity, cost_type_pc, start_date_cost, end_date_cost, price_list_cost
FROM boom.tin_data_promotion_in
order by 1 desc;

SELECT id, source_type_ph, source_type_pc, file_name, number_line, buffer_str, creation_date, update_date, last_user, transaction_code, processing_step1, processing_step2, processing_step3, processing_step4, error_message, processing_supplier, processing_client, third_party_types_pc, third_party, description, business_name, status_pc, vat_number, fiscal_code, language_code_pc, is_subject_to_vat, supplier_type_pc, is_local, operational_agreement, description_agreement, currency_pc, order_valorization_pc, reception_valorization_pc, can_return, can_manages_residue, day_manages_residue, can_article_not_order, service_agreement_model, network_code, start_date, end_date, order_type_pc, delivery_type_pc, sending_mode_pc, can_order_mon, can_order_tue, can_order_wed, can_order_thu, can_order_fri, can_order_sat, can_order_sun, preparation_time_mon, preparation_time_tue, preparation_time_wed, preparation_time_thu, preparation_time_fri, preparation_time_sat, preparation_time_sun, delivery_time_mon, delivery_time_tue, delivery_time_wed, delivery_time_thu, delivery_time_fri, delivery_time_sat, delivery_time_sun, address_type_pc, address_nation_pc, street_address, city, region, postcode, contact_role_pc, contact, telephone_number, mobile_number, fax, mail, contact_nation_pc
FROM boom.tin_data_third_party_in
order by 1 desc;




/**************************************************
/**************************************************        ricevimenti       
/*************************************************

java -jar acquisizione_file/acquisizione_file.jar acquisizione_file/config

nohup java -Xms512m -Xmx4g -jar ./elaborazione_interfacce/elaborazione_interfacce.jar ./elaborazione_interfacce/config reception > output.log 2>&1 &

nohup java -Xms512m -Xmx4g -jar ./caricamento_boom/caricamento_boom.jar ./caricamento_boom/config reception >  output.log 2>&1 &


nohup ./pr_lancio_procedura_impianto.sh > pr_lancio.log 2>&1 &


docker cp ./pr_lancio_procedura_impianto.sh e0dc76ccee20:/usr/src/app/shell


------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

docker ps -a | grep init

docker run -it --rm --name ispezione_temporanea storemanagement-registry.tesisquare.com/utilities/initial-load:LAB_BO /bin/sh

