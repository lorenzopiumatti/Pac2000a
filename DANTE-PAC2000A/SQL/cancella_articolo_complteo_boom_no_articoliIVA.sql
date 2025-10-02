DELETE from tmd_sale_codes ticv   where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

delete from tmd_sale_codes_var  ticv   where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

DELETE from tmd_item_codes ticv   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_item_codes_var ticv   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_structure_item_links tsilv   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_structure_item_links_var  tsilv   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_supplier_item_codes   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_orderable_assortments  tsa  where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_orderable_assortments_var  tsa  where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_saleable_assortments tsa where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

delete  from tmd_feature_item_links where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_purchase_prices_var tpp    where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_purchase_prices tpp   where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from tmd_sale_prices_var where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

DELETE from tmd_sale_prices  where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

DELETE from ttr_item_sales where item_sale_id not in (select id from tmd_item_Sales where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like )) ;

DELETE from tmd_item_Sales where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );


DELETE from boom.tsm_delivery_notes_detail



DELETE from boom.tsm_delivery_notes



DELETE from tmd_logistic_units  where item_logistic_id not in (select  id from tmd_item_logistics  where item_id in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like));




DELETE from tmd_item_logistics  where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

DELETE from ttr_items where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

delete from tin_trace_in  where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

delete  from tmd_kit_components where item_id not in (SELECT id
FROM boom.tmd_items 
where item like 'ART%' collate case_like );

ALTER TABLE boom.tmd_items DISABLE TRIGGER tmd_items_tr1;

ALTER TABLE boom.tmd_items DISABLE TRIGGER tmd_items_tr2;

ALTER TABLE boom.tmd_items DISABLE TRIGGER tmd_items_tr3;

DO $$
DECLARE
    cur_items CURSOR FOR
        SELECT id
        FROM boom.tmd_items
        WHERE item NOT LIKE 'ART%' COLLATE case_like; -- seleziona quelli da eliminare
    v_id bigint;
    v_count integer := 0;
BEGIN
    OPEN cur_items;
    LOOP
        FETCH cur_items INTO v_id;
        EXIT WHEN NOT FOUND;
        
        delete from tmd_item_anag WHERE id = v_id;

        DELETE FROM boom.tmd_items WHERE id = v_id;
        v_count := v_count + 1;

        -- Segnalazione ogni 1000 cancellazioni
        IF v_count % 1000 = 0 THEN
            RAISE NOTICE 'Cancellati finora: %', v_count;
        END IF;
    END LOOP;

    CLOSE cur_items;

    -- Segnalazione finale se non multiplo di 1000
    IF v_count % 1000 <> 0 THEN
        RAISE NOTICE 'Cancellazione terminata. Totale cancellati: %', v_count;
    END IF;
END$$;


ALTER TABLE boom.tmd_items ENABLE TRIGGER tmd_items_tr1;

ALTER TABLE boom.tmd_items ENABLE TRIGGER tmd_items_tr2;

ALTER TABLE boom.tmd_items ENABLE TRIGGER tmd_items_tr3;
