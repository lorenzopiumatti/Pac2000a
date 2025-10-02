DO $$
DECLARE
    -- VARIABILE DA MODIFICARE: Inserisci qui il codice dell'articolo che vuoi cancellare
    V_ITEM_CODE_TO_DELETE constant character varying := '1929-01'; 
    
    V_ITEM_ID bigint;
    V_ITEM_SALE_IDS bigint[];       -- Array per gli ID di tmd_item_Sales
    V_ITEM_LOGISTIC_IDS bigint[];   -- Array per gli ID di tmd_item_logistics
    V_ROWS_DELETED integer;
BEGIN
    -- 1. Trova l'ID dell'articolo (item_id)
    SELECT id INTO V_ITEM_ID
    FROM boom.tmd_items
    WHERE item = V_ITEM_CODE_TO_DELETE;

    -- Controlla se l'articolo è stato trovato
    IF V_ITEM_ID IS NULL THEN
        RAISE NOTICE 'Articolo % non trovato in boom.tmd_items. Nessuna cancellazione eseguita.', V_ITEM_CODE_TO_DELETE;
        RETURN;
    END IF;

    RAISE NOTICE 'Inizio cancellazione per Articolo: % (ID: %)', V_ITEM_CODE_TO_DELETE, V_ITEM_ID;

    -- 2. Trova gli ID delle vendite (item_sale_ids) e delle logistica (item_logistic_ids) associati
    SELECT array_agg(id) INTO V_ITEM_SALE_IDS
    FROM tmd_item_Sales
    WHERE item_id = V_ITEM_ID;

    SELECT array_agg(id) INTO V_ITEM_LOGISTIC_IDS
    FROM tmd_item_logistics
    WHERE item_id = V_ITEM_ID;
    

    --------------------------------------------------------------------------------
    -- 4. Esegui le cancellazioni
    --------------------------------------------------------------------------------
    DELETE FROM tmd_orderable_assortments WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_orderable_assortments.', V_ROWS_DELETED;

    DELETE FROM tmd_orderable_assortments_var WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_orderable_assortments_var.', V_ROWS_DELETED;

    -- Dipendenti da tmd_item_Sales (usando V_ITEM_SALE_IDS)
    DELETE FROM tmd_sale_codes WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_sale_codes.', V_ROWS_DELETED;

    DELETE FROM tmd_sale_codes_var WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_sale_codes_var.', V_ROWS_DELETED;

    DELETE FROM tmd_item_images WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_images.', V_ROWS_DELETED;

    DELETE FROM tmd_saleable_assortments WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_saleable_assortments.', V_ROWS_DELETED;

    DELETE FROM tmd_sale_prices_var WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_sale_prices_var.', V_ROWS_DELETED;

    DELETE FROM tmd_sale_prices WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_sale_prices.', V_ROWS_DELETED;

    DELETE FROM ttr_item_sales WHERE item_sale_id = ANY(V_ITEM_SALE_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da ttr_item_sales.', V_ROWS_DELETED;

    -- Dipendenti da tmd_item_logistics (usando V_ITEM_LOGISTIC_IDS)
    DELETE FROM tmd_logistic_units WHERE item_logistic_id = ANY(V_ITEM_LOGISTIC_IDS);
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_logistic_units.', V_ROWS_DELETED;

    -- Dipendenti da tmd_items (usando V_ITEM_ID) - Tabelle che usano item_id
    DELETE FROM tmd_item_codes WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_codes.', V_ROWS_DELETED;

    DELETE FROM tmd_item_codes_var WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_codes_var.', V_ROWS_DELETED;

    DELETE FROM tmd_structure_item_links WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_structure_item_links.', V_ROWS_DELETED;

    DELETE FROM tmd_structure_item_links_var WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_structure_item_links_var.', V_ROWS_DELETED;

    DELETE FROM tmd_supplier_item_codes WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_supplier_item_codes.', V_ROWS_DELETED;

    DELETE FROM tmd_feature_item_links WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_feature_item_links.', V_ROWS_DELETED;

    DELETE FROM tmd_purchase_prices_var WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_purchase_prices_var.', V_ROWS_DELETED;

    DELETE FROM tmd_purchase_prices WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_purchase_prices.', V_ROWS_DELETED;

    DELETE FROM tmd_item_logistics WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_logistics.', V_ROWS_DELETED;

    DELETE FROM ttr_items WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da ttr_items.', V_ROWS_DELETED;

    DELETE FROM tin_trace_in WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tin_trace_in.', V_ROWS_DELETED;

    DELETE FROM tmd_kit_components WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_kit_components.', V_ROWS_DELETED;

    -- Ultimo step: Cancella dalle tabelle intermedie (Sales e Logistics)
    DELETE FROM tmd_item_Sales WHERE item_id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_Sales.', V_ROWS_DELETED;

    -- Penultimo step: Cancella da tmd_item_anag (come nel tuo blocco DO originale)
    DELETE FROM tmd_item_anag WHERE id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da tmd_item_anag.', V_ROWS_DELETED;

    -- Ultimo step: Cancella dalla tabella principale
    DELETE FROM boom.tmd_items WHERE id = V_ITEM_ID;
    GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
    RAISE NOTICE 'Cancellati % record da boom.tmd_items.', V_ROWS_DELETED;


    RAISE NOTICE 'Cancellazione per Articolo: % completata con successo. ✅', V_ITEM_CODE_TO_DELETE;
    
END$$;