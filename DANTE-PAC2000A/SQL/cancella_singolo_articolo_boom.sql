DO $$
DECLARE
    -- Variabile di filtro puramente per chiarezza nel WHERE finale.
    V_ITEM_CODE_FILTER constant text := 'Articoli con processing_step3 = 0';
    
    -- In una singola operazione set-based, otteniamo l'elenco degli ID da eliminare una sola volta
    V_ITEMS_TO_DELETE_IDS bigint[]; 
    V_ITEM_SALE_IDS_TO_DELETE bigint[];
    V_ITEM_LOGISTIC_IDS_TO_DELETE bigint[];
    V_ROWS_DELETED integer; 
    
BEGIN
    RAISE NOTICE '🚀 Inizio della procedura di cancellazione massiva a set. Verranno eliminati fino a 400 articoli.';
    
    -- 1. Identifica *tutti* gli ID da cancellare (ID di tmd_items)
    SELECT array_agg(id) INTO V_ITEMS_TO_DELETE_IDS
    FROM boom.tmd_items
    WHERE item IN (SELECT item FROM tin_Data_item_in WHERE processing_step3 = 0 LIMIT 400);


    IF V_ITEMS_TO_DELETE_IDS IS NULL OR array_length(V_ITEMS_TO_DELETE_IDS, 1) IS NULL THEN
        RAISE NOTICE '✅ Nessun articolo trovato da cancellare con il filtro specificato.';
        RETURN;
    END IF;

    RAISE NOTICE '👉 Trovati % articoli da elaborare.', array_length(V_ITEMS_TO_DELETE_IDS, 1);

    -- 2. Identifica *tutti* gli ID di tmd_item_Sales e tmd_item_logistics correlati
    RAISE NOTICE '   - Recupero ID intermedie...';
    SELECT array_agg(id) INTO V_ITEM_SALE_IDS_TO_DELETE
    FROM tmd_item_Sales
    WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);

    SELECT array_agg(id) INTO V_ITEM_LOGISTIC_IDS_TO_DELETE
    FROM tmd_item_logistics
    WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);

    IF V_ITEM_SALE_IDS_TO_DELETE IS NULL THEN V_ITEM_SALE_IDS_TO_DELETE := '{}'; END IF;
    IF V_ITEM_LOGISTIC_IDS_TO_DELETE IS NULL THEN V_ITEM_LOGISTIC_IDS_TO_DELETE := '{}'; END IF;

    -- 3. Esegui la cancellazione per tutte le tabelle dipendenti (DAL FIGLIO AL GENITORE)

    RAISE NOTICE '--- Inizio cancellazioni LOGISTICHE ---';

    -- A) Cancellazione dipendenze LOGISTICHE
    BEGIN
        
        -- tmd_logistic_units figli 
        DELETE FROM tor_pre_orders_detail WHERE item_logistic_id = ANY(V_ITEM_LOGISTIC_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tor_pre_orders_detail.', V_ROWS_DELETED;
        
        DELETE FROM tsm_receptions_detail WHERE item_logistic_id = ANY(V_ITEM_LOGISTIC_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tsm_receptions_detail.', V_ROWS_DELETED;

        DELETE FROM tor_orders_detail WHERE item_logistic_id = ANY(V_ITEM_LOGISTIC_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tor_orders_detail.', V_ROWS_DELETED;
        
        -- tmd_item_logistics figli
        DELETE FROM tmd_orderable_assortments_var WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_orderable_assortments_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_orderable_assortments WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_orderable_assortments.', V_ROWS_DELETED;

        -- tmd_logistic_units (Genitore Logistico Intermedio)
        DELETE FROM tmd_logistic_units WHERE item_logistic_id = ANY(V_ITEM_LOGISTIC_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_logistic_units.', V_ROWS_DELETED;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING '⚠️ Errore durante la cancellazione di massa (tabelle Logistics/Orderable/Receptions): %', SQLERRM;
    END;

    RAISE NOTICE '--- Inizio cancellazioni VENDITE ---';

    -- B) Cancellazione dipendenze SALES
    BEGIN
        -- Figli di tmd_item_Sales
        DELETE FROM tmd_sale_codes_var WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_sale_codes_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_sale_codes WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_sale_codes.', V_ROWS_DELETED;
        
        DELETE FROM tmd_item_images WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_images.', V_ROWS_DELETED;
        
        DELETE FROM tmd_saleable_assortments WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_saleable_assortments.', V_ROWS_DELETED;
        
        DELETE FROM tmd_sale_prices_var WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_sale_prices_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_sale_prices WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_sale_prices.', V_ROWS_DELETED;
        
        DELETE FROM ttr_item_sales WHERE item_sale_id = ANY(V_ITEM_SALE_IDS_TO_DELETE);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da ttr_item_sales.', V_ROWS_DELETED;
        
        -- tmd_item_Sales è il genitore intermedio
        DELETE FROM tmd_item_Sales WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS); 
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_Sales.', V_ROWS_DELETED;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING '⚠️ Errore durante la cancellazione di massa (tabelle Sales): %', SQLERRM;
    END;

    
    RAISE NOTICE '--- Inizio cancellazioni TABELLE BASE ---';

    -- C) Cancellazione dipendenze TABELLE BASE
    BEGIN
        
        -- tgr_main_sales dipende da tmd_items
        DELETE FROM tgr_main_sales WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tgr_main_sales.', V_ROWS_DELETED;

        -- **NUOVA CORREZIONE**: tsm_stocks è figlio di tsm_movements (cancellato per primo)
        DELETE FROM tsm_stocks WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tsm_stocks.', V_ROWS_DELETED;
        
        -- tsm_stocks_history è figlio di tsm_movements (cancellato per secondo)
        DELETE FROM tsm_stocks_history WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tsm_stocks_history.', V_ROWS_DELETED;
        
        -- tsm_movements è genitore di entrambi, cancellato per terzo (dopo i figli)
        DELETE FROM tsm_movements WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tsm_movements.', V_ROWS_DELETED;

        -- Tutte le altre tabelle dipendenti da item_id
        DELETE FROM tmd_item_codes WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_codes.', V_ROWS_DELETED;
        
        DELETE FROM tmd_item_codes_var WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_codes_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_structure_item_links WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_structure_item_links.', V_ROWS_DELETED;
        
        DELETE FROM tmd_structure_item_links_var WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_structure_item_links_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_supplier_item_codes WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_supplier_item_codes.', V_ROWS_DELETED;
        
        DELETE FROM tmd_feature_item_links WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_feature_item_links.', V_ROWS_DELETED;
        
        DELETE FROM tmd_purchase_prices_var WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_purchase_prices_var.', V_ROWS_DELETED;
        
        DELETE FROM tmd_purchase_prices WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_purchase_prices.', V_ROWS_DELETED;
        
        DELETE FROM ttr_items WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da ttr_items.', V_ROWS_DELETED;
        
        DELETE FROM tin_trace_in WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tin_trace_in.', V_ROWS_DELETED;
        
        DELETE FROM tmd_kit_components WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_kit_components.', V_ROWS_DELETED;

        -- tmd_item_logistics (Dipende da tmd_items)
        DELETE FROM tmd_item_logistics WHERE item_id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_logistics (spostata).', V_ROWS_DELETED;
        
        -- Penultimo step: Cancella da tmd_item_anag (Genitore intermedio)
        DELETE FROM tmd_item_anag WHERE id = ANY(V_ITEMS_TO_DELETE_IDS);
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da tmd_item_anag.', V_ROWS_DELETED;
        
        -- Ultimo step: Cancella dalla tabella principale (Genitore finale)
        DELETE FROM boom.tmd_items WHERE id = ANY(V_ITEMS_TO_DELETE_IDS); 
        GET DIAGNOSTICS V_ROWS_DELETED = ROW_COUNT;
        RAISE NOTICE '   - Cancellati % record da boom.tmd_items (principale).', V_ROWS_DELETED;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING '⚠️ Errore durante la cancellazione di massa (tabelle Item Base/Stock): %', SQLERRM;
    END;


    RAISE NOTICE '🏁 Cancellazione massiva completata per % articoli. La procedura è stata riordinata per rispettare le chiavi esterne.', array_length(V_ITEMS_TO_DELETE_IDS, 1);
    
END$$;