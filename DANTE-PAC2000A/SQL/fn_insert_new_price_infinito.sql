-- DROP FUNCTION boom.fn_insert_new_sale_prices_infinito(varchar);

CREATE OR REPLACE FUNCTION boom.fn_insert_new_sale_prices_infinito(p_item character varying DEFAULT NULL::character varying)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    -- Variabili di conteggio e stato
    v_inserted_count INTEGER := 0; -- Usata per contare INSERT su VAR e l'ultimo UPDATE
    v_target_count INTEGER := 0;
    v_item_filter_log TEXT := 'Tutti gli articoli';
    v_insert_ok BOOLEAN := FALSE; -- Flag per tracciare il successo della prima INSERT
    v_error_message TEXT;

    -- Variabili per il Logging
    w_process_name CONSTANT TEXT := 'FN_INSERT_NEW_SALE_PRICES_INFINITO';
    w_log_text TEXT;
    w_log_return INTEGER; 

BEGIN
    -- Logging: INIZIO PROCEDURA
    w_log_text := 'INIZIO PROCEDURA. Parametro articolo: ' || COALESCE(p_item, 'Nessuno');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    -- 1. Log iniziale e determinazione del filtro
    IF p_item IS NOT NULL THEN
        v_item_filter_log := 'Articolo specifico: ' || p_item;
    END IF;
    
    RAISE NOTICE '--- INIZIO ELABORAZIONE BATCH (NO UPDATE, ULTIMO ID) ---';
    RAISE NOTICE 'Filtro Articolo: %', v_item_filter_log;
    RAISE NOTICE 'Passo 1: Identificazione degli item_sale_id target.';
    
    -- Logging: START IDENTIFICAZIONE TARGET
    w_log_text := 'Passo 1: Identificazione degli item_sale_id target (Articoli con variazione di prezzo e senza end_date 2099-12-31).';
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    -- ***************************************************************
    -- BLOCCO GESTITO CON ECCEZIONI (PRIMA INSERT: tmd_sale_prices_var)
    -- ***************************************************************
    BEGIN
        
        -- 1. Identificazione dei Target (usando la logica MultiplePrices)
        -- Questa CTE TargetItemIDs filtra gli articoli che sono validi, hanno codici vendita attivi,
        -- non hanno già un prezzo infinito, E hanno avuto più di un prezzo non promozionale (variazione).
        WITH TargetItemIDs AS (
            SELECT DISTINCT tis.id AS item_sale_id
            FROM tmd_items ti 
            INNER JOIN ttr_items ttr ON ttr.item_id = ti.id AND ttr.language_id = 1
            INNER JOIN tmd_item_sales tis ON tis.item_id = ti.id
            INNER JOIN tmd_saleable_assortments sa ON sa.item_sale_id = tis.id
            -- LOGICA DI FILTRO: Articoli con almeno 2 prezzi non promozionali (variazione)
            INNER JOIN (
                SELECT sp.item_sale_id 
                FROM tmd_sale_prices_var sp  
                WHERE sp.promo_code IS NULL
                GROUP BY sp.item_sale_id
                HAVING COUNT(sp.item_sale_id) > 1 
            ) AS MultiplePrices ON MultiplePrices.item_sale_id = tis.id
            -- FINE LOGICA DI FILTRO
            WHERE 
                ti.item_type_pc = 2 
              AND (ti.item = p_item OR p_item IS NULL)             
              AND EXISTS (
                  SELECT 1 FROM tmd_sale_codes sc
                  WHERE sc.item_sale_id = tis.id AND CURRENT_DATE BETWEEN sc.start_date AND sc.end_date
                    AND sc.code_type_pc IN (5,1,4,10)
              )
              AND NOT EXISTS (
                  SELECT 1 FROM tmd_sale_prices_var sp
                  WHERE sp.item_sale_id = tis.id AND sp.end_date = '2099-12-31' and promo_code is null
              )
        ),
        -- 2. Trova l'Ultimo Record (Basato sull'ID più alto) e i Dati Base per l'Inserimento
        -- Questa CTE seleziona l'ultimo record di prezzo NON promozionale (ID MAX) per gli articoli target.
        LastRecordData AS (
            SELECT DISTINCT ON (sp.item_sale_id)
                sp.*
            FROM 
                tmd_sale_prices_var sp
            INNER JOIN 
                TargetItemIDs ti ON sp.item_sale_id = ti.item_sale_id 
            where sp.promo_code is null
			ORDER BY 
                sp.item_sale_id, sp.id DESC 
        )
        -- 3. Inserimento dei Nuovi Record
        -- Viene inserito un nuovo record di prezzo con start_date = end_date precedente + 1 giorno e end_date = 2099-12-31.
        INSERT INTO tmd_sale_prices_var (
            item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, 
            sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, 
            start_date, end_date, is_sent, insert_type_ph, insert_type_pc, 
            creation_date, update_date, last_user, transaction_code
        )
        SELECT
            lrd.item_sale_id, lrd.network_id, lrd.price, lrd.base_price, lrd.unit_price_ph, lrd.unit_price_pc, 
            lrd.sale_price_type_ph, lrd.sale_price_type_pc, 
            NULL, -- promo_code = NULL
            lrd.vat_id,
            (lrd.end_date + INTERVAL '1 day')::DATE AS start_date, -- Nuova data di inizio
            '2099-12-31'::DATE, -- Data di fine infinita
            lrd.is_sent, lrd.insert_type_ph, lrd.insert_type_pc, 
            NOW(), NOW(), 'fn_insert_new_sale_prices_infinito', lrd.transaction_code
        FROM 
            LastRecordData lrd;
            
        GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
        v_insert_ok := TRUE; -- Se arriviamo qui, l'inserimento è avvenuto senza errori

        -- Logging: INSERT INTO tmd_sale_prices_var SUCCESSO
        w_log_text := 'INSERT su tmd_sale_prices_var completato. Inseriti ' || v_inserted_count || ' record infiniti.';
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    EXCEPTION
        WHEN OTHERS THEN
            -- Cattura l'errore, registra il messaggio e imposta il flag a FALSE
            GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
            RAISE WARNING 'ATTENZIONE: Errore durante l''INSERT su tmd_sale_prices_var. La transazione non è stata interrotta. Errore: %', v_error_message;
            -- Logging: ERRORE INSERT
            w_log_text := 'ERRORE nell''INSERT su tmd_sale_prices_var. Errore: ' || v_error_message;
            w_log_return := fn_log('ERROR', w_process_name, w_log_text, 0);
            
            v_inserted_count := 0; 
            v_insert_ok := FALSE;
    END;
    -- ***************************************************************
    -- FINE BLOCCO GESTITO CON ECCEZIONI
    -- ***************************************************************


    -- ***************************************************************
    -- LOGICA DI CONTROLLO E SECONDA INSERT (Tmd_sale_prices)
    -- ***************************************************************

    -- Ricalcolo del count degli item target per il logging (Utilizza la logica TargetItemIDs per coerenza)
    SELECT count(tis.id) INTO v_target_count
    FROM tmd_items ti 
    INNER JOIN ttr_items ttr ON ttr.item_id = ti.id AND ttr.language_id = 1
    INNER JOIN tmd_item_sales tis ON tis.item_id = ti.id
    INNER JOIN tmd_saleable_assortments sa ON sa.item_sale_id = tis.id
    -- LOGICA DI FILTRO AGGIUNTA (MultiplePrices)
    INNER JOIN (
        SELECT sp.item_sale_id 
        FROM tmd_sale_prices_var sp  
        WHERE sp.promo_code IS NULL
        GROUP BY sp.item_sale_id
        HAVING COUNT(sp.item_sale_id) > 1 
    ) AS MultiplePrices ON MultiplePrices.item_sale_id = tis.id
    -- FINE LOGICA DI FILTRO AGGIUNTA
    WHERE ti.item_type_pc = 2  
       AND (ti.item = p_item OR p_item IS NULL)
       AND EXISTS (
            SELECT 1 FROM tmd_sale_codes sc
            WHERE sc.item_sale_id = tis.id AND CURRENT_DATE BETWEEN sc.start_date AND sc.end_date
              AND sc.code_type_pc IN (5,1,4,10)
        )
        AND NOT EXISTS (
            SELECT 1 FROM tmd_sale_prices_var sp
            WHERE sp.item_sale_id = tis.id AND sp.end_date = '2099-12-31' and sp.promo_code is null
        );
        
    -- Esegui la seconda INSERT (su tmd_sale_prices) SOLO se la prima INSERT è andata a buon fine
    IF v_insert_ok THEN
        RAISE NOTICE 'Passo 2: Inserimento su tmd_sale_prices (shadow table).';
        
        -- Logging: START INSERT tmd_sale_prices
        w_log_text := 'Passo 2: Tentativo di INSERT su tmd_sale_prices (tabella shadow).';
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
        
        -- Inserisce i nuovi record da _var a tmd_sale_prices, escludendo ID già presenti.
	   INSERT INTO tmd_sale_prices
	        (id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, 
	         sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, 
	         end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, 
	         last_user, transaction_code)
	    SELECT 
	        tspv.id, tspv.item_sale_id, tspv.network_id, tspv.price, tspv.base_price, 
	        tspv.unit_price_ph, tspv.unit_price_pc, tspv.sale_price_type_ph, tspv.sale_price_type_pc, 
	        tspv.promo_code, tspv.vat_id, tspv.start_date, tspv.end_date, tspv.is_sent, 
	        tspv.insert_type_ph, tspv.insert_type_pc, tspv.creation_date, tspv.update_date, 
	        tspv.last_user, tspv.transaction_code
	    FROM tmd_sale_prices_var tspv
	    WHERE (item_sale_id, network_id) IN (
	        SELECT item_sale_id, network_id
	        FROM (
	            SELECT item_sale_id, network_id
	            FROM tmd_sale_prices_var
	            WHERE last_user = 'fn_insert_new_sale_prices_infinito' and promo_Code is null
	              AND creation_date >= CURRENT_DATE 
	        ) AS t
	        GROUP BY item_sale_id, network_id
	        HAVING count(*) = 1
	    )
	    -- FILTRO: Esclude i record il cui ID è già presente in tmd_sale_prices
	    AND NOT EXISTS (
	        SELECT 1
	        FROM tmd_sale_prices tsp
	        WHERE tsp.id = tspv.id
	    )
	    -- Filtri originali
	    AND tspv.last_user = 'fn_insert_new_sale_prices_infinito'
	    AND tspv.creation_date >= CURRENT_DATE
	    AND tspv.promo_code IS NULL;

        -- Logging: INSERT INTO tmd_sale_prices completato
        GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
        w_log_text := 'INSERT su tmd_sale_prices completato. Inseriti ' || v_inserted_count || ' record.';
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
	
    ELSE
        RAISE NOTICE 'Passo 2: Inserimento su tmd_sale_prices saltato a causa di errori nel Passo 1.';
        -- Logging: INSERT tmd_sale_prices SALTATO
        w_log_text := 'Passo 2: INSERT su tmd_sale_prices saltato.';
        w_log_return := fn_log('WARNING', w_process_name, w_log_text, 0);
    END IF;

    RAISE NOTICE '  - Trovati e Processati % item_sale_id che soddisfano i criteri.', v_target_count;
    RAISE NOTICE '  - Inseriti % nuovi record con end_date = 2099-12-31 (nella tabella VAR).', v_inserted_count;

    -- ***************************************************************
    -- AGGIORNAMENTO SU Tmd_sale_codes_var (Logica: Notifica Batch)
    -- ***************************************************************

    -- Logging: START UPDATE tmd_sale_codes_var
    w_log_text := 'Passo 3: Aggiornamento tmd_sale_codes_var per notifica batch.';
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    -- Identifica gli item_sale_id dei nuovi record infiniti (per il JOIN)
    -- Questa CTE identifica gli articoli per cui è stato creato il prezzo infinito in questa esecuzione.
    WITH NewlyProcessedItems AS (
        SELECT item_sale_id
        FROM tmd_sale_prices_var
        WHERE last_user = 'fn_insert_new_sale_prices_infinito'
          AND promo_code IS NULL
          AND creation_date >= CURRENT_DATE 
          AND end_date = '2099-12-31'::DATE
    )
    -- Aggiorna i codici di vendita attivi per questi articoli
	UPDATE tmd_sale_codes_var tscv
	SET 
	    is_updated = 1,
	    last_user = 'user_batch'
	FROM NewlyProcessedItems npi
	WHERE tscv.item_sale_id = npi.item_sale_id
      AND CURRENT_DATE BETWEEN tscv.start_date AND tscv.end_date
      AND tscv.code_type_pc IN (5,1,4,10); 

    -- Cattura il conteggio delle righe aggiornate 
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT; 

    -- Usa la variabile v_inserted_count per il NOTICE
    RAISE NOTICE 'Aggiornati % codici di vendita (tmd_sale_codes_var) per notifica al batch.', v_inserted_count;
    
    -- Logging: UPDATE tmd_sale_codes_var completato
    w_log_text := 'UPDATE su tmd_sale_codes_var completato. Righe aggiornate: ' || v_inserted_count;
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    
    -- ***************************************************************
    -- FINE AGGIORNAMENTO
    -- ***************************************************************


    RAISE NOTICE '--- FINE ELABORAZIONE BATCH ---';
    -- Logging: FINE PROCEDURA
    w_log_text := 'FINE PROCEDURA.';
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

END;
$function$
;