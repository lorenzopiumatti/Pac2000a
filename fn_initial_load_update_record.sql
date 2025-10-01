-- DROP FUNCTION boom.fn_initial_load_update_record(int4, int4);

CREATE OR REPLACE FUNCTION boom.fn_initial_load_update_record(p_batch_size integer, p_step integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
/****************************************************************************************/
/* Data Creazione   : 10/09/2025                                                        */
/* Autore           : angelica.ciravegna@tesisquare.com (U0092)                         */
/* Scopo            : Aggiornamento dei record a tranche per garantire commit intermedie*/
/****************************************************************************************/
DECLARE
---
--- DICHIARAZIONE VARIABILI GENERALI
---
    w_process_name        VARCHAR(50)      := 'fn_initial_load_update_record';
    w_log_return          INT              := 0;
    w_log_text            TEXT             := NULL;
    w_err_state           TEXT             := NULL;
    w_err_context         TEXT             := NULL;
    w_err_mess            TEXT             := NULL;    
    ---
    w_n_rec               INT              := 0;
BEGIN
    ---
    --- INIZIO PROCEDURA
    w_log_text := 'INIZIO PROCEDURA '||w_process_name;
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
    IF p_step = 1 THEN
        ---
        --- Passo 1: AGGIORNO I RECORD DA ELABOARE CON STATO 5
        w_log_text := 'AGGIORNO I RECORD DA ELABOARE CON STATO 5';
        UPDATE tin_data_item_in
        SET    processing_step3 = 5
        WHERE  processing_step3 = 0 
        AND    processing_step1 = 1
        AND    processing_step2 = 1 
        AND    processing_step4 = 0;
        ---
        GET DIAGNOSTICS w_n_rec = ROW_COUNT;
        w_log_text := w_log_text||' - N° RECORD TOTALI DA ELABORARE '||w_n_rec::text;
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ELSIF p_step = 2 THEN
        ---
	    ---
	    --- Passo 2: AGGIORNO UNA TRANCHE DI RECORD (PRIMI p_batch_size ARTICOLI)
	    w_log_text := 'AGGIORNO UNA TRANCHE DI RECORD (PRIMI '||p_batch_size::text||' ARTICOLI)';
		UPDATE tin_data_item_in t
		SET    processing_step3 = 0
		WHERE  t.id IN (
		    SELECT id
		    FROM tin_data_item_in
		    WHERE processing_step3 = 5
		      AND processing_step2 = 1
		      AND processing_step1 = 1
		      AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 
		           OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
		      AND item IN (
		          SELECT item
		          FROM (
		              SELECT item,
		                     MIN(processing_item) AS min_proc_item,
		                     MIN(transaction_code) AS min_tx_code,
		                     MIN(CASE WHEN processing_sale_code = 0 AND processing_item != 0 THEN id ELSE 1 END) AS sort_key,
		                     MIN(id) AS min_id
		              FROM tin_data_item_in
		              WHERE processing_step3 = 5
		                AND processing_step2 = 1
		                AND processing_step1 = 1
		                AND (processing_item = 0 OR processing_sale_code = 0 OR processing_assortment = 0 
		                     OR processing_puchase_price = 0 OR processing_sale_price = 0 OR processing_feature = 0)
		              GROUP BY item
		              ORDER BY min_proc_item, min_tx_code, sort_key DESC, min_id
		              LIMIT p_batch_size
		          ) AS ranked_items
		      )
		);
	    ---
	    GET DIAGNOSTICS w_n_rec = ROW_COUNT;
	    w_log_text := w_log_text||' - TRANCHE DI ARTICOLI DA ELABORARE '||w_n_rec::text;
	    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    END IF;
    ---
    RETURN w_n_rec;
    ---
    --- FINE PROCEDURA
    w_log_text := 'FINE PROCEDURA '||w_process_name;
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
EXCEPTION
    WHEN OTHERS then
        get stacked diagnostics
        w_err_context = pg_exception_context,
        w_err_mess  = message_text,
        w_err_state = returned_sqlstate;
        ---
        w_log_text := w_log_text || ' - ERROR STATE: ' || w_err_state ||  '- ERROR MESSAGE: ' ||  w_err_mess ||  '- ERROR CONTEXT: ' || w_err_context ;
        w_log_return := fn_log('ERROR', w_process_name, w_log_text, 0);
        ---
        ---
        IF w_log_return <> 0 THEN
            w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ';
            RAISE NOTICE USING MESSAGE = w_log_text;
        END IF;
        ---
        RETURN -1;
END ;
$function$
;
