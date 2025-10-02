DO $$
DECLARE
    -- Dichiarazione del cursore per contenere i risultati della query
    cur_updates CURSOR FOR
        SELECT a.id, a.item_id, a.logistic_unit_id, a.idcollo
        FROM (
            SELECT
                (SELECT tlu.id FROM boom.tmd_logistic_units tlu, tmd_item_logistics til WHERE tlu.item_logistic_id = til.id AND til.item_ID = toa.item_id AND logistic_unit_pc = 1) AS idpezzo,
                (SELECT tlu.id FROM boom.tmd_logistic_units tlu, tmd_item_logistics til WHERE tlu.item_logistic_id = til.id AND til.item_ID = toa.item_id AND logistic_unit_pc = 41) AS idcollo,
                toa.id,
				toa.item_id,
                toa.logistic_unit_id
            FROM tmd_orderable_assortments toa
        ) a
        WHERE a.logistic_unit_id <> a.idcollo;
-----        AND a.item_id in (select id from tmd_items where item = '708606-01');
	    -- Dichiarazione delle variabili per memorizzare i valori del cursore
	    v_id        tmd_orderable_assortments.id%TYPE;
	    v_logistic_unit_id        tmd_orderable_assortments.logistic_unit_id%TYPE;
	    v_item_id        tmd_orderable_assortments.item_id%TYPE;
	    v_idcollo        tmd_logistic_units.id%TYPE;
		---
BEGIN
    -- Apre il cursore
    OPEN cur_updates;
    
    -- Inizia il loop sui risultati del cursore
    LOOP
        -- Recupera la riga successiva e la inserisce nelle variabili
        FETCH cur_updates INTO v_id, v_item_id, v_logistic_unit_id, v_idcollo;
        
        -- Esce dal loop se non ci sono più righe
        EXIT WHEN NOT FOUND;
        
        -- Aggiorna la tabella tmd_orderable_assortments
        UPDATE tmd_orderable_assortments
        SET logistic_unit_id = v_idcollo
        WHERE id = v_id and  item_id = v_item_id ;
    END LOOP;
    
    -- Chiude il cursore
    CLOSE cur_updates;
    
    -- Mostra un messaggio di completamento
    RAISE NOTICE 'Aggiornamento completato per item_ID .';
END $$;

commit;


