DO $$
DECLARE
    -- Dichiarazione del cursore per contenere i risultati della query
    cur_updates CURSOR FOR
        SELECT a.id, a.item_id, a.logistic_unit_id, a.idcollo, a.idpezzo
        FROM (
            SELECT
                (SELECT tlu.id FROM boom.tmd_logistic_units tlu, tmd_item_logistics til 
                WHERE tlu.item_logistic_id = til.id AND til.item_ID = toa.item_id AND logistic_unit_pc = 1) AS idpezzo,
                (SELECT tlu.id FROM boom.tmd_logistic_units tlu, tmd_item_logistics til 
                WHERE tlu.item_logistic_id = til.id AND til.item_ID = toa.item_id AND logistic_unit_pc = 41) AS idcollo,
                toa.id,
                toa.item_id,
                toa.logistic_unit_id
            FROM tmd_orderable_assortments_var toa
            where 1 = 1 
            AND operational_agreement_id in  
                    (
                    select id from tmd_operational_agreements toa  where description like '%MONGE%' collate case_like
                    )
        ) a
        WHERE 1=1
        and a.logistic_unit_id =  a.idcollo;
	    -- Dichiarazione delle variabili per memorizzare i valori del cursore
	    v_id        tmd_orderable_assortments_var.id%TYPE;
	    v_logistic_unit_id        tmd_orderable_assortments_var.logistic_unit_id%TYPE;
	    v_item_id        tmd_orderable_assortments_var.item_id%TYPE;
	    v_idcollo        tmd_logistic_units.id%TYPE;
	    v_idpezzo        tmd_logistic_units.id%TYPE;
		---
BEGIN
    -- Apre il cursore
    OPEN cur_updates;
    
    -- Inizia il loop sui risultati del cursore
    LOOP
        -- Recupera la riga successiva e la inserisce nelle variabili
        FETCH cur_updates INTO v_id, v_item_id, v_logistic_unit_id, v_idcollo, v_idpezzo;
        
        -- Esce dal loop se non ci sono più righe
        EXIT WHEN NOT FOUND;
        
        -- Aggiorna tmd_orderable_assortments_var: logistic_unit_id da collo a pezzo
        UPDATE tmd_orderable_assortments_var
        SET logistic_unit_id = v_idpezzo
        WHERE id = v_id AND item_id = v_item_id;
    END LOOP;
    
    -- Chiude il cursore
    CLOSE cur_updates;
    
    -- Mostra un messaggio di completamento
    RAISE NOTICE 'Aggiornamento completato: logistic_unit_id da collo a pezzo in tmd_orderable_assortments_var.';
END $$;

commit;


