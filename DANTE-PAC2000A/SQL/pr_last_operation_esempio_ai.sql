-- DROP PROCEDURE boom.pr_last_operations(varchar);

CREATE OR REPLACE PROCEDURE boom.pr_last_operations_esempio_ai(update_type character varying DEFAULT '0'::character varying)
 LANGUAGE plpgsql
AS $procedure$
/****************************************************************************************/
/* Data Creazione   : 18/11/2022                                                        */
/* Autore           : vladstefan.amariutei@tesisquare.com (u1139)                       */
/* Scopo            : Procedura post elaborazioni Java                                  */
/****************************************************************************************/
/* Data modifica    : 30/01/2026                                                        */
/* Autore           : u1105 edoardo.scirghi@tesisquare.com                              */
/* Tag              : ES20260130                                                        */
/* Scopo            : Aggiunta procedura di aggioramento prezzo con marginalita' aggiun */
/*                    tiva                                                              */
/****************************************************************************************/
/* Data modifica    : 12/02/2026                                                        */
/* Scopo            : Esecuzione statement da tabella boom.tin_data_query (update_type, */
/*                    ord, sql, active_flag); IF per update_type + solo step con flag attivo */
/****************************************************************************************/
declare
    --
    w_updated_rows          integer         := 0;
    w_updated_rows_sect     integer         := 0;
    --
    w_log_text              text            := '' ;
    w_process_name          text            := 'pr_last_operations' ;
    w_log_return            integer         := 0 ;
    --
    w_err_state             text            := '' ;
    w_err_mess              text            := '' ;
    w_err_context           text            := '' ;
    --
    w_update_type           character varying;
    rec                     record;
begin
    w_update_type := update_type;
    --
    w_log_text := 'Inizio procedura';
    --
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
    IF w_log_return <> 0 THEN
        w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ' || sqlstate;
        RAISE NOTICE USING MESSAGE = w_log_text;
    END IF;
    --
    --- Esecuzione statement da tabella tin_data_query: per ogni update_type gli step con active_flag = '1', ordinati per ord
    --
    IF w_update_type = '1' THEN
        FOR rec IN
            SELECT tqd.ord, tqd.sql, tqd.descrizione_step
            FROM boom.tin_data_query tqd
            WHERE tqd.update_type = '1'
              AND tqd.active_flag = '1'
            ORDER BY tqd.ord
        LOOP
            w_log_text := format('Eseguito step: update_type 1, ordine %s - %s', rec.ord, coalesce(rec.descrizione_step, ''));
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
            EXECUTE rec.sql;
            GET DIAGNOSTICS w_updated_rows = ROW_COUNT;
            w_updated_rows_sect := w_updated_rows_sect + w_updated_rows;
            w_log_text := format('Step completato: update_type 1, ordine %s - %s - righe elaborate: %s', rec.ord, coalesce(rec.descrizione_step, ''), w_updated_rows);
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
        END LOOP;
    ELSIF w_update_type = '2' THEN
        FOR rec IN
            SELECT tqd.ord, tqd.sql, tqd.descrizione_step
            FROM boom.tin_data_query tqd
            WHERE tqd.update_type = '2'
              AND tqd.active_flag = '1'
            ORDER BY tqd.ord
        LOOP
            w_log_text := format('Eseguito step: update_type 2, ordine %s - %s', rec.ord, coalesce(rec.descrizione_step, ''));
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
            EXECUTE rec.sql;
            GET DIAGNOSTICS w_updated_rows = ROW_COUNT;
            w_updated_rows_sect := w_updated_rows_sect + w_updated_rows;
            w_log_text := format('Step completato: update_type 2, ordine %s - %s - righe elaborate: %s', rec.ord, coalesce(rec.descrizione_step, ''), w_updated_rows);
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
        END LOOP;
    ELSIF w_update_type = '3' THEN
        FOR rec IN
            SELECT tqd.ord, tqd.sql, tqd.descrizione_step
            FROM boom.tin_data_query tqd
            WHERE tqd.update_type = '3'
              AND tqd.active_flag = '1'
            ORDER BY tqd.ord
        LOOP
            w_log_text := format('Eseguito step: update_type 3, ordine %s - %s', rec.ord, coalesce(rec.descrizione_step, ''));
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
            EXECUTE rec.sql;
            GET DIAGNOSTICS w_updated_rows = ROW_COUNT;
            w_updated_rows_sect := w_updated_rows_sect + w_updated_rows;
            w_log_text := format('Step completato: update_type 3, ordine %s - %s - righe elaborate: %s', rec.ord, coalesce(rec.descrizione_step, ''), w_updated_rows);
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
        END LOOP;
    ELSIF w_update_type = '3.5' THEN
        FOR rec IN
            SELECT tqd.ord, tqd.sql, tqd.descrizione_step
            FROM boom.tin_data_query tqd
            WHERE tqd.update_type = '3.5'
              AND tqd.active_flag = '1'
            ORDER BY tqd.ord
        LOOP
            w_log_text := format('Eseguito step: update_type 3.5, ordine %s - %s', rec.ord, coalesce(rec.descrizione_step, ''));
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
            EXECUTE rec.sql;
            GET DIAGNOSTICS w_updated_rows = ROW_COUNT;
            w_updated_rows_sect := w_updated_rows_sect + w_updated_rows;
            w_log_text := format('Step completato: update_type 3.5, ordine %s - %s - righe elaborate: %s', rec.ord, coalesce(rec.descrizione_step, ''), w_updated_rows);
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
        END LOOP;
    ELSIF w_update_type = '4' THEN
        FOR rec IN
            SELECT tqd.ord, tqd.sql, tqd.descrizione_step
            FROM boom.tin_data_query tqd
            WHERE tqd.update_type = '4'
              AND tqd.active_flag = '1'
            ORDER BY tqd.ord
        LOOP
            w_log_text := format('Eseguito step: update_type 4, ordine %s - %s', rec.ord, coalesce(rec.descrizione_step, ''));
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
            EXECUTE rec.sql;
            GET DIAGNOSTICS w_updated_rows = ROW_COUNT;
            w_updated_rows_sect := w_updated_rows_sect + w_updated_rows;
            w_log_text := format('Step completato: update_type 4, ordine %s - %s - righe elaborate: %s', rec.ord, coalesce(rec.descrizione_step, ''), w_updated_rows);
            w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
            IF w_log_return <> 0 THEN
                w_log_text := w_log_text || ' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                RAISE NOTICE USING MESSAGE = w_log_text;
            END IF;
        END LOOP;
    END IF;
    --
    --
    --commit;
    --
    w_log_text := format('Righe aggiornate fase %s: %s',update_type,w_updated_rows_sect);
    --
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
    IF w_log_return <> 0 THEN
        w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ' || sqlstate;
        RAISE NOTICE USING MESSAGE = w_log_text;
    END IF;
    --
    exception
        when others then
            get stacked diagnostics
                w_err_context = pg_exception_context,
                w_err_mess  = message_text,
                w_err_state = returned_sqlstate;
                ---
                w_log_text := w_log_text || ' - ERROR STATE: ' || w_err_state ||  '- ERROR MESSAGE: ' ||  w_err_mess ||  '- ERROR CONTEXT: ' || w_err_context ;
                                w_log_return := fn_log('ERROR', w_process_name, w_log_text, 0);
                        ---
                if w_log_return <> 0 THEN
                    w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ' || sqlstate;
                    raise notice using message = w_log_text;
                end if;

        --rollback;
    --
end;
$procedure$
;
