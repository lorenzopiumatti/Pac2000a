DO
$$
DECLARE
    schema_target CONSTANT TEXT := 'boom';
    riga_relazione RECORD;
    max_id BIGINT;
    next_val BIGINT;
    last_value BIGINT;
    
    query_relazioni CONSTANT TEXT := format(
        $q$
            SELECT 
                t.table_name,
                t.column_name AS pk_column,
                t.column_default AS extracted_default_full
            FROM 
                information_schema.columns t
            WHERE 
                t.table_schema = %L
                AND t.column_name = 'id'
                AND t.column_default LIKE 'nextval(%%::regclass)'
            ORDER BY t.table_name;
        $q$,
        schema_target
    );
BEGIN
    RAISE NOTICE '--- SIMULAZIONE: VERIFICA SEQUENZE ERRATE PER SCHEMA % ---', schema_target;
    RAISE NOTICE '--- Vengono mostrate SOLO le tabelle con MAX(id) >= Valore Corrente Sequenza ---';
    RAISE NOTICE '-------------------------------------------------------------------';
    RAISE NOTICE 'Tabella                               | MAX ID | Valore Corrente | Prossimo Valore (Target)';
    RAISE NOTICE '-------------------------------------------------------------------';

    FOR riga_relazione IN EXECUTE query_relazioni
    LOOP
        DECLARE
            table_name_clean TEXT := riga_relazione.table_name;
            full_table_name TEXT := format('%I.%I', schema_target, table_name_clean);
            sequence_name_cleaned TEXT;
            sequence_name_unqualified TEXT;
            full_sequence_name TEXT;
        BEGIN
            -- Estrae il nome della sequenza
            sequence_name_cleaned := TRIM(BOTH FROM REGEXP_REPLACE(riga_relazione.extracted_default_full, 'nextval\(''(.*)''::regclass\)', '\1'));
            sequence_name_cleaned := TRIM(BOTH '''' FROM sequence_name_cleaned);
            sequence_name_unqualified := SPLIT_PART(sequence_name_cleaned, '.', 2);
            IF sequence_name_unqualified = '' THEN
                sequence_name_unqualified := sequence_name_cleaned;
            END IF;

            full_sequence_name := format('%I.%I', schema_target, sequence_name_unqualified);

            -- Trova MAX(id)
            EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %s', riga_relazione.pk_column, full_table_name) INTO max_id;

            -- Legge il valore corrente della sequenza (last_value)
            EXECUTE format('SELECT last_value FROM %s', full_sequence_name) INTO last_value;

            -- Calcola il prossimo valore target
            next_val := max_id + 1;

            -- CONDIZIONE DI FILTRO: Mostra solo se la sequenza è in ritardo o pari a MAX(id)
            -- Nota: last_value dovrebbe essere maggiore di max_id per essere corretto.
            IF last_value <= max_id THEN
                
                RAISE NOTICE '% | % | % | %', 
                             RPAD(table_name_clean, 30, ' '), 
                             LPAD(max_id::TEXT, 6, ' '), 
                             LPAD(last_value::TEXT, 15, ' '),
                             LPAD(next_val::TEXT, 23, ' ');
            END IF;
            
        EXCEPTION
            WHEN others THEN
                RAISE WARNING 'ERRORE: impossibile elaborare Tabella "%s" (Seq. "%s"). Dettaglio: %s', 
                              table_name_clean, full_sequence_name, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '-------------------------------------------------------------------';
    RAISE NOTICE 'Simulazione filtro completata.';
END
$$
LANGUAGE plpgsql;