DO
$$
DECLARE
    -- **PARAMETRO:** Definisci qui lo schema di cui vuoi contare gli oggetti.
    schema_target CONSTANT TEXT := 'boom';
    
    -- Variabile record per contenere la riga del cursore
    riga_relazione RECORD;
    
    -- Variabile per memorizzare il conteggio dei record
    conteggio_record BIGINT;
    
    -- Variabili temporanee per separare schema e nome per la citazione corretta
    v_schema_part TEXT;
    v_table_part TEXT;
    
    -- Definiamo la query che preleva i nomi completi.
    query_relazioni CONSTANT TEXT := format(
        $q$
            SELECT 
                table_schema || '.' || table_name AS full_name
            FROM 
                information_schema.tables
            WHERE 
                table_schema = %L
                AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY 
                table_name;
        $q$,
        schema_target
    );
BEGIN
    -- Intestazione dell'output
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'Schema: % (Mostrando solo oggetti con Conteggio > 0)', schema_target;
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'Nome Tabella/Vista            | Conteggio Record ';
    RAISE NOTICE '---------------------------------------------------';

    -- Cicla usando la sintassi FOR...IN EXECUTE che gestisce la query dinamica.
    FOR riga_relazione IN EXECUTE query_relazioni
    LOOP
        
        -- Separa lo schema dal nome della tabella/vista.
        v_schema_part := SPLIT_PART(riga_relazione.full_name, '.', 1);
        v_table_part  := SPLIT_PART(riga_relazione.full_name, '.', 2);
        
        conteggio_record := 0; -- Inizializza il conteggio a 0
        
        BEGIN
            -- 1. Esegue il conteggio dinamico
            EXECUTE format('SELECT COUNT(*) FROM %I.%I', v_schema_part, v_table_part)
            INTO conteggio_record;

            -- 2. VERIFICA CONDIZIONALE: Stampa solo se il conteggio è maggiore di 0
            IF conteggio_record > 0 THEN
                RAISE NOTICE '% | %',
                             RPAD(riga_relazione.full_name, 30, ' '),
                             conteggio_record;
            END IF;

        EXCEPTION
            -- Correzione: Uso esplicito di %s per garantire che i due parametri siano passati correttamente.
            WHEN others THEN
                RAISE WARNING 'Errore durante l''elaborazione di "%s": %s', riga_relazione.full_name, SQLERRM;
        END;

    END LOOP;

    RAISE NOTICE '---------------------------------------------------';
END
$$
LANGUAGE plpgsql;