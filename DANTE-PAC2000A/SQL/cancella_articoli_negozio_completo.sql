-- DBeaver: Execute script su tutto il file (CREATE PROCEDURE + CALL).
-- Cancellazione a blocchi da v_chunk_size articoli (default 1000).
--
-- COMMIT dopo ogni chunk: richiede PostgreSQL 11+ (il COMMIT in PL/pgSQL è consentito solo nelle PROCEDURE, non nei blocchi DO).

CREATE OR REPLACE PROCEDURE public.p_cancella_articoli_negozio_completo()
LANGUAGE plpgsql
AS $$
DECLARE
    v_chunk_size integer := 1000;

    v_chunk_item_ids bigint[];
    v_item_sale_ids bigint[];
    v_item_logistic_ids bigint[];
    v_logistic_unit_ids bigint[];

    v_total integer;
    v_chunk_num integer := 0;
    v_done integer := 0;

    v_t0 timestamptz;
    v_t1 timestamptz;
    v_rows bigint;
BEGIN
    IF v_chunk_size < 1 THEN
        RAISE EXCEPTION 'v_chunk_size deve essere >= 1';
    END IF;

    PERFORM set_config('synchronous_commit', 'off', true);
    PERFORM set_config('work_mem', '256MB', true);

    RAISE NOTICE '[%] === Inizio (chunk % articoli, COMMIT a fine ogni chunk) ===',
        to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS'), v_chunk_size;

    DROP TABLE IF EXISTS tmp_del_items_ids;

    v_t0 := clock_timestamp();
    CREATE TEMP TABLE tmp_del_items_ids (id bigint PRIMARY KEY);
    v_t1 := clock_timestamp();
    RAISE NOTICE '[%] CREATE TEMP tmp_del_items_ids | % ms',
        to_char(v_t1, 'YYYY-MM-DD HH24:MI:SS.MS'),
        round(1000 * EXTRACT(EPOCH FROM (v_t1 - v_t0))::numeric, 2);

    v_t0 := clock_timestamp();
    INSERT INTO tmp_del_items_ids (id)
    SELECT i.id
    FROM boom.tmd_items i , tmd_item_sales tis
    WHERE tis.item_id = i.id and tis.id in  (
        SELECT item_sale_id  from tmd_saleable_Assortments where network_id =3 
    );
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_t1 := clock_timestamp();
    RAISE NOTICE '[%] INSERT tmp_del_items_ids | % ms | % righe',
        to_char(v_t1, 'YYYY-MM-DD HH24:MI:SS.MS'),
        round(1000 * EXTRACT(EPOCH FROM (v_t1 - v_t0))::numeric, 2), v_rows;

    SELECT count(*)::integer INTO v_total FROM tmp_del_items_ids;

    IF v_total = 0 THEN
        RAISE NOTICE '[%] Nessun articolo trovato. Uscita.',
            to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS');
        DROP TABLE IF EXISTS tmp_del_items_ids;
        COMMIT;
        RETURN;
    END IF;

    RAISE NOTICE '[%] Articoli totali da smontare: %',
        to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS'), v_total;

    LOOP
        v_t0 := clock_timestamp();
        SELECT array_agg(id) INTO v_chunk_item_ids
        FROM (
            SELECT id FROM tmp_del_items_ids ORDER BY id LIMIT v_chunk_size
        ) s;

        IF v_chunk_item_ids IS NULL THEN
            EXIT;
        END IF;

        v_chunk_num := v_chunk_num + 1;
        v_done := v_done + cardinality(v_chunk_item_ids);

        RAISE NOTICE '[%] --- Chunk % | % articoli nel blocco | elaborati fin qui ~% / % ---',
            to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS'),
            v_chunk_num,
            cardinality(v_chunk_item_ids),
            v_done,
            v_total;

        SELECT array_agg(id) INTO v_item_sale_ids
        FROM boom.tmd_item_sales WHERE item_id = ANY (v_chunk_item_ids);
        SELECT array_agg(id) INTO v_item_logistic_ids
        FROM boom.tmd_item_logistics WHERE item_id = ANY (v_chunk_item_ids);

        v_item_sale_ids := COALESCE(v_item_sale_ids, '{}');
        v_item_logistic_ids := COALESCE(v_item_logistic_ids, '{}');

        SELECT array_agg(id) INTO v_logistic_unit_ids
        FROM boom.tmd_logistic_units
        WHERE item_logistic_id = ANY (v_item_logistic_ids);

        v_logistic_unit_ids := COALESCE(v_logistic_unit_ids, '{}');

        -- FASE 1
        DELETE FROM boom.tmd_logistic_unit_codes c
        WHERE c.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tsm_delivery_notes_detail t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tsm_receptions_detail t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tor_pre_orders_detail t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tor_orders_detail t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tmd_orderable_assortments_var t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tmd_orderable_assortments t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        DELETE FROM boom.tgr_main_sales t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.logistic_unit_id = ANY (v_logistic_unit_ids);

        -- FASE 2
        DELETE FROM boom.tmd_palbox_components t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.component_item_id = ANY (v_chunk_item_ids);

        DELETE FROM boom.tmd_kit_components t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.component_item_id = ANY (v_chunk_item_ids);

        DELETE FROM boom.tmd_recipe_components t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.component_item_id = ANY (v_chunk_item_ids);

        -- FASE 3
        DELETE FROM boom.tsm_inventories_detail WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tsm_inventory_inputs WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tsm_receptions_last_cost WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_purchase_prices_var WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_purchase_prices WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_supplier_item_codes WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_discount_items_var WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_discount_items WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_feature_item_links WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_item_codes_var WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_item_codes WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_structure_item_links_var WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_structure_item_links WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tin_trace_in WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.ttr_items WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tsm_stocks_history WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tsm_stocks WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tsm_movements WHERE item_id = ANY (v_chunk_item_ids);

        -- FASE 4
        DELETE FROM boom.tmd_sale_codes_var WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.tmd_sale_codes WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.tmd_item_images WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.tmd_saleable_assortments WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.tmd_sale_prices_var WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.tmd_sale_prices WHERE item_sale_id = ANY (v_item_sale_ids);
        DELETE FROM boom.ttr_item_sales WHERE item_sale_id = ANY (v_item_sale_ids);

        -- FASE 5
        UPDATE boom.tmd_items i SET parent_item_id = NULL
        WHERE i.parent_item_id = ANY (v_chunk_item_ids);

        DELETE FROM boom.tmd_logistic_units WHERE item_logistic_id = ANY (v_item_logistic_ids);
        DELETE FROM boom.tmd_item_logistics WHERE item_id = ANY (v_chunk_item_ids);
        DELETE FROM boom.tmd_item_sales WHERE item_id = ANY (v_chunk_item_ids);

        -- FASE 6
        DELETE FROM boom.tmd_item_anag t
        WHERE t.item_id = ANY (v_chunk_item_ids)
           OR t.id = ANY (v_chunk_item_ids);

        DELETE FROM boom.tmd_items WHERE id = ANY (v_chunk_item_ids);

        DELETE FROM tmp_del_items_ids WHERE id = ANY (v_chunk_item_ids);

        v_t1 := clock_timestamp();
        RAISE NOTICE '[%] Chunk % terminato in % ms. COMMIT.',
            to_char(v_t1, 'YYYY-MM-DD HH24:MI:SS.MS'),
            v_chunk_num,
            round(1000 * EXTRACT(EPOCH FROM (v_t1 - v_t0))::numeric, 2);

        COMMIT;
    END LOOP;

    DROP TABLE IF EXISTS tmp_del_items_ids;

    RAISE NOTICE '[%] === Fine: % chunk eseguiti (ultimo COMMIT già nel loop) ===',
        to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS'), v_chunk_num;

    COMMIT;
END;
$$;

CALL public.p_cancella_articoli_negozio_completo();
