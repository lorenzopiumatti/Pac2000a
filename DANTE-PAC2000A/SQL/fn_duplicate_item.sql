CREATE OR REPLACE FUNCTION boom.fn_duplicate_item_v2(p_item_id bigint, p_network_id bigint[], p_item_description character varying, p_item_type_pc bigint, p_stock_unit_pc bigint, p_is_multi_vat bigint, p_item_code character varying, p_sale_vat_id bigint, p_unit_price_pc bigint, p_price numeric, p_cost numeric, p_shop_item_category_pc bigint, p_structure_id bigint, p_coefficient bigint, p_is_val_weight numeric, p_is_weight_unit_measure_pc bigint, p_user character varying, p_transaction bigint)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$


/****************************************************************************************/
/* Data Creazione   : 07/02/2024                                                        */
/* Autore           : stefano.gonella@tesisquare.com (U0876)  sss                       */
/* Scopo            : Duplicazione articolo partendo dalle informazioni immesse da      */
/*                    maschera                                                          */
/****************************************************************************************/

DECLARE
---
--- DICHIARAZIONE VARIABILI GENERALI
---
    W_PROCESS_NAME          VARCHAR(50)                                     := 'PR_DUPLICATE_ITEM';
    W_LOG_RETURN            INT                                             := 0;
    W_LOG_TEXT              TEXT                                            := '';
    W_ERR_STATE             TEXT                                            := NULL;
    W_ERR_CONTEXT           TEXT                                            := NULL;
    W_ERR_MESS              TEXT                                            := NULL;
---
--- DICHIARAZIONE VARIABILI DI COMODO
---
    W_ITEM_FIGLIO_ID            TMD_ITEMS.ID%TYPE                               := NULL;
    W_PARAM_ID                  TPA_PARAMETERS.ID%TYPE                          := NULL;
    W_ITEM_CODE                 TMD_ITEMS.ITEM%TYPE                             := NULL;
    W_ITEM_SALE_ID              TMD_ITEM_SALES.ID%TYPE                          := NULL;
    w_item_logistic_id          TMD_ITEM_LOGISTICS.ID%TYPE                      := NULL;
    W_PRICE_UNIT                TMD_PURCHASE_PRICES_VAR.COST_TYPE_PH%TYPE       := NULL;
    W_STOCK_UNIT_PC             TMD_ITEMS.stock_unit_pc%TYPE                    := NULL;

	    W_BRAND_PC                  TMD_ITEMS.BRAND_PC%TYPE                         := NULL;



---
--- DICHIARAZIONE VARIABILI DI DUPLICA V2
---
	w_count_item_logistics				BIGINT 									:= NULL;
	w_count_feature_item_links  		BIGINT 									:= NULL;
	w_count_logistic_units 				BIGINT 									:= NULL;
	w_count_structure_item_links_var	BIGINT 									:= NULL;
	w_count_saleable_assortments		BIGINT 									:= NULL;
	w_count_purchase_prices_var			BIGINT 									:= NULL;
	w_count_orderable_assortments_var 	BIGINT 									:= NULL;
	w_count_sale_prices_var 			BIGINT 									:= NULL;
 w_count_supplier_item_codes				BIGINT 								:= NULL;

	W_NETWORK_CHILD_ID					INT 									:= 0;

	W_ORDERABLE_ASSORTMENTS_ID	TMD_ORDERABLE_ASSORTMENTS_VAR.ID%TYPE			:= NULL;
	W_PURCHASE_PRICES_ID		TMD_PURCHASE_PRICES_VAR.ID%TYPE					:= NULL;
	W_SALE_PRICES_VAR_ID		TMD_SALE_PRICES_VAR.ID%TYPE     				:= NULL;

	W_ITEM_PADRE_ID            	TMD_ITEMS.ID%TYPE                               := NULL;
	W_LOGISTIC_UNIT_MEASURE     TMD_ITEM_LOGISTICS.UNIT_MEASURE_PC%TYPE         := NULL;
    W_LOGISTIC_UNIT_ID          TMD_LOGISTIC_UNITS.ID%TYPE                      := NULL;
	W_VAT_ID                    TMD_VAT.ID%TYPE                                 := NULL;
	W_CASH_DEP                  TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
	W_CASH_DEP_FEATURE_ID       TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
    W_CASH_DEP_DESCRIPTION      TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
	W_CONSI_ID                  TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
	W_CONSI                     TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
    W_CONSI_DESCRIPTION         TMD_FEATURE_ITEM_LINKS.STR_VAL%TYPE             := NULL;
	W_MAG_RIF_ID                TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
    W_MAG_RIF                   TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
    W_MAG_RIF_DESCRIPTION       TMD_FEATURE_ITEM_LINKS.STR_VAL%TYPE             := NULL;
	W_F_PRZ_ID                  TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
    W_F_PRZ                     TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
    W_F_PRZ_ID_DESCRIPTION      TMD_FEATURE_ITEM_LINKS.STR_VAL%TYPE             := NULL;
	W_STAG_ID                   TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
    W_STAG                      TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
    W_STAG_DESCRIPTION          TMD_FEATURE_ITEM_LINKS.STR_VAL%TYPE             := NULL;
    W_F_V_PESO_C_ID             TMD_FEATURE_ITEM_LINKS.SPECIFIC_FEATURE_ID%TYPE := NULL;
    W_F_V_PESO_C                TMD_FEATURE_ITEM_LINKS.FEATURES_VALUE%TYPE      := NULL;
    W_F_V_PESO_C_DESCRIPTION    TMD_FEATURE_ITEM_LINKS.STR_VAL%TYPE             := NULL;
    W_OPERATIONAL_AGREEMENT_ID  TMD_OPERATIONAL_AGREEMENTS.ID%TYPE              := NULL;

---
BEGIN
    ---
    w_log_text := '--- INIZIO PROCEDURA ---';
    ---
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---

    IF w_log_return <> 0 THEN
        w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ' || sqlstate;
        RAISE NOTICE USING MESSAGE = w_log_text;
    END IF;

	----
	W_ITEM_PADRE_ID:=p_item_id;
    W_ITEM_CODE	:=P_ITEM_CODE;
	----

    IF(P_ITEM_CODE is null) THEN
        ---
        w_log_text := 'STACCO IL CODICE ARTICOLO | generazione da parametri header=40 code=1';
        ---
        SELECT ID,NUM_VAL_4+1
        INTO W_PARAM_ID,W_ITEM_CODE
        FROM VPA_PARAMETERS VP
        WHERE PARAMETER_HEADER =40
        AND PARAMETER_CODE =1
        AND IS_DEFAULT =1;

        UPDATE TPA_PARAMETERS TP
        SET NUM_VAL_4 =W_ITEM_CODE::NUMERIC
        WHERE ID=W_PARAM_ID;
        ---
        W_ITEM_CODE := W_ITEM_CODE||'-01';
        w_log_text := w_log_text || ' | param_id=' || coalesce(W_PARAM_ID::text,'NULL') || ' nuovo_codice=' || coalesce(W_ITEM_CODE,'');
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
    end if;
    ---
    w_log_text := 'STRUTTURA/CODICE ARTICOLO | item_padre_id=' || W_ITEM_PADRE_ID || ' codice=' || coalesce(W_ITEM_CODE,'');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---

	w_log_text := 'ESTRAGGO UNITA DI MISURA LOGISTICA | stock_unit_pc=' || coalesce(p_stock_unit_pc::text,'NULL');
    ---
    SELECT PARAMETER_CODE
    INTO W_LOGISTIC_UNIT_MEASURE
    FROM VPA_PARAMETERS VP
    WHERE PARAMETER_HEADER =30
    AND IS_DEFAULT =1
    AND STR_VAL_1 = (SELECT DESCRIPTION
                     FROM VPA_PARAMETERS VP
                     WHERE PARAMETER_HEADER =10
                     AND IS_DEFAULT =1
                     AND PARAMETER_CODE =P_STOCK_UNIT_PC);
    w_log_text := w_log_text || ' | logistic_unit_measure=' || coalesce(W_LOGISTIC_UNIT_MEASURE::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO IVA | is_multi_vat=' || coalesce(p_is_multi_vat::text,'NULL');
    ---
    IF p_is_multi_vat = 0 THEN
    ---
        SELECT TV.ID
        INTO W_VAT_ID
        FROM TMD_VAT TV ,VPA_PARAMETERS VP
        WHERE 1=1
        AND PARAMETER_HEADER=96
        AND VP.DESCRIPTION ='VAT_VALUE'
        AND IS_DEFAULT =1
        AND VAT_VALUE::VARCHAR=STR_VAL_1::VARCHAR;
    ELSE
    ---
        SELECT TV.ID
        INTO W_VAT_ID
        FROM TMD_VAT TV ,VPA_PARAMETERS VP
        WHERE 1=1
        AND PARAMETER_HEADER=96
        AND VP.DESCRIPTION ='MULTI_VAT_CODE'
        AND IS_DEFAULT =1
        AND VAT::VARCHAR=STR_VAL_1::VARCHAR;
    END IF;
    w_log_text := w_log_text || ' | vat_id=' || coalesce(W_VAT_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);


    ---
	w_log_text := 'ESTRAGGO REPARTO CASSA | shop_item_category_pc=' || coalesce(p_shop_item_category_pc::text,'NULL');
    ---
    SELECT VP.STR_VAL_1,
           (select ID from tpa_specific_features tsf where specific_feature ='REP' ) ,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature ='REP' )
           AND FEATURES_VALUE=p_shop_item_category_pc::VARCHAR)
    INTO W_CASH_DEP,W_CASH_DEP_FEATURE_ID,W_CASH_DEP_DESCRIPTION
    FROM VPA_PARAMETERS VP,TMD_STRUCTURES TS
    WHERE PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='CASH_DEP'
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | cash_dep=' || coalesce(W_CASH_DEP,'') || ' feature_id=' || coalesce(W_CASH_DEP_FEATURE_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO FLAG CONSIGLIATO';
    ---
    SELECT VP.str_val_1,tsf.id,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature =VP.DESCRIPTION )
           AND FEATURES_VALUE=VP.STR_VAL_1)
    into W_CONSI,W_CONSI_ID,W_CONSI_DESCRIPTION
    FROM tpa_specific_features tsf ,VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='CONSI'
    and VP.description = TSF.specific_feature
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | consi=' || coalesce(W_CONSI,'') || ' feature_id=' || coalesce(W_CONSI_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO MAGAZZINO/RIF';
    ---
    SELECT VP.str_val_1 ,tsf.id,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature =VP.DESCRIPTION )
           AND FEATURES_VALUE=VP.STR_VAL_1)
    into W_MAG_RIF,W_MAG_RIF_ID,W_MAG_RIF_DESCRIPTION
    FROM tpa_specific_features tsf ,VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='MAG_RIF'
    and VP.description = TSF.specific_feature
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | mag_rif=' || coalesce(W_MAG_RIF,'') || ' feature_id=' || coalesce(W_MAG_RIF_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO FLAG RICHIEDI PRZ';
    ---
    SELECT VP.str_val_1 ,tsf.id,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature =VP.DESCRIPTION )
           AND FEATURES_VALUE=VP.STR_VAL_1)
    into W_F_PRZ,W_F_PRZ_ID,W_F_PRZ_ID_DESCRIPTION
    FROM tpa_specific_features tsf ,VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='F_PRZ'
    and VP.description = TSF.specific_feature
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | f_prz=' || coalesce(W_F_PRZ,'') || ' feature_id=' || coalesce(W_F_PRZ_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO STAGIONALITA';
    ---
    SELECT VP.str_val_1 ,tsf.id,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature =VP.DESCRIPTION )
           AND FEATURES_VALUE=VP.STR_VAL_1)
    into W_STAG,W_STAG_ID,W_STAG_DESCRIPTION
    FROM tpa_specific_features tsf ,VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='STAG'
    and VP.description = TSF.specific_feature
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | stag=' || coalesce(W_STAG,'') || ' feature_id=' || coalesce(W_STAG_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO FLAG VERIFICA PESO CASSA';
    ---
    SELECT VP.str_val_1 ,tsf.id,
           (select str_val
           from tpa_features_models tfm
           where specific_feature_id =(select ID from tpa_specific_features tsf where specific_feature =VP.DESCRIPTION )
           AND FEATURES_VALUE=VP.STR_VAL_1)
    into W_F_V_PESO_C,W_F_V_PESO_C_ID,W_F_V_PESO_C_DESCRIPTION
    FROM tpa_specific_features tsf ,VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND VP.DESCRIPTION ='F_V_PESO_C'
    and VP.description = TSF.specific_feature
    AND IS_DEFAULT =1;
    w_log_text := w_log_text || ' | f_v_peso_c=' || coalesce(W_F_V_PESO_C,'') || ' feature_id=' || coalesce(W_F_V_PESO_C_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	---
    w_log_text := 'ESTRAGGO ACCORDO OPERATIVO';
    ---
    SELECT MAX(TOA.ID)
    INTO W_OPERATIONAL_AGREEMENT_ID
    FROM TMD_OPERATIONAL_AGREEMENTS TOA  ,VPA_PARAMETERS VP
    WHERE OPERATIONAL_AGREEMENT  =STR_VAL_1
    AND PARAMETER_HEADER=96
    AND IS_DEFAULT =1
    AND VP.DESCRIPTION ='OP_AGREEMENT';
    w_log_text := w_log_text || ' | operational_agreement_id=' || coalesce(W_OPERATIONAL_AGREEMENT_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
	-------------------
	    w_log_text := 'ESTRAGGO BRAND';
    ---
    SELECT (SELECT PARAMETER_CODE FROM VPA_PARAMETERS VP2 WHERE PARAMETER_HEADER=12 AND IS_DEFAULT=1 AND STR_VAL_1=VP.STR_VAL_1)
    INTO W_BRAND_PC
    FROM VPA_PARAMETERS VP
    WHERE 1=1
    AND PARAMETER_HEADER=96
    AND IS_DEFAULT =1
    AND DESCRIPTION ='BRAND';
    w_log_text := w_log_text || ' | brand_pc=' || coalesce(W_BRAND_PC::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	----
	--INIZIO LOGICA DI DUPLICAZIONE--
	----
    w_log_text := 'INSERISCO RIGA TMD_ITEMS | da_item_id=' || W_ITEM_PADRE_ID || ' codice=' || coalesce(W_ITEM_CODE,'');
	---
    INSERT INTO boom.tmd_items (item, item_type_ph, item_type_pc, item_category_ph, item_category_pc, stock_unit_ph, stock_unit_pc, collection_ph, collection_pc,  brand_pc, supplier_expiry_days, depot_expiry_days, shop_expiry_days, consumer_expiry_days, purchase_vat_id, sale_vat_id, insert_type_ph, insert_type_pc, is_local, is_updated,  parent_item_id, under_brand_ph, under_brand_pc, is_multi_vat,last_user,transaction_code)
    SELECT W_ITEM_CODE, item_type_ph, p_item_type_pc, item_category_ph, item_category_pc, stock_unit_ph, p_stock_unit_pc, collection_ph, collection_pc,  w_brand_pc, supplier_expiry_days, depot_expiry_days, shop_expiry_days, consumer_expiry_days, p_sale_vat_id, p_sale_vat_id, insert_type_ph, insert_type_pc, is_local, 1,  parent_item_id, under_brand_ph, under_brand_pc, p_is_multi_vat,P_USER,p_transaction
    FROM tmd_items
    WHERE id = W_ITEM_PADRE_ID
    returning ID,stock_unit_pc into W_ITEM_FIGLIO_ID,W_STOCK_UNIT_PC;
    w_log_text := w_log_text || ' | nuovo_item_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' stock_unit_pc=' || coalesce(W_STOCK_UNIT_PC::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    ---
    w_log_text := 'INSERISCO DESCRIZIONE ARTICOLO | item_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' da_padre=' || W_ITEM_PADRE_ID;
    ---
    INSERT INTO boom.ttr_items (item_id, language_id, short_description, description,last_user,transaction_code)
    SELECT W_ITEM_FIGLIO_ID, language_id, p_item_description, p_item_description, P_USER,P_TRANSACTION
    FROM ttr_items
    WHERE item_id = W_ITEM_PADRE_ID;
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    ---
    w_log_text := 'ESTRAGGO UNITA DI MISURA PREZZO | stock_unit_pc=' || coalesce(W_STOCK_UNIT_PC::text,'NULL');
    ---
    SELECT PARAMETER_CODE
    INTO W_PRICE_UNIT
    FROM VPA_PARAMETERS VP
    WHERE PARAMETER_HEADER =31
    AND IS_DEFAULT =1
    AND STR_VAL_1 = (SELECT DESCRIPTION
                     FROM VPA_PARAMETERS VP
                     WHERE PARAMETER_HEADER =10
                     AND IS_DEFAULT =1
                     AND PARAMETER_CODE =W_STOCK_UNIT_PC);
    w_log_text := w_log_text || ' | price_unit=' || coalesce(W_PRICE_UNIT::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
    w_log_text := 'INSERISCO VARIANTE DI VENDITA | item_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' da_padre=' || W_ITEM_PADRE_ID;
    ---
    INSERT INTO boom.tmd_item_sales (
        item_id,
        item_sale,
        val_weight,
        weight_unit_measure_pc,
        pieces,
        is_updated,
        last_user,
        transaction_code
    )
    SELECT
        W_ITEM_FIGLIO_ID,
        item_sale,
        p_is_val_weight,
        p_is_weight_unit_measure_pc,
        pieces,
        1,
        P_USER,
        p_transaction
    FROM tmd_item_sales
    WHERE item_id = W_ITEM_PADRE_ID
    returning ID into W_ITEM_SALE_ID;
    w_log_text := w_log_text || ' | item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

    ---
    w_log_text := 'INSERISCO DESCRIZIONE VARIANTE VENDITA | item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
    ---
    INSERT INTO boom.ttr_item_sales (
        item_sale_id,
        language_id,
        description,
        last_user,
        transaction_code
    )
    SELECT
        W_ITEM_SALE_ID,
        language_id,
        p_item_description,
        P_USER,
        p_transaction
    FROM ttr_item_sales TS
    WHERE item_sale_id in (select id from tmd_item_sales where item_id=W_ITEM_PADRE_ID);
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	--CONTEGGIO RECORD SE ESISTENTI FEATURES
	select count (*)
    into w_count_feature_item_links
	FROM tmd_feature_item_links
    WHERE item_id = W_ITEM_PADRE_ID
	 AND specific_feature_id<>(select id from tpa_specific_features tsf where specific_feature ='MAG_RIF');

    w_log_text := 'INSERISCO FEATURES | item_padre_id=' || W_ITEM_PADRE_ID || ' count_feature_links=' || coalesce(w_count_feature_item_links::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	IF (w_count_feature_item_links IS NOT NULL AND w_count_feature_item_links <> 0) THEN

		w_log_text := 'INSERISCO FEATURES - RECORD ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	    INSERT INTO boom.tmd_feature_item_links (
	        specific_feature_id,
	        item_id,
	        features_value,
	        num_val,
	        str_val,
	        dat_val,
	        is_updated,
	        last_user,
	        transaction_code
	    )
	    SELECT
	        specific_feature_id,
	        W_ITEM_FIGLIO_ID,
	        features_value,
	        num_val,
	        str_val,
	        dat_val,
	        1,
	        P_USER,
	        p_transaction
	    FROM tmd_feature_item_links
	    WHERE item_id = W_ITEM_PADRE_ID
	    AND specific_feature_id NOT IN (select id from tpa_specific_features tsf where specific_feature IN ('MAG_RIF','REP'))
	    UNION
	    SELECT
	        (select id from tpa_specific_features tsf where specific_feature ='MAG_RIF'),
	        W_ITEM_FIGLIO_ID,
	        'R99',
	        null,
	        'MAGAZZINO RIORDINO RIFATTURAZ.',
	        null,
	        1,
	        P_USER,
	        p_transaction;


	   INSERT INTO boom.tmd_feature_item_links (
	        specific_feature_id,
	        item_id,
	        features_value,
	        num_val,
	        str_val,
	        dat_val,
	        is_updated,
	        last_user,
	        transaction_code
	    )
	    SELECT
	      (select id from tpa_specific_features tsf where specific_feature ='REP'),
	        W_ITEM_FIGLIO_ID,
	        p_shop_item_category_pc,
	        null,
	        W_CASH_DEP_DESCRIPTION,
	        null,
	        1,
	        P_USER,
	        p_transaction;

	ELSE

		w_log_text := 'INSERISCO FEATURES - RECORD NON ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' p_shop_item_category_pc=' || coalesce(p_shop_item_category_pc::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		INSERT INTO boom.tmd_feature_item_links (
	        specific_feature_id,
	        item_id,
	        features_value,
	        num_val,
	        str_val,
	        dat_val,
	        is_updated,
	        last_user,
	        transaction_code
	    ) VALUES (W_CASH_DEP_FEATURE_ID,W_ITEM_FIGLIO_ID,p_shop_item_category_pc,NULL,W_CASH_DEP_DESCRIPTION,NULL,1,p_user,p_transaction),
	             (W_CONSI_ID,W_ITEM_FIGLIO_ID,W_CONSI,NULL,W_CONSI_DESCRIPTION,NULL,1,p_user,p_transaction),
	             (W_MAG_RIF_ID,W_ITEM_FIGLIO_ID,W_MAG_RIF,NULL,W_MAG_RIF_DESCRIPTION,NULL,1,p_user,p_transaction),
	             (W_F_PRZ_ID,W_ITEM_FIGLIO_ID,W_F_PRZ,NULL,W_F_PRZ_ID_DESCRIPTION,NULL,1,p_user,p_transaction),
	             (W_STAG_ID,W_ITEM_FIGLIO_ID,W_STAG,NULL,W_STAG_DESCRIPTION,NULL,1,p_user,p_transaction),
	             (W_F_V_PESO_C_ID,W_ITEM_FIGLIO_ID,W_F_V_PESO_C,NULL,W_F_V_PESO_C_DESCRIPTION,NULL,1,p_user,p_transaction);

	END IF;


    ---
    w_log_text := 'INSERISCO VARIANTE LOGISTICA | item_padre_id=' || W_ITEM_PADRE_ID;
    ---
	--CONTEGGIO RECORD SE ESISTENTI VARIANTE LOGISTICA
	---
	select count (*) into w_count_item_logistics FROM tmd_item_logistics WHERE item_id = W_ITEM_PADRE_ID;

    w_log_text := w_log_text || ' count_item_logistics=' || coalesce(w_count_item_logistics::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	IF (w_count_item_logistics IS NOT NULL AND w_count_item_logistics <> 0) THEN

		w_log_text := 'INSERISCO VARIANTE LOGISTICA - RECORD ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

        INSERT INTO boom.tmd_item_logistics (
            item_logistic,
            item_id,
            unit_measure_pc,
            medium_weight,
            is_updated,
            last_user,
            transaction_code
        )
        SELECT
            item_logistic,
            W_ITEM_FIGLIO_ID,
            unit_measure_pc,
            medium_weight,
            1,
            P_USER,
            p_transaction
        FROM tmd_item_logistics
        WHERE item_id = W_ITEM_PADRE_ID
        RETURNING id into  w_item_logistic_id;

	ELSE

		w_log_text := 'INSERISCO VARIANTE LOGISTICA - RECORD NON ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' unit_measure=' || coalesce(W_LOGISTIC_UNIT_MEASURE::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		 INSERT INTO boom.tmd_item_logistics (
		    item_logistic,
		    item_id,
		    unit_measure_pc,
		    medium_weight,
		    is_updated,
		    last_user,
		    transaction_code
	    ) VALUES (
	        1, -- item_logistic
	        W_ITEM_FIGLIO_ID, -- item_id
	        W_LOGISTIC_UNIT_MEASURE, -- unit_measure_pc
	        1, -- medium_weight
	        0, -- is_updated
	        P_USER, -- last_user
	        P_TRANSACTION -- transaction_code
	    ) returning ID into W_ITEM_LOGISTIC_ID;

	END IF;

    ---
    w_log_text := 'INSERISCO UNITA LOGISTICA | item_logistic_id=' || coalesce(w_item_logistic_id::text,'NULL') || ' padre=' || W_ITEM_PADRE_ID;
    ---
	--CONTEGGIO RECORD SE ESISTENTI UNITA LOGISTICA
	---
	select --min(id),
count (*) into ---W_LOGISTIC_UNIT_ID,
w_count_logistic_units FROM tmd_logistic_units  WHERE item_logistic_id in (select id from tmd_item_logistics where item_id=W_ITEM_PADRE_ID);

    w_log_text := w_log_text || ' count_logistic_units=' || coalesce(w_count_logistic_units::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	IF (w_count_logistic_units IS NOT NULL AND w_count_logistic_units <> 0) THEN

		w_log_text := 'INSERISCO UNITA LOGISTICA - RECORD ESISTENTE | item_logistic_id=' || coalesce(w_item_logistic_id::text,'NULL') || ' coefficient=' || coalesce(p_coefficient::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	    INSERT INTO boom.tmd_logistic_units (
	        item_logistic_id,
	        logistic_unit_pc,
	        weight_unit_measure_pc,
	        val_height,
	        val_length,
	        val_width,
	        val_volume,
	        val_weight,
	        pieces,
	        packs,
	        coefficient,
	        is_updated,
	        last_user,
	        transaction_code
	    )
	    SELECT
	        w_item_logistic_id,
	        logistic_unit_pc,
	        p_is_weight_unit_measure_pc,
	        val_height,
	        val_length,
	        val_width,
	        val_volume,
	        p_is_val_weight,
	        pieces,
	        packs,
	        p_coefficient,
	        1,
	        P_USER,
	        p_transaction
	    FROM tmd_logistic_units TS
	    WHERE item_logistic_id in (select id from tmd_item_logistics where item_id=W_ITEM_PADRE_ID);

 	ELSE


		w_log_text := 'INSERISCO UNITA LOGISTICA - RECORD NON ESISTENTE | item_logistic_id=' || coalesce(W_ITEM_LOGISTIC_ID::text,'NULL') || ' coefficient=' || coalesce(p_coefficient::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		INSERT INTO boom.tmd_logistic_units (
	        item_logistic_id,
	        logistic_unit_pc,
	        weight_unit_measure_pc,
	        val_height,
	        val_length,
	        val_width,
	        val_volume,
	        val_weight,
	        pieces,
	        packs,
	        coefficient,
	        is_updated,
	        last_user,
	        transaction_code
	    ) VALUES (
	        W_ITEM_LOGISTIC_ID, -- item_logistic_id
	        1, -- logistic_unit_pc
	        p_is_weight_unit_measure_pc, -- weight_unit_measure_pc
	        NULL, -- val_height
	        NULL, -- val_length
	        NULL, -- val_width
	        0.000, -- val_volume
	        p_is_val_weight, -- val_weight
	        1, -- pieces
	        1, -- packs
	        p_coefficient, -- coefficient
	        1, -- is_updated
	        P_USER, -- last_user
	        P_TRANSACTION -- transaction_code
	    ) returning ID into W_LOGISTIC_UNIT_ID;

	    -------

	    INSERT INTO boom.tmd_logistic_units (
	        item_logistic_id,
	        logistic_unit_pc,
	        weight_unit_measure_pc,
	        val_height,
	        val_length,
	        val_width,
	        val_volume,
	        val_weight,
	        pieces,
	        packs,
	        coefficient,
	        is_updated,
	        last_user,
	        transaction_code
	    ) VALUES (
	        W_ITEM_LOGISTIC_ID, -- item_logistic_id
	        41, -- logistic_unit_pc
	        p_is_weight_unit_measure_pc, -- weight_unit_measure_pc
	        NULL, -- val_height
	        NULL, -- val_length
	        NULL, -- val_width
	        0.000, -- val_volume
	        p_is_val_weight, -- val_weight
	        1, -- pieces
	        1, -- packs
	        p_coefficient, -- coefficient
	        1, -- is_updated
	        P_USER, -- last_user
	        P_TRANSACTION -- transaction_code
	    ) returning ID into W_LOGISTIC_UNIT_ID;

	END IF;

    ---
    w_log_text := 'INSERISCO LEGAME STRUTTURA ECR | structure_id=' || coalesce(p_structure_id::text,'NULL') || ' item_padre=' || W_ITEM_PADRE_ID;
    ---
	--CONTEGGIO RECORD SE ESISTENTI LEGAME STRUTTURA ECR
	---
	select count (*) into w_count_structure_item_links_var FROM tmd_structure_item_links_var tslv,tmd_structures ts,tmd_merchandise_structures tms
	WHERE item_id = W_ITEM_PADRE_ID
    and current_Date between start_date AND end_date
    and tslv.structure_id =ts.id
    and ts.merchandise_structure_id = tms.id
    and tms.is_default =1;

    w_log_text := w_log_text || ' count_structure_links=' || coalesce(w_count_structure_item_links_var::text,'NULL');
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	IF (w_count_structure_item_links_var IS NOT NULL AND w_count_structure_item_links_var <> 0) THEN

		w_log_text := 'INSERISCO LEGAME STRUTTURA ECR - RECORD ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' structure_id=' || coalesce(p_structure_id::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	    INSERT INTO boom.tmd_structure_item_links_var (
	        structure_id,
	        item_id,
	        start_date,
	        end_date,
	        is_updated,
	        last_user,
	        transaction_code
	    )
	    SELECT
	        p_structure_id,
	        W_ITEM_FIGLIO_ID,
	        greatest(current_date,start_date),
	        end_date,
	        1,
	        P_USER,
	        p_transaction
	    FROM tmd_structure_item_links_var tslv,tmd_structures ts,tmd_merchandise_structures tms
	    WHERE item_id = W_ITEM_PADRE_ID
	    and current_Date between start_date AND end_date
	    and tslv.structure_id =ts.id
	    and ts.merchandise_structure_id = tms.id
	    and tms.is_default =1;

	ELSE

		w_log_text := 'INSERISCO LEGAME STRUTTURA ECR - RECORD NON ESISTENTE | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' structure_id=' || coalesce(p_structure_id::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		INSERT INTO boom.tmd_structure_item_links_var (
	        structure_id,
	        item_id,
	        start_date,
	        end_date,
	        is_updated,
	        last_user,
	        transaction_code
	    ) VALUES (
	        p_structure_id, -- structure_id
	        W_ITEM_FIGLIO_ID, -- item_id
	        CURRENT_DATE, -- start_date
	        TO_DATE('31/12/2099','DD/MM/YYYY'), -- end_date
	        1, -- is_updated
	        P_USER, -- last_user
	        P_TRANSACTION -- transaction_code
	    );

	END IF;

    ----
    w_log_text := 'SINCRONIZZO TMD_STRUCTURE_ITEM_LINKS | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL');
    INSERT INTO TMD_STRUCTURE_ITEM_LINKS
        (ID, STRUCTURE_ID, ITEM_ID, START_DATE, END_DATE, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
    SELECT ID, STRUCTURE_ID, ITEM_ID, START_DATE, END_DATE, CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE
      FROM TMD_STRUCTURE_ITEM_LINKS_VAR
     WHERE CURRENT_DATE BETWEEN START_DATE AND END_DATE
       AND ITEM_ID=W_ITEM_FIGLIO_ID;
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	 w_log_text := 'INSERISCO BARCODE RIORDINO (VAR) | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
    ---
    insert into tmd_sale_codes_var(item_sale_id,code_type_pc,sale_code,start_date,end_date,is_label,is_updated,last_user,transaction_code)
        select tis.id item_sale_id,
               12 code_type_pc,
               '7999'||substr(lpad(replace(ti.item collate case_like,'-',''),8,'0'),1,8)||fn_get_check_digit('7999'||lpad(replace(ti.item collate case_like,'-',''),8,'0'))  sale_code,
               current_date start_date,
               to_date('31122099','DDMMYYYY') end_date,
               0 is_label,
               1 is_updated,
               P_USER,
               p_transaction
        from tmd_items ti
        inner join tmd_item_sales tis on ti.id = tis.item_id
        where ti.id=W_ITEM_FIGLIO_ID
        and regexp_match(item collate case_like,'[A-Za-z]') is null;  --tolgo gli articoli che creano problemi con codice alfanumerico
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---
        w_log_text := 'INSERISCO BARCODE RIORDINO (TMD_SALE_CODES) | item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
        insert into tmd_sale_codes
                    (id, item_sale_id, network_id, code_type_ph, code_type_pc, sale_code, start_date, end_date, is_label, tare, is_variable_price, plu_code, bilance_department_ph, bilance_department_pc, bilance_code, last_user, transaction_code)
        select id, item_sale_id, network_id, code_type_ph, code_type_pc, sale_code, start_date, end_date, is_label, tare, is_variable_price, plu_code, bilance_department_ph, bilance_department_pc, bilance_code, p_user, p_transaction
        from  tmd_sale_codes_var tppv
        where item_sale_id=w_item_sale_id;
        w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	-----								  -----
	---- INIZIO LOGICA LOOP PER NETWORK ID ----
	-----								  -----

    FOR w_network_child_id IN SELECT UNNEST(p_network_id) LOOP

	----
	w_log_text := '--- INIZIO LOGICA LOOP PER NETWORK ID: ' || w_network_child_id;
	w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
	----

		--CONTEGGIO RECORD SE ESISTENTI ASSORTIMENTO VENDIBILE
		--
		select count (*) into w_count_saleable_assortments FROM tmd_saleable_assortments
		WHERE item_sale_id in (select id from tmd_item_sales where item_id=W_ITEM_PADRE_ID)
		and network_id = w_network_child_id;

		w_log_text := 'ASSORTIMENTO VENDIBILE | network_id=' || w_network_child_id || ' item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL') || ' count_padre=' || coalesce(w_count_saleable_assortments::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		IF (w_count_saleable_assortments IS NOT NULL AND w_count_saleable_assortments <> 0) THEN

  			w_log_text := 'INSERISCO ASSORTIMENTO VENDIBILE - RECORD ESISTENTE | network_id=' || w_network_child_id || ' item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
  			w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		    INSERT INTO boom.tmd_saleable_assortments (
				id,
		        item_sale_id,
		        network_id,
		        status_pc,
		        last_user,
		        transaction_code
		    )
		    SELECT
				nextval('tmd_saleable_assortments_id_seq'::regclass),
		        W_ITEM_SALE_ID,
		        W_NETWORK_CHILD_ID,  -- network_id child in loop
		        status_pc,
		        P_USER,
		        p_transaction
		    FROM tmd_saleable_assortments TS
		    WHERE item_sale_id in (select id from tmd_item_sales where item_id=W_ITEM_PADRE_ID)
			and network_id = w_network_child_id;

		ELSE

			w_log_text := 'INSERISCO ASSORTIMENTO VENDIBILE - RECORD NON ESISTENTE | network_id=' || w_network_child_id || ' item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL');
			w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

			INSERT INTO boom.tmd_saleable_assortments (
				id,
		        item_sale_id,
		        network_id,
		        status_pc,
		        last_user,
		        transaction_code
		    ) VALUES (
				nextval('tmd_saleable_assortments_id_seq'::regclass),
		        W_ITEM_SALE_ID, -- item_sale_id
		        w_network_child_id, -- network_id child in loop
		        1, -- status_pc
		        p_user, -- last_user
		        p_transaction -- transaction_code
		    );

		END IF;

		--CONTEGGIO RECORD SE ESISTENTI FORNITORE CENTRALE

		select count (*) into w_count_orderable_assortments_var  FROM tmd_orderable_assortments_var toav
		WHERE item_id = W_ITEM_PADRE_ID and main_supplier = 1 and current_date between start_date and end_date and network_id = w_network_child_id;

		w_log_text := 'ASSORTIMENTO FORNITORE CENTRALE | network_id=' || w_network_child_id || ' item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' count_var=' || coalesce(w_count_orderable_assortments_var::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		IF (w_count_orderable_assortments_var IS NOT NULL AND w_count_orderable_assortments_var <> 0) THEN

			-- Generazione nuovo ID
	 		SELECT nextval('tmd_orderable_assortments_id_seq'::regclass)
		    INTO W_ORDERABLE_ASSORTMENTS_ID;

  			w_log_text := 'INSERISCO ASSORTIMENTO FORNITORE CENTRALE - RECORD ESISTENTE | orderable_id=' || coalesce(W_ORDERABLE_ASSORTMENTS_ID::text,'NULL') || ' network_id=' || w_network_child_id;
  			w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

			INSERT INTO boom.tmd_orderable_assortments_var (
					id,
		            item_id,
		            item_logistic_id,
		            logistic_unit_id,
		            operational_agreement_id,
		            network_id,
		            start_date,
		            end_date,
		            main_supplier,
		            min_order,
		            max_order,
		            multiple_reorder,
		            assortment_status_pc,
		            delivery_status_pc,
		            is_updated,
		            last_user,
		            transaction_code
		    )SELECT
					W_ORDERABLE_ASSORTMENTS_ID,
		            W_ITEM_FIGLIO_ID,
		            w_item_logistic_id,
		            (select id from tmd_logistic_units where item_logistic_id=w_item_logistic_id and logistic_unit_pc=41),
		            W_OPERATIONAL_AGREEMENT_ID,
		            w_network_child_id,
		            greatest(current_date,start_date),
		            end_date,
		            main_supplier,
		            min_order,
		            max_order,
		            multiple_reorder,
		            assortment_status_pc,
		            delivery_status_pc,
		            1,
		            P_USER,
		            p_transaction
		    FROM  tmd_orderable_assortments_var toav
			WHERE item_id = W_ITEM_PADRE_ID and current_date between start_date and end_date and network_id = w_network_child_id
					and main_supplier = 1 ;

		ELSE

			-- Generazione nuovo ID
	 		SELECT nextval('tmd_orderable_assortments_id_seq'::regclass)
		    INTO W_ORDERABLE_ASSORTMENTS_ID;

		 	w_log_text := 'INSERISCO ASSORTIMENTO FORNITORE CENTRALE - RECORD NON ESISTENTE | orderable_id=' || coalesce(W_ORDERABLE_ASSORTMENTS_ID::text,'NULL') || ' network_id=' || w_network_child_id;
		 	w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

			INSERT INTO boom.tmd_orderable_assortments_var (
					id,
			        item_id,
			        item_logistic_id,
			        logistic_unit_id,
			        operational_agreement_id,
			        network_id,
			        start_date,
			        end_date,
			        main_supplier,
			        min_order,
			        max_order,
			        multiple_reorder,
			        assortment_status_pc,
			        delivery_status_pc,
			        is_updated,
			        last_user,
			        transaction_code
			 ) VALUES (
					W_ORDERABLE_ASSORTMENTS_ID,
			        W_ITEM_FIGLIO_ID, -- item_id
			        W_ITEM_LOGISTIC_ID, -- item_logistic_id
			       (select id from tmd_logistic_units where item_logistic_id=w_item_logistic_id and logistic_unit_pc=41), ---  W_LOGISTIC_UNIT_ID, -- logistic_unit_id
			        W_OPERATIONAL_AGREEMENT_ID, -- operational_agreement_id
			        w_network_child_id, -- network_id in loop
			        CURRENT_DATE, -- start_date
			        TO_DATE('31/12/2099','DD/MM/YYYY'), -- end_date
			        1, -- main_supplier
			        1.000, -- min_order
			        999999.990, -- max_order
			        1.000, -- multiple_reorder
			        1, -- assortment_status_pc
			        1, -- delivery_status_pc
			        1, -- is_updated
			        P_USER, -- last_user
			        P_TRANSACTION -- transaction_code
		      ) ;

		END IF;

	    w_log_text := 'SINCRONIZZO TMD_ORDERABLE_ASSORTMENTS | item_figlio_id=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL') || ' orderable_id=' || coalesce(W_ORDERABLE_ASSORTMENTS_ID::text,'NULL') || ' network_id=' || w_network_child_id;
	    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
	    INSERT INTO TMD_ORDERABLE_ASSORTMENTS
		                (ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, ASSORTMENT_STATUS_PC, DELIVERY_STATUS_PH, DELIVERY_STATUS_PC,CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE)
		SELECT W_ORDERABLE_ASSORTMENTS_ID, ITEM_ID, ITEM_LOGISTIC_ID, LOGISTIC_UNIT_ID, OPERATIONAL_AGREEMENT_ID, NETWORK_ID, START_DATE, END_DATE, MAIN_SUPPLIER, MIN_ORDER, MAX_ORDER, MULTIPLE_REORDER, ASSORTMENT_STATUS_PH, ASSORTMENT_STATUS_PC, DELIVERY_STATUS_PH, DELIVERY_STATUS_PC,CREATION_DATE, UPDATE_DATE, LAST_USER, TRANSACTION_CODE
		FROM  TMD_ORDERABLE_ASSORTMENTS_VAR TPPV
	    WHERE ITEM_ID=W_ITEM_FIGLIO_ID
		 AND ID = W_ORDERABLE_ASSORTMENTS_ID
		and main_supplier = 1
	     and network_id = w_network_child_id
 	     AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;


		--CONTEGGIO RECORD SE ESISTENTI PREZZO DI ACQUISTO

		select count (*) into w_count_purchase_prices_var FROM tmd_purchase_prices_var ts
		        WHERE item_id = W_ITEM_PADRE_ID
				 --AND ID = W_ORDERABLE_ASSORTMENTS_ID
				AND network_id = w_network_child_id and  cost_type_pc = 1
               and operational_agreement_id = W_OPERATIONAL_AGREEMENT_ID
		        AND CURRENT_DATE BETWEEN START_DATE AND END_DATE;

		w_log_text := 'PREZZO DI ACQUISTO | network_id=' || w_network_child_id || ' op_agreement_id=' || coalesce(W_OPERATIONAL_AGREEMENT_ID::text,'NULL') || ' count_var=' || coalesce(w_count_purchase_prices_var::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		IF (w_count_purchase_prices_var IS NOT NULL AND w_count_purchase_prices_var <> 0) THEN

			-- Generazione nuovo ID
			SELECT nextval('tmd_purchase_prices_id_seq'::regclass)
	    	INTO W_PURCHASE_PRICES_ID;

	  		w_log_text := 'INSERISCO PREZZO DI ACQUISTO - RECORD ESISTENTE | purchase_price_id=' || coalesce(W_PURCHASE_PRICES_ID::text,'NULL') || ' network_id=' || w_network_child_id || ' cost=' || coalesce(p_cost::text,'NULL');
	  		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		    INSERT INTO boom.tmd_purchase_prices_var (id, item_id, item_logistic_id, operational_agreement_id, network_id,
													price, unit_price_ph, unit_price_pc, vat_id, start_date, end_date,
													insert_type_ph, insert_type_pc, cost_type_ph, cost_type_pc, promo_code,
													 is_updated,last_user,transaction_code
			)SELECT
					W_PURCHASE_PRICES_ID,
		            W_ITEM_FIGLIO_ID,
		            w_item_logistic_id,
		            operational_agreement_id,
  					network_id,
			        p_cost,
					31,
			        p_unit_price_pc,
			        p_sale_vat_id,
		            greatest(current_date,start_date),
		            end_date,
		            insert_type_ph,
		            insert_type_pc,
		            cost_type_ph,
		            cost_type_pc,
		            promo_code,
		            1,
		            P_USER,
		            p_transaction
		     FROM tmd_purchase_prices_var ts
		     WHERE item_id = W_ITEM_PADRE_ID
		     --AND ID = W_ORDERABLE_ASSORTMENTS_ID
			 AND network_id = w_network_child_id and cost_type_pc =1 
		     AND CURRENT_DATE BETWEEN START_DATE AND END_DATE
			 and operational_agreement_id = W_OPERATIONAL_AGREEMENT_ID;


		ELSE

			-- Generazione nuovo ID
			SELECT nextval('tmd_purchase_prices_id_seq'::regclass)
	    	INTO W_PURCHASE_PRICES_ID;

   			w_log_text := 'INSERISCO PREZZO DI ACQUISTO - RECORD NON ESISTENTE | purchase_price_id=' || coalesce(W_PURCHASE_PRICES_ID::text,'NULL') || ' network_id=' || w_network_child_id || ' cost=' || coalesce(p_cost::text,'NULL');
   			w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		    INSERT INTO boom.tmd_purchase_prices_var (id, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_ph,
													 unit_price_pc, vat_id, start_date, end_date, cost_type_pc, is_updated,last_user,transaction_code
			) SELECT
					W_PURCHASE_PRICES_ID,
			        W_ITEM_FIGLIO_ID,
			        (select id from tmd_item_logistics where item_id=W_ITEM_FIGLIO_ID and item_logistic=(select item_logistic from tmd_item_logistics where id=ts.item_logistic_id)),
			        operational_agreement_id,
			        network_id,
			        p_cost,
					31,
			        p_unit_price_pc,
			        p_sale_vat_id,
			        current_date,
			        to_date('31/12/2099','DD/MM/YYYY'),
			        1,
		            1,
		            P_USER,
		            p_transaction
			FROM tmd_orderable_assortments_var ts
		    WHERE item_id = W_ITEM_FIGLIO_ID
	         	AND network_id = w_network_child_id
			     and operational_agreement_id = W_OPERATIONAL_AGREEMENT_ID
				AND ID = W_ORDERABLE_ASSORTMENTS_ID
				AND CURRENT_DATE BETWEEN START_DATE AND END_DATE ;

		END IF;

		    w_log_text := 'SINCRONIZZO TMD_PURCHASE_PRICES | purchase_price_id=' || coalesce(W_PURCHASE_PRICES_ID::text,'NULL') || ' network_id=' || w_network_child_id;
		    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
		    INSERT INTO tmd_purchase_prices
		                (id, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_ph, unit_price_pc, vat_id, start_date, end_date, insert_type_ph, insert_type_pc, cost_type_ph, cost_type_pc, promo_code, creation_date, update_date, last_user, transaction_code)
		    SELECT W_PURCHASE_PRICES_ID, item_id, item_logistic_id, operational_agreement_id, network_id, price, unit_price_ph, unit_price_pc, vat_id, start_date, end_date, insert_type_ph, insert_type_pc, cost_type_ph, cost_type_pc, promo_code, creation_date, update_date, last_user, transaction_code
		    FROM  tmd_purchase_prices_var tspv
		    WHERE CURRENT_DATE BETWEEN START_DATE AND END_DATE
		      AND network_id = w_network_child_id
               and operational_agreement_id = W_OPERATIONAL_AGREEMENT_ID
		      AND item_id  = W_ITEM_FIGLIO_ID;
			 --AND ID = W_ORDERABLE_ASSORTMENTS_ID;

		--CONTEGGIO RECORD SE ESISTENTI PREZZO DI VENDITA

		SELECT count (*) INTO w_count_sale_prices_var FROM tmd_sale_prices_var
		       WHERE item_sale_id IN (select id from tmd_item_sales where item_id=W_ITEM_PADRE_ID)
		         AND network_id = w_network_child_id and sale_price_type_pc = 1 
                 AND current_date BETWEEN start_date AND end_date;

		w_log_text := 'PREZZO DI VENDITA | network_id=' || w_network_child_id || ' item_sale_id=' || coalesce(W_ITEM_SALE_ID::text,'NULL') || ' count_var=' || coalesce(w_count_sale_prices_var::text,'NULL');
		w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		IF (w_count_sale_prices_var IS NOT NULL AND  w_count_sale_prices_var <> 0) THEN

				w_log_text := 'INSERISCO PREZZO DI VENDITA - RECORD ESISTENTE | network_id=' || w_network_child_id || ' price=' || coalesce(p_price::text,'NULL');

				-- Generazione nuovo ID
				SELECT nextval('tmd_sale_prices_var_id_seq'::regclass)
				INTO W_SALE_PRICES_VAR_ID;
				w_log_text := w_log_text || ' sale_price_var_id=' || coalesce(W_SALE_PRICES_VAR_ID::text,'NULL');
				w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

		    INSERT INTO boom.tmd_sale_prices_var (id, item_sale_id, network_id, price, base_price,
													unit_price_pc, sale_price_type_pc,  vat_id, start_date, end_date, is_sent,
													last_user,transaction_code
			)SELECT
					W_SALE_PRICES_VAR_ID,
		            W_ITEM_SALE_ID,
		            w_network_child_id,
		            p_price,
		            0,
		            p_unit_price_pc,
		            1,
		            p_sale_vat_id,
		            current_date,
		            to_date('31/12/2099','DD/MM/YYYY'),
		            0,
		            P_USER,
		            p_transaction
		        FROM  tmd_sale_prices_var WHERE item_sale_id in (select id from tmd_item_sales where item_id=W_ITEM_PADRE_ID)
		         and network_id = w_network_child_id and sale_price_type_pc = 1 
                 and current_date between start_date and end_date;

		ELSE

				-- Generazione nuovo ID
				SELECT nextval('tmd_sale_prices_var_id_seq'::regclass)
		    	INTO W_SALE_PRICES_VAR_ID;

				w_log_text := 'INSERISCO PREZZO DI VENDITA - RECORD NON ESISTENTE | sale_price_var_id=' || coalesce(W_SALE_PRICES_VAR_ID::text,'NULL') || ' network_id=' || w_network_child_id || ' price=' || coalesce(p_price::text,'NULL');
				w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);

	 			INSERT INTO boom.tmd_sale_prices_var (id, item_sale_id, network_id, price, base_price,  unit_price_pc,  sale_price_type_pc,
													vat_id, start_date, end_date, is_sent,last_user,transaction_code)
				SELECT
					W_SALE_PRICES_VAR_ID,
		            W_ITEM_SALE_ID,
		            w_network_child_id,
		            p_price,
		            0,
		            p_unit_price_pc,
		            1,
		            p_sale_vat_id, ------ (SELECT SALE_VAT_ID FROM TMD_ITEMS WHERE ID=W_ITEM_FIGLIO_ID),
		            current_date,
		            to_date('31/12/2099','DD/MM/YYYY'),
		            0,
		            P_USER,
		            p_transaction
		        FROM tmd_orderable_assortments_var toav,tmd_item_sales ts
		        WHERE toav.item_id = W_ITEM_FIGLIO_ID
		        and toav.item_id=ts.item_id
		        and current_date between start_date and end_date
                and network_id = w_network_child_id
		        and main_supplier=1
				AND toav.ID = W_ORDERABLE_ASSORTMENTS_ID
		        limit 1;

		END IF;

		    w_log_text := 'SINCRONIZZO TMD_SALE_PRICES | sale_price_var_id=' || coalesce(W_SALE_PRICES_VAR_ID::text,'NULL') || ' network_id=' || w_network_child_id;
		    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
		    INSERT INTO tmd_sale_prices
		                (id,item_sale_id,network_id,price,base_price,unit_price_ph,unit_price_pc,sale_price_type_ph,sale_price_type_pc,promo_code,vat_id,start_date,end_date,is_sent,insert_type_ph,insert_type_pc,creation_date,update_date,last_user,transaction_code)
		    SELECT W_SALE_PRICES_VAR_ID,item_sale_id,network_id,price,base_price,unit_price_ph,unit_price_pc,sale_price_type_ph,sale_price_type_pc,promo_code,vat_id,start_date,end_date,is_sent,insert_type_ph,insert_type_pc,creation_date,update_date,last_user,transaction_code
		    	FROM tmd_sale_prices_var tspv
		    	WHERE item_sale_id IN (select id from tmd_item_sales where item_id=W_ITEM_FIGLIO_ID)
		    	 AND network_id = w_network_child_id
		    	 AND current_date BETWEEN start_date AND end_date
				 AND ID = W_SALE_PRICES_VAR_ID;

	END LOOP;
	-----
	-- FINE LOGICA LOOP PER NETWORK ID
	---

    ---
    w_log_text := '--- FINE PROCEDURA E COMMIT --- | item_figlio_id_restituito=' || coalesce(W_ITEM_FIGLIO_ID::text,'NULL');
    ---
    w_log_return := fn_log('INFO', w_process_name, w_log_text, 0);
    ---

    IF w_log_return <> 0 THEN
        w_log_text:= w_log_text||' - LOG_FUNCTION IN ERRORE ' || sqlstate;
        RAISE NOTICE  USING MESSAGE = w_log_text;
    END IF;

    refresh materialized view boom.vmd_items_structures_features;

    return W_ITEM_FIGLIO_ID;
   -- COMMIT;
EXCEPTION
    WHEN OTHERS then
        --rollback;
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
        return -1;
END ;
$function$
;
