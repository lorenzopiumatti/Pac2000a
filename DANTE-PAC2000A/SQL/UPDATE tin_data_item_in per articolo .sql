UPDATE tin_data_item_in
SET 
    processing_step3 = 0,
    processing_step4 = 0,
    error_message = NULL,
    processing_item = CASE WHEN processing_item <> 9 THEN 0 ELSE processing_item END,
    processing_sale_code = CASE WHEN processing_sale_code <> 9 THEN 0 ELSE processing_sale_code END,
    processing_assortment = CASE WHEN processing_assortment <> 9 THEN 0 ELSE processing_assortment END,
    processing_puchase_price = CASE WHEN processing_puchase_price <> 9 THEN 0 ELSE processing_puchase_price END,
    processing_sale_price = CASE WHEN processing_sale_price <> 9 THEN 0 ELSE processing_sale_price END,
    processing_feature = CASE WHEN processing_feature <> 9 THEN 0 ELSE processing_feature END
WHERE 
    creation_date >= current_Date - INTERVAL '10 day' 
    AND processing_step1 = 1 
    AND processing_step2 = 1
-------    AND processing_step3 = 5  -- Filtra solo quelli che hanno l'errore che vuoi resettare
    AND item = '1929-01';      -- Filtra solo l'articolo specifico