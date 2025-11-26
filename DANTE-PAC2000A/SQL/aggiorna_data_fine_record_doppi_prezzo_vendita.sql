

create table save_prezzi_doppi as 
SELECT
    A.tab,
    A.item,
    A.item_sale_id,
    A.vat_id,
    A.network_id,
    A.sale_price_type_pc,
    A.promo_code,
    A.count_righe,
    -- Dettagli della riga con ID MINIMO
    A.idmin,
    T_MIN.start_date AS start_date_min,
    T_MIN.end_date AS end_date_min,
    -- Dettagli della riga con ID MASSIMO
    A.idmax,
    T_MAX.start_date AS start_date_max,
    T_MAX.end_date AS end_date_max
    -- ... altri campi che vuoi estrarre da T_MAX
FROM
    (
        -- Subquery A: Trova i gruppi e i rispettivi ID min/max
        SELECT
            'tmd_sale_prices_var' AS tab,
            min(tsc2.id) AS idmin,
            max(tsc2.id) AS idmax,
            ti.item,
            tsc2.item_sale_id,
            tsc2.vat_id,
            tsc2.network_id,
            tsc2.sale_price_type_pc,
            tsc2.promo_code,
            count(*) AS count_righe
        FROM
            tmd_sale_prices_var tsc2
        JOIN
            tmd_item_sales tis ON tis.id = tsc2.item_sale_id
        JOIN
            tmd_items ti ON ti.id = tis.item_id
        WHERE
            CURRENT_DATE BETWEEN tsc2.start_date AND tsc2.end_date
        GROUP BY
            ti.item,
            tsc2.item_sale_id,
            tsc2.vat_id,
            tsc2.network_id,
            tsc2.sale_price_type_pc,
            tsc2.promo_code
        HAVING
            count(*) =2
    ) A
JOIN
    tmd_sale_prices_var T_MIN ON T_MIN.id = A.idmin -- JOIN per i dettagli della riga minima
JOIN
    tmd_sale_prices_var T_MAX ON T_MAX.id = A.idmax; -- JOIN per i dettagli della riga massima
   
    
  UPDATE tmd_sale_prices_var T_MIN -- T_MIN è la tabella che vogliamo aggiornare (la riga con ID minimo)
SET end_date = T_MAX.start_date - INTERVAL '1' DAY -- Qui usiamo T_MAX che è unito sotto
FROM
    (
        -- Subquery_A: Identifica idmin e idmax per i gruppi con count=2
        SELECT
            min(tsc2.id) AS idmin,
            max(tsc2.id) AS idmax
        FROM
            tmd_sale_prices_var tsc2
        JOIN
            tmd_item_sales tis ON tis.id = tsc2.item_sale_id
        JOIN
            tmd_items ti ON ti.id = tis.item_id
        WHERE
            CURRENT_DATE BETWEEN tsc2.start_date AND tsc2.end_date
        GROUP BY
            ti.item,
            tsc2.item_sale_id,
            tsc2.vat_id,
            tsc2.network_id,
            tsc2.sale_price_type_pc,
            tsc2.promo_code
        HAVING
            count(*) = 2
    ) AS Gruppi_Da_Aggiornare
JOIN
    tmd_sale_prices_var T_MAX ON T_MAX.id = Gruppi_Da_Aggiornare.idmax -- T_MAX fornisce la start_date successiva
WHERE
    T_MIN.id = Gruppi_Da_Aggiornare.idmin; -- Applichiamo l'UPDATE solo alla riga T_MIN (idmin)
    
    
    select * from tmd_sale_prices_var where item_Sale_id in  ( select item_SAle_id from save_prezzi_doppi )
    order by item_sale_id , start_date
    
    DELETE FROM tmd_sale_prices tsp
WHERE (item_sale_id, network_id) IN (
    SELECT item_sale_id, network_id
    FROM (
        -- Unione dei dati da tmd_sale_prices_var (validi oggi) e tmd_sale_prices
        SELECT id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code
        FROM tmd_sale_prices_var
        WHERE CURRENT_DATE BETWEEN start_date AND end_date
                UNION ALL
                SELECT id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code
        FROM tmd_sale_prices
    ) AS t
    WHERE t.item_sale_id IN (SELECT item_sale_id FROM save_prezzi_doppi) -- FILTRO AGGIUNTO QUI
    GROUP BY id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date
    HAVING COUNT(*) = 1
);

INSERT INTO tmd_sale_prices
    (id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code)
SELECT
    id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code
FROM tmd_sale_prices_var tspv
WHERE (item_sale_id, network_id) IN (
    SELECT item_sale_id, network_id
    FROM (
        -- Unione dei dati da tmd_sale_prices_var (validi oggi) e tmd_sale_prices
        SELECT id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code
        FROM tmd_sale_prices_var
        WHERE CURRENT_DATE BETWEEN start_date AND end_date
                UNION ALL
                SELECT id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date, is_sent, insert_type_ph, insert_type_pc, creation_date, update_date, last_user, transaction_code
        FROM tmd_sale_prices
    ) AS t
    WHERE t.item_sale_id IN (SELECT item_sale_id FROM save_prezzi_doppi) -- FILTRO AGGIUNTO QUI
    GROUP BY id, item_sale_id, network_id, price, base_price, unit_price_ph, unit_price_pc, sale_price_type_ph, sale_price_type_pc, promo_code, vat_id, start_date, end_date
    HAVING COUNT(*) = 1
)
AND CURRENT_DATE BETWEEN start_date AND end_date;
    
    update  tmd_sale_prices_var  set is_sent = 0,  last_user = 'user_batch'  where id in (
select item_sale_id 
from save_prezzi_doppi )