
create table save20250917_item_barcode as 
select (select item from tmd_items where id in (select item_id from tmd_item_sales where id = item_sale_id)) item , 
 (select item_id from tmd_item_sales where id = item_sale_id)  item_id ,
item_sale_id, network_id,   code_type_ph, 
code_type_pc , sale_code , is_label , plu_code , bilance_department_ph, bilance_department_pc, bilance_code
from tmd_sale_codes tsc 
where current_date between start_date and end_date 
order by item

export--- da dbeaver 
import ---- da dbeaver 

/********************************************** aggiorna is label tmd_sale_codes_var *******************************************/
/********************************************** aggiorna is label tmd_sale_codes_var *******************************************/

DO $do$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT ts.id AS ts_id,
               sib.is_label AS new_label
        FROM tmd_sale_codes_var ts
        JOIN tmd_item_sales tis ON tis.id = ts.item_sale_id
        JOIN tmd_items ti ON ti.id = tis.item_id
        JOIN boom.save20250917_item_barcode sib
             ON sib.item = ti.item
            AND sib.sale_code = ts.sale_code
            AND sib.code_type_pc = ts.code_type_pc
        WHERE CURRENT_DATE BETWEEN ts.start_date AND ts.end_date
          AND ts.is_label <> sib.is_label
    LOOP
        UPDATE tmd_sale_codes_var
        SET is_label = rec.new_label,
            is_updated = 1
        WHERE id = rec.ts_id;

        RAISE NOTICE 'Aggiornato id=% con nuovo is_label=%', rec.ts_id, rec.new_label;
    END LOOP;
END;
$do$;


/********************************************** aggiorna is label tmd_sale_codes *******************************************/
/********************************************** aggiorna is label tmd_sale_codes *******************************************/


DO $do$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT ts.id AS ts_id,
               sib.is_label AS new_label
        FROM tmd_sale_codes ts
        JOIN tmd_item_sales tis ON tis.id = ts.item_sale_id
        JOIN tmd_items ti ON ti.id = tis.item_id
        JOIN boom.save20250917_item_barcode sib
             ON sib.item = ti.item
            AND sib.sale_code = ts.sale_code
            AND sib.code_type_pc = ts.code_type_pc
        WHERE CURRENT_DATE BETWEEN ts.start_date AND ts.end_date
          AND ts.is_label <> sib.is_label
    LOOP
        UPDATE tmd_sale_codes
        SET is_label = rec.new_label
        WHERE id = rec.ts_id;

        RAISE NOTICE 'Aggiornato id=% con nuovo is_label=%', rec.ts_id, rec.new_label;
    END LOOP;
END;
$do$;


commit;


commit;


/********************************************** aggiorna network tmd_sale_codes *******************************************/
/********************************************** aggiorna network tmd_sale_codes *******************************************/


DO $do$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT ts.id          AS ts_id,
               sib.network_id AS new_network_id
        FROM (
            SELECT 
                (SELECT item 
                   FROM tmd_items 
                  WHERE id IN (
                        SELECT item_id 
                        FROM tmd_item_sales 
                        WHERE id = ts.item_sale_id)) AS ts_item,
                (SELECT item_id 
                   FROM tmd_item_sales 
                  WHERE id = ts.item_sale_id) AS ts_item_id,
                ts.id, 
                ts.item_sale_id, 
                ts.network_id, 
                ts.code_type_ph, 
                ts.code_type_pc,
                ts.sale_code, 
                ts.is_label,
                ts.plu_code, 
                ts.bilance_department_pc, 
                ts.bilance_code
            FROM tmd_sale_codes_var ts
            WHERE CURRENT_DATE BETWEEN ts.start_date AND ts.end_date
        ) ts
        JOIN boom.save20250917_item_barcode sib
             ON sib.item = ts.ts_item
            AND sib.sale_code = ts.sale_code
            AND sib.code_type_pc = ts.code_type_pc
        WHERE COALESCE(sib.network_id,999999) <> COALESCE(ts.network_id,999999)
    LOOP
        BEGIN
            UPDATE tmd_sale_codes_var
            SET network_id = rec.new_network_id
            WHERE id = rec.ts_id;

            RAISE NOTICE 'Aggiornato id=% con nuovo network_id=%', rec.ts_id, rec.new_network_id;

        EXCEPTION WHEN unique_violation THEN
            RAISE NOTICE 'Salto aggiornamento id=%: duplicato', rec.ts_id;
        END;
    END LOOP;
END;
$do$;
