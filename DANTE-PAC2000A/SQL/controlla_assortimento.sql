create table boom.save_20250917_controlla_assortimento as
SELECT ta.operational_agreement, ta.description, ta.business_name , ti.item, item_id, item_logistic_id, logistic_unit_id, operational_agreement_id, network_id, 
start_date, end_date, main_supplier, min_order, max_order, multiple_reorder, assortment_status_ph, 
assortment_status_pc, delivery_status_ph, delivery_status_pc
FROM boom.tmd_orderable_assortments, tmd_items ti , tmd_operational_agreements ta
where current_date between  start_date and end_date
and ti.id = item_id 
and ta.id= operational_agreement_id 


-- export da  dbeaver

-- import da dbeaver 


DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT 
            tma.id AS tma_id,
            save.delivery_status_pc AS new_status
        FROM boom.tmd_orderable_assortments tma
        JOIN tmd_items ti ON ti.id = tma.item_id
        JOIN tmd_operational_agreements ta 
             ON ta.id = tma.operational_agreement_id
        JOIN save20250917_controlla_assortimento save
             ON save.item = ti.item
            AND save.operational_agreement = ta.operational_agreement
        WHERE current_date BETWEEN tma.start_date AND tma.end_date
          AND tma.main_supplier = 1
          AND tma.delivery_status_pc <> save.delivery_status_pc
        ORDER BY ti.item
    LOOP
        UPDATE boom.tmd_orderable_assortments
        SET delivery_status_pc = rec.new_status
        WHERE id = rec.tma_id;
    END LOOP;
END$$;


  