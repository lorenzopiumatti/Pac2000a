select tdio.item, file_name, description_item, network, processing_step1, processing_step2 , processing_step3, processing_step4 , error_message,
processing_item, processing_sale_code, processing_assortment, processing_puchase_price, processing_sale_price, processing_feature, 
tdio.structure_code, tdio.operational_agreement , tdio.purchase_price , tdio.sale_price,
' tmd_items + descr ' tmditems, ti.id, ti.item, boom.fn_tmd_items_getdescription(ti.id,'IT') descart,
' barcode ' barcode, tsc.sale_code ,
' struct_code ' structcode, vmd.structure_code,  vmd.description,
' assortforn ' assortforn, tas.operational_agreement_id , tma.operational_agreement , tma.description,
' purchaseprice ' purchaseprice, tpp.price ,
' salesprice ' salesprice, tsp.price
from tin_data_item_in tdio , tmd_networks tn ,  tmd_items ti , tmd_item_sales tis , 
       vmd_items_structures vmd ,   
       tmd_orderable_assortments tas ,tmd_operational_agreements tma ,tmd_purchase_prices tpp ,
       tmd_sale_prices tsp ,tmd_sale_codes tsc
where tdio.creation_date >= current_date-6
and tdio.file_name like '%PH%' collate case_like 
and tn.network_code = tdio.network
and ti.item = tdio.item
and tis.item_id = ti.id
and tis.item_sale = 1 
and tsc.item_Sale_id = tis.id 
and current_date between tsc.start_Date and tsc.end_date 
and tsc.is_label = 1
and vmd.item_id  = ti.id 
and vmd.merchandise_structure_id = 1 
and vmd.structure_level  = 4
and tas.item_id = ti.id 
and tas.network_id = tn.id
and tas.main_supplier = 1 
and current_date between tas.start_Date and tas.end_date 
and tma.id = tas.operational_agreement_id 
and tpp.item_id = ti.id 
and tpp.operational_agreement_id = tas.operational_agreement_id 
and tpp.network_id = tn.id
and current_date between tpp.start_Date and tpp.end_date 
and tsp.network_id = tn.id
and tsp.item_sale_id  =  tis.id 
and current_date between tsp.start_Date and tsp.end_date 
--and processing_step1 = 1 
--and processing_step2 = 1 
--and processing_step3 = 1 
--and processing_step4 = 0 

group by item,  file_name, description_item, network, processing_step1, processing_step2 , processing_step3, processing_step4 , error_message,
processing_item, processing_sale_code, processing_assortment, processing_puchase_price, processing_sale_price, processing_feature