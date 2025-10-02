cursore 1 sulle transazioni da elaborare 
---------------------
-----------------------------------
select transaction_code,processing_item  from tin_data_item_in td 
where processing_step3=0  and processing_step2=1   and processing_step1=1  
and td.transaction_code=#TRANSACTION#  
and (processing_item=0 or processing_sale_code=0 or processing_assortment=0 or processing_puchase_price=0 or processing_sale_price=0 or processing_feature=0)  
group by transaction_code,processing_item order by processing_item,transaction_code


cursore 2 sugli articoli da elaborare, passando l'ntra del cursore sopra 

------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

select td.*,  
  coalesce(tn.id,-1,tn.id) network_id,  
  coalesce(ti.id,-1,ti.id) item_id,  
  case  
    when ti.id is not null and (coalesce(ti.item_type_pc,0) != coalesce(td.item_type_pc,0) or coalesce(ti.item_category_pc,0) != coalesce(td.item_category_pc,0) or coalesce(ti.stock_unit_pc,0) != coalesce(td.stock_unit_pc,0) or coalesce(ti.brand_pc,0) != coalesce(td.brand_pc,0) or coalesce(ti.supplier_expiry_days,0) != coalesce(td.supplier_expiry_days,0) or coalesce(ti.depot_expiry_days,0) != coalesce(td.depot_expiry_days,0) or coalesce(ti.shop_expiry_days,0) != coalesce(td.shop_expiry_days,0) or coalesce(ti.consumer_expiry_days,0) != coalesce(td.consumer_expiry_days,0) or coalesce(ti.short_description,'#NULL#') != coalesce(td.description_item,'#NULL#') or coalesce(ti.description,'#NULL#') != coalesce(td.description_item,'#NULL#') or coalesce(ti.under_brand_pc,0) != coalesce(td.under_brand_pc,0) or coalesce(ti.is_multi_vat,0) != coalesce(td.is_multi_vat,0))  
      then 1  
    when ti.id is null then 1  
    else -1  
  end as "update_item",  
  coalesce(tis.id,-1,tis.id) sale_id,  
  case  
    when tis.id is not null and (coalesce(tis.val_length,0) != coalesce(td.val_length_sale,0) or coalesce(tis.val_width,0) != coalesce(td.val_length_sale,0) or coalesce(tis.val_height,0)  != coalesce(td.val_Width_Sale,0) or coalesce(tis.val_weight,0) != coalesce(td.val_Weight_Sale,0) or coalesce(tis.size_unit_measure_pc,0) != coalesce(td.size_Unit_Measure_Sale_Pc, 0) or coalesce(tis.weight_unit_measure_pc,0) != coalesce(td.weight_Unit_Measure_Sale_Pc,0) or coalesce(tis.pieces,0) != coalesce(td.pieces_sale,0) or coalesce(tis.description,'#NULL#') != coalesce(td.description_sale,'#NULL#'))  
      then 1  
    when tis.id is null then 1  
    else -1  
  end as "update_sale",  
  coalesce(til.id,-1,til.id) logistic_id,  
  case  
    when til.id is not null and (coalesce(til.unit_measure_pc,0) != coalesce(td.unit_Measure_Pc,0) or coalesce(til.medium_weight,0) != coalesce(td.medium_Weight,0))  
      then 1  
    when til.id is null then 1  
    else -1  
  end as "update_logistic",  
  coalesce(tlu1.id,-1,tlu1.id) logistic_unit_id_1,  
  case  
    when tlu1.id is not null and (coalesce(tlu1.size_unit_measure_pc,0) != coalesce(td.size_Unit_Measure_sale_Pc,0) or coalesce(tlu1.weight_unit_measure_pc,0) != coalesce(td.weight_Unit_Measure_sale_Pc,0) )  
      then 1  
    when tlu1.id is null then 1  
    else -1  
  end as "update_logistic_unit_1",  
  coalesce(tlu41.id,-1,tlu41.id) logistic_unit_id_41,  
  case  
    when tlu41.id is not null and (coalesce(tlu41.size_unit_measure_pc,0) != coalesce(td.size_Unit_Measure_sale_Pc,0) or coalesce(tlu41.weight_unit_measure_pc,0) != coalesce(td.weight_Unit_Measure_sale_Pc,0) or coalesce(tlu41.coefficient,0) != coalesce(td.pieces_pack,0) or coalesce(tlu41.val_Height,0) != coalesce(td.val_Height_Pack,0) or coalesce(tlu41.val_Length,0) != coalesce(td.val_Length_Pack,0) or coalesce(tlu41.val_Width,0) != coalesce(td.val_Width_Pack,0))  
      then 1  
    when tlu41.id is null then 1  
    else -1  
  end as "update_logistic_unit_41",  
  coalesce(tlu61.id,-1,tlu61.id) logistic_unit_id_61,  
  case  
    when tlu61.id is not null and (coalesce(tlu61.size_unit_measure_pc,0) != coalesce(td.size_Unit_Measure_sale_Pc,0) or coalesce(tlu61.weight_unit_measure_pc,0) != coalesce(td.weight_Unit_Measure_sale_Pc,0) or coalesce(tlu61.coefficient,0) != coalesce(td.pieces_layer,0) or coalesce(tlu61.val_Height,0) != coalesce(td.val_height_layer,0) or coalesce(tlu61.val_Length,0) != coalesce(td.val_Length_layer,0) or coalesce(tlu61.val_Width,0) != coalesce(td.val_Width_layer,0))  
      then 1  
    when tlu61.id is null then 1  
    else -1  
  end as "update_logistic_unit_61",  
  coalesce(tlu81.id,-1,tlu81.id) logistic_unit_id_81,  
  case  
    when tlu81.id is not null and (coalesce(tlu81.size_unit_measure_pc,0) != coalesce(td.size_Unit_Measure_sale_Pc,0) or coalesce(tlu81.weight_unit_measure_pc,0) != coalesce(td.weight_Unit_Measure_sale_Pc,0) or coalesce(tlu81.coefficient,0) != coalesce(td.pieces_pallet,0) or coalesce(tlu81.val_Height,0) != coalesce(td.val_Height_Pack,0) or coalesce(tlu81.val_Length,0) != coalesce(td.val_Length_Pack,0) or coalesce(tlu81.val_Width,0) != coalesce(td.val_Width_Pack,0))  
      then 1  
    when tlu81.id is null then 1  
    else -1  
  end as "update_logistic_unit_81",  
  coalesce(ts.id,ts_ecr_def.id,ts.id) structure_id,  
  coalesce(tsil.id,-1,tsil.id) structure_link_id,  
  coalesce(tsff1.id,-1,tsff1.id) feature_id1,  
  coalesce(tsff2.id,-1,tsff2.id) feature_id2,  
  coalesce(tsff3.id,-1,tsff3.id) feature_id3,  
  coalesce(tsff4.id,-1,tsff4.id) feature_id4,  
  coalesce(tsff5.id,-1,tsff5.id) feature_id5,  
  coalesce(tsff6.id,-1,tsff6.id) feature_id6,  
  coalesce(tsff7.id,-1,tsff7.id) feature_id7,  
  coalesce(tsff8.id,-1,tsff8.id) feature_id8,  
  coalesce(tsff9.id,-1,tsff9.id) feature_id9,  
  coalesce(tsff10.id,-1,tsff10.id) feature_id10,  
  coalesce(tsff11.id,-1,tsff11.id) feature_id11,  
  coalesce(tsff12.id,-1,tsff12.id) feature_id12,  
  coalesce(tsff13.id,-1,tsff13.id) feature_id13,  
  coalesce(tsff14.id,-1,tsff14.id) feature_id14,  
  coalesce(tsff15.id,-1,tsff15.id) feature_id15,  
  coalesce(tfe1.id,-1,tfe1.id) attribute1_id,  
  case  
    when tfe1.id is not null and (coalesce(tfe1.features_value, '#NULL#') != coalesce(td.attribute_Value_1,'#NULL#') )  
      then 1  
    when tfe1.id is null then 1  
    else -1  
  end as "update_attribute1",  
  coalesce(tfe2.id,-1,tfe2.id) attribute2_id,  
  case  
    when tfe2.id is not null and (coalesce(tfe2.features_value, '#NULL#') != coalesce(td.attribute_Value_2,'#NULL#') )  
      then 1  
    when tfe2.id is null then 1  
    else -1  
  end as "update_attribute2",  
  coalesce(tfe3.id,-1,tfe3.id) attribute3_id,  
  case  
    when tfe3.id is not null and (coalesce(tfe3.features_value, '#NULL#') != coalesce(td.attribute_Value_3,'#NULL#') )  
      then 1  
    when tfe3.id is null then 1  
    else -1  
  end as "update_attribute3",  
  coalesce(tfe4.id,-1,tfe4.id) attribute4_id,  
  case  
    when tfe4.id is not null and (coalesce(tfe4.features_value, '#NULL#') != coalesce(td.attribute_Value_4,'#NULL#') )  
      then 1  
    when tfe4.id is null then 1  
    else -1  
  end as "update_attribute4",  
  coalesce(tfe5.id,-1,tfe5.id) attribute5_id,  
  case  
    when tfe5.id is not null and (coalesce(tfe5.features_value, '#NULL#') != coalesce(td.attribute_Value_5,'#NULL#') )  
      then 1  
    when tfe5.id is null then 1  
    else -1  
  end as "update_attribute5",  
  coalesce(tfe6.id,-1,tfe6.id) attribute6_id,  
  case  
    when tfe6.id is not null and (coalesce(tfe6.features_value, '#NULL#') != coalesce(td.attribute_Value_6,'#NULL#') )  
      then 1  
    when tfe6.id is null then 1  
    else -1  
  end as "update_attribute6",  
  coalesce(tfe7.id,-1,tfe7.id) attribute7_id,  
  case  
    when tfe7.id is not null and (coalesce(tfe7.features_value, '#NULL#') != coalesce(td.attribute_Value_7,'#NULL#') )  
      then 1  
    when tfe7.id is null then 1  
    else -1  
  end as "update_attribute7",  
  coalesce(tfe8.id,-1,tfe8.id) attribute8_id,  
  case  
    when tfe8.id is not null and (coalesce(tfe8.features_value, '#NULL#') != coalesce(td.attribute_Value_8,'#NULL#') )  
      then 1  
    when tfe8.id is null then 1  
    else -1  
  end as "update_attribute8",  
  coalesce(tfe9.id,-1,tfe9.id) attribute9_id,  
  case  
    when tfe9.id is not null and (coalesce(tfe9.features_value, '#NULL#') != coalesce(td.attribute_Value_9,'#NULL#') )  
      then 1  
    when tfe9.id is null then 1  
    else -1  
  end as "update_attribute9",  
  coalesce(tfe10.id,-1,tfe10.id) attribute10_id,  
  case  
    when tfe10.id is not null and (coalesce(tfe10.features_value, '#NULL#') != coalesce(td.attribute_Value_10,'#NULL#') )  
      then 1  
    when tfe10.id is null then 1  
    else -1  
  end as "update_attribute10",  
  coalesce(tfe11.id,-1,tfe11.id) attribute11_id,  
  case  
    when tfe11.id is not null and (coalesce(tfe11.features_value, '#NULL#') != coalesce(td.attribute_Value_11,'#NULL#') )  
      then 1  
    when tfe11.id is null then 1  
    else -1  
  end as "update_attribute11",  
  coalesce(tfe12.id,-1,tfe12.id) attribute12_id,  
  case  
    when tfe12.id is not null and (coalesce(tfe12.features_value, '#NULL#') != coalesce(td.attribute_Value_12,'#NULL#') )  
      then 1  
    when tfe12.id is null then 1  
    else -1  
  end as "update_attribute12",  
  coalesce(tfe13.id,-1,tfe13.id) attribute13_id,  
  case  
    when tfe13.id is not null and (coalesce(tfe13.features_value, '#NULL#') != coalesce(td.attribute_Value_13,'#NULL#') )  
      then 1  
    when tfe13.id is null then 1  
    else -1  
  end as "update_attribute13",  
  coalesce(tfe14.id,-1,tfe14.id) attribute14_id,  
  case  
    when tfe14.id is not null and (coalesce(tfe14.features_value, '#NULL#') != coalesce(td.attribute_Value_14,'#NULL#') )  
      then 1  
    when tfe14.id is null then 1  
    else -1  
  end as "update_attribute14",  
  coalesce(tfe15.id,-1,tfe15.id) attribute15_id,  
  case  
    when tfe15.id is not null and (coalesce(tfe15.features_value, '#NULL#') != coalesce(td.attribute_Value_15,'#NULL#') )  
      then 1  
    when tfe15.id is null then 1  
    else -1  
  end as "update_attribute15",  
  coalesce(tsil.structure_id ,-1,tsil.structure_id) current_structure_code_id ,  
  coalesce((select max(tsil2.end_Date) from tmd_structure_item_links_var tsil2 where tsil2.item_id = ti.id and tsil2.structure_id =ts.id and tsil2.start_Date <= current_date), '2001-01-01'::date) last_date_structure,  
  coalesce(ticv.id ,-1,ticv.id) item_code_id,  
  case  
    when ticv.id is not null and (coalesce(ticv.code_type_pc,0) != coalesce(td.item_code_type,0) or coalesce(ticv.item_code, '#NULL#') != coalesce(td.item_code,'#NULL#') or coalesce(ticv.end_date, '2099-12-31') != coalesce(td.end_date_item_code, '2099-12-31'))  
      then 1  
    when ticv.id is null then 1  
    else -1  
  end as "update_item_code",  
  coalesce(ticv.item_code ,'-1',ticv.item_code) current_item_code,  
  coalesce((select max(ticv2.end_Date) from tmd_item_codes_var ticv2 where ti.id = ticv.item_id and td.item_code_type = ticv.code_type_pc and ticv2.start_Date <= current_date), '2001-01-01'::date) last_date_item_code,  
  coalesce(toav.id,-1,toav.id) assortment_id1,  
  case  
    when toav.id is not null and (td.min_order<>toav.min_order or td.max_order<>toav.max_order or td.multiple_reorder<>toav.multiple_reorder or toav.assortment_status_pc<>td.assortment_status_pc or toav.delivery_status_pc<>td.delivery_status_pc or td.end_date_assortment<>toav.end_date) and td.start_date_assortment = toav.start_date  
    then 1  
    when toav.id is null then 1  
    else -1  
  end as "update_assortment",  
  coalesce((select max(toav2.end_Date) from tmd_orderable_assortments_var toav2 where toav2.item_id = ti.id and toav2.item_logistic_id = til.id and toav2.logistic_unit_id = tlu_ass.id and tn.id = toav2.network_id and toa.id = toav2.operational_agreement_id and toav2.start_Date <= current_date), '2001-01-01'::date) last_date_assortment,  
  coalesce(ttp.id,-1,ttp.id) third_party_id,  
  coalesce(toa.id,-1,toa.id) operational_agreement_id,  
  coalesce(tsic1.id,-1,tsic1.id) supplier_item_code1_id,  
  case  
    when tsic1.id is not null and (coalesce(tsic1.code_type_pc,0) != coalesce(td.supplier_code_type_1,0) or coalesce(tsic1.supplier_item_code, '#NULL#') != coalesce(td.supplier_item_1,'#NULL#') or coalesce(tsic1.description_item, '#NULL#') != coalesce(td.description_supplier_item_1,'#NULL#') or coalesce(tsic1.is_active,0) != coalesce(td.supplier_is_active_1,0))  
      then 1  
    when tsic1.id is null then 1  
    else -1  
  end as "update_supplier_item_code1",  
  coalesce(tsic2.id,-1,tsic2.id) supplier_item_code2_id,  
  case  
    when tsic2.id is not null and (coalesce(tsic2.code_type_pc,0) != coalesce(td.supplier_code_type_2,0) or coalesce(tsic2.supplier_item_code, '#NULL#') != coalesce(td.supplier_item_2,'#NULL#') or coalesce(tsic2.description_item, '#NULL#') != coalesce(td.description_supplier_item_2,'#NULL#') or coalesce(tsic2.is_active,0) != coalesce(td.supplier_is_active_2,0))  
      then 1  
    when tsic2.id is null then 1  
    else -1  
  end as "update_supplier_item_code2",  
  coalesce((select tscv3.ID from tmd_sale_codes_var tscv3 where td.sale_code != tscv3.sale_code and tscv3.item_sale_id = tis.id and td.sale_code_type_pc = tscv3.code_type_pc and td.sale_code_type_pc = 12 and tscv3.network_id IS NULL and td.START_date_sale_code <= tscv3.end_date limit 1),-1) close_barcode_id,  
  coalesce(tscv.id,'-1',tscv.id) sale_code_id,  
  case  
    when tscv.id is not null and (tscv.end_date <> td.end_date_sale_code )  
    then 1  
    when tscv.id is null then 1  
    else -1  
  end as "update_sale_code",  
  coalesce((select max(tscv4.end_Date) from tmd_sale_codes_var tscv4 where td.sale_code = tscv4.sale_code  and td.sale_code_type_pc = tscv4.code_type_pc and (tscv4.network_id IS null or tscv4.network_id = tn.id) and tscv4.start_Date <= current_date), '2001-01-01'::date) last_date_sale_code , 
  coalesce(tppv.id,-1,tppv.id) purchase_price_id,  
  coalesce(tppv.price,-1) current_purchase_price,  
  case  
    when tppv.id is not null and (tppv.price <> td.purchase_price or tppv.unit_price_pc <> td.unit_purchase_price_pc or tppv.vat_id<>tv_acq.id or tppv.end_date <> td.end_date_purchase)  
    then 1  
    when tppv.id is null then 1  
    else -1  
  end as "update_purchase_price",  
  coalesce((select max(tppv2.end_Date) from tmd_purchase_prices_var tppv2 where tppv2.item_id = ti.id and tppv2.operational_agreement_id = toa.id and tppv2.network_id = tn.id and tppv2.unit_price_pc = td.unit_purchase_price_pc and tppv2.cost_type_pc = 1 and tppv2.start_Date <= current_date ), '2001-01-01'::date) last_date_purchase_price,  
  coalesce(tspw.id,-1) sale_price_id,  
  coalesce(tspw.price,-1) current_sale_price,  
  case  
    when tspw.id is not null and (tspw.price <> td.sale_price or tspw.unit_price_pc <> td.unit_sale_price_pc or tspw.vat_id<>tv_sale.id or tspw.end_date <> td.end_date_sale)  
    then 1  
    when tspw.id is null then 1  
    else -1  
  end as "update_sale_price",  
  coalesce((select max(tspw2.end_Date) from tmd_sale_prices_var tspw2 where tis.id = tspw2.item_sale_id and tspw2.sale_price_type_pc=1 and tn.id = tspw2.network_id and tspw2.start_Date <= current_date), '2001-01-01'::date) last_date_sale_price,  
  coalesce(tv_acq.id,-1) purchase_vat_id,  
  coalesce(tv_sale.id,-1) sale_vat_id,  
coalesce((select case when tspw3.price = td.sale_price then -1 else tspw3.id end from tmd_sale_prices_var tspw3 where tis.id = tspw3.item_sale_id and tspw3.sale_price_type_pc=1 and tn.id = tspw3.network_id and (td.start_date_purchase between tspw3.start_Date and tspw3.end_date or td.end_date_purchase between tspw3.start_Date and tspw3.end_date ) and tspw3.start_Date>=current_date limit 1),-2)  sale_price_future, 
coalesce((select case when tppv3.price = td.purchase_price then -1 else tppv3.id end from tmd_purchase_prices_var tppv3 where tppv3.item_id = ti.id and tppv3.operational_agreement_id = toa.id and tppv3.network_id = tn.id and tppv3.unit_price_pc = td.unit_purchase_price_pc and tppv3.cost_type_pc = 1 and (td.start_date_purchase between tppv3.start_Date and tppv3.end_date or td.end_date_purchase between tppv3.start_Date and tppv3.end_date ) and tppv3.start_Date>=current_date limit 1),-2) purchase_price_future,  
coalesce((select tscv5.ID from tmd_sale_codes_var tscv5,vpa_parameters vp   where tscv5.sale_code = td.sale_code and tscv5.item_sale_id != tis.id and tscv5.code_type_pc = td.sale_code_type_pc and tscv5.end_date >= td.START_date_sale_code and tscv5.start_date <= current_date and vp.parameter_header=15 and vp.parameter_code =tscv5.code_type_pc and vp.num_val_4 =1 and vp.is_default =1 and exists (select 1 from vpa_parameters vp where vp.parameter_header = 86 and parameter_code =2 and num_val_1 =1 and is_default =1) limit 1),-1 ) close_sale_code_id, 
coalesce((select tscv5.start_Date from tmd_sale_codes_var tscv5,vpa_parameters vp   where tscv5.sale_code = td.sale_code and tscv5.item_sale_id != tis.id and tscv5.code_type_pc = td.sale_code_type_pc and tscv5.end_date >= td.START_date_sale_code and tscv5.start_date <= current_date and vp.parameter_header=15 and vp.parameter_code =tscv5.code_type_pc and vp.num_val_4 =1 and vp.is_default =1 and exists (select 1 from vpa_parameters vp where vp.parameter_header = 86 and parameter_code =2 and num_val_1 =1 and is_default =1) limit 1), '2001-01-01'::date) close_sale_code_date, 
coalesce(ti.purchase_vat_id,-1) master_data_purchase_vat_id, 
coalesce(ti.sale_vat_id,-1) master_data_sale_vat_id, 
case when (td.attribute_value_15='S' or cnt_lock>0) then 1 else 0 end lock_prz_std, 
case when (td.item_category_pc<>ti.item_category_pc) then 1 else 0 end upd_item_category_pc, 
case when (td.sale_vat<>tv_sale.vat) then 1 else 0 end upd_sale_vat, 
case when (td.brand_pc<>ti.brand_pc) then 1 else 0 end upd_brand_pc, 
case when ts_rep.id is null then '1' else 0 end use_default_rep, 
ts_rep_def.structure_code default_rep 
from tin_data_item_in td  
left join tmd_networks tn on tn.network_code = td.network  
left join vmd_items ti on td.item = ti.item  
left join vmd_item_sales tis on ti.id = tis.item_id and td.item_sale = tis.item_sale  
left join tmd_item_logistics til on ti.id = til.item_id and td.item_logistic = til.item_logistic  
left join tmd_logistic_units tlu1 on tlu1.item_logistic_id = til.id and tlu1.logistic_unit_pc = 1  
left join tmd_logistic_units tlu41 on tlu41.item_logistic_id = til.id and tlu41.logistic_unit_pc = 41  
left join tmd_logistic_units tlu61 on tlu61.item_logistic_id = til.id and tlu61.logistic_unit_pc = 61  
left join tmd_logistic_units tlu81 on tlu81.item_logistic_id = til.id and tlu81.logistic_unit_pc = 81  
left join tmd_logistic_units tlu_ass on tlu_ass.item_logistic_id = til.id and tlu_ass.logistic_unit_pc = td.item_logistic_type  
left join vmd_structures ts on ts.structure_code = td.structure_code  
left join LATERAL(SELECT tsil.* FROM tmd_structure_item_links_var tsil,vmd_structures ts2  where tsil.item_id = ti.id and current_date between tsil.start_Date and tsil.end_date  and ts2.id = tsil.structure_id ) tsil on 1=1 
left join tpa_specific_features tsff1 on td.attribute_code_1 = tsff1.specific_feature  
left join tpa_specific_features tsff2 on td.attribute_code_2 = tsff2.specific_feature  
left join tpa_specific_features tsff3 on td.attribute_code_3 = tsff3.specific_feature  
left join tpa_specific_features tsff4 on td.attribute_code_4 = tsff4.specific_feature  
left join tpa_specific_features tsff5 on td.attribute_code_5 = tsff5.specific_feature  
left join tpa_specific_features tsff6 on td.attribute_code_6 = tsff6.specific_feature  
left join tpa_specific_features tsff7 on td.attribute_code_7 = tsff7.specific_feature  
left join tpa_specific_features tsff8 on td.attribute_code_8 = tsff8.specific_feature  
left join tpa_specific_features tsff9 on td.attribute_code_9 = tsff9.specific_feature  
left join tpa_specific_features tsff10 on td.attribute_code_10 = tsff10.specific_feature  
left join tpa_specific_features tsff11 on td.attribute_code_11 = tsff11.specific_feature  
left join tpa_specific_features tsff12 on td.attribute_code_12 = tsff12.specific_feature  
left join tpa_specific_features tsff13 on td.attribute_code_13 = tsff13.specific_feature  
left join tpa_specific_features tsff14 on td.attribute_code_14 = tsff14.specific_feature  
left join tpa_specific_features tsff15 on td.attribute_code_15 = tsff15.specific_feature  
left join vmd_specific_feature_item_linked tfe1 on td.attribute_code_1 = tfe1.specific_feature and tfe1.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe2 on td.attribute_code_2 = tfe2.specific_feature and tfe2.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe3 on td.attribute_code_3 = tfe3.specific_feature and tfe3.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe4 on td.attribute_code_4 = tfe4.specific_feature and tfe4.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe5 on td.attribute_code_5 = tfe5.specific_feature and tfe5.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe6 on td.attribute_code_6 = tfe6.specific_feature and tfe6.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe7 on td.attribute_code_7 = tfe7.specific_feature and tfe7.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe8 on td.attribute_code_8 = tfe8.specific_feature and tfe8.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe9 on td.attribute_code_9 = tfe9.specific_feature and tfe9.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe10 on td.attribute_code_10 = tfe10.specific_feature and tfe10.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe11 on td.attribute_code_11 = tfe11.specific_feature and tfe11.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe12 on td.attribute_code_12 = tfe12.specific_feature and tfe12.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe13 on td.attribute_code_13 = tfe13.specific_feature and tfe13.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe14 on td.attribute_code_14 = tfe14.specific_feature and tfe14.item_id = ti.id  
left join vmd_specific_feature_item_linked tfe15 on td.attribute_code_15 = tfe15.specific_feature and tfe15.item_id = ti.id  
left join tmd_item_codes_var ticv on ti.id = ticv.item_id and td.item_code_type = ticv.code_type_pc and current_date between ticv.start_Date and ticv.end_date  
left join tmd_third_parties ttp on ttp.third_party = td.supplier_code  
left join tmd_operational_agreements toa on toa.third_party_id = ttp.id and toa.operational_agreement = td.operational_agreement  
left join tmd_orderable_assortments_var toav on toav.item_id = ti.id and toav.item_logistic_id = til.id and toav.logistic_unit_id = tlu_ass.id and tn.id = toav.network_id and toa.id = toav.operational_agreement_id and current_date between toav.start_Date and toav.end_date  
left join tmd_supplier_item_codes tsic1 on tsic1.item_id = ti.id and tsic1.item_logistic_id = til.id and tsic1.third_party_id = ttp.id and td.supplier_code_type_1 = tsic1.code_type_pc and td.supplier_item_1 = tsic1.supplier_item_code  
left join tmd_supplier_item_codes tsic2 on tsic2.item_id = ti.id and tsic2.item_logistic_id = til.id and tsic2.third_party_id = ttp.id and td.supplier_code_type_1 = tsic2.code_type_pc and td.supplier_item_1 = tsic2.supplier_item_code  
left join tmd_purchase_prices_var tppv on tppv.item_id = ti.id and tppv.operational_agreement_id = toa.id and tppv.network_id = tn.id and tppv.cost_type_pc = 1 and current_date between tppv.start_Date and tppv.end_date  
left join tmd_sale_codes_var tscv on td.sale_code = tscv.sale_code and tscv.item_sale_id = tis.id and td.sale_code_type_pc = tscv.code_type_pc and (tscv.network_id IS null or tscv.network_id = tn.id) and current_date between tscv.start_date and tscv.end_date  
left join tmd_sale_prices_var tspw on tis.id = tspw.item_sale_id and tn.id = tspw.network_id and current_date between tspw.start_Date and tspw.end_date and tspw.sale_price_type_pc=1  
left join tmd_vat tv_acq on tv_acq.vat = td.purchase_vat  
left join tmd_vat tv_sale on tv_sale.vat = td.sale_vat  
left join LATERAL(select count(*) cnt_lock from tpa_specific_features tsf , tmd_feature_item_links tfil  where specific_feature ='LK_STD_PR' and tfil.specific_feature_id =tsf.id  and tfil.item_id=ti.id and UPPER(tfil.features_value)='S' limit 1 ) lock_prz_std on 1=1 
left join vpa_parameter_trans vptecr on vptecr.table_field ='default_ecr' 
left join vpa_parameter_trans vptdep on vptdep.table_field ='default_dep' 
left join tmd_merchandise_structures tms_rep on tms_rep.merchandise_structure='REPARTO' 
left join tmd_structures ts_rep on ts_rep.structure_code=td.item_category_pc::varchar and  ts_rep.merchandise_structure_id =tms_rep.id 
left join tmd_structures ts_rep_def on ts_rep_def.structure_code=vptdep.boom_parameter and ts_rep_def.merchandise_structure_id =tms_rep.id 
left join vmd_structures ts_ecr_def on ts_ecr_def.structure_code=vptecr.boom_parameter 
where processing_step3 = 0  
and processing_step2 = 1  
and processing_step1 = 1  
and (processing_item = 0 or processing_sale_code = 0 or processing_assortment = 0 or processing_puchase_price = 0 or processing_sale_price = 0 or processing_feature = 0)  
and td.transaction_code=#TRANSACTION#   
order by processing_item, td.transaction_code, case when td.processing_sale_code = 0 and processing_item != 0 then td.id else 1 end desc, td.id asc  
limit 100



--------------------------------
insert prezzi vendita 	
-----------------------------------------

insert into tmd_sale_prices_var    (item_sale_id, network_id, price,  unit_price_pc, vat_id, start_date, end_date,insert_type_pc,last_user, sale_price_type_pc )   
select tis.id,tn.id,td.sale_price,td.unit_sale_price_pc::bigint,tv.id::bigint, ? ,?,1,'user_batch',1  
 from tin_data_item_in td   left join tmd_networks tn on tn.network_code = td.network   
 left  join tmd_items ti on td.item  = ti.item     
 left join tmd_item_sales tis on ti.id=tis.item_id and td.item_sale = tis.item_sale     
 left join tmd_vat tv on tv.vat=td.sale_vat   
 where processing_step3=0   and processing_step2 =1   and processing_sale_price=0    and td.id=?
 
 ---------------------------------------
 update prezzi vendita 	
 ---------------------------------------------
 
 UPDATE tmd_sale_prices_var  SET end_date = ?, vat_id=?,price=?  where id=?
 
 
 
 select vat,vat_value,law,paragraph from tmd_vat tv
 
 select * from vpa_parameter_trans
 
 select api_name,api_base_url,  api_relative_url  from tpa_api_anag 
 
 -----> richiama le varie api.
 
 http://34.79.204.143:10001/api/masterData/items - end
 - {"stockUnitPc":"1","consumerExpiryDays":0,"insertTypePh":13,"description":"PORTACANDELA ORO VETRO","insertTypePc":"","supplierExpiryDays":"99999","flagUpdated":true,"flagMultiVat":false,"shopExpiryDays":0,"stockUnitPh":10,"item":"100249-01","itemCategoryPc":"800","languageId":1,"saleVatId":"3","brandPh":12,"depotExpiryDays":0,"shortDescription":"PORTACANDELA ORO VETRO","itemCategoryPh":5,"brandPc":"1","itemTypePh":9,"collectionPh":11,"purchaseVatId":"3","itemTypePc":"2","collectionPc":"","flagLocal":false} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":75136,"item":"100249-01","itemTypePh":9,"itemTypePc":2,"itemCategoryPh":5,"itemCategoryPc":800,"stockUnitPh":10,"stockUnitPc":1,"collectionPh":11,"collectionPc":null,"brandPh":12,"brandPc":1,"subBrandPh":94,"subBrandPc":null,"supplierExpiryDays":99999,"depotExpiryDays":0,"shopExpiryDays":0,"consumerExpiryDays":0,"purchaseVatId":3,"saleVatId":3,"insertTypePh":13,"insertTypePc":null,"flagLocal":false,"flagUpdated":true,"stockUnitPieces":true,"creationDate":"2025-05-23T11:27:17.237+02:00","cloneLinkedItemData":null,"parentItemId":null,"flagMultiVat":false,"languageId":1,"description":"PORTACANDELA ORO VETRO","shortDescription":"PORTACANDELA ORO VETRO"},"success":true,"message":"Operazione completata"} - end
T - Carico dati struttura merc file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/structureItemLinksVar - end
 - {"itemId":75136,"endDate":"2099-12-31T00:00:00.000Z","structureId":4712,"startDate":"2025-05-21T00:00:00.000Z","flagUpdated":true} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":301828,"structureId":4712,"itemId":75136,"startDate":"2025-05-21T02:00:00.000+02:00","endDate":"2099-12-31T01:00:00.000+01:00","flagUpdated":true,"creationDate":"2025-05-23T11:27:18.026+02:00"},"success":true,"message":"Operazione completata"} - end
T - Carico dati unità vendita file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/itemSales - end
 - {"pieces":"1","itemId":75136,"weightUnitMeasurePc":"10","itemSale":"1","unitMeasurePh":30,"languageId":1,"description":"PORTACANDELA ORO VET","valWeight":"1.000","flagUpdated":true} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":75131,"itemId":75136,"itemSale":1,"valLength":null,"valWidth":null,"valHeight":null,"valWeight":1.000,"unitMeasurePh":30,"sizeUnitMeasurePc":null,"weightUnitMeasurePc":10,"flagUpdated":true,"pieces":1,"languageId":1,"description":"PORTACANDELA ORO VET"},"success":true,"message":"Operazione completata"} - end
T - Carico dati variante logistica file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/itemLogistics - end
 - {"itemId":75136,"unitMeasurePh":30,"unitMeasurePc":"1","itemLogistic":"1","mediumWeight":"1.000","flagUpdated":true} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":75080,"itemLogistic":1,"itemId":75136,"unitMeasurePh":30,"unitMeasurePc":1,"mediumWeight":1.000,"flagUpdated":true},"success":true,"message":"Operazione completata"} - end
T - Carico dati unità logistica pezzo file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/logisticUnits - end
 - {"logisticUnitPc":1,"itemLogisticId":75080,"weightUnitMeasurePc":"1","unitMeasurePh":30,"coefficient":1,"logisticUnitPh":17,"flagUpdated":true} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":150164,"itemLogisticId":75080,"logisticUnitPh":17,"logisticUnitPc":1,"unitMeasurePh":30,"sizeUnitMeasurePc":null,"weightUnitMeasurePc":1,"valHeight":null,"valLength":null,"valWidth":null,"valVolume":0,"valWeight":null,"pieces":1,"packs":0,"flagUpdated":true,"coefficient":1},"success":true,"message":"Operazione completata"} - end
T - Carico dati unità logistica Collo file SPH_ESSEPIU.000 articolo 100249-01 - end
http://34.79.204.143:10001/api/masterData/logisticUnits - end
{"logisticUnitPc":41,"itemLogisticId":75080,"weightUnitMeasurePc":"1","unitMeasurePh":30,"coefficient":"1","logisticUnitPh":17,"valWeight":1,"flagUpdated":true} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":150165,"itemLogisticId":75080,"logisticUnitPh":17,"logisticUnitPc":41,"unitMeasurePh":30,"sizeUnitMeasurePc":null,"weightUnitMeasurePc":1,"valHeight":null,"valLength":null,"valWidth":null,"valVolume":0,"valWeight":1.0,"pieces":1,"packs":1,"flagUpdated":true,"coefficient":1},"success":true,"message":"Operazione completata"} - end
T - Carico dati codici articolo alternativi file SPH_ESSEPIU.000 articolo 100249-01 - end
T - Carico dati attributi articolo file SPH_ESSEPIU.000 articolo 100249-01 - end
http://34.79.204.143:10001/api/masterData/featureItemLinks - end
{"itemId":75136,"featuresValue":"R99","datVal":"2025-05-23T11:27:19.707Z","flagUpdated":true,"specificFeatureId":12} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":1320008,"specificFeatureId":12,"itemId":75136,"networkId":null,"networkDescription":null,"networkCodeAndDescription":null,"featuresValue":"R99","numVal":null,"strVal":null,"datVal":"2025-05-23T13:27:19.707+02:00","flagUpdated":true},"success":true,"message":"Operazione completata"} - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"featuresValue":"20101108","datVal":"2025-05-23T11:27:19.953Z","flagUpdated":true,"specificFeatureId":1} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":1320009,"specificFeatureId":1,"itemId":75136,"networkId":null,"networkDescription":null,"networkCodeAndDescription":null,"featuresValue":"20101108","numVal":null,"strVal":null,"datVal":"2025-05-23T13:27:19.953+02:00","flagUpdated":true},"success":true,"message":"Operazione completata"} - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"networkDescription":"","featuresValue":"9","networkId":"14","datVal":"2025-05-23T11:27:20.080Z","flagUpdated":true,"specificFeatureId":2} - end
 - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
T - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
POST - Elemento non trovato: Reti/Siti, id:14 - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - end
equests - Error updating feature - end
Requests - Errore aggiornamento attributo 3  - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"networkDescription":"","featuresValue":"N","networkId":"14","datVal":"2025-05-23T11:27:20.180Z","flagUpdated":true,"specificFeatureId":3} - end
 - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
T - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
POST - Elemento non trovato: Reti/Siti, id:14 - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - end
equests - Error updating feature - end
Requests - Errore aggiornamento attributo 4  - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"networkDescription":"","featuresValue":"N","networkId":"14","datVal":"2025-05-23T11:27:20.276Z","flagUpdated":true,"specificFeatureId":11} - end
 - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
T - {"status":500,"title":"Elemento non trovato: Reti/Siti, id:14","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Reti/Siti, id:14"} - end
POST - Elemento non trovato: Reti/Siti, id:14 - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - end
equests - Error updating feature - end
Requests - Errore aggiornamento attributo 11  - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"featuresValue":"N","datVal":"2025-05-23T11:27:20.364Z","flagUpdated":true,"specificFeatureId":16} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":1320010,"specificFeatureId":16,"itemId":75136,"networkId":null,"networkDescription":null,"networkCodeAndDescription":null,"featuresValue":"N","numVal":null,"strVal":null,"datVal":"2025-05-23T13:27:20.364+02:00","flagUpdated":true},"success":true,"message":"Operazione completata"} - end
 - http://34.79.204.143:10001/api/masterData/featureItemLinks - end
 - {"itemId":75136,"featuresValue":"U1.0","datVal":"2025-05-23T11:27:20.544Z","flagUpdated":true,"specificFeatureId":49} - end
 - {"status":200,"title":"Operazione completata","payloadType":"standard","data":{"insert":false,"id":1320011,"specificFeatureId":49,"itemId":75136,"networkId":null,"networkDescription":null,"networkCodeAndDescription":null,"featuresValue":"U1.0","numVal":null,"strVal":null,"datVal":"2025-05-23T13:27:20.544+02:00","flagUpdated":true},"success":true,"message":"Operazione completata"} - end
T - Carico dati assortimento file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/orderableAssortmentsVar - end
 - {"operationalAgreementCode":"100010-01","endDate":"2099-12-31T00:00:00.000Z","logisticUnitId":150165,"deliveryStatusPh":66,"mainSupplier":true,"minOrder":"1.000","flagUpdated":true,"deliveryStatusPc":"1","itemId":75136,"itemLogisticId":75080,"thirdPartyId":-1,"operationalAgreementId":-1,"multipleReorder":"1.000","networkCode":"2198","assortmentStatusPc":"1","thirdPartyCode":"100010","networkId":"14","maxOrder":"999999.000","assortmentStatusPh":64,"startDate":"2025-05-21T00:00:00.000Z"} - end
 - {"status":500,"title":"Elemento non trovato: Accordi, id:-1","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Accordi, id:-1"} - end
T - {"status":500,"title":"Elemento non trovato: Accordi, id:-1","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Accordi, id:-1"} - end
POST - Elemento non trovato: Accordi, id:-1 - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - Errore apertura assortimento articolo 100249-01 fornitore 100010 Elemento non trovato: Accordi, id:-1 - end
equests - Error opening assortment - end
Requests - Errore apertura assortimento  - end
uests - Carico dati cod articolo fornitore file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/supplierItemCodes - end
 - {"itemId":75136,"itemLogisticId":75080,"thirdPartyId":-1,"supplierItemCode":"117410","descriptionItem":"PORTACANDELA ORO VETRO             ","codeTypePc":"1","flagActive":true,"codeTypePh":16,"flagUpdated":true} - end
 - {"status":500,"title":"Elemento non trovato: Terzi, id:-1","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Terzi, id:-1"} - end
T - {"status":500,"title":"Elemento non trovato: Terzi, id:-1","payloadType":"standard","data":{},"success":false,"message":"Elemento non trovato: Terzi, id:-1"} - end
POST - Elemento non trovato: Terzi, id:-1 - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - Errore apertura assortimento articolo 100249-01 fornitore 100010 Elemento non trovato: Accordi, id:-1 - Errore caricamento articolo fornitore1 articolo 100249-01 articolo fornitore117410 Elemento non trovato: Terzi, id:-1 - end
equests - Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - Errore apertura assortimento articolo 100249-01 fornitore 100010 Elemento non trovato: Accordi, id:-1 - Errore caricamento articolo fornitore1 articolo 100249-01 articolo fornitore117410 Elemento non trovato: Terzi, id:-1 - end
Requests - Errore caricamento articolo fornitore1  - end
uests - Carico dati listino d'acquisto file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/purchasePricesVar - end
 - {"unitPricePh":31,"operationalAgreementCode":"100010-01","endDate":"2099-12-31T00:00:00.000Z","unitPricePc":"1","insertTypePh":13,"vatId":"3","insertTypePc":"","flagCostTypePromotional":false,"flagUpdated":true,"itemId":75136,"thirdPartyId":-1,"itemLogisticId":75080,"operationalAgreementId":-1,"price":"1.29000","networkCode":"2198","thirdPartyCode":"117410","networkId":"14","promoCode":"","costTypePc":1,"startDate":"2010-11-08T00:00:00.000Z"} - end
 - {"status":500,"title":"No value present","payloadType":"standard","data":{},"success":false,"message":"No value present"} - end
T - {"status":500,"title":"No value present","payloadType":"standard","data":{},"success":false,"message":"No value present"} - end
POST - No value present - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - Errore apertura assortimento articolo 100249-01 fornitore 100010 Elemento non trovato: Accordi, id:-1 - Errore caricamento articolo fornitore1 articolo 100249-01 articolo fornitore117410 Elemento non trovato: Terzi, id:-1 - Errore apertura listino d'acquisto 100249-01 costo 1.29000 No value present - end
equests - Error opening purchase price - end
Requests - Errore apertura nuovo listino d'acquisto  - end
uests - Carico dati prezzo vendita file SPH_ESSEPIU.000 articolo 100249-01 - end
 - http://34.79.204.143:10001/api/masterData/salePricesVar - end
 - {"unitPricePh":31,"endDate":"2099-12-31T00:00:00.000Z","unitPricePc":"1","insertTypePh":13,"salePriceTypePh":88,"vatId":"3","insertTypePc":"","flagUpdated":true,"itemId":75136,"itemLogisticId":75080,"price":4.8,"networkId":"14","promoCode":null,"salePriceTypePc":1,"startDate":"2010-11-08T00:00:00.000Z","itemSaleId":75131,"basePrice":4.8} - end
 - {"status":500,"title":"No value present","payloadType":"standard","data":{},"success":false,"message":"No value present"} - end
T - {"status":500,"title":"No value present","payloadType":"standard","data":{},"success":false,"message":"No value present"} - end
POST - No value present - end
- Errore caricamento attributo STAG Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo CONSI Elemento non trovato: Reti/Siti, id:14 - Errore caricamento attributo PES Elemento non trovato: Reti/Siti, id:14 - Errore apertura assortimento articolo 100249-01 fornitore 100010 Elemento non trovato: Accordi, id:-1 - Errore caricamento articolo fornitore1 articolo 100249-01 articolo fornitore117410 Elemento non trovato: Terzi, id:-1 - Errore apertura listino d'acquisto 100249-01 costo 1.29000 No value present - Errore apertura nuovo prezzo di vendita articolo 100249-01 prezzo 4.80000 No value present - end
equests - Error opening sale price - end
Requests - Errore apertura nuovo prezzo di vendita  - end