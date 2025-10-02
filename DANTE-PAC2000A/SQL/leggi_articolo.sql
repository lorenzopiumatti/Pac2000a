select *  from TMD_ITEMS where ITEM = ':p_item_code' 

select *  from ttr_items where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_item_logistics  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_logistic_units  where item_logistic_id in (select  id from tmd_item_logistics  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_item_Sales where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from ttr_item_sales where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_sale_prices  where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_sale_prices_var  where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_purchase_prices tpp   where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_purchase_prices_var tpp    where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_feature_item_links where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_saleable_assortments tsa where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_orderable_assortments_var  tsa where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_orderable_assortments  tsa where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_supplier_item_codes  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_structure_item_links_var tsilv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_icreate table il_TMD_ITEMS as tem_code');

select *  from tmd_structure_item_links tsilv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_item_codes_var ticv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_item_codes ticv   where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

select *  from tmd_sale_codes_var  ticv   where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

select *  from tmd_sale_codes ticv   where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))