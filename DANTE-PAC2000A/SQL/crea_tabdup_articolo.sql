     DROP TABLE boom.il_tmd_feature_item_links             ;
     DROP TABLE boom.il_tmd_item_code                      ;
     DROP TABLE boom.il_tmd_item_codes_var                 ;
     DROP TABLE boom.il_tmd_item_logistics                 ;
     DROP TABLE boom.il_tmd_item_sales                     ;
     DROP TABLE boom.il_tmd_items                          ;
     DROP TABLE boom.il_tmd_logistic_units                 ;
     DROP TABLE boom.il_tmd_orderable_assortments          ;
     DROP TABLE boom.il_tmd_orderable_assortments_var      ;
     DROP TABLE boom.il_tmd_purchase_prices                ;
     DROP TABLE boom.il_tmd_purchase_prices_var            ;
     DROP TABLE boom.il_tmd_sale_codes                     ;
     DROP TABLE boom.il_tmd_sale_codes_var                 ;
     DROP TABLE boom.il_tmd_sale_prices                    ;
     DROP TABLE boom.il_tmd_sale_prices_var                ;
     DROP TABLE boom.il_tmd_saleable_assortments           ;
     DROP TABLE boom.il_tmd_structure_item_links           ;
     DROP TABLE boom.il_tmd_structure_item_links_var       ;
     DROP TABLE boom.il_tmd_supplier_item_codes            ;
     DROP TABLE boom.il_ttr_item_sales                     ;
     DROP TABLE boom.il_ttr_items                          ;


create table il_TMD_ITEMS as 
select *  from TMD_ITEMS where ITEM = ':p_item_code' 

create table il_ttr_items as 
select *  from ttr_items where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_item_logistics as 
select *  from tmd_item_logistics  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_logistic_units as 
select *  from tmd_logistic_units  where item_logistic_id in (select  id from tmd_item_logistics  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_item_Sales as 
select *  from tmd_item_Sales where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_ttr_item_sales as 
select *  from ttr_item_sales where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_sale_prices as 
select *  from tmd_sale_prices  where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_sale_prices_var as 
select *  from tmd_sale_prices_var  where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_purchase_prices as 
select *  from tmd_purchase_prices tpp   where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_purchase_prices_var as 
select *  from tmd_purchase_prices_var tpp    where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_feature_item_links as 
select *  from tmd_feature_item_links where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_saleable_assortments as 
select *  from tmd_saleable_assortments tsa where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_orderable_assortments_var as 
select *  from tmd_orderable_assortments_var  tsa where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_orderable_assortments as 
select *  from tmd_orderable_assortments  tsa where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_supplier_item_codes as 
select *  from tmd_supplier_item_codes  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_structure_item_links_var as 
select *  from tmd_structure_item_links_var tsilv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_structure_item_links as 
select *  from tmd_structure_item_links tsilv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_item_codes_var as 
select *  from tmd_item_codes_var ticv  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_item_codes as 
select *  from tmd_item_codes ticv   where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code');

create table il_tmd_sale_codes_var as 
select *  from tmd_sale_codes_var  ticv   where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))

create table il_tmd_sale_codes as 
select *  from tmd_sale_codes ticv   where item_sale_id in (select  id from tmd_item_sales  where item_id in (select id  from TMD_ITEMS where ITEM = ':p_item_code'))