**************** schema boom ***************************

delete from  boom.tsm_stocks_history
 
delete from boom.tsm_stocks
 
delete from boom.tsm_receptions_last_cost
 
delete from boom.tsm_receptions_detail
 
delete from boom.tsm_receptions
 
delete from boom.tsm_movements_monitor
 
delete from boom.tsm_movements
  
delete from boom.tsm_inventory_steps
  
delete from boom.tsm_inventory_inputs 
  
 delete from boom.tsm_inventories_detail
  
delete from  boom.tsm_inventories

delete from boom.tsm_delivery_notes_detail

delete from boom.tsm_delivery_notes
  
delete from boom.tor_orders_detail

delete from boom.tor_orders

delete from boom.tin_terminal_errors

delete from boom.tin_data_terminal_detail

delete from boom.tin_data_terminal_header

**************** schema ts ***************************

delete FROM ts.ts_003_pagamenti;

delete FROM ts.ts_002_dettaglio_promo;

delete  FROM ts.ts_001_dettaglio;

delete FROM ts.ts_000_testata;

**************** schema report su statistico ***************************

DELETE FROM report.tre_sales_article 

DELETE FROM report.tre_sales_article_ean

DELETE FROM report.tre_sales_department 

DELETE FROM report.tre_sales_department_hours 

DELETE FROM report.tre_sales_month 

DELETE FROM report.tre_sales_payment 

DELETE FROM report.tre_sales_promotions 

DELETE FROM report.tre_sales_week 

DELETE from report.ts_003_pagamenti

DELETE from report.ts_002_dettaglio_promo

DELETE from report.ts_001_dettaglio

DELETE from report.ts_000_testata

**************** schema tesi_invoice su BOSS ***************************

delete FROM tesi_invoice.invoice_by_ddt;

delete FROM tesi_invoice.invoice_footer;

delete FROM tesi_invoice.invoice_row;

delete FROM tesi_invoice.invoice_head;

delete FROM ts.piede_fattura;
 
delete FROM ts.ts_003_pagamenti;

delete FROM ts.ts_002_dettaglio_promo;

delete FROM ts.ts_001_dettaglio;

delete FROM ts.ts_000_testata; 

delete FROM ts.ts_strigpa;