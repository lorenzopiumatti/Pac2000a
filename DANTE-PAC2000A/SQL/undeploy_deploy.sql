smcli undeploy storemngm-front-office-services
smcli undeploy storemngm-front-office
smcli undeploy storemngm-back-office-services
smcli undeploy storemngm-back-office

smcli stop rabbit 
smcli stop ofelia 
smcli stop db-stat 
smcli stop db  

smcli start db 
smcli start db-stat 
smcli start rabbit 
smcli deploy storemngm-back-office
smcli deploy storemngm-back-office-services
smcli deploy storemngm-front-office
smcli deploy storemngm-front-office-services
smcli start ofelia 



---- smcli start ms-email
---- smcli start ms-invoice
---- smcli start ms-queue
---- smcli start ms-xml
---- smcli start ms-restapi
---- smcli start ms-eod