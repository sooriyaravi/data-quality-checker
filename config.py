SQL_SERVER_CONFIG = {
    "driver":             "ODBC Driver 17 for SQL Server",
    "server":             "DESKTOP-S15EJL9\\SQLEXPRESS",   # ← paste from SSMS
    "database":           "datawarehouse", # ← your database name
    "trusted_connection": "yes",              # ← this handles auth
}

TABLES = [
    "bronze.CRM_cust_info",
    "bronze.CRM_prd_info",
    "bronze.CRM_sales_detail",
    "bronze.ERP_CUST_AZ12",
    "bronze.ERP_LOC_A101",
    "bronze.ERP_PX_CAT_G1V2",
    "silver.CRM_cust_info",
    "silver.CRM_prd_info",
    "silver.CRM_sales_details",
    "silver.ERP_CUST_AZ12",
    "silver.ERP_LOC_A101",
    "silver.ERP_PX_CAT_G1V2",
]