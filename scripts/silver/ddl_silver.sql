IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO
CREATE TABLE silver.crm_cust_info (
	cust_id INT,
	cust_key VARCHAR(50),
	cust_firstname VARCHAR(50),
	cust_lastname VARCHAR(50),
	cust_marital_status VARCHAR(50),
	cust_gndr VARCHAR(50),
	cust_create_date DATE
);

IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prod_info;
GO
CREATE TABLE silver.crm_prod_info (
	prod_id INT,
	cat_id VARCHAR(50),
	prod_key VARCHAR(50),
	prod_nm VARCHAR(50),
	prod_cost VARCHAR(50),
	prod_line VARCHAR(50),
	prod_start_dt VARCHAR(50),
	prod_end_dt DATE
);
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO
CREATE TABLE silver.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
IF OBJECT_ID('silver.erp_loc', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc;
GO
CREATE TABLE silver.erp_loc(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

IF OBJECT_ID('silver.erp_cust', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust;
GO
CREATE TABLE silver.erp_cust(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

IF OBJECT_ID('silver.erp_cat', 'U') IS NOT NULL
    DROP TABLE silver.erp_cat;
GO
CREATE TABLE silver.erp_cat(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);


