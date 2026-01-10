IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO
CREATE TABLE bronze.crm_cust_info (
	cust_id INT,
	cust_key VARCHAR(50),
	cust_firstname VARCHAR(50),
	cust_lastname VARCHAR(50),
	cust_marital_status VARCHAR(50),
	cust_gndr VARCHAR(50),
	cust_create_date DATE
);

IF OBJECT_ID('bronze.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prod_info;
GO
CREATE TABLE bronze.crm_prod_info (
	prod_id INT,
	prod_key VARCHAR(50),
	prod_nm VARCHAR(50),
	prod_cost VARCHAR(50),
	prod_line VARCHAR(50),
	prod_start_dt DATETIME,
	prod_end_dt DATETIME
);
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO
CREATE TABLE bronze.crm_sales_details (
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
IF OBJECT_ID('bronze.erp_loc', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc;
GO
CREATE TABLE bronze.erp_loc(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust;
GO
CREATE TABLE bronze.erp_cust(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cat', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cat;
GO
CREATE TABLE bronze.erp_cat(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);


