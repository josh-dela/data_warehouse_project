CREATE TABLE bronze.crm_cust_info (
	cust_id INT,
	cust_key VARCHAR(50),
	cust_firstname VARCHAR(50),
	cust_lastname VARCHAR(50),
	cust_material_status VARCHAR(50),
	cust_gndr VARCHAR(50),
	cust_create_date DATE
);

CREATE TABLE bronze.crm_prod_info (
	prod_id INT,
	prod_key VARCHAR(50),
	prod_nm VARCHAR(50),
	prod_cost VARCHAR(50),
	prod_line VARCHAR(50),
	prod_start_dt VARCHAR(50),
	prod_end_dt DATE
);

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




CREATE TABLE bronze.erp_loc(
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

CREATE TABLE bronze.erp_cust(
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

CREATE TABLE bronze.erp_cat(
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);