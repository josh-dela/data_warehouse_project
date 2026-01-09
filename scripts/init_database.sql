/*
This script 'init_database.sql' is used to create a database named 'DataWarehouse' and three schemas within it: 'bronze', 'silver', and 'gold'.
*/

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;