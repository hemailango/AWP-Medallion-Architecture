-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Your Password'
-------------------------------------------------
-- Creating Credential
-------------------------------------------------

CREATE DATABASE SCOPED CREDENTIAL hema_cred
WITH
    IDENTITY = 'Managed Identity'

-------------------------------------------------
-- Creating External Data Source
-------------------------------------------------

CREATE EXTERNAL DATA SOURCE Source_Silver
WITH
    (
        LOCATION = 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/',
        CREDENTIAL = hema_cred
    )

CREATE EXTERNAL DATA SOURCE Source_Gold
WITH
    (
        LOCATION = 'https://awprojstorage.dfs.core.windows.net/gold-serving-data/',
        CREDENTIAL = hema_cred
    )

-------------------------------------------------
-- Creating External File Format
-------------------------------------------------

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
     (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
     )


-------------------------------------------------
-- Creating External Tables
-------------------------------------------------
-------------------------------------------------
-- Creating External Table Ext_Calender
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Calender
WITH
    (
        LOCATION = 'ExtCalender',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    ) 
AS
SELECT * FROM Gold.Calender

--Querying Data

SELECT * FROM Ext_Calender
-------------------------------------------------
-- Creating External Table Ext_Customer
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Customer
WITH
    (
        LOCATION = 'ExtCustomer',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Customer

--Querying Data

SELECT * FROM Ext_Customer

-------------------------------------------------
-- Creating External Table Ext_Product_Category
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Prod_Cat
WITH
    (
        LOCATION = 'Ext_Procuct_Category',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Prod_Cat

--Querying Data

SELECt * FROM Ext_Prod_Cat
-------------------------------------------------
-- Creating External Table Ext_Product_SubCategory
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Prod_SubCat
WITH
    (
        LOCATION = 'Ext_Product_SubCategory',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Prod_SubCat

--Querying Data

SELECt * FROM Ext_Prod_SubCat

-------------------------------------------------
-- Creating External Table Ext_Products
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Products
WITH
    (
        LOCATION = 'Ext_Products',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Products

--Querying Data

SELECT * FROM Ext_Products

-------------------------------------------------
-- Creating External Table Ext_Returns
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Returns
WITH
    (
        LOCATION = 'Ext_Returns',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Ret

--Querying Data

SELECT * FROM Ext_Returns

-------------------------------------------------
-- Creating External Table Ext_Sales
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Sales
WITH
    (
        LOCATION = 'Ext_Sales',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Sales

--Querying Data

SELECT * FROM Ext_Sales

-------------------------------------------------
-- Creating External Table Ext_Territories
-------------------------------------------------

CREATE EXTERNAL TABLE Ext_Territories
WITH
    (
        LOCATION = 'Ext_Territories',
        DATA_SOURCE = Source_Gold,
        FILE_FORMAT = format_parquet
    )
AS
SELECT * FROM Gold.Terr

--Querying Data

SELECT * FROM Ext_Territories