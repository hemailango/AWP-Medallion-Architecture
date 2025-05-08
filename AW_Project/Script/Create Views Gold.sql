-------------------------------------
-- Creating Schema
-------------------------------------
CREATE SCHEMA Gold;

-------------------------------------
-- Creating View Calender
-------------------------------------

CREATE VIEW Gold.Calender
AS
SELECT *
FROM 
    OPENROWSET
                (
                    BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Calendar/',
                    FORMAT = 'PARQUET'
                ) AS Quer1
                
-------------------------------------
-- Creating View Customer
-------------------------------------

CREATE VIEW Gold.Customer
AS
    SELECT *
    FROM    
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Customers',
                        FORMAT = 'PARQUET'
                    ) AS Quer2

-------------------------------------
-- Creating View Product Category
-------------------------------------

CREATE VIEW Gold.Prod_Cat
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Product_Categories',
                        FORMAT = 'PARQUET'
                    ) AS Quer3

-------------------------------------
-- Creating View Product SubCategory
-------------------------------------

CREATE VIEW Gold.Prod_SubCat
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Product_Subcategory',
                        FORMAT = 'PARQUET'
                    ) AS Quer4

-------------------------------------
-- Creating View Products
-------------------------------------

CREATE VIEW Gold.Products
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Products',
                        FORMAT = 'PARQUET'
                    ) AS Quer5

-------------------------------------
-- Creating View Returns
-------------------------------------

CREATE VIEW Gold.Ret
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Returns',
                        FORMAT = 'PARQUET'
                    ) AS Quer6
                    
-------------------------------------
-- Creating View Sales
-------------------------------------

CREATE VIEW Gold.Sales
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Sales',
                        FORMAT = 'PARQUET'
                    ) AS Quer7

-------------------------------------
-- Creating View Territories
-------------------------------------

CREATE VIEW Gold.Terr
AS
    SELECT *
    FROM
        OPENROWSET
                    (
                        BULK 'https://awprojstorage.dfs.core.windows.net/silver-transformed-data/AdventureWorks_Territories',
                        FORMAT = 'PARQUET'
                    ) AS Quer8

