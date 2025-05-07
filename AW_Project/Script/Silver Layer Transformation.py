# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access Using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awprojstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awprojstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awprojstorage.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.awprojstorage.dfs.core.windows.net", "service_credential")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awprojstorage.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Adventure Works Data Source

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Calender Data

# COMMAND ----------

df_cal = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Customer Data

# COMMAND ----------

df_Cus = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
        .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Product Category Data

# COMMAND ----------

df_prod_cat = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Product Subcategories Data

# COMMAND ----------

df_prod_Subcat = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Products Data

# COMMAND ----------

df_prod = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Returns Data

# COMMAND ----------

df_ret = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Sales Data

# COMMAND ----------

df_sal = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Territories Data

# COMMAND ----------

df_ter = spark.read.format('csv')\
    .option("Header",True)\
        .option("Inferschema",True)\
            .load("abfss://bronze-raw-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations on Calender Data

# COMMAND ----------

df_cal = df_cal.withColumn("Month",month(col('Date')))\
    .withColumn("Year",year(col('Date')))
df_cal.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Transformed Calender Data into Silver Layer

# COMMAND ----------

df_cal.write.format("Parquet")\
  .mode("append")\
    .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Calendar")\
      .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations on Customer Data

# COMMAND ----------

df_Cus.display()

# COMMAND ----------

#df_Cus = df_Cus.withColumn("FullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName')))
df_Cus = df_Cus.withColumn("FirstName",concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_Cus.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Transformed Customer Data into Silver Layer

# COMMAND ----------

df_Cus.write.format('Parquet')\
    .mode('append')\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Product Categories Data into Silver Layer

# COMMAND ----------

df_prod_cat.write.format('Parquet')\
    .mode('append')\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Product Subcategories Data into Silver Layer
# MAGIC

# COMMAND ----------

df_prod_Subcat.display()
df_prod_Subcat.write.format("Parquet")\
    .mode("append")\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Product_Subcategory")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations on Products Data

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod = df_prod.withColumn('ProductSKU', split(col('ProductSKU'),'-')[0])\
    .withColumn('ProductName', split(col('ProductName'),' ')[0])
df_prod.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Transformed Products Data into Silver Layer

# COMMAND ----------

df_prod.write.format('Parquet')\
    .mode('append')\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Returns Data into Silver Layer

# COMMAND ----------

df_ret.write.format('Parquet')\
    .mode('append')\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Territories Data into Silver Layer
# MAGIC

# COMMAND ----------

df_ter.write.format('Parquet')\
    .mode('append')\
        .option("PATH","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations on Sales Data

# COMMAND ----------

df_sal.display()

# COMMAND ----------

df_sal = df_sal.withColumn('StockDate',to_timestamp(col('StockDate')))

# COMMAND ----------

df_sal = df_sal.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','N'))

# COMMAND ----------

df_sal = df_sal.withColumn('Multyply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sal.display()

# COMMAND ----------

df_sal.write.format('Parquet')\
    .mode('append')\
        .option("Path","abfss://silver-transformed-data@awprojstorage.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Analysis

# COMMAND ----------

df_sal.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Order')).display()


# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_ter.display()
