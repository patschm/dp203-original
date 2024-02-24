-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sql Warehouse
-- MAGIC Yet another money pit. When they talk about Small they actually mean 32 vCores. 2X-Small is 8 vCores (times 2??) and 64 GiB memory.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Execute the first 3 cells from M9_3 Delta Lake
-- MAGIC You cannot read directly from an sas url. The first 3 cells create a delta lake which is local

-- COMMAND ----------

CREATE SCHEMA SalesDB

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a table

-- COMMAND ----------

CREATE TABLE SalesDB.Dim_Products (
  Id LONG,
  BrandName STRING,
  Name STRING,
  Price DECIMAL(10,2)
)
USING DELTA
LOCATION 'moneypit/products'

-- COMMAND ----------

INSERT INTO SalesDB.Dim_Products
SELECT 
  CAST(Id AS LONG),
  BrandName,
  Name,
  CAST(Price AS DECIMAL(10,2))
FROM delta.`/delta/sales/products`

-- COMMAND ----------

SELECT * FROM SalesDB.dim_products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Dashboard in SQL Warehous
-- MAGIC - Create grouping
-- MAGIC ```sql
-- MAGIC SELECT BrandName, count(*) As NrProducts FROM salesdb.dim_products
-- MAGIC GROUP BY BrandName
-- MAGIC ```
-- MAGIC - Create List query
-- MAGIC ```sql
-- MAGIC SELECT BrandName, Name FROM salesdb.dim_products
-- MAGIC ```
-- MAGIC - Create a dashbord and add visualizations for the queries
