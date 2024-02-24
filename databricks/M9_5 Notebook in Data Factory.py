# Databricks notebook source
# MAGIC %md
# MAGIC # Run in Azure Data Factory
# MAGIC #### Todo in ADF
# MAGIC - Create a Linked Service to databricks (Under tab Compute)
# MAGIC - Create a pipeline
# MAGIC - Add a notebook to the pipeline and configure it referencing this notebook

# COMMAND ----------

dbutils.widgets.text("account", "4dndatalake")
dbutils.widgets.text("container", "lake4bricks")
dbutils.widgets.text("sas", "sp=racwdlme&st=2024-02-23T15:17:08Z&se=2024-04-30T22:17:08Z&spr=https&sv=2022-11-02&sr=c&sig=I1R7Y%2BZAb0pUgOCQWnPEw0fYgA36kaBNYPF%2F7ruVF94%3D")

account = dbutils.widgets.get("account")
container = dbutils.widgets.get("container")
sas = dbutils.widgets.get("sas")

spark.conf.set(f"fs.azure.account.auth.type.{account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{account}.dfs.core.windows.net", sas)

from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("Id", LongType()),
    StructField("BrandName", StringType()),
    StructField("Name", StringType()),
    StructField("Price", DecimalType(10,2))
])

# COMMAND ----------

dfproducts = spark.read.load(f"abfss://{container}@{account}.dfs.core.windows.net/sales_small/csv/prod*.csv", format="csv", header=True, schema=productSchema)

# COMMAND ----------

dbutils.notebook.exit(dfproducts.toJSON())
