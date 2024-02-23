# Databricks notebook source
# MAGIC %md
# MAGIC # Set Credentials for the storage
# MAGIC Many ways to achieve this. Here we use SAS but there are other options. (https://docs.databricks.com/en/connect/storage/azure-storage.html)

# COMMAND ----------

storageName = "4dndatalake"
containerName = "lake4bricks"
sas = "sp=racwdlme&st=2024-02-23T15:17:08Z&se=2024-04-30T22:17:08Z&spr=https&sv=2022-11-02&sr=c&sig=I1R7Y%2BZAb0pUgOCQWnPEw0fYgA36kaBNYPF%2F7ruVF94%3D"

spark.conf.set(f"fs.azure.account.auth.type.{storageName}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageName}.dfs.core.windows.net", sas)


# COMMAND ----------

# MAGIC %md
# MAGIC # Read from container

# COMMAND ----------

df = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema?
# MAGIC The default schema is not quite right.
# MAGIC
# MAGIC ### Note on nullable:
# MAGIC In general Spark Datasets either inherit nullable property from its parents, or infer based on the external data types.
# MAGIC
# MAGIC You can argue if it is a good approach or not but ultimately it is sensible. If semantics of a data source doesn't support nullability constraints, then application of a schema cannot either. At the end of the day it is always better to assume that things can be null, than fail on the runtime if this the opposite assumption turns out to be incorrect.

# COMMAND ----------

df.printSchema()

from pyspark.sql.types import *
from pyspark.sql.functions import *

customerSchema = StructType([
    StructField("Id", LongType()),
    StructField("FirstName", StringType()),
    StructField("LastName", StringType()),
    StructField("CompanyName", StringType()),
    StructField("StreetName", StringType()),
    StructField("Number", IntegerType()),
    StructField("City", StringType()),
    StructField("Region", StringType()),
    StructField("Country", StringType())
])

df = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True, schema=customerSchema)
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering & Projecting

# COMMAND ----------

df = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True, schema=customerSchema)
#df = df.where(col("FirstName").startswith("A"))
# Alternatives
#df = df.where(df["FirstName"].startswith("A"))
#df = df.where(df.FirstName.startswith("A"))
#df = df.select("FirstName", "LastName")
# Or use the filter method
#df = df.filter(col("FirstName").startswith("A"))

# Multilpe columns
#df = df.where(df.FirstName.startswith("A") & df.LastName.startswith("J"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Grouping

# COMMAND ----------

df = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True, schema=customerSchema)

dfg = df.select("Id", "City").groupBy("City").count()
display(dfg)

# COMMAND ----------

# MAGIC %md
# MAGIC # Using Sql
# MAGIC - Step 1: Create a temporary view
# MAGIC - Step 2: Use spark.sql

# COMMAND ----------

df = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True, schema=customerSchema)
df.createOrReplaceTempView("customers")

dfq = spark.sql("SELECT * FROM customers")
display(dfq)

# COMMAND ----------

# MAGIC %md
# MAGIC # Or use sql

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM customers
