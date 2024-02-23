# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lakes
# MAGIC First the variables

# COMMAND ----------

storageName = "4dndatalake"
containerName = "lake4bricks"
sas = "sp=racwdlme&st=2024-02-23T15:17:08Z&se=2024-04-30T22:17:08Z&spr=https&sv=2022-11-02&sr=c&sig=I1R7Y%2BZAb0pUgOCQWnPEw0fYgA36kaBNYPF%2F7ruVF94%3D"

spark.conf.set(f"fs.azure.account.auth.type.{storageName}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storageName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storageName}.dfs.core.windows.net", sas)

# COMMAND ----------

# MAGIC %md
# MAGIC # Now create a delta lake table from existing data

# COMMAND ----------

# MAGIC %md
# MAGIC ## First some schemas (not mandatory)

# COMMAND ----------

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
productSchema = StructType([
    StructField("Id", LongType()),
    StructField("BrandName", StringType()),
    StructField("Name", StringType()),
    StructField("Price", DecimalType(10,2))
])
ordersSchema = StructType([
    StructField("Id", LongType()),
    StructField("Quantity", StringType()),
    StructField("TotalPrice", DecimalType(10,2)),
    StructField("OrderDate", StringType()), # <--- Conversion to timestamp follows later! 
    StructField("ProductId", LongType()),
    StructField("CustomerId", LongType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Delta Tables

# COMMAND ----------

dfcustomers = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/cust*.csv", format="csv", header=True, schema=customerSchema)
dfproducts = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/prod*.csv", format="csv", header=True, schema=productSchema)
dforders = spark.read.load(f"abfss://{containerName}@{storageName}.dfs.core.windows.net/sales_small/csv/ord*.csv", format="csv", header=True, schema=ordersSchema) \
       .withColumn("OrderDate", to_timestamp("OrderDate", "MM/dd/yyyy HH:mm:ss"))

deltaPath = "/delta/sales"
productsTable = f"{deltaPath}/products"
customersTable = f"{deltaPath}/customers"
ordersTable = f"{deltaPath}/orders"

dfcustomers.write.format("delta").mode("overwrite").save(customersTable)
dfproducts.write.format("delta").mode("overwrite").save(productsTable)
dforders.write.format("delta").mode("overwrite").save(ordersTable)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the table

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE delta.`{ordersTable}`")
df_orders = spark.read.format("delta").load(ordersTable)
display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

df_orders = spark.read.format("delta").load(ordersTable)
df_customers = spark.read.format("delta").load(customersTable)
df_products = spark.read.format("delta").load(productsTable)


dfmerge = df_orders \
    .join(df_products, df_orders.ProductId==df_products.Id, "inner") \
    .join(df_customers, df_orders.CustomerId==df_customers.Id, "inner")
display(dfmerge)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Updates
# MAGIC And read with time travel

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, ordersTable)

deltaTable.update(
    condition = "Id == 1",
    set = { "Quantity": "40", "TotalPrice":"40*558.29" })

# Show versions
spark.sql(f"DESCRIBE HISTORY delta.`{ordersTable}`")

# LATEST VERSION
df_orders = spark.read.format("delta").load(ordersTable)
display(df_orders)

# OLDER VERSION
df_orders = spark.read.format("delta").option("versionAsOf", 4).load(ordersTable)
display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert data

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, productsTable)
df_product = spark.createDataFrame([(100, "Paling", "Philips", 24.99)], ["Id", "Name", "BrandName", "Price"])

deltaTable.alias("t").merge(df_product.alias("s"), "s.Id==t.Id") \
    .whenMatchedUpdate( set = {"t.Name":"s.Name", "t.BrandName": "s.BrandName", "t.Price": "s.Price"}) \
    .whenNotMatchedInsertAll() \
    .execute()

display(spark.read.format("delta").load(productsTable))



# COMMAND ----------

# MAGIC %md
# MAGIC # Delete Files from delta lake

# COMMAND ----------

dbutils.fs.rm(deltaPath, recurse=True)




# COMMAND ----------


