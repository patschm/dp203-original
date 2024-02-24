// Databricks notebook source
// MAGIC %md
// MAGIC # Azure Event Hub

// COMMAND ----------

// MAGIC %md
// MAGIC ## Parameters

// COMMAND ----------

dbutils.widgets.text("conString", "Endpoint=sb://hubfordotnet.servicebus.windows.net/;SharedAccessKeyName=Reader;SharedAccessKey=hZgFcoNHztVUtDQtuYdqlTOUn74XaOTud+AEhA8KdgM=;EntityPath=events")
dbutils.widgets.text("hubName", "events")
dbutils.widgets.text("consumerGroup", "bricks-cg")

val conString = dbutils.widgets.get("conString")
val hubName = dbutils.widgets.get("hubName")
val consumer = dbutils.widgets.get("consumerGroup")

// COMMAND ----------

import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val conf = EventHubsConf(conString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  .setConsumerGroup(consumer)
  .setMaxEventsPerTrigger(100)

val streamingInputDF = 
  spark.readStream
    .format("eventhubs")
    .options(conf.toMap)
    .load()
val df = streamingInputDF.select(get_json_object(($"body").cast("string"), "$").alias("event"))

val deltaTable = "/delta/events"
val checkpoints = "/delta/checkpoint"

val streamingQuery = df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoints)
  .trigger(Trigger.ProcessingTime("10 seconds")) // Specify the desired trigger interval
  .start(deltaTable)


// COMMAND ----------

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import spark.implicits._

val schemaTemprature = (new StructType)
  .add("CreationTime", TimeStampType())
  .add("Device", IntegerTyp())
  .add("Series", IntegerType())
  .add("Value", IntegerType())
  .add("MinTemp", IntegerType())
  .add("MaxTemp", IntegerType())

implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Row

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE STREAMING LIVE TABLE events (
// MAGIC  CreationTime TIMESTAMP,
// MAGIC  Device INT,
// MAGIC  Series INT,
// MAGIC  Value INT,
// MAGIC  MinTemp INT,
// MAGIC  MaxTemp INT
// MAGIC )
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/events'

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("/delta", recurse=True)
