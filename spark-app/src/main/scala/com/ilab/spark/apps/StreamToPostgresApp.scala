package com.ilab.spark.apps

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

import java.util.UUID
object StreamToPostgresApp extends App {
  val url = "jdbc:postgresql://localhost:5432/abc_db"
  val user = "abc"
  val pwd = "abc@123"
  val jdbcWriter = new PostgreSqlSink(url,user,pwd)
  val jdbcBatchWriter = new PostgresBatchSqlSink(url, "random_stream", user, pwd)

  var dataDir = "data/" + UUID.randomUUID().toString

  val spark = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("DeltaScala")
    .master("local[1]")
    .getOrCreate

  val pgDf: Dataset[Row] = spark.readStream.format("rate").load

  val writeData = pgDf.writeStream
    //.foreach(jdbcWriter)
    .foreachBatch(jdbcBatchWriter.savePostgres)
    .outputMode("Append")
    .trigger(ProcessingTime("30 seconds"))
    .option("checkpointLocation", dataDir + "/check")
    .start()

  writeData.awaitTermination
}
