package com.ilab.spark.apps

import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File


object DeltaUpsertApp extends App {

  val spark = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("DeltaScala")
    .master("local[1]")
    .getOrCreate

  val inputStream =
    spark.readStream
      .format("rate")
      .option("numPartitions", 10)
      .option("rowsPerSecond", 10)
      .load()
      .selectExpr(
        "md5( CAST (value AS STRING) ) as md5"
        , "value"
        , "value%100 as id"
        , "timestamp"
      )


  val transformedStream: DataFrame = inputStream
    .selectExpr("md5", "value", "id", "timestamp")


  // Define your Delta table
  val tableLocation = new File(f"data/delta_table").getCanonicalFile.toURI.toString;

  val deltaTable = DeltaTable.createIfNotExists()
    .addColumn(StructField("md5", StringType, false))
    .addColumn(StructField("value", LongType, false))
    .addColumn(StructField("id", LongType, false))
    .addColumn(StructField("timestamp", TimestampType, false))
    .location(tableLocation).execute()


  // Define your foreachBatch logic to perform upserts
  def upsertToDelta(microBatchDF: DataFrame, batchId: Long): Unit = {
    deltaTable.as("target")
      .merge(
        microBatchDF.as("updates"),
        "target.id = updates.id"
      )
      .whenMatched()
      .updateAll()
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  // Define your streaming query with foreachBatch sink
  val query = transformedStream.writeStream
    .foreachBatch(upsertToDelta _)
    .outputMode("append") // or "append" based on your use case
    .start()

  query.awaitTermination()

}
