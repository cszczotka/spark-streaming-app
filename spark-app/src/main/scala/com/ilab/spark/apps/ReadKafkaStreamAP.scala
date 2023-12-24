package com.ilab.spark.apps

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.{Calendar, Date}


/**
 * https://www.projectpro.io/recipes/perform-spark-streaming-using-rate-source-and-console-sink
 */
object ReadKafkaStreamAP extends App {
  val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
  val KAFKA_TOPIC = "test_topic2"

  val spark = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("DeltaScala")
    .master("local[1]")
    .getOrCreate


  val df = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe","test_topic")
    .option("startingOffsets", "earliest").load()


  spark.udf.register(
    "day_of_week",
    (date: Date) => {
      val cal = Calendar.getInstance()
      cal.setTime(date)
      cal.get(Calendar.DAY_OF_WEEK)
    }
  )


  df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
}

