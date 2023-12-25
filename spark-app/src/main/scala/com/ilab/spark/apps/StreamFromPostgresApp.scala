package com.ilab.spark.apps

import com.ilab.streaming.sources.JDBCStreamSourceProvider
import org.apache.spark.sql.SparkSession

object StreamFromPostgresApp extends App {

  val spark = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("DeltaScala")
    .master("local[1]")
    .getOrCreate


  val jdbcOptions = Map(
    "user" -> "abc",
    "password" -> "abc@123",
    "database" -> "abc_db",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/abc_db",
    "dbtable" -> "random_stream",
    "offsetColumn" -> "timestamp"
  )
  val formatName = new JDBCStreamSourceProvider().shortName()

  val stream = spark.readStream
    .format(formatName)
    .options(jdbcOptions)
    .load

  val query = stream.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()


}
