package com.ilab.spark.apps

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import java.sql.Timestamp

object JdbcForeachUpsertApp extends App {

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
        , "value%100 as hash"
        , "timestamp"
      )

  val transformedStream = inputStream
    .selectExpr("md5", "value", "hash", "timestamp")

  class JDBCUpsertWriter(jdbcUrl: String, tableName: String, batchSize: Int) extends ForeachWriter[Row] {
    var connection: java.sql.Connection = _
    var statement: java.sql.PreparedStatement = _

    override def open(partitionId: Long, version: Long): Boolean = {
      connection = java.sql.DriverManager.getConnection(jdbcUrl, "root", "abc123")
      statement = connection.prepareStatement(
        s"""
           |INSERT INTO $tableName (md5, value, hash, timestamp)
           |VALUES (?, ?, ?, ?)
           |ON DUPLICATE KEY UPDATE
           |value = VALUES(value),
           |md5 = VALUES(md5),
           |timestamp = VALUES(timestamp)
           |""".stripMargin)
      true
    }

    override def process(row: Row): Unit = {
      // Bind values to PreparedStatement
      statement.setString(1, row.getAs[String]("md5"))
      statement.setLong(2, row.getAs[Long]("value"))
      statement.setLong(3, row.getAs[Long]("hash"))
      statement.setTimestamp(4, row.getAs[Timestamp]("timestamp"))
      statement.addBatch()

      // Execute batch update if batch size is reached
      if (statement.getUpdateCount % batchSize == 0) {
        statement.executeBatch()
      }
    }

    override def close(errorOrNull: Throwable): Unit = {
      // Execute any remaining updates
      statement.executeBatch()
      if (connection != null) {
        connection.close()
      }
    }
  }

  // Define your JDBC sink parameters
  val jdbcUrl = "jdbc:mysql://localhost:3306/test"
  val tableName = "random_stream"
  val batchSize = 100 // Adjust batch size as per your requirement

  // Define your JDBC sink writer
  val jdbcSinkWriter = new JDBCUpsertWriter(jdbcUrl, tableName, batchSize)

  // Define your streaming query with foreach sink
  val query = transformedStream.writeStream
    .foreach(jdbcSinkWriter)
    .outputMode("update") // or "append" based on your use case
    .start()

  query.awaitTermination()

}
