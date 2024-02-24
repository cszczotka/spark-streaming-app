package com.ilab.spark.apps

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

import java.util.Properties
object JdbcForeachBatchUpsertApp extends App {

  // Create SparkSession
  val spark = SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .appName("DeltaScala")
    .master("local[1]")
    .getOrCreate

  // Define your input streaming DataFrame
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

  // Define your JDBC sink parameters
  val jdbcUrl = "jdbc:mysql://localhost:3306/testdb"
  val tableName = "random_stream"
  val batchSize = 100 // Adjust batch size as per your requirement

  // Define your foreachBatch logic to perform upserts
  val upsertFunc: (DataFrame, Long) => Unit = (batchDF, batchId) => {
    batchDF.persist() // Cache the DataFrame for better performance if necessary

    // Configure JDBC connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "abc123")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    // Perform upsert operation using foreachPartition
    batchDF.foreachPartition((partition: Iterator[Row]) => {
      val connection = java.sql.DriverManager.getConnection(jdbcUrl, connectionProperties)
      /*val statement = connection.prepareStatement(
        """
          |MERGE INTO random_stream AS t
          |USING (SELECT ?, ?, ?, ? ) AS s ( md5, value, hash,timestamp)
          |ON t.hash = s.hash
          |WHEN MATCHED THEN
          |  UPDATE SET t.md5 = s.md5, t.value = s.value, t.timestamp = s.timestamp
          |WHEN NOT MATCHED THEN
          |  INSERT (md5, value, hash, timestamp) VALUES ( s.md5, s.value, s.hash, s.timestamp)
      """.stripMargin)*/
      val statement = connection.prepareStatement(
        s"""
           |INSERT INTO $tableName (md5, value, hash, timestamp)
           |VALUES (?, ?, ?, ?)
           |ON DUPLICATE KEY UPDATE
           |value = VALUES(value),
           |md5 = VALUES(md5),
           |timestamp = VALUES(timestamp)
           |""".stripMargin)

      partition.foreach { row =>
        statement.setString(1, row.getString(0))
        statement.setLong(2, row.getLong(1))
        statement.setLong(3, row.getLong(2))
        statement.setTimestamp(4, row.getTimestamp(3))
        statement.addBatch()
      }

      statement.executeBatch()
      statement.close()
      connection.close()
    })
    batchDF.unpersist() // Unpersist the DataFrame after processing
  }

  // Define your streaming query with foreachBatch sink
  val query = transformedStream.writeStream
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      // Perform upsert logic using batchDF
      // You can use any upsert strategy suitable for your database
      // For example, you can use merge statement in case of databases supporting it (e.g., PostgreSQL)
      // Here, we are just calling the upsertToJDBC function defined earlier
      upsertFunc(batchDF, batchId)
    }
    .start()

  query.awaitTermination()

}
