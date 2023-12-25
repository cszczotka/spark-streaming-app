package com.ilab.spark.apps

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row}

class PostgresBatchSqlSink(url: String, table: String, user: String, pwd: String) {

  def savePostgres: (Dataset[Row], Long) => Unit = (df: Dataset[Row], batchId: Long) => {
    df
      .withColumn("batchId", lit(batchId))
      .write.format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", pwd)
      .mode("append")
      .save()
  }
}
