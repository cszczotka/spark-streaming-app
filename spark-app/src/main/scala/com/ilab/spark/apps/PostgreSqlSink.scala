package com.ilab.spark.apps

class PostgreSqlSink(url: String, user: String, pwd: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {

  val driver = "org.postgresql.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.PreparedStatement = _
  val v_sql = "insert INTO public.random_stream (timestamp, value) values ( ?, ?)"

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    connection.setAutoCommit(false)
    statement = connection.prepareStatement(v_sql)
    true
  }
  def process(value: org.apache.spark.sql.Row): Unit = {
    // ignoring value(0) as this is address
    statement.setString(1, value(0).toString)
    statement.setString(2, value(1).toString)
    statement.executeUpdate()
  }
  def close(errorOrNull: Throwable): Unit = {
    connection.commit()
    connection.close
  }
}
