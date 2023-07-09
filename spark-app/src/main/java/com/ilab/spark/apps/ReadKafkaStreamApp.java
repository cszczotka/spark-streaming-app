package com.ilab.spark.apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionWithDelta;

public class ReadKafkaStreamApp {

    public static void main(String[] args) {

        SparkSession spark =  createSparkSessionWithDelta("KafkaStreamingApp");

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9091")
                .option("subscribe", "my_events_topic")
                .load();

        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value");


    }
}
