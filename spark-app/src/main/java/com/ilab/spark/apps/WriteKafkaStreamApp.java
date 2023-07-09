package com.ilab.spark.apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionForStreaming;

public class WriteKafkaStreamApp {

    public static void main(String[] args) {

        SparkSession spark =  createSparkSessionForStreaming("KafkaStreamingApp");


        Dataset<Row> streamingDs = spark.readStream().format("rate").load();

        try {
            streamingDs.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)")
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9091")
                    .option("topic", "my_events_topic")
                    .start().awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
