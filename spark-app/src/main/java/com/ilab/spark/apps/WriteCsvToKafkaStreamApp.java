package com.ilab.spark.apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionForStreaming;

public class WriteCsvToKafkaStreamApp {
    public static void main(String[] args) {

        SparkSession spark =  createSparkSessionForStreaming("KafkaStreamingApp");

        StructType mySchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("year", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> streamingDs = spark.readStream().schema(mySchema).csv("data/feed/");

        try {
            streamingDs.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                    .writeStream()
                    .trigger(Trigger.ProcessingTime(10000))
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9091")
                    .option("topic", "my_events_topic")
                    .start().awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
