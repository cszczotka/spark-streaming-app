package com.ilab.spark.apps;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(WriteCsvToKafkaStreamApp.class);

    public static void main(String[] args) {


        SparkSession spark =  createSparkSessionForStreaming("KafkaStreamingApp");
        //1000,test_1000_1,test,1,type1,2023-01-21 12:10:10
        StructType mySchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("aggregated_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("source", DataTypes.StringType, false, Metadata.empty()),
                new StructField("version", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("createDate", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> streamingDs = spark.readStream()
                .option("mode", "DROPMALFORMED")
                .option("timestampFormat", "yyyy-MM-dd hh:mm:ss")
                .schema(mySchema).csv("data/feed/");

        try {
            streamingDs.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                    .writeStream()
                    .trigger(Trigger.ProcessingTime(10000))
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9091")
                    .option("topic", "my_events_topic")
                    .start().awaitTermination();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
