package com.ilab.spark.apps;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionForStreaming;

public class ReadKafkaStreamApp {

    private static final Logger logger = LogManager.getLogger(ReadKafkaStreamApp.class);

    public static void main(String[] args) {

        SparkSession spark =  createSparkSessionForStreaming("ReadKafkaStreamApp");

        StructType mySchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("aggregated_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("source", DataTypes.StringType, false, Metadata.empty()),
                new StructField("version", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("createDate", DataTypes.TimestampType, false, Metadata.empty())
        });

        Dataset<Row> ds = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9091")
                .option("subscribe", "my_events_topic")
                .option("startingOffsets", "{\"my_events_topic\":{\"0\":1}}")
                //.option("endingOffsets", "{\"my_events_topic\":{\"0\":1}}")
                .load();

        Dataset<Row> ds2 = ds.selectExpr("CAST(value AS STRING) as value", "CAST(timestamp AS TIMESTAMP) as timestamp")
                         .select(functions.from_json(new Column("value"), mySchema).as("data"), new Column("timestamp"))
                .select("data.*", "timestamp");

        try {
        ds2.writeStream()
                .format("console")
                .option("truncate","false")
                .start()
                .awaitTermination();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
