package com.ilab.spark.apps;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

import java.util.UUID;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionWithDelta;

public class JoinRandomStreamApp {

    private static String DATA_DIR = "data/" + UUID.randomUUID().toString();


    public static void main(String[] args) {
        try {
            SparkSession session =  createSparkSessionWithDelta("JoinRandomStreamApp");

            Dataset<Row> testStream = session.readStream().format("rate")
                    .option("rowsPerSecond", "1").option("numPartitions", "1").load();

            Dataset<Row> impressions = testStream
                    .select(
                            functions.col("value").as("impressionAdId"),
                            functions.col("timestamp").as("impressionTime"));

            Dataset<Row> clicks = testStream
                    .select(
                            (functions.col("value")).plus(15).as("clickAdId"),
                            functions.col("timestamp").as("clickTime"));

            Dataset<Row> impressionsWithWatermark =
                    impressions.withWatermark("impressionTime", "20 seconds");
            Dataset<Row> clicksWithWatermark =
                    clicks.withWatermark("clickTime", "30 seconds");


            Dataset<Row> result = impressionsWithWatermark.join(
                    clicksWithWatermark,
                    functions.expr(
                            "clickAdId = impressionAdId AND " +
                                    "clickTime >= impressionTime AND " +
                                    "clickTime <= impressionTime + interval + 30 seconds"),
                    "leftOuter"
            );

            /*
            result.writeStream().outputMode("append").format("console").option("checkpointLocation", DATA_DIR + "/output-checkpoint")
                    .start().awaitTermination();
            */
            result.writeStream().outputMode("append").format("delta").option("checkpointLocation", DATA_DIR + "/output-checkpoint")
                    .start(DATA_DIR+"/output").awaitTermination();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
