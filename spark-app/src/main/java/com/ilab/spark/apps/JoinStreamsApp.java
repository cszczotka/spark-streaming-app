package com.ilab.spark.apps;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionForStreaming;

public class JoinStreamsApp {

    private static String DATA_DIR = "data/" + UUID.randomUUID().toString();

    private static void createDeltaTableWithSampleData(SparkSession spark) {
        StructType mySchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("aggregated_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("source", DataTypes.StringType, false, Metadata.empty()),
                new StructField("version", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("createDate", DataTypes.TimestampType, false, Metadata.empty())
        });
        long now = new Date().getTime();
        List<Row> rows = Arrays.asList(
                RowFactory.create(1000l, "xxx_1000", "xxx", 1, "type1", new Timestamp(now)),
                RowFactory.create(1001l, "xxx_1001", "xxx", 1, "type1", new Timestamp(now)),
                RowFactory.create(1001l, "xxx_1001", "xxx", 1, "type2", new Timestamp(now))
        );

        spark.createDataFrame(rows, mySchema).write().mode("overwrite").format("delta").save(DATA_DIR+"/input");

    }

    private static void approach2() throws Exception {
        SparkSession spark =  createSparkSessionForStreaming("JoinTwoStreamsApp");
        createDeltaTableWithSampleData(spark);
        Dataset<Row> ds1 = spark
                .readStream()
                .format("delta")
                .load(DATA_DIR + "/input")
                .filter((FilterFunction<Row>) r -> "type1".equalsIgnoreCase(r.getAs("type")))
                .withWatermark("createDate", "5 minute").alias("s1");;

        ds1.mapPartitions((MapPartitionsFunction<Row, Object>) input -> null, new Encoder<Object>() {
            @Override
            public StructType schema() {
                return null;
            }

            @Override
            public ClassTag<Object> clsTag() {
                return null;
            }
        });

    }
    public static void main(String[] args) {
        try {
            SparkSession spark =  createSparkSessionForStreaming("JoinTwoStreamsApp");
            createDeltaTableWithSampleData(spark);

            Dataset<Row> ds1 = spark
                    .readStream()
                    .format("delta")
                    .load(DATA_DIR + "/input")
                    .filter((FilterFunction<Row>) r -> "type1".equalsIgnoreCase(r.getAs("type")))
                    .withWatermark("createDate", "5 minute").alias("s1");

            Dataset<Row> ds2 = spark
                    .readStream()
                    .format("delta")
                    .load(DATA_DIR + "/input")
                    .filter((FilterFunction<Row>) r -> "type2".equalsIgnoreCase(r.getAs("type")))
                    .withWatermark("createDate", "5 minute").alias("s2");

            //inner join
            //Dataset<Row>  result = ds1.join(ds2, "id");
            //left outer
            Dataset<Row>  result = ds1.join(ds2, functions.expr(" s1.id = s2.id " +
                    " and s1.createDate between s2.createDate and s2.createDate + interval 5 minute  "), "leftOuter");

            /*
            StreamingQuery query = result.select("s1.id", "s1.type").writeStream().outputMode("append").format("console").option("truncate", false).start();
            query.awaitTermination();
            */

            result.select("s2.id", "s2.type").writeStream().outputMode("append").format("delta")
                    .start(DATA_DIR+"/output").awaitTermination(60000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
