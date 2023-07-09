package com.ilab.spark.utils;

import org.apache.spark.sql.SparkSession;

public class SparkSessionUtils {

    public static SparkSession createSparkSessionWithDelta(String appName) {
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                //.config("spark.sql.warehouse.dir", "C:/tmp/hive/spark-warehouse")
                //.config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=C:/tmp/hive/metastore_db;create=true")
                .appName("DemoApp")
                .master("local[1]")
                .enableHiveSupport()
                .getOrCreate();
        return spark;
    }

    public static SparkSession createSparkSessionForStreaming(String appName) {
        SparkSession spark = SparkSession.builder()
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.streaming.checkpointLocation", "data/checkpoints")
                //.config("spark.sql.warehouse.dir", "C:/tmp/hive/spark-warehouse")
                //.config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=C:/tmp/hive/metastore_db;create=true")
                .appName("DemoApp")
                .master("local[1]")
                .enableHiveSupport()
                .getOrCreate();
        return spark;
    }
}
