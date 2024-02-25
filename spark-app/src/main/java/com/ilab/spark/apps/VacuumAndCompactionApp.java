package com.ilab.spark.apps;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionWithDelta;

public class VacuumAndCompactionApp {
    public static void main(String[] args) {
        try {
            SparkSession spark =  createSparkSessionWithDelta("VacuumAndCompactionApp");
            spark.conf().set("spark.databricks.delta.retentionDurationCheck.enabled", "false");
            spark.conf().set("spark.databricks.delta.targetFileSize", "50m");
            DeltaTable deltaTable = DeltaTable.forPath(spark, "data\\delta");
            deltaTable.optimize().executeCompaction();
            deltaTable.vacuum(0.01);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
