package com.ilab.spark.apps;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.io.File;

import static com.ilab.spark.utils.SparkSessionUtils.createSparkSessionWithDelta;

public class DeltaUpsertJavaApp {
    public static void main(String[] args) throws Exception {

        SparkSession spark =  createSparkSessionWithDelta("DeltaUpsertJavaApp");

        spark.conf().set("spark.databricks.delta.schema.autoMerge.enabled", "true");

        Dataset<Row> inputStream =
                spark.readStream()
                        .format("rate")
                        .option("numPartitions", 10)
                        .option("rowsPerSecond", 1)
                        .load()
                        .selectExpr(
                                "md5( CAST (value AS STRING) ) as md5"
                                , "value"
                                , "value%100 as id"
                                , "timestamp"
                        );


        Dataset<Row> transformedStream = inputStream.selectExpr("md5", "value", "id", "timestamp");
        String tableLocation = new File("data/delta").getCanonicalFile().toURI().toString();

        DeltaTable.createIfNotExists()
                .addColumn(new StructField("id", DataTypes.LongType, false, Metadata.empty()))
                .location(tableLocation).execute();

        StreamingQuery query = transformedStream.writeStream()
                .foreachBatch( (Dataset<Row> ds,Long batchId) -> upsertToDelta(ds, batchId, tableLocation))
                .outputMode("append") // or "append" based on your use case
                .start();

        query.awaitTermination();
    }

    private static void upsertToDelta(Dataset<Row> microBatchDF, Long batchId, String tableLocation) {
        DeltaTable deltaTable = DeltaTable.forPath(tableLocation);

        deltaTable.as("target")
                .merge(
                        microBatchDF.as("updates"),
                        "target.id = updates.id"
                )
                .whenMatched()
                .updateAll()
                .whenNotMatched()
                .insertAll()
                .execute();
    }
}
