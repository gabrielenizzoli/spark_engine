package dataengine.pipeline.core.source.impl;

import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class EmptyDataframeSourceTest extends SparkSessionBase {

    @Test
    void get() {

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("str", DataTypes.StringType, true))
        );

        Dataset<Row> ds = EmptyDataframeSource.builder().schema(schema.toDDL()).build().get();

        Assertions.assertEquals(0, ds.count());
        Assertions.assertEquals(1, ds.schema().size());
        Assertions.assertEquals("str", ds.schema().fields()[0].name());

    }
}