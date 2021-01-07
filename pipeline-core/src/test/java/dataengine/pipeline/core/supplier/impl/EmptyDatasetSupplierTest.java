package dataengine.pipeline.core.supplier.impl;

import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EmptyDatasetSupplierTest extends SparkSessionBase {

    @Test
    void get() {

        Dataset<String> ds = EmptyDatasetSupplier.<String>builder().encoder(Encoders.STRING()).build().get();

        Assertions.assertEquals(0, ds.count());
        Assertions.assertEquals(1, ds.schema().size());
        Assertions.assertEquals("value", ds.schema().fields()[0].name());
    }

}