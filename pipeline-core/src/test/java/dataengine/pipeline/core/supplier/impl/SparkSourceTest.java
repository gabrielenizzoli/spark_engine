package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SparkSourceTest extends SparkSessionBase {

    @Test
    void getTestDatasetAsText() {
        DatasetSupplier<String> ds = SparkSource.builder().text().path("src/test/resources/datasets/testDataset.json").batch().build();
        Assertions.assertEquals(6, ds.get().count());
    }

    @Test
    void getTestDatasetAsJson() {
        DatasetSupplier<Row> ds = SparkSource.builder().json().path("src/test/resources/datasets/testDataset.json").batch().build();
        List<Integer> is = ds.get().sort("num").select("num").as(Encoders.DECIMAL()).collectAsList().stream().map(bi -> bi.intValue()).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), is);
    }

}