package dataengine.pipelines.spark;

import dataengine.pipelines.DataSource;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class SparkDataSourceTest {

    static SparkSession sparkSession;

    @BeforeAll
    static void init() {
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

    @Test
    void getTestDatasetAsText() {
        DataSource<String> ds = SparkEncoderDataSource.builder().text().path("src/test/resources/datasets/testDataset.json").build();
        Assertions.assertEquals(6, ds.get().count());
    }

    @Test
    void getTestDatasetAsJson() {
        DataSource<Row> ds = SparkRowDataSource.builder().json().path("src/test/resources/datasets/testDataset.json").build();
        List<Integer> is = ds.get().sort("num").select("num").as(Encoders.DECIMAL()).collectAsList().stream().map(bi -> bi.intValue()).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), is);
    }

}