package dataengine.pipelines;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class DataPipeTest {

    static SparkSession sparkSession;

    @BeforeAll
    static void init() {
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

    @org.junit.jupiter.api.Test
    void readAndWritePipeline() {
        List<String> data = new LinkedList<>();

        DataSource<String> dataSource = () -> SparkSession.builder().master("local").getOrCreate().createDataset(Arrays.asList("a", "aa", "aaa"), Encoders.STRING());
        DataSink<String> dataSink = d -> data.addAll(d.collectAsList());


        DataPipe.read(dataSource)
                .transformation(s -> s.map((MapFunction<String, String>) ss -> ss + ss.length(), Encoders.STRING()))
                .write(dataSink);

        Assertions.assertEquals(Arrays.asList("a1", "aa2", "aaa3"), data);
    }

}