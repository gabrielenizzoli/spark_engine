package dataengine.pipelines;

import dataengine.pipelines.transformations.Transformation;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class DataPipeTest extends SparkSessionTest {

    @Test
    void readAndWritePipeline() {
        List<String> data = new LinkedList<>();

        DataSource<String> dataSource = () -> sparkSession.createDataset(Arrays.asList("a", "aa", "aaa"), Encoders.STRING());
        DataSink<String> dataSink = d -> data.addAll(d.collectAsList());

        DataTransformation<String, String> tx = Transformation.map(s -> s + s.length(), Encoders.STRING());

        DataPipe.read(dataSource)
                .transformation(tx)
                .write(dataSink);

        Assertions.assertEquals(Arrays.asList("a1", "aa2", "aaa3"), data);
    }

}