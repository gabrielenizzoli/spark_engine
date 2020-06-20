package dataengine.pipeline.core.source;

import dataengine.pipeline.core.SparkSessionBase;
import dataengine.pipeline.core.sink.DataSink;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class SourceTest extends SparkSessionBase {

    @Test
    void readAndWritePipeline() {

        // given
        List<String> data = new LinkedList<>();
        DataSource<String> dataSource = () -> sparkSession.createDataset(Arrays.asList("a", "aa", "aaa"), Encoders.STRING());
        DataSink<String> dataSink = d -> data.addAll(d.collectAsList());
        DataTransformation<String, String> tx = Transformations.map(s -> s + s.length(), Encoders.STRING());

        // when
        dataSource.transform(tx).write(dataSink);

        // then
        Assertions.assertEquals(Arrays.asList("a1", "aa2", "aaa3"), data);
    }

}