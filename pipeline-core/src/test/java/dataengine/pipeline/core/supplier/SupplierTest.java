package dataengine.pipeline.core.supplier;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.spark.test.SparkSessionBase;
import dataengine.spark.transformation.DataTransformation;
import dataengine.spark.transformation.Transformations;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class SupplierTest extends SparkSessionBase {

    @Test
    void readAndWritePipeline() {

        // given
        List<String> data = new LinkedList<>();
        DatasetSupplier<String> datasetSupplier = () -> sparkSession.createDataset(Arrays.asList("a", "aa", "aaa"), Encoders.STRING());
        DatasetConsumer<String> datasetConsumer = d -> data.addAll(d.collectAsList());
        DataTransformation<String, String> tx = Transformations.map(s -> s + s.length(), Encoders.STRING());

        // when
        datasetSupplier.transform(tx).writeTo(datasetConsumer);

        // then
        Assertions.assertEquals(Arrays.asList("a1", "aa2", "aaa3"), data);
    }

}