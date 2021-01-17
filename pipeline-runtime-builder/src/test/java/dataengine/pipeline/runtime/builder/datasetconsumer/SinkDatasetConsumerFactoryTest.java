package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.catalog.SinkCatalog;
import dataengine.pipeline.model.sink.impl.ViewSink;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkDatasetConsumerFactoryTest extends SparkSessionBase {

    @Test
    void testFactory() throws DatasetConsumerException, DatasetConsumerFactoryException {

        // given
        var ds = sparkSession.createDataset(List.of(1, 2, 3), Encoders.INT());
        var catalog = SinkCatalog.ofMap(Map.of("get", ViewSink.builder().withName("tempTable").build()));
        var factory = SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();

        // when
        factory.<Integer>buildConsumer("get").readFrom(ds);

        // then
        var list = sparkSession.sql("select * from tempTable").as(Encoders.INT()).collectAsList();
        assertEquals(List.of(1, 2, 3), list);
    }


}