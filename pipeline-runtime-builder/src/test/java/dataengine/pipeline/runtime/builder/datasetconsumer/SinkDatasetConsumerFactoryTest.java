package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.sink.impl.ViewSink;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.SimplePipelineFactory;
import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkDatasetConsumerFactoryTest extends SparkSessionBase {

    @Test
    void buildConsumer() throws DatasetConsumerException, DatasetConsumerFactoryException {

        // given
        var catalog = SinkCatalogFromMap.of(Map.of("get", ViewSink.builder().withName("tempTable").build()));
        var factory = SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();

        var ds = sparkSession.createDataset(List.of(1, 2, 3), Encoders.INT());

        // when
        factory.<Integer>buildConsumer("get").readFrom(ds);

        // then
        var list = sparkSession.sql("select * from tempTable").as(Encoders.INT()).collectAsList();
        assertEquals(List.of(1, 2, 3), list);
    }

    @Test
    void testForeachStreamWithCatalog() throws DatasetConsumerException, DatasetConsumerFactoryException, DatasetFactoryException, StreamingQueryException {

        // given
        var ds = (Dataset)sparkSession.readStream().format("rate").load();

        var pipe = SimplePipelineFactory.builder()
                .datasetConsumerFactory(SinkDatasetConsumerFactory.of(TestCatalog.getSinkCatalog("testStreamForeachCatalog")))
                .datasetFactory(ComponentDatasetFactory.of(sparkSession, ComponentCatalog.EMPTY).toBuilder().datasetCache(Map.of("rate", ds)).build())
                .build();

        // when
        GlobalCounterConsumer.COUNTER.clear();
        pipe.<String>buildPipeline("rate", "foreachSink").run();
        sparkSession.streams().awaitAnyTermination(3000);

        // then
        assertTrue(GlobalCounterConsumer.COUNTER.get("testForeachStreamWithCatalog").get() > 0);

    }


}