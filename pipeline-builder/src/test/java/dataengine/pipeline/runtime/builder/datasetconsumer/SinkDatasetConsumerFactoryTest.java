package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.impl.CollectSink;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkDatasetConsumerFactoryTest extends SparkSessionBase {

    @Test
    void buildConsumer() throws DatasetConsumerException, DatasetConsumerFactoryException {

        // given
        var catalog = SinkCatalogFromMap.of(Map.of("get", CollectSink.builder().build()));
        var factory = SinkDatasetConsumerFactory.builder().sinkCatalog(catalog).build();

        var ds = sparkSession.createDataset(List.of(1, 2, 3), Encoders.INT());

        // when
        var list = ((CollectConsumer<Integer>) factory.<Integer>buildConsumer("get").readFrom(ds)).getList();

        // then
        assertEquals(List.of(1, 2, 3), list);
    }

}