package sparkengine.plan.runtime.builder.datasetconsumer;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import sparkengine.plan.model.sink.catalog.SinkCatalog;
import sparkengine.plan.model.sink.impl.ViewSink;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SinkDatasetConsumerFactoryTest extends SparkSessionManager {

    @Test
    void testFactory() throws DatasetConsumerException, DatasetConsumerFactoryException {

        // given
        var ds = sparkSession.createDataset(List.of(1, 2, 3), Encoders.INT());
        var catalog = SinkCatalog.ofMap(Map.of("get", ViewSink.builder().withName("tempTable").build()));
        var factory = SinkDatasetConsumerFactory.builder().runtimeContext(RuntimeContext.init(sparkSession)).sinkCatalog(catalog).build();

        // when
        factory.<Integer>buildConsumer("get").readFrom(ds);

        // then
        var list = sparkSession.sql("select * from tempTable").as(Encoders.INT()).collectAsList();
        assertEquals(List.of(1, 2, 3), list);
    }


}