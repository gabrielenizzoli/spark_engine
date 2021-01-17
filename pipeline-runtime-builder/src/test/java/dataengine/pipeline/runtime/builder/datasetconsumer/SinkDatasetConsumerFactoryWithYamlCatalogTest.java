package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkDatasetConsumerFactoryWithYamlCatalogTest extends SparkSessionBase {

    @Test
    void testFactoryWithForeachStreamAndYamlCatalog() throws DatasetConsumerException, DatasetConsumerFactoryException, DatasetFactoryException, StreamingQueryException {

        // given
        var ds = (Dataset) sparkSession.readStream().format("rate").load();
        var factory = SinkDatasetConsumerFactory.of(TestCatalog.getSinkCatalog("testStreamForeachCatalog"));

        // when
        GlobalCounterConsumer.COUNTER.clear();
        factory.<Integer>buildConsumer("foreachSink").readFrom(ds);
        sparkSession.streams().awaitAnyTermination(3000);

        // then
        assertTrue(GlobalCounterConsumer.COUNTER.get("testForeachStreamWithCatalog").get() > 0);

    }


}