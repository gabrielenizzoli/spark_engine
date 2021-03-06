package sparkengine.plan.runtime.builder.datasetconsumer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.TestCatalog;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactoryException;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.test.SparkSessionManager;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkDatasetConsumerFactoryWithYamlCatalogTest extends SparkSessionManager {

    @Test
    void testFactoryWithForeachStreamAndYamlCatalog() throws DatasetConsumerException, DatasetConsumerFactoryException, DatasetFactoryException, StreamingQueryException {

        // given
        var ds = (Dataset) sparkSession.readStream().format("rate").load();
        var factory = SinkDatasetConsumerFactory.of(RuntimeContext.init(sparkSession), TestCatalog.getSinkCatalog("testStreamForeachCatalog"));

        // when
        GlobalCounterConsumer.COUNTER.clear();
        factory.<Integer>buildConsumer("foreachSink").readFrom(ds);
        sparkSession.streams().awaitAnyTermination(3000);

        // then
        assertTrue(GlobalCounterConsumer.COUNTER.get("testForeachStreamWithCatalog").get() > 0);

        
    }


}