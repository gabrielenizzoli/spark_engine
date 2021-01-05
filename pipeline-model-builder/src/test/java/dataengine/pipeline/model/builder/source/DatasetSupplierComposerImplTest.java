package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.Pipeline;
import dataengine.pipeline.core.consumer.catalog.DatasetConsumerCatalogException;
import dataengine.pipeline.core.consumer.factory.DatasetConsumerFactoryException;
import dataengine.pipeline.core.consumer.impl.CollectConsumer;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalog;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogException;
import dataengine.pipeline.core.supplier.catalog.DatasetSupplierCatalogImpl;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class DatasetSupplierComposerImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DatasetSupplierCatalogException, DatasetConsumerCatalogException {

        // given
        Pipeline pipeline = TestUtils.getPipeline(null);

        // when
        CollectConsumer<Row> dataSink = (CollectConsumer<Row>)pipeline.<Row>run("tx", "collect");

        // then
        Assertions.assertEquals(
                Arrays.asList("a-p1:xxx-p2", "b-p1:yyy-p2"),
                dataSink.getRows().stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

    @Test
    void testBuilderWithAggregation() throws DatasetSupplierCatalogException, DatasetConsumerFactoryException, DatasetConsumerCatalogException {

        // given
        Pipeline pipeline = TestUtils.getPipeline("testAggregationComponentsCatalog");

        // when
        CollectConsumer<Row> dataSink = (CollectConsumer<Row>)pipeline.<Row>run("tx", "collect");

        // then
        Map<String, Double> avgs = dataSink.getRows().stream()
                .collect(Collectors.toMap(r -> r.getString(r.fieldIndex("key")), r -> r.getDouble(r.fieldIndex("avg"))));

        Map<String, Double> avgsBuiltin = dataSink.getRows().stream()
                .collect(Collectors.toMap(r -> r.getString(r.fieldIndex("key")), r -> r.getDouble(r.fieldIndex("avgBuiltin"))));

        Assertions.assertEquals(avgsBuiltin, avgs, () -> "test avg function does not match default avg function");

        Map<String, Double> avgsExpected = new HashMap<>();
        avgsExpected.put("a", 1.5);
        avgsExpected.put("b", 150.0);
        avgsExpected.put("c", 1.0);
        avgsExpected.put("d", 2.0);

        Assertions.assertEquals(avgsExpected, avgs);

    }

    @Test
    void testStream() throws DatasetSupplierCatalogException, StreamingQueryException, DatasetConsumerCatalogException {

        // given
        Pipeline pipeline = TestUtils.getPipeline("testStreamComponentsCatalog");

        // when
        pipeline.<Row>run("tx", "stream");
        sparkSession.streams().awaitAnyTermination(5000);

        // then - at least one event is recorded
        long count = sparkSession.sql("select count(*) as count from memoryTable").as(Encoders.LONG()).collectAsList().get(0);
        Assertions.assertTrue(count > 0);

        // then - udf works ok
        int casesWhereUdfIsWrong = sparkSession.sql("select value, valuePlusOne from memoryTable where value != valuePlusOne -1").collectAsList().size();
        Assertions.assertEquals(0, casesWhereUdfIsWrong);

    }

    @Test
    void testBuilderWithBadComponents() {

        Assertions.assertThrows(DatasetSupplierCatalogException.class, () -> {
            DatasetSupplierCatalog DatasetSupplierCatalog = DatasetSupplierCatalogImpl.ofCatalog(TestUtils.getComponentCatalog("testBadComponentsCatalog"));
            DatasetSupplierCatalog.lookup("tx");
        });

    }

}