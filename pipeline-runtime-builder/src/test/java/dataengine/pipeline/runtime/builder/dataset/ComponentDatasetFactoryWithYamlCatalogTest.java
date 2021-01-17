package dataengine.pipeline.runtime.builder.dataset;

import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentDatasetFactoryWithYamlCatalogTest extends SparkSessionBase {

    @Test
    void testFactoryWithBadYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testBadComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // then
        assertThrows(DatasetFactoryException.DatasetCircularReference.class, () -> factory.buildDataset("source2"));

    }

    @Test
    void testFactoryWithYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var ds = factory.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then
        assertEquals(
                Arrays.asList("a-p1:xxx-p2", "b-p1:yyy-p2"),
                rows.stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

    @Test
    void testFactoryWithAggregationInYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testAggregationComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var ds = factory.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then
        Map<String, Double> avgs = rows.stream()
                .collect(Collectors.toMap(r -> r.getString(r.fieldIndex("key")), r -> r.getDouble(r.fieldIndex("avg"))));

        Map<String, Double> avgsBuiltin = rows.stream()
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
    void testFactoryWithTransformationInYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testTransformationComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var ds = factory.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then

        Assertions.assertEquals(10, rows.size());
    }

    @Test
    void testFactoryWithStreamCatalog() throws DatasetFactoryException, StreamingQueryException, TimeoutException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testStreamComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var ds = factory.<Row>buildDataset("tx");
        ds.writeStream().format("memory").outputMode("append").trigger(Trigger.ProcessingTime(1000)).queryName("memoryTable").start();
        sparkSession.streams().awaitAnyTermination(5000);

        // then - at least one event is recorded
        long count = sparkSession.sql("select count(*) as count from memoryTable").as(Encoders.LONG()).collectAsList().get(0);
        Assertions.assertTrue(count > 0);

        // then - udf works ok
        int casesWhereUdfIsWrong = sparkSession.sql("select value, valuePlusOne from memoryTable where value != valuePlusOne -1").collectAsList().size();
        Assertions.assertEquals(0, casesWhereUdfIsWrong);

    }

}