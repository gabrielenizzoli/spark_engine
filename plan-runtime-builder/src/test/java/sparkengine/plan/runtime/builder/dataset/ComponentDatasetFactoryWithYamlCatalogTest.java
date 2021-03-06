package sparkengine.plan.runtime.builder.dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.TestCatalog;
import sparkengine.plan.runtime.builder.dataset.utils.UdfContextFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.sql.udf.context.UdfContext;
import sparkengine.spark.test.SparkSessionManager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentDatasetFactoryWithYamlCatalogTest extends SparkSessionManager {

    @Test
    void testFactoryWithBadYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testBadComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

        // then
        assertThrows(DatasetFactoryException.DatasetCircularReference.class, () -> factory.buildDataset("source2"));

    }

    @Test
    void testFactoryWithYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

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
    void testFactoryWithFragmentAndWrapperYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testWrapperAndFragmentCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

        // when
        var data = factory.buildDataset("wrapper").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);
    }

    @Test
    void testFactoryWithAggregationInYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testAggregationComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

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
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

        // when
        var ds = factory.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then
        Assertions.assertEquals(10, rows.size());
    }

    @Test
    void testFactoryWithTransformationAndParamsInYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testTransformationComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

        // when
        var ds = factory.<Row>buildDataset("txWithParams");
        var schema = ds.schema();
        var rows = ds.collectAsList();

        // then
        Assertions.assertEquals(10, rows.size());
        Assertions.assertEquals(1, Arrays.stream(schema.fields()).filter(field -> field.name().equalsIgnoreCase("newCol") && field.dataType().sameType(DataTypes.BooleanType)).count());
        Assertions.assertEquals(10, rows.stream().map(row -> row.<Boolean>getAs("newCol")).filter(flag -> flag).count());
    }

    @Test
    void testFactoryWithMapInYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testTransformationComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

        // when
        var ds = factory.<Row>buildDataset("map");
        var rows = ds.collectAsList();

        // then
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(310, rows.get(0).<Long>getAs("sum(value)"));
    }

    @Test
    void testFactoryWithStreamCatalog() throws DatasetFactoryException, StreamingQueryException, TimeoutException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testStreamComponentsCatalog");
        var factory = ComponentDatasetFactory.of(RuntimeContext.init(sparkSession), catalog);

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