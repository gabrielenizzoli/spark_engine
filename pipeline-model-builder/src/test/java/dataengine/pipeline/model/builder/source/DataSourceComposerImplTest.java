package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.sink.impl.DataSinkCollectRows;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.composer.DataSourceComposerImpl;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

class DataSourceComposerImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DataSourceComposerException {

        // given
        ComponentCatalog componentCatalog = TestUtils.getComponentCatalog(null);
        DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(componentCatalog);

        // when
        DataSinkCollectRows<Row> dataSink = new DataSinkCollectRows<>();
        dataSourceComposer.lookup("tx").encodeAsRow().writeTo(dataSink);

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
    void testBuilderWithAggregation() throws DataSourceComposerException {

        // given
        ComponentCatalog componentCatalog = TestUtils.getComponentCatalog("testAggregationComponentsCatalog");
        DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(componentCatalog);

        // when
        DataSinkCollectRows<Row> dataSink = new DataSinkCollectRows<>();
        dataSourceComposer.lookup("tx")
                .cache(StorageLevel.MEMORY_ONLY())
                .encodeAsRow().writeTo(dataSink);

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
    void testBuilderWithBadComponents() {

        Assertions.assertThrows(DataSourceComposerException.class, () -> {
            DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(TestUtils.getComponentCatalog("testBadComponentsCatalog"));
            dataSourceComposer.lookup("tx");
        });

    }

}