package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.sink.factory.DataSinkFactoryBuilder;
import dataengine.pipeline.core.sink.factory.DataSinkFactoryException;
import dataengine.pipeline.core.sink.impl.DataSinkCollect;
import dataengine.pipeline.core.sink.impl.SinkFormat;
import dataengine.pipeline.core.sink.impl.SparkStreamSink;
import dataengine.pipeline.core.source.composer.DataSourceComposer;
import dataengine.pipeline.core.source.composer.DataSourceComposerException;
import dataengine.pipeline.core.source.composer.DataSourceComposerImpl;
import dataengine.pipeline.model.description.source.ComponentCatalog;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class DataSourceComposerImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DataSourceComposerException {

        // given
        ComponentCatalog componentCatalog = TestUtils.getComponentCatalog(null);
        DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(componentCatalog);

        DataSinkFactoryBuilder dataSinkFactoryBuilder = DataSinkFactoryBuilder
                .ofCatalog(TestUtils.getSinkCatalog());

        // when
        DataSinkCollect<Row> dataSink = (DataSinkCollect<Row>)dataSinkFactoryBuilder.<Row>lookup("collect");
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
    void testBuilderWithAggregation() throws DataSourceComposerException, DataSinkFactoryException {

        // given
        DataSourceComposer dataSourceComposer = DataSourceComposerImpl
                .ofCatalog(TestUtils.getComponentCatalog("testAggregationComponentsCatalog"));

        DataSinkFactoryBuilder dataSinkFactoryBuilder = DataSinkFactoryBuilder
                .ofCatalog(TestUtils.getSinkCatalog());

        // when
        DataSinkCollect<Row> dataSink = (DataSinkCollect<Row>)dataSinkFactoryBuilder.<Row>lookup("collect");
        List<Row> rows = dataSourceComposer.lookup("tx").encodeAsRow().writeTo(dataSink).getRows();

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
    void testStream() throws DataSourceComposerException, StreamingQueryException {

        // given
        ComponentCatalog componentCatalog = TestUtils.getComponentCatalog("testStreamComponentsCatalog");
        DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(componentCatalog);

        // when
        SparkStreamSink<Row> sparkSink = SparkStreamSink.<Row>builder()
                .queryName("memoryTable")
                .format(SinkFormat.builder().format("memory").build())
                .trigger(Trigger.ProcessingTime(1000))
                .outputMode(OutputMode.Append())
                .build();
        dataSourceComposer.lookup("tx").encodeAsRow().writeTo(sparkSink);
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

        Assertions.assertThrows(DataSourceComposerException.class, () -> {
            DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(TestUtils.getComponentCatalog("testBadComponentsCatalog"));
            dataSourceComposer.lookup("tx");
        });

    }

}