package dataengine.pipeline.model.builder.source;

import dataengine.pipeline.core.Pipeline;
import dataengine.pipeline.core.sink.DataSink;
import dataengine.pipeline.core.sink.composer.DataSinkComposer;
import dataengine.pipeline.core.sink.composer.DataSinkComposerException;
import dataengine.pipeline.core.sink.composer.DataSinkComposerImpl;
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
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class DataSourceComposerImplTest extends SparkSessionBase {

    @Test
    void testBuilder() throws DataSourceComposerException, DataSinkComposerException {

        // given
        Pipeline pipeline = TestUtils.getPipeline(null);

        // when
        DataSinkCollect<Row> dataSink = (DataSinkCollect<Row>)pipeline.<Row>run("tx", "collect");

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
    void testBuilderWithAggregation() throws DataSourceComposerException, DataSinkFactoryException, DataSinkComposerException {

        // given
        Pipeline pipeline = TestUtils.getPipeline("testAggregationComponentsCatalog");

        // when
        DataSinkCollect<Row> dataSink = (DataSinkCollect<Row>)pipeline.<Row>run("tx", "collect");

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
    void testStream() throws DataSourceComposerException, StreamingQueryException, DataSinkComposerException {

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

        Assertions.assertThrows(DataSourceComposerException.class, () -> {
            DataSourceComposer dataSourceComposer = DataSourceComposerImpl.ofCatalog(TestUtils.getComponentCatalog("testBadComponentsCatalog"));
            dataSourceComposer.lookup("tx");
        });

    }

}