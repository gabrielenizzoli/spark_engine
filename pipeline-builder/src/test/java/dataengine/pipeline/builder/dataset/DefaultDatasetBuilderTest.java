package dataengine.pipeline.builder.dataset;

import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.ComponentCatalog;
import dataengine.pipeline.model.source.component.InlineSource;
import dataengine.pipeline.model.source.component.Sql;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultDatasetBuilderTest extends SparkSessionBase {

    @Test
    void buildDataset() throws DatasetBuilderException {

        // given
        Map<String, Component> componentMap = Map.of(
                "inlineSrc", InlineSource.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", Sql.builder().withUsing(List.of("inlineSrc")).withSql("select col1 from inlineSrc where col2 like 'value22'").build()
        );

        var catalog = (ComponentCatalog) ((name) -> Optional.ofNullable(componentMap.get(name)));

        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // when
        var data = builder.buildDataset("sql").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void buildDatasetTwoSources() throws DatasetBuilderException {

        // given
        Map<String, Component> componentMap = Map.of(
                "sqlSrc", Sql.builder().withSql("select 'value01' as clo1, 'value22' as col2").build(),
                "inlineSrc", InlineSource.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", Sql.builder().withUsing(List.of("inlineSrc", "sqlSrc")).withSql("select col1 from inlineSrc join sqlSrc on inlineSrc.col2 = sqlSrc.col2").build()
        );

        var catalog = (ComponentCatalog) ((name) -> Optional.ofNullable(componentMap.get(name)));
        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // when
        var data = builder.buildDataset("sql").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void buildDatasetWrongName() throws DatasetBuilderException {

        // given
        Map<String, Component> componentMap = Map.of(
                "inlineSrc", InlineSource.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", Sql.builder().withUsing(List.of("wrongSrcName")).withSql("select col1 from inlineSrc where col2 like 'value22'").build()
        );

        var catalog = (ComponentCatalog) ((name) -> Optional.ofNullable(componentMap.get(name)));

        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // then
        assertThrows(DatasetBuilderException.class, () -> builder.buildDataset("sql").as(Encoders.STRING()).collectAsList());

    }

    @Test
    void buildDatasetWithYamlCatalog() throws DatasetBuilderException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testComponentsCatalog");
        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // when
        var ds = builder.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then
        Assertions.assertEquals(
                Arrays.asList("a-p1:xxx-p2", "b-p1:yyy-p2"),
                rows.stream()
                        .map(r -> r.get(r.fieldIndex("str")) + ":" + r.getString(r.fieldIndex("str2")))
                        .sorted()
                        .collect(Collectors.toList())
        );
    }

    @Test
    void buildDatasetWithAggregationInYamlCatalog() throws DatasetBuilderException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testAggregationComponentsCatalog");
        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // when
        var ds = builder.<Row>buildDataset("tx");
        var rows = ds.collectAsList();

        // then
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
    void buildDatasetWithBadYamlCatalog() throws DatasetBuilderException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testBadComponentsCatalog");
        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // then
        assertThrows(DatasetBuilderException.CircularReference.class, () -> builder.buildDataset("source2"));

    }

    @Test
    void buildDatasetWithStreamCatalog() throws DatasetBuilderException, StreamingQueryException, TimeoutException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testStreamComponentsCatalog");
        var builder = DefaultDatasetBuilder.builder().componentCatalog(catalog).build();

        // when
        var ds = builder.<Row>buildDataset("tx");
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