package dataengine.pipeline.runtime.builder.dataset;

import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.impl.InlineComponent;
import dataengine.pipeline.model.component.impl.SqlComponent;
import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentDatasetFactoryTest extends SparkSessionBase {

    @Test
    void testFactory() throws DatasetFactoryException {

        // given
        var componentMap = Map.of(
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder().withUsing(List.of("inlineSrc")).withSql("select col1 from inlineSrc where col2 like 'value22'").build()
        );
        var factory = ComponentDatasetFactory.of(sparkSession, ComponentCatalog.ofMap(componentMap));

        // when
        var data = factory.buildDataset("sql").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void testFactoryWithTwoSources() throws DatasetFactoryException {

        // given
        var componentMap = Map.of(
                "sqlSrc", SqlComponent.builder().withSql("select 'value01' as clo1, 'value22' as col2").build(),
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder().withUsing(List.of("inlineSrc", "sqlSrc")).withSql("select col1 from inlineSrc join sqlSrc on inlineSrc.col2 = sqlSrc.col2").build()
        );
        var catalog = ComponentCatalog.ofMap(componentMap);
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var data = factory.buildDataset("sql").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void testFactoryWithWrongName() throws DatasetFactoryException {

        // given
        var componentMap = Map.of(
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder().withUsing(List.of("wrongSrcName")).withSql("select col1 from inlineSrc where col2 like 'value22'").build()
        );
        var factory = ComponentDatasetFactory.of(sparkSession, ComponentCatalog.ofMap(componentMap));

        // then
        assertThrows(DatasetFactoryException.class, () -> factory.buildDataset("sql").as(Encoders.STRING()).collectAsList());

    }

    @Test
    void buildDatasetWithYamlCatalog() throws DatasetFactoryException {

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
    void buildDatasetWithAggregationInYamlCatalog() throws DatasetFactoryException {

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
    void buildDatasetWithTransformationInYamlCatalog() throws DatasetFactoryException {

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
    void buildDatasetWithBadYamlCatalog() throws DatasetFactoryException {

        // given
        var catalog = TestCatalog.getComponentCatalog("testBadComponentsCatalog");
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // then
        assertThrows(DatasetFactoryException.DatasetCircularReference.class, () -> factory.buildDataset("source2"));

    }

}