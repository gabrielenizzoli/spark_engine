package sparkengine.plan.runtime.builder.dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.impl.FragmentComponent;
import sparkengine.plan.model.component.impl.InlineComponent;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.component.impl.WrapperComponent;
import sparkengine.plan.runtime.datasetfactory.DatasetFactoryException;
import sparkengine.spark.test.SparkSessionManager;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ComponentDatasetFactoryTest extends SparkSessionManager {

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
                "sqlSrc", SqlComponent.builder().withSql("select 'value01' as col1, 'value22' as col2").build(),
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder().withUsing(List.of("inlineSrc", "sqlSrc")).withSql("select inlineSrc.col1 from inlineSrc join sqlSrc on inlineSrc.col2 = sqlSrc.col2").build()
        );
        var catalog = ComponentCatalog.ofMap(componentMap);
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var data = factory.buildDataset("sql").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void testFactoryWithFragment() throws DatasetFactoryException {

        // given
        var componentFragmentMap =Map.of(
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder()
                        .withUsing(List.of("inlineSrc", "sqlSrc"))
                        .withSql("select inlineSrc.col1 from inlineSrc join sqlSrc on inlineSrc.col2 = sqlSrc.col2")
                        .build()
        );
        var componentMap = Map.<String, Component>of(
                "sqlSrc", SqlComponent.builder()
                        .withSql("select 'value01' as col1, 'value22' as col2")
                        .build(),
                "fragment", FragmentComponent.builder()
                        .withUsing(List.of("sqlSrc"))
                        .withComponents(componentFragmentMap)
                        .withProviding("sql")
                        .build()
        );
        var catalog = ComponentCatalog.ofMap(componentMap);
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var data = factory.buildDataset("fragment").as(Encoders.STRING()).collectAsList();

        // then
        assertEquals(List.of("valueExpected"), data);

    }

    @Test
    void testFactoryWithWrapperAndFragment() throws DatasetFactoryException {

        // given
        var componentFragmentMap =Map.of(
                "inlineSrc", InlineComponent.builder()
                        .withData(List.of(Map.of("col1", "valueNotExpected", "col2", "value12"), Map.of("col1", "valueExpected", "col2", "value22")))
                        .withSchema(DataTypes.createStructType(List.of(DataTypes.createStructField("col1", DataTypes.StringType, true), DataTypes.createStructField("col2", DataTypes.StringType, true))).toDDL())
                        .build(),
                "sql", SqlComponent.builder()
                        .withUsing(List.of("inlineSrc", "sqlSrc"))
                        .withSql("select inlineSrc.col1 from inlineSrc join sqlSrc on inlineSrc.col2 = sqlSrc.col2")
                        .build()
        );
        var componentMap = Map.<String, Component>of(
                "sqlSrcExternal", SqlComponent.builder()
                        .withSql("select 'value01' as col1, 'value22' as col2")
                        .build(),
                "wrapper", WrapperComponent
                        .builder()
                        .withUsing(List.of("sqlSrcExternal"))
                        .withComponent(FragmentComponent.builder()
                                .withUsing(List.of("sqlSrc"))
                                .withComponents(componentFragmentMap)
                                .withProviding("sql")
                                .build())
                        .build()
        );
        var catalog = ComponentCatalog.ofMap(componentMap);
        var factory = ComponentDatasetFactory.of(sparkSession, catalog);

        // when
        var data = factory.buildDataset("wrapper").as(Encoders.STRING()).collectAsList();

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

}