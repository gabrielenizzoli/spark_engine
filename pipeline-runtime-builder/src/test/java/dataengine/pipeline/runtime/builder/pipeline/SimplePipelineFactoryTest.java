package dataengine.pipeline.runtime.builder.pipeline;

import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.component.impl.SqlComponent;
import dataengine.pipeline.model.encoder.DataType;
import dataengine.pipeline.model.encoder.ValueEncoder;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.model.sink.impl.ViewSink;
import dataengine.pipeline.runtime.SimplePipelineFactory;
import dataengine.pipeline.runtime.builder.TestCatalog;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SimplePipelineFactoryTest extends SparkSessionBase {

    @Test
    void run() throws DatasetConsumerException, DatasetConsumerFactoryException, DatasetFactoryException {

        // given
        var datasetFactory = ComponentDatasetFactory.builder()
                .sparkSession(sparkSession)
                .componentCatalog(ComponentCatalogFromMap.of(
                        Map.<String, Component>of(
                                "sql",
                                SqlComponent.builder()
                                        .withSql("select 'value01' as col1")
                                        .withEncodedAs(ValueEncoder.builder().withType(DataType.STRING).build())
                                        .build()
                        )
                ))
                .build();
        var datasetConsumerFactory = SinkDatasetConsumerFactory.builder()
                .sinkCatalog(SinkCatalogFromMap.of(Map.of("get", ViewSink.builder().withName("view").build())))
                .build();

        var pipe = SimplePipelineFactory.builder()
                .datasetConsumerFactory(datasetConsumerFactory)
                .datasetFactory(datasetFactory).build();

        // when
        pipe.<String>buildPipeline("sql", "get").run();

        // then
        var list = sparkSession.sql("select * from view").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value01"), list);

    }

    @Test
    void runWithYamlCatalogs() throws DatasetConsumerException, DatasetConsumerFactoryException, DatasetFactoryException {

        // given

        var pipe = SimplePipelineFactory.builder()
                .datasetConsumerFactory(SinkDatasetConsumerFactory.of(TestCatalog.getSinkCatalog("testSinks")))
                .datasetFactory(ComponentDatasetFactory.of(sparkSession, TestCatalog.getComponentCatalog("testPipeline")))
                .build();

        // when
        pipe.<String>buildPipeline("sql", "collect").run();

        // then
        var list = sparkSession.sql("select * from tmpView").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value"), list);

    }

}