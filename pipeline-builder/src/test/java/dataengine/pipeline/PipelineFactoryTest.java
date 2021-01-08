package dataengine.pipeline;

import dataengine.pipeline.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.datasetconsumer.impl.CollectConsumer;
import dataengine.pipeline.datasetconsumer.impl.SinkDatasetConsumerFactory;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.datasetfactory.impl.ComponentDatasetFactory;
import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.component.impl.SqlComponent;
import dataengine.pipeline.model.encoder.DataType;
import dataengine.pipeline.model.encoder.ValueEncoder;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.model.sink.impl.CollectSink;
import dataengine.spark.test.SparkSessionBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineFactoryTest extends SparkSessionBase {

    @Test
    void run() throws DatasetConsumerFactoryException, DatasetFactoryException {

        // given
        var datasetFactory = ComponentDatasetFactory.builder()
                .componentCatalog(ComponentCatalogFromMap.of(
                        Map.<String, Component>of(
                                "sql", SqlComponent.builder()
                                        .withSql("select 'value01' as col1")
                                        .withEncodedAs(ValueEncoder.builder().withType(DataType.STRING).build())
                                        .build()
                        )
                ))
                .build();
        var datasetConsumerFactory = SinkDatasetConsumerFactory.builder()
                .sinkCatalog(SinkCatalogFromMap.of(Map.of("get", CollectSink.builder().build())))
                .build();

        var pipe = PipelineFactory.builder()
                .datasetConsumerFactory(datasetConsumerFactory)
                .datasetFactory(datasetFactory).build();

        // when
        var list = ((CollectConsumer<String>) pipe.<String>buildPipeline("sql", "get").run()).getList();

        // then
        assertEquals(List.of("value01"), list);

    }

    @Test
    void runWithYamlCatalogs() throws DatasetConsumerFactoryException, DatasetFactoryException {

        // given

        var pipe = PipelineFactory.builder()
                .datasetConsumerFactory(SinkDatasetConsumerFactory.of(TestCatalog.getSinkCatalog("testSinks")))
                .datasetFactory(ComponentDatasetFactory.of(TestCatalog.getComponentCatalog("testPipeline")))
                .build();

        // when
        var list = ((CollectConsumer<String>) pipe.<String>buildPipeline("sql", "collect").run()).getList();

        // then
        assertEquals(List.of("value"), list);

    }

}