package dataengine.pipeline;

import dataengine.pipeline.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.datasetconsumer.utils.CollectConsumer;
import dataengine.pipeline.datasetfactory.ComponentDatasetFactory;
import dataengine.pipeline.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.model.encoder.DataType;
import dataengine.pipeline.model.encoder.ValueEncoder;
import dataengine.pipeline.model.sink.CollectSink;
import dataengine.pipeline.model.sink.SinkCatalogFromMap;
import dataengine.pipeline.model.source.Component;
import dataengine.pipeline.model.source.ComponentCatalogFromMap;
import dataengine.pipeline.model.source.component.Sql;
import dataengine.spark.test.SparkSessionBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineTest extends SparkSessionBase {

    @Test
    void run() throws DatasetConsumerException, DatasetFactoryException {

        // given
        var datasetFactory = ComponentDatasetFactory.builder()
                .componentCatalog(ComponentCatalogFromMap.of(
                        Map.<String, Component>of(
                                "sql", Sql.builder()
                                        .withSql("select 'value01' as col1")
                                        .withEncodedAs(ValueEncoder.builder().withType(DataType.STRING).build())
                                        .build()
                        )
                ))
                .build();
        var datasetConsumerFactory = SinkDatasetConsumerFactory.builder()
                .sinkCatalog(SinkCatalogFromMap.of(Map.of("get", CollectSink.builder().build())))
                .build();

        var pipe = Pipeline.builder()
                .datasetConsumerFactory(datasetConsumerFactory)
                .datasetFactory(datasetFactory).build();

        // when
        var list = ((CollectConsumer<String>) pipe.<String>run("sql", "get")).getList();

        // then
        assertEquals(List.of("value01"), list);

    }
}