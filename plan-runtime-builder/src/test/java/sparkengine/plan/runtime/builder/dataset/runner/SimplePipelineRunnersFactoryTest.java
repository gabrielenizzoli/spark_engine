package sparkengine.plan.runtime.builder.dataset.runner;

import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sparkengine.plan.model.component.Component;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.component.impl.SqlComponent;
import sparkengine.plan.model.encoder.DataType;
import sparkengine.plan.model.encoder.ValueEncoder;
import sparkengine.plan.model.sink.catalog.SinkCatalog;
import sparkengine.plan.model.sink.impl.ViewSink;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.runner.PipelineRunnersFactoryException;
import sparkengine.plan.runtime.runner.impl.SimplePipelineRunnersFactory;
import sparkengine.spark.test.SparkSessionBase;

import java.util.List;
import java.util.Map;

class SimplePipelineRunnersFactoryTest extends SparkSessionBase {

    @Test
    void testPipelineFactory() throws DatasetConsumerException, PipelineRunnersFactoryException {

        // given
        var datasetFactory = ComponentDatasetFactory.builder()
                .sparkSession(sparkSession)
                .componentCatalog(ComponentCatalog.ofMap(
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
                .sinkCatalog(SinkCatalog.ofMap(Map.of("get", ViewSink.builder().withName("view").build())))
                .build();

        var factory = SimplePipelineRunnersFactory.builder()
                .pipelineDefinitions(Map.of("pipe", SimplePipelineRunnersFactory.PipelineDefinition.of("sql", "get")))
                .datasetFactory(datasetFactory)
                .datasetConsumerFactory(datasetConsumerFactory)
                .build();

        // when
        factory.buildPipelineRunner("pipe").run();

        // then
        var list = sparkSession.sql("select * from view").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value01"), list);

    }

}