package dataengine.pipeline.runtime.builder.plan;

import dataengine.pipeline.model.component.Component;
import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.component.impl.SqlComponent;
import dataengine.pipeline.model.encoder.DataType;
import dataengine.pipeline.model.encoder.ValueEncoder;
import dataengine.pipeline.model.sink.catalog.SinkCatalog;
import dataengine.pipeline.model.sink.impl.ViewSink;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import dataengine.pipeline.runtime.plan.PipelineName;
import dataengine.pipeline.runtime.plan.PlanFactoryException;
import dataengine.pipeline.runtime.plan.SimplePlanFactory;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class SimplePlanFactoryTest extends SparkSessionBase {

    @Test
    void testPipelineFactory() throws DatasetConsumerException, PlanFactoryException {

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
        var key = PipelineName.of("sql", "get");

        var factory = SimplePlanFactory.builder()
                .pipelineNames(List.of(key))
                .datasetFactory(datasetFactory)
                .datasetConsumerFactory(datasetConsumerFactory)
                .build();

        // when
        factory.buildPipelineRunner(key).run();

        // then
        var list = sparkSession.sql("select * from view").as(Encoders.STRING()).collectAsList();
        Assertions.assertEquals(List.of("value01"), list);

    }

}