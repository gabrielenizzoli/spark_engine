package dataengine.pipeline.runtime.builder.pipeline;

import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.pipeline.Plan;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.PipelineFactory;
import dataengine.pipeline.runtime.SimplePipelineFactory;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import org.apache.spark.sql.SparkSession;

public class ModelPipelineFactory {

    public static PipelineFactory ofPipelines(SparkSession sparkSession, Plan plan) {
        var datasetFactory = ComponentDatasetFactory.of(sparkSession, ComponentCatalogFromMap.of(plan.getComponents()));
        var datasetConsumerFactory = SinkDatasetConsumerFactory.of(SinkCatalogFromMap.of(plan.getSinks()));
        return SimplePipelineFactory.builder().datasetFactory(datasetFactory).datasetConsumerFactory(datasetConsumerFactory).build();
    }

}
