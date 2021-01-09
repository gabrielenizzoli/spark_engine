package dataengine.pipeline.runtime.builder.pipeline;

import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.pipeline.Pipelines;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.PipelineFactory;
import dataengine.pipeline.runtime.SimplePipelineFactory;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;

public class ModelPipelineFactory {

    public static PipelineFactory ofPipelines(Pipelines pipelines) {
        var datasetFactory = ComponentDatasetFactory.of(ComponentCatalogFromMap.of(pipelines.getComponents()));
        var datasetConsumerFactory = SinkDatasetConsumerFactory.of(SinkCatalogFromMap.of(pipelines.getSinks()));
        return SimplePipelineFactory.builder().datasetFactory(datasetFactory).datasetConsumerFactory(datasetConsumerFactory).build();
    }

}
