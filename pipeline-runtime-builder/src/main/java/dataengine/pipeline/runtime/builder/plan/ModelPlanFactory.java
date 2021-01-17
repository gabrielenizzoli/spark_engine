package dataengine.pipeline.runtime.builder.plan;

import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.plan.Plan;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.runtime.plan.PipelineName;
import dataengine.pipeline.runtime.plan.PlanFactory;
import dataengine.pipeline.runtime.plan.SimplePlanFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ModelPlanFactory {

    public static PlanFactory ofPlan(SparkSession sparkSession, Plan plan) {
        return ofPlan(sparkSession, plan, null);
    }

    public static PlanFactory ofPlan(@Nonnull SparkSession sparkSession,
                                     @Nonnull Plan plan,
                                     @Nullable Map<String, Dataset<Object>> overrides) {

        var componentCatalog = ComponentCatalog.ofMap(plan.getComponents());
        var datasetFactory = ComponentDatasetFactory.of(sparkSession, componentCatalog);
        if (overrides != null && overrides.size() > 0) {
            datasetFactory = datasetFactory.toBuilder().datasetCache(new HashMap<String, Dataset<Object>>(overrides)).build();
        }

        var sinkCatalog = SinkCatalogFromMap.of(plan.getSinks());
        var datasetConsumerFactory = SinkDatasetConsumerFactory.of(sinkCatalog);

        var keys = plan.getPipelines().stream().map(pipeline -> PipelineName.of(pipeline.getSource(), pipeline.getSink())).collect(Collectors.toList());

        return SimplePlanFactory.builder()
                .pipelineNames(keys)
                .datasetFactory(datasetFactory)
                .datasetConsumerFactory(datasetConsumerFactory)
                .build();
    }

}
