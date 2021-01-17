package dataengine.pipeline.runtime.builder.plan;

import dataengine.pipeline.model.component.catalog.ComponentCatalog;
import dataengine.pipeline.model.plan.Plan;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactory;
import dataengine.pipeline.runtime.datasetfactory.DatasetFactory;
import dataengine.pipeline.runtime.plan.PipelineName;
import dataengine.pipeline.runtime.plan.PlanFactory;
import dataengine.pipeline.runtime.plan.SimplePlanFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ModelPlanFactory {

    public static PlanFactory ofPlan(SparkSession sparkSession, Plan plan) {
        return ofPlan(sparkSession, plan, null);
    }

    public static PlanFactory ofPlan(@Nonnull SparkSession sparkSession,
                                     @Nonnull Plan plan,
                                     @Nullable Map<String, Dataset> predefinedDatasets) {
        return SimplePlanFactory.builder()
                .pipelineNames(getPipelineNames(plan))
                .datasetFactory(getDatasetFactory(sparkSession, plan, predefinedDatasets))
                .datasetConsumerFactory(getConsumerFactory(plan))
                .build();
    }

    @Nonnull
    private static List<PipelineName> getPipelineNames(@Nonnull Plan plan) {
        return plan.getPipelines().stream()
                .map(pipeline -> PipelineName.of(pipeline.getSource(), pipeline.getSink()))
                .collect(Collectors.toList());
    }

    private static DatasetFactory getDatasetFactory(@Nonnull SparkSession sparkSession,
                                                    @Nonnull Plan plan,
                                                    @Nullable Map<String, Dataset> predefinedDatasets) {
        var componentCatalog = ComponentCatalog.ofMap(plan.getComponents());
        var datasetFactory = ComponentDatasetFactory.of(sparkSession, componentCatalog);

        return Optional.ofNullable(predefinedDatasets)
                .filter(newCache -> !newCache.isEmpty())
                .map(newCache -> datasetFactory.withDatasetCache(new HashMap<>(newCache)))
                .orElse(datasetFactory);
    }

    private static DatasetConsumerFactory getConsumerFactory(@Nonnull Plan plan) {
        var sinkCatalog = SinkCatalogFromMap.of(plan.getSinks());
        return SinkDatasetConsumerFactory.of(sinkCatalog);
    }



}
