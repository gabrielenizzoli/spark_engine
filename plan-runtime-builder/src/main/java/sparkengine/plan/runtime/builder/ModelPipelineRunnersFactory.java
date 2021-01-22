package sparkengine.plan.runtime.builder;

import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.Plan;
import sparkengine.plan.model.sink.catalog.SinkCatalogFromMap;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.PipelineName;
import sparkengine.plan.runtime.PipelineRunnersFactory;
import sparkengine.plan.runtime.impl.SimplePipelineRunnersFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ModelPipelineRunnersFactory {

    public static PipelineRunnersFactory ofPlan(SparkSession sparkSession, Plan plan) {
        return ofPlan(sparkSession, plan, null);
    }

    public static PipelineRunnersFactory ofPlan(@Nonnull SparkSession sparkSession,
                                                @Nonnull Plan plan,
                                                @Nullable Map<String, Dataset> predefinedDatasets) {
        return SimplePipelineRunnersFactory.builder()
                .pipelineNames(getPipelineNames(plan))
                .datasetFactory(getDatasetFactory(sparkSession, plan, predefinedDatasets))
                .datasetConsumerFactory(getConsumerFactory(plan))
                .build();
    }

    @Nonnull
    private static List<PipelineName> getPipelineNames(@Nonnull Plan plan) {
        return plan.getPipelines().stream()
                .map(pipeline -> PipelineName.of(pipeline.getComponent(), pipeline.getSink()))
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
