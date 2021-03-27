package sparkengine.plan.runtime.builder.runner;

import org.apache.spark.sql.Dataset;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.catalog.SinkCatalogFromMap;
import sparkengine.plan.runtime.builder.RuntimeContext;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.datasetconsumer.SinkDatasetConsumerFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerFactory;
import sparkengine.plan.runtime.datasetfactory.DatasetFactory;
import sparkengine.plan.runtime.runner.PipelineRunnersFactory;
import sparkengine.plan.runtime.runner.impl.SimplePipelineRunnersFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ModelPipelineRunnersFactory {

    public static PipelineRunnersFactory ofPlan(Plan plan, RuntimeContext runtimeContext) {
        return ofPlan(plan, runtimeContext, null);
    }

    public static PipelineRunnersFactory ofPlan(@Nonnull Plan plan,
                                                @Nonnull RuntimeContext runtimeContext,
                                                @Nullable Map<String, Dataset> predefinedDatasets) {

        return SimplePipelineRunnersFactory.builder()
                .pipelineDefinitions(getPipelineDefinitions(plan))
                .datasetFactory(getDatasetFactory(plan, runtimeContext, predefinedDatasets))
                .datasetConsumerFactory(getConsumerFactory(runtimeContext, plan))
                .build();
    }

    @Nonnull
    private static Map<String, SimplePipelineRunnersFactory.PipelineDefinition> getPipelineDefinitions(@Nonnull Plan plan) {
        return plan.getPipelines()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> SimplePipelineRunnersFactory.PipelineDefinition.of(e.getValue().getComponent(), e.getValue().getSink())));
    }

    private static DatasetFactory getDatasetFactory(@Nonnull Plan plan,
                                                    @Nonnull RuntimeContext runtimeContext,
                                                    @Nullable Map<String, Dataset> predefinedDatasets) {
        var componentCatalog = ComponentCatalog.ofMap(plan.getComponents());
        var datasetFactory = ComponentDatasetFactory.of(runtimeContext, componentCatalog);

        return Optional.ofNullable(predefinedDatasets)
                .filter(newCache -> !newCache.isEmpty())
                .map(newCache -> datasetFactory.withDatasetCache(new HashMap<>(newCache)))
                .orElse(datasetFactory);
    }

    private static DatasetConsumerFactory getConsumerFactory(RuntimeContext runtimeContext, @Nonnull Plan plan) {
        var sinkCatalog = SinkCatalogFromMap.of(plan.getSinks());
        return SinkDatasetConsumerFactory.of(runtimeContext, sinkCatalog);
    }


}
