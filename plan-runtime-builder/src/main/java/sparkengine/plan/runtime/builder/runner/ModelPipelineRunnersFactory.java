package sparkengine.plan.runtime.builder.runner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import sparkengine.plan.model.component.catalog.ComponentCatalog;
import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.model.sink.catalog.SinkCatalogFromMap;
import sparkengine.plan.runtime.builder.dataset.ComponentDatasetFactory;
import sparkengine.plan.runtime.builder.dataset.utils.GlobalUdfContextFactory;
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

    public static PipelineRunnersFactory ofPlan(SparkSession sparkSession, Plan plan) {
        return ofPlan(sparkSession, plan, null);
    }

    public static PipelineRunnersFactory ofPlan(@Nonnull SparkSession sparkSession,
                                                @Nonnull Plan plan,
                                                @Nullable Map<String, Dataset> predefinedDatasets) {

        GlobalUdfContextFactory.get().orElseGet(() -> GlobalUdfContextFactory.init(sparkSession));

        return SimplePipelineRunnersFactory.builder()
                .pipelineDefinitions(getPipelineDefinitions(plan))
                .datasetFactory(getDatasetFactory(sparkSession, plan, predefinedDatasets))
                .datasetConsumerFactory(getConsumerFactory(plan))
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
