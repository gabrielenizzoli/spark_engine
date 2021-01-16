package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.component.catalog.ComponentCatalogFromMap;
import dataengine.pipeline.model.pipeline.Plan;
import dataengine.pipeline.model.sink.catalog.SinkCatalogFromMap;
import dataengine.pipeline.runtime.PipelineFactory;
import dataengine.pipeline.runtime.SimplePipelineFactory;
import dataengine.pipeline.runtime.builder.dataset.ComponentDatasetFactory;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class ForeachConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    WriterFormatter.Stream<T> formatter;
    @Nonnull
    String batchComponentName;
    @Nonnull
    Plan plan;

    @Override
    public void readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (!dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is not a streaming dataset");


        var writer = formatter.apply(dataset.writeStream());

        writer.foreachBatch((ds, time) -> {

            var batchDataset = plan.getPipelines().size() > 1 ? ds.persist() : ds;

            try {
                var pipelineFactory = getPipelineFactory(batchDataset);
                for (var pipeline : plan.getPipelines()) {
                    pipelineFactory.buildPipeline(pipeline.getSource(), pipeline.getSink()).run();
                }
            } finally {
                if (plan.getPipelines().size() > 1)
                    batchDataset.unpersist();
            }

        });

        try {
            writer.start();
        } catch (TimeoutException e) {
            throw new DatasetConsumerException("error starting stream", e);
        }
    }

    private PipelineFactory getPipelineFactory(Dataset<T> batchDataset) {
        var cache = new HashMap<String, Dataset<Object>>();
        cache.put(batchComponentName, (Dataset<Object>) batchDataset);

        var datasetFactory = ComponentDatasetFactory
                .builder()
                .sparkSession(batchDataset.sparkSession())
                .componentCatalog(ComponentCatalogFromMap.of(plan.getComponents()))
                .datasetCache(cache)
                .build();
        var datasetConsumerFactory = SinkDatasetConsumerFactory.of(SinkCatalogFromMap.of(plan.getSinks()));
        return SimplePipelineFactory.builder()
                .datasetFactory(datasetFactory)
                .datasetConsumerFactory(datasetConsumerFactory)
                .build();
    }


}
