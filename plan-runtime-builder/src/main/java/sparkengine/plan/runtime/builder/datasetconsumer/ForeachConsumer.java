package sparkengine.plan.runtime.builder.datasetconsumer;

import sparkengine.plan.model.plan.Plan;
import sparkengine.plan.runtime.builder.ModelPipelineRunnersFactory;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
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
                var planFactory = ModelPipelineRunnersFactory.ofPlan(batchDataset.sparkSession(), plan, Map.of(batchComponentName, (Dataset<Object>)ds));
                for (var pipelineName : planFactory.getPipelineNames()) {
                    planFactory.buildPipelineRunner(pipelineName).run();
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

}
