package sparkengine.plan.runtime.runner.impl;

import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
import sparkengine.plan.runtime.runner.PipelineRunner;

import javax.annotation.Nonnull;

@Value
@Builder
public class SimplePipelineRunner<T> implements PipelineRunner {

    @Nonnull
    Dataset<T> dataset;
    @Nonnull
    DatasetConsumer<T> datasetConsumer;

    @Override
    public void run() throws DatasetConsumerException {
        datasetConsumer.readFrom(dataset);
    }

}
