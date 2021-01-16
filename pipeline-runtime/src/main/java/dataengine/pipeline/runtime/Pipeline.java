package dataengine.pipeline.runtime;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value
@Builder
public class Pipeline<T> {

    @Nonnull
    Dataset<T> dataset;
    @Nonnull
    DatasetConsumer<T> datasetConsumer;

    public void run() throws DatasetConsumerException {
        datasetConsumer.readFrom(dataset);
    }

}
