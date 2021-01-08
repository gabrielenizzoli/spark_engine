package dataengine.pipeline;

import dataengine.pipeline.datasetconsumer.DatasetConsumer;
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

    public DatasetConsumer<T> run() {
        datasetConsumer.readFrom(dataset);
        return datasetConsumer;
    }

}
