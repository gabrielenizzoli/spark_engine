package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;

@Value(staticConstructor = "of")
public class BatchConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    WriterFormatter.Batch<T> formatter;

    @Override
    public void readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is a streaming dataset");

        formatter.apply(dataset.write()).save();
    }

}