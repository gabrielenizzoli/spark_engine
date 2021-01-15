package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.impl.BatchSink;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

@Value(staticConstructor = "of")
public class BatchConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    WriterFormatter.Batch<T> formatter;

    @Override
    public DatasetConsumer<T> readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is a streaming dataset");

        formatter.apply(dataset.write()).save();
        return this;
    }

}
