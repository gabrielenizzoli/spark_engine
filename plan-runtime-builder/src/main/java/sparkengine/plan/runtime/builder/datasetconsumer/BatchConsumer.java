package sparkengine.plan.runtime.builder.datasetconsumer;

import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;
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
