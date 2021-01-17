package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import lombok.Value;
import org.apache.spark.sql.Dataset;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeoutException;

@Value(staticConstructor = "of")
public class StreamConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    WriterFormatter.Stream<T> formatter;

    @Override
    public void readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (!dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is not a streaming dataset");

        try {
            formatter.apply(dataset.writeStream()).start();
        } catch (TimeoutException e) {
            throw new DatasetConsumerException("error starting stream", e);
        }
    }

}
