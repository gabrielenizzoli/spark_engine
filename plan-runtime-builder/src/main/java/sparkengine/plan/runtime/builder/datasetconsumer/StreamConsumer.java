package sparkengine.plan.runtime.builder.datasetconsumer;

import lombok.Value;
import org.apache.spark.sql.Dataset;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumer;
import sparkengine.plan.runtime.datasetconsumer.DatasetConsumerException;

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
