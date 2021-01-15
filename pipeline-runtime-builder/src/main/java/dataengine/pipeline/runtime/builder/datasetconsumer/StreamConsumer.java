package dataengine.pipeline.runtime.builder.datasetconsumer;

import dataengine.pipeline.model.sink.impl.StreamSink;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumer;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerException;
import dataengine.pipeline.runtime.datasetconsumer.DatasetConsumerFactoryException;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Value(staticConstructor = "of")
public class StreamConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    WriterFormatter.Stream<T> formatter;

    @Override
    public DatasetConsumer<T> readFrom(Dataset<T> dataset) throws DatasetConsumerException {
        if (!dataset.isStreaming())
            throw new DatasetConsumerException("input dataset is not a streaming dataset");

        try {
            formatter.apply(dataset.writeStream()).start();
        } catch (TimeoutException e) {
            throw new DatasetConsumerException("error starting stream", e);
        }

        return this;
    }

}
