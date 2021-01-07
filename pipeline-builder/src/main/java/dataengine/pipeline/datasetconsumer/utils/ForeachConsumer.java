package dataengine.pipeline.datasetconsumer.utils;

import dataengine.pipeline.datasetconsumer.DatasetConsumer;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class ForeachConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    String queryName;
    @Nonnull
    Trigger trigger;
    @Nullable
    OutputMode outputMode;
    @Nonnull
    DatasetConsumer<T> sink;

    @Override
    public DatasetConsumer<T> readFrom(Dataset<T> dataset) {
        if (!dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is not a streaming dataset");

        var writer = dataset.writeStream().queryName(queryName).trigger(trigger);
        Optional.ofNullable(outputMode).ifPresent(o -> writer.outputMode(outputMode));

        writer.foreachBatch((ds, time) -> {

            var cachedDataset = ds.persist();
            try {
                sink.readFrom(cachedDataset);
            } finally {
                cachedDataset.unpersist();
            }

        });

        try {
            writer.start();
        } catch (TimeoutException e) {
            throw new IllegalStateException("error starting stream", e);
        }

        return this;
    }

}
