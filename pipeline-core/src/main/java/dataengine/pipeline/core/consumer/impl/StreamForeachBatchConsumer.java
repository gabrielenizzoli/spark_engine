package dataengine.pipeline.core.consumer.impl;

import dataengine.pipeline.core.consumer.DatasetConsumer;
import dataengine.pipeline.core.supplier.DatasetSupplier;
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
public class StreamForeachBatchConsumer<T> implements DatasetConsumer<T> {

    @Nonnull
    String queryName;
    @Nonnull
    Trigger trigger;
    @Nullable
    OutputMode outputMode;
    @Nonnull
    DatasetConsumer<T> sink;

    @Override
    public void accept(Dataset<T> dataset) {
        if (!dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is not a streaming dataset");

        var writer = dataset.writeStream().queryName(queryName).trigger(trigger);
        Optional.ofNullable(outputMode).ifPresent(o -> writer.outputMode(outputMode));

        writer.foreachBatch((ds, time) -> {

            var cachedDataset = ds.persist();
            try {
                DatasetSupplier<T> datasetSupplier = () -> cachedDataset;
                datasetSupplier.writeTo(sink);
            } finally {
                cachedDataset.unpersist();
            }

        });

        try {
            writer.start();
        } catch (TimeoutException e) {
            throw new IllegalStateException("error starting stream", e);
        }
    }

}
