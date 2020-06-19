package dataengine.pipeline.sink;

import dataengine.pipeline.DataSink;
import dataengine.pipeline.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Value
@Builder
public class SparkStreamForeachBatchSink<T> implements DataSink<T> {

    @Nonnull
    String queryName;
    @Nonnull
    Trigger trigger;
    @Nullable
    OutputMode outputMode;
    @Nonnull
    DataSink<T> sink;


    @Override
    public void accept(Dataset<T> dataset) {
        if (!dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is not a streaming dataset");

        DataStreamWriter<T> writer = dataset.writeStream().queryName(queryName).trigger(trigger);
        Optional.ofNullable(outputMode).ifPresent(o -> writer.outputMode(outputMode));

        writer.foreachBatch((ds, time) -> {

            Dataset<T> cachedDataset = ds.persist();
            try {
                DataSource<T> dataSource = () -> cachedDataset;
                dataSource.write(sink);
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
