package dataengine.pipelines.sinks;

import dataengine.pipelines.DataSink;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

@Value
@Builder
public class SparkStreamSink<T> implements DataSink<T> {

    @Nonnull
    SinkFormat<T> format;
    @Nonnull
    String queryName;
    @Nonnull
    Trigger trigger;
    @Nullable
    OutputMode outputMode;

    @Override
    public void accept(Dataset<T> dataset) {
        if (!dataset.isStreaming())
            throw new IllegalArgumentException("input dataset is not a streaming dataset");

        DataStreamWriter<T> writer = format.configureStream(dataset.writeStream()).queryName(queryName).trigger(trigger);
        Optional.ofNullable(outputMode).ifPresent(o -> writer.outputMode(outputMode));
        writer.start();
    }

}
