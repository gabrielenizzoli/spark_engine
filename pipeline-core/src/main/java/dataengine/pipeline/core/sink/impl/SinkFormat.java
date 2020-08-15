package dataengine.pipeline.core.sink.impl;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.streaming.DataStreamWriter;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

@Value
@Builder
public class SinkFormat {

    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;
    @Nonnull
    @Singular
    List<String> partitionColumns;

    public static class Builder {

        public Builder path(String path) {
            return option("path", path);
        }

    }

    <T> DataFrameWriter<T> configureBatch(DataFrameWriter<T> writer) {
        writer = writer.format(format).options(options);
        if (!partitionColumns.isEmpty())
            writer = writer.partitionBy(partitionColumns.stream().toArray(String[]::new));
        return writer;
    }

    <T> DataStreamWriter<T> configureStream(DataStreamWriter<T> writer) {
        writer = writer.format(format).options(options);
        if (!partitionColumns.isEmpty())
            writer = writer.partitionBy(partitionColumns.stream().toArray(String[]::new));
        return writer;
    }

}
