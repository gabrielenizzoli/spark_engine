package dataengine.pipeline.datasetconsumer.utils;

import dataengine.pipeline.model.sink.SinkWithFormat;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.streaming.DataStreamWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

@Value
@Builder
public class DatasetWriterFormat {

    @Nonnull
    String format;
    @Nullable
    @Singular(ignoreNullCollections = true)
    Map<String, String> options;
    @Nullable
    @Singular(ignoreNullCollections = true)
    List<String> partitionColumns;

    public static class Builder {

        public Builder path(String path) {
            return option("path", path);
        }

    }

    public static DatasetWriterFormat getSinkFormat(SinkWithFormat sink) {
        return DatasetWriterFormat.builder()
                .format(sink.getFormat())
                .options(sink.getOptions())
                .partitionColumns(sink.getPartitionColumns())
                .build();
    }

    <T> DataFrameWriter<T> configureBatch(DataFrameWriter<T> writer) {
        writer = writer.format(format);
        if (options != null && !options.isEmpty())
            writer = writer.options(options);
        if (partitionColumns != null && !partitionColumns.isEmpty())
            writer = writer.partitionBy(partitionColumns.stream().toArray(String[]::new));
        return writer;
    }

    <T> DataStreamWriter<T> configureStream(DataStreamWriter<T> writer) {
        writer = writer.format(format);
        if (options != null && !options.isEmpty())
            writer = writer.options(options);
        if (partitionColumns != null && !partitionColumns.isEmpty())
            writer = writer.partitionBy(partitionColumns.stream().toArray(String[]::new));
        return writer;
    }

}
