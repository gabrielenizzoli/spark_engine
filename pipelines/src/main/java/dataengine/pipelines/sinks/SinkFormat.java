package dataengine.pipelines.sinks;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.streaming.DataStreamWriter;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

@Value
@Builder
public class SinkFormat<T> {

    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;
    @Nonnull
    @Singular
    List<String> partitionColumns;

    public static class Builder<T> {

        public Builder<T> path(String path) {
            return option(DataSourceOptions.PATH_KEY, path);
        }

    }

    DataFrameWriter<T> configureBatch(DataFrameWriter<T> writer) {
        return writer.format(format).options(options).partitionBy(partitionColumns.stream().toArray(String[]::new));
    }

    DataStreamWriter<T> configureStream(DataStreamWriter<T> writer) {
        return writer.format(format).options(options).partitionBy(partitionColumns.stream().toArray(String[]::new));
    }

}
