package dataengine.pipelines.sources;

import dataengine.pipelines.DataSource;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

@Value
@Builder
public class SparkSource<T> implements DataSource<T> {

    enum SourceType {
        BATCH,
        STREAM
    }

    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;
    @Nonnull
    SourceType type;
    @Nullable
    Encoder<T> encoder;
    @Nullable
    StructType schema;

    public static class Builder<T> {

        public Builder<T> stream() {
            return type(SourceType.STREAM);
        }

        public Builder<T> batch() {
            return type(SourceType.BATCH);
        }

        public Builder<Row> row() {
            return (Builder<Row>) this;
        }

        public Builder<String> text() {
            return ((Builder<String>) this).format("text").encoder(Encoders.STRING());
        }

        public Builder<Row> json() {
            return ((Builder<Row>) this).format("json");
        }

        public Builder<T> path(String path) {
            return option(DataSourceOptions.PATH_KEY, path);
        }

        public Dataset<T> getDataset() {
            return build().get();
        }

        public Dataset<T> getEncodedDataset() {
            return build().getEncodedDataset();
        }

        public Dataset<T> getEncodedDataset(Encoder<T> encoder) {
            return encoder(encoder).build().getEncodedDataset();
        }

        public Dataset<Row> getRowDataset() {
            return build().getRowDataset();
        }

    }

    public static Builder<Row> rowBuilder() {
        return builder().row();
    }

    @Override
    public Dataset<T> get() {
        return Objects.nonNull(encoder) ? getEncodedDataset() : (Dataset<T>) getRowDataset();
    }

    public Dataset<T> getEncodedDataset() {
        return getRowDataset().as(Objects.requireNonNull(encoder, "encoder must be specified"));
    }

    public Dataset<Row> getRowDataset() {
        switch (type) {
            case BATCH:
                return SparkSession.active().read().format(format).options(options).schema(schema).load();
            case STREAM:
                return SparkSession.active().readStream().format(format).options(options).schema(schema).load();
            default:
                throw new IllegalArgumentException("unmanaged dataset type: " + type);
        }
    }

}
