package dataengine.pipeline.runtime.builder.dataset.supplier;

import dataengine.pipeline.runtime.datasetfactory.DatasetFactoryException;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

@Value
@Builder
public class DatasetSupplierForSpark<T> implements DatasetSupplier<T> {

    public enum SourceType {
        BATCH,
        STREAM
    }

    @Nonnull
    SparkSession sparkSession;
    @Nonnull
    SourceType type;
    @Nonnull
    String format;
    @Nonnull
    @Singular
    Map<String, String> options;
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

        public Builder<String> text(String path) {
            return ((Builder<String>) this).format("text").encoder(Encoders.STRING()).path(path);
        }

        public Builder<Row> parquet(String path) {
            return row().format("parquet").path(path);
        }

        public Builder<Row> json(String path) {
            return row().format("json").path(path);
        }

        public Builder<T> path(String path) {
            return option("path", path);
        }

    }

    @Override
    public Dataset<T> provides() throws DatasetFactoryException {
        return Objects.nonNull(encoder) ? getEncodedDataset() : (Dataset<T>) getRowDataset();
    }

    private Dataset<T> getEncodedDataset() throws DatasetFactoryException {
        return getRowDataset().as(Objects.requireNonNull(encoder, "encoder must be specified"));
    }

    public Dataset<Row> getRowDataset() throws DatasetFactoryException {
        switch (type) {
            case BATCH: {
                var reader = sparkSession.read().format(format);
                if (!options.isEmpty())
                    reader = reader.options(options);
                if (schema != null)
                    reader = reader.schema(schema);
                return reader.load();
            }
            case STREAM: {
                var reader = sparkSession.readStream().format(format);
                if (!options.isEmpty())
                    reader = reader.options(options);
                if (schema != null)
                    reader = reader.schema(schema);
                return reader.load();
            }
            default:
                throw new DatasetFactoryException("unmanaged dataset type: " + type);
        }
    }

}
