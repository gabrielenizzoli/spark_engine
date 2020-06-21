package dataengine.pipeline.core.source.impl;

import dataengine.pipeline.core.source.DataSource;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Value
@Builder
public class SparkSqlSource<T> implements dataengine.pipeline.core.source.DataSource<T> {

    @Nonnull
    String sql;
    @Nullable
    Encoder<T> encoder;

    public static DataSource<Row> sql(String sql) {
        return SparkSqlSource.<Row>builder().sql(sql).build();
    }

    public Dataset<T> get() {
        return Objects.nonNull(encoder) ? getEncodedDataset() : (Dataset<T>) getRowDataset();
    }

    public Dataset<T> getEncodedDataset() {
        return getRowDataset().as(Objects.requireNonNull(encoder, "encoder must be specified"));
    }

    public Dataset<Row> getRowDataset() {
        return SparkSession.active().sql(sql);
    }

}
