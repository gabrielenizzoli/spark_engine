package dataengine.pipeline.core.source.impl;

import dataengine.pipeline.core.source.DataSource;
import dataengine.spark.sql.udf.UdfCollection;
import dataengine.spark.transformation.SqlTransformations;
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
public class SparkSqlSource<T> implements DataSource<T> {

    @Nonnull
    String sql;
    @Nullable
    UdfCollection udfCollection;
    @Nullable
    Encoder<T> encoder;

    public Dataset<T> get() {
        return Objects.nonNull(encoder) ? getEncodedDataset() : (Dataset<T>) getRowDataset();
    }

    private Dataset<T> getEncodedDataset() {
        return getRowDataset().as(Objects.requireNonNull(encoder, "encoder must be specified"));
    }

    private Dataset<Row> getRowDataset() {
        return SqlTransformations.sqlSource(sql, udfCollection);
    }

}
