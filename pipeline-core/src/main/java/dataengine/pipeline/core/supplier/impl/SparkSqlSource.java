package dataengine.pipeline.core.supplier.impl;

import dataengine.pipeline.core.supplier.DatasetSupplier;
import dataengine.spark.sql.logicalplan.PlanMapperException;
import dataengine.spark.sql.logicalplan.SqlCompiler;
import dataengine.spark.sql.udf.SqlFunction;
import dataengine.spark.transformation.TransformationException;
import lombok.Builder;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

@Value
@Builder
public class SparkSqlSource<T> implements DatasetSupplier<T> {

    @Nonnull
    String sql;
    @Nullable
    Collection<SqlFunction> sqlFunctions;
    @Nullable
    Encoder<T> encoder;

    public Dataset<T> get() {
        return Objects.nonNull(encoder) ? getEncodedDataset() : (Dataset<T>) getRowDataset();
    }

    private Dataset<T> getEncodedDataset() {
        return getRowDataset().as(Objects.requireNonNull(encoder, "encoder must be specified"));
    }

    private Dataset<Row> getRowDataset() {
        try {
            return SqlCompiler.sql(sqlFunctions, sql);
        } catch (PlanMapperException e) {
            throw new TransformationException("issues compiling sql: " + sql, e);
        }
    }

}
