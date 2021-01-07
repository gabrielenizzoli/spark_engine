package dataengine.pipeline.builder.utils;

import dataengine.pipeline.builder.dataset.DatasetBuilderException;
import dataengine.pipeline.model.source.component.SqlSource;
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
public class SqlSourceFactory<T> implements Factories.DatasetFactory<T> {

    @Nonnull
    String sql;
    @Nullable
    Collection<SqlFunction> sqlFunctions;
    @Nullable
    Encoder<T> encoder;

    public static SqlSourceFactory<?> of(SqlSource sqlSource) throws DatasetBuilderException {
        return SqlSourceFactory.builder()
                .sql(sqlSource.getSql())
                .sqlFunctions(UdfUtils.buildSqlFunctionCollection(sqlSource.getUdfs()))
                .encoder(EncoderUtils.buildEncoder(sqlSource.getEncodedAs()))
                .build();
    }

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
