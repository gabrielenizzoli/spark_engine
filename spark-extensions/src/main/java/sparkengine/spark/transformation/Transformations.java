package sparkengine.spark.transformation;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import sparkengine.spark.sql.logicalplan.PlanMapperException;
import sparkengine.spark.sql.logicalplan.SqlCompiler;
import sparkengine.spark.sql.logicalplan.tableresolver.Table;
import sparkengine.spark.sql.udf.SqlFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Transformations {

    public static <S, D> DataTransformationN<S, D> empty(SparkSession sparkSession, Encoder<D> encoder) {
        return (sources) -> sparkSession.emptyDataset(encoder);
    }

    public static <S> DataTransformationN<S, Row> empty(SparkSession sparkSession) {
        return (sources) -> sparkSession.emptyDataFrame();
    }

    public static <S, D> DataTransformation<S, D> map(MapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.map(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> flatMap(FlatMapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.flatMap(map, encoder);
    }

    public static <S> DataTransformation<S, S> cache(StorageLevel storageLevel) {
        return s -> s.persist(storageLevel);
    }

    public static <S, D> DataTransformation<S, D> encodeAs(Encoder<D> encoder) {
        return s -> s.as(encoder);
    }

    public static <S> DataTransformation<S, S> verifySchemaWith(StructType testSchema) {
        return s -> {
            var sourceSchema = s.schema();
            if (!sourceSchema.sameType(testSchema)) {
                throw new TransformationException.InvalidSchema(String.format("dataset schema [%s] does not match expected schema [%s]", sourceSchema, testSchema), testSchema);
            }
            return s;
        };
    }

    public static <S> DataTransformation<S, Row> encodeAsRow() {
        return dataset -> {
            if (Row.class.isAssignableFrom(dataset.encoder().clsTag().runtimeClass()))
                return (Dataset<Row>) dataset;
            return dataset.toDF();
        };
    }

    public static <S> DataTransformation<S, Row> sql(@Nonnull String sourceName,
                                                     @Nonnull String sql) {
        return sql(sourceName, sql, Collections.emptyList());
    }

    public static <S> DataTransformation<S, Row> sql(@Nonnull String sourceName,
                                                     @Nonnull String sql,
                                                     @Nullable Collection<SqlFunction> sqlFunctions) {
        return (dataset) -> {
            var sparkSession = dataset.sparkSession();
            var tables = List.of(Table.ofDataset(sourceName, dataset));
            try {
                return SqlCompiler.sql(sparkSession, tables, sqlFunctions, sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }

    /**
     * Applies a sql transformation to all the input datasets of rows.
     *
     * @param sourceNames name of the input datasets
     * @param sql         sql statement
     * @return outcome of the transformation operation
     */
    public static DataTransformationN<Row, Row> sql(@Nonnull SparkSession sparkSession,
                                                    @Nullable List<String> sourceNames,
                                                    @Nonnull String sql) {
        return sql(sparkSession, sourceNames, sql, Collections.emptyList());
    }

    /**
     * Applies a sql transformation to all the input datasets of rows.
     *
     * @param nullableSourceNames name of the input datasets
     * @param sql                 sql statement
     * @param sqlFunctions        a list of sql functions to be resolved in the sql statement
     * @return outcome of the transformation operation
     */
    public static DataTransformationN<Row, Row> sql(@Nonnull SparkSession sparkSession,
                                                    @Nullable List<String> nullableSourceNames,
                                                    @Nonnull String sql,
                                                    @Nullable Collection<SqlFunction> sqlFunctions) {

        // cleanup names
        var sourceNames = nullableSourceNames == null ?
                List.<String>of() :
                nullableSourceNames.stream().map(String::strip).filter(source -> !source.isBlank()).collect(Collectors.toList());

        return (datasets) -> {

            if (datasets.size() != sourceNames.size()) {
                throw new TransformationException("datasets provided count (" + datasets.size() + ") different than source names count provided (" + sourceNames + ")");
            }

            var tables = sourceNames.isEmpty() ?
                    List.<Table>of() :
                    IntStream
                            .range(0, sourceNames.size())
                            .mapToObj(i -> Table.ofDataset(sourceNames.get(i), datasets.get(i)))
                            .collect(Collectors.toList());

            try {
                return SqlCompiler.sql(sparkSession, tables, sqlFunctions, sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }

    /**
     * Applies a sql transformation to the 2 input datasets.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sql         sql statement
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @return outcome of the transformation operation
     */
    public static <S1, S2> DataTransformation2<S1, S2, Row> sql(@Nonnull String sourceName1,
                                                                @Nonnull String sourceName2,
                                                                @Nonnull String sql) {
        return (s1, s2) -> {
            var sparkSession = s1.sparkSession();
            var tables = List.of(
                    Table.ofDataset(sourceName1, s1),
                    Table.ofDataset(sourceName2, s2));
            try {
                return SqlCompiler.sql(sparkSession, tables, Collections.emptyList(), sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }

    /**
     * Applies a sql transformation to the 3 input datasets.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sql         sql statement
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @return outcome of the transformation operation
     */
    public static <S1, S2, S3> DataTransformation3<S1, S2, S3, Row> sql(@Nonnull String sourceName1,
                                                                        @Nonnull String sourceName2,
                                                                        @Nonnull String sourceName3,
                                                                        @Nonnull String sql) {
        return (s1, s2, s3) -> {
            var sparkSession = s1.sparkSession();
            var tables = List.of(
                    Table.ofDataset(sourceName1, s1),
                    Table.ofDataset(sourceName2, s2),
                    Table.ofDataset(sourceName3, s3));
            try {
                return SqlCompiler.sql(sparkSession, tables, Collections.emptyList(), sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }

    /**
     * Applies a sql transformation to the 4 input datasets.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sql         sql statement
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @return outcome of the transformation operation
     */
    public static <S1, S2, S3, S4> DataTransformation4<S1, S2, S3, S4, Row> sql(@Nonnull String sourceName1,
                                                                                @Nonnull String sourceName2,
                                                                                @Nonnull String sourceName3,
                                                                                @Nonnull String sourceName4,
                                                                                @Nonnull String sql) {
        return (s1, s2, s3, s4) -> {
            var sparkSession = s1.sparkSession();
            var tables = List.of(
                    Table.ofDataset(sourceName1, s1),
                    Table.ofDataset(sourceName2, s2),
                    Table.ofDataset(sourceName3, s3),
                    Table.ofDataset(sourceName4, s4));
            try {
                return SqlCompiler.sql(sparkSession, tables, Collections.emptyList(), sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }

    /**
     * Applies a sql transformation to the 5 input datasets.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sql         sql statement
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @return outcome of the transformation operation
     */
    public static <S1, S2, S3, S4, S5> DataTransformation5<S1, S2, S3, S4, S5, Row> sql(@Nonnull String sourceName1,
                                                                                        @Nonnull String sourceName2,
                                                                                        @Nonnull String sourceName3,
                                                                                        @Nonnull String sourceName4,
                                                                                        @Nonnull String sourceName5,
                                                                                        @Nonnull String sql) {
        return (s1, s2, s3, s4, s5) -> {
            var sparkSession = s1.sparkSession();
            var tables = List.of(
                    Table.ofDataset(sourceName1, s1),
                    Table.ofDataset(sourceName2, s2),
                    Table.ofDataset(sourceName3, s3),
                    Table.ofDataset(sourceName4, s4),
                    Table.ofDataset(sourceName5, s5));
            try {
                return SqlCompiler.sql(sparkSession, tables, Collections.emptyList(), sql);
            } catch (PlanMapperException e) {
                throw new TransformationException("issues compiling sql: " + sql, e);
            }
        };
    }


}
