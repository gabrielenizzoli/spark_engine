package dataengine.spark.transformation;

import dataengine.spark.sql.LogicalPlanMapper;
import dataengine.spark.sql.SparkSqlPlanMapper;
import dataengine.spark.sql.relation.RelationResolver;
import lombok.SneakyThrows;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class SqlTransformations {

    public static SparkSqlPlanMapper planResolver(LogicalPlanMapper planMapper) {
        return SparkSqlPlanMapper.builder().planMapper(planMapper).build();
    }

    public static SparkSqlPlanMapper planResolvers(LogicalPlanMapper... planMappers) {
        return SparkSqlPlanMapper.builder().planMappers(Arrays.asList(planMappers)).build();
    }

    public static <S> DataTransformation<S, Row> sql(@Nonnull String sourceName1, @Nonnull String sql) {
        return (s1) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    // TODO remove this one and find a better solution!!
    @SneakyThrows
    public static Dataset<Row> getRowDataset(SparkSqlPlanMapper resolver, @Nonnull String sql) {
        return resolver.mapAsDataset(SparkSession.active(), sql);
    }

    public static DataTransformationN<Row, Row> sqlMerge(@Nonnull List<String> sourceNames, @Nonnull String sql) {
        return (datasets) -> {

            RelationResolver.RelationResolverBuilder builder = RelationResolver.builder();
            for (int i = 0; i < sourceNames.size(); i++)
                builder.plan(sourceNames.get(i), datasets.get(i).logicalPlan());
            SparkSqlPlanMapper resolver = planResolver(builder.build());

            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @return outcome of the merge operation
     */
    public static <S1, S2> DataTransformation2<S1, S2, Row> sqlMerge(@Nonnull String sourceName1,
                                                                     @Nonnull String sourceName2,
                                                                     @Nonnull String sql) {
        return (s1, s2) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    // start of code generated by a utility

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3> DataTransformation3<S1, S2, S3, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sql) {
        return (s1, s2, s3) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4> DataTransformation4<S1, S2, S3, S4, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sql) {
        return (s1, s2, s3, s4) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5> DataTransformation5<S1, S2, S3, S4, S5, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sourceName6 name of input dataset #6
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @param <S6>        type of the input dataset #6
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6> DataTransformation6<S1, S2, S3, S4, S5, S6, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sourceName6,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5, s6) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .plan(sourceName6, s6.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sourceName6 name of input dataset #6
     * @param sourceName7 name of input dataset #7
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @param <S6>        type of the input dataset #6
     * @param <S7>        type of the input dataset #7
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7> DataTransformation7<S1, S2, S3, S4, S5, S6, S7, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sourceName6,
            @Nonnull String sourceName7,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5, s6, s7) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .plan(sourceName6, s6.logicalPlan())
                    .plan(sourceName7, s7.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sourceName6 name of input dataset #6
     * @param sourceName7 name of input dataset #7
     * @param sourceName8 name of input dataset #8
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @param <S6>        type of the input dataset #6
     * @param <S7>        type of the input dataset #7
     * @param <S8>        type of the input dataset #8
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8> DataTransformation8<S1, S2, S3, S4, S5, S6, S7, S8, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sourceName6,
            @Nonnull String sourceName7,
            @Nonnull String sourceName8,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5, s6, s7, s8) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .plan(sourceName6, s6.logicalPlan())
                    .plan(sourceName7, s7.logicalPlan())
                    .plan(sourceName8, s8.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1 name of input dataset #1
     * @param sourceName2 name of input dataset #2
     * @param sourceName3 name of input dataset #3
     * @param sourceName4 name of input dataset #4
     * @param sourceName5 name of input dataset #5
     * @param sourceName6 name of input dataset #6
     * @param sourceName7 name of input dataset #7
     * @param sourceName8 name of input dataset #8
     * @param sourceName9 name of input dataset #9
     * @param sql         sql transformation
     * @param <S1>        type of the input dataset #1
     * @param <S2>        type of the input dataset #2
     * @param <S3>        type of the input dataset #3
     * @param <S4>        type of the input dataset #4
     * @param <S5>        type of the input dataset #5
     * @param <S6>        type of the input dataset #6
     * @param <S7>        type of the input dataset #7
     * @param <S8>        type of the input dataset #8
     * @param <S9>        type of the input dataset #9
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9> DataTransformation9<S1, S2, S3, S4, S5, S6, S7, S8, S9, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sourceName6,
            @Nonnull String sourceName7,
            @Nonnull String sourceName8,
            @Nonnull String sourceName9,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5, s6, s7, s8, s9) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .plan(sourceName6, s6.logicalPlan())
                    .plan(sourceName7, s7.logicalPlan())
                    .plan(sourceName8, s8.logicalPlan())
                    .plan(sourceName9, s9.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }

    /**
     * Applies a sql merge transformation to all the input DataPipes.
     *
     * @param sourceName1  name of input dataset #1
     * @param sourceName2  name of input dataset #2
     * @param sourceName3  name of input dataset #3
     * @param sourceName4  name of input dataset #4
     * @param sourceName5  name of input dataset #5
     * @param sourceName6  name of input dataset #6
     * @param sourceName7  name of input dataset #7
     * @param sourceName8  name of input dataset #8
     * @param sourceName9  name of input dataset #9
     * @param sourceName10 name of input dataset #10
     * @param sql          sql transformation
     * @param <S1>         type of the input dataset #1
     * @param <S2>         type of the input dataset #2
     * @param <S3>         type of the input dataset #3
     * @param <S4>         type of the input dataset #4
     * @param <S5>         type of the input dataset #5
     * @param <S6>         type of the input dataset #6
     * @param <S7>         type of the input dataset #7
     * @param <S8>         type of the input dataset #8
     * @param <S9>         type of the input dataset #9
     * @param <S10>        type of the input dataset #10
     * @return outcome of the merge operation
     */
    public static <S1, S2, S3, S4, S5, S6, S7, S8, S9, S10> DataTransformation10<S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, Row> sqlMerge(
            @Nonnull String sourceName1,
            @Nonnull String sourceName2,
            @Nonnull String sourceName3,
            @Nonnull String sourceName4,
            @Nonnull String sourceName5,
            @Nonnull String sourceName6,
            @Nonnull String sourceName7,
            @Nonnull String sourceName8,
            @Nonnull String sourceName9,
            @Nonnull String sourceName10,
            @Nonnull String sql) {
        return (s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) -> {
            SparkSqlPlanMapper resolver = planResolver(RelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .plan(sourceName4, s4.logicalPlan())
                    .plan(sourceName5, s5.logicalPlan())
                    .plan(sourceName6, s6.logicalPlan())
                    .plan(sourceName7, s7.logicalPlan())
                    .plan(sourceName8, s8.logicalPlan())
                    .plan(sourceName9, s9.logicalPlan())
                    .plan(sourceName10, s10.logicalPlan())
                    .build());
            return getRowDataset(resolver, sql);
        };
    }
}
