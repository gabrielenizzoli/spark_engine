package dataengine.pipelines.transformations;

import dataengine.pipelines.DataBiTransformation;
import dataengine.pipelines.DataPipe;
import dataengine.pipelines.DataTransformation;
import dataengine.pipelines.sql.SparkSqlUnresolvedRelationResolver;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.annotation.Nonnull;

public class Transformation {

    public static <S, D> DataTransformation<S, D> map(MapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.map(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> flatMap(FlatMapFunction<S, D> map, Encoder<D> encoder) {
        return s -> s.flatMap(map, encoder);
    }

    public static <S, D> DataTransformation<S, D> encode(Encoder<D> encoder) {
        return s -> s.as(encoder);
    }

    public static <S> DataTransformation<S, Row> sql(@Nonnull String sourceName, @Nonnull String sql) {
        return s -> {
            SparkSqlUnresolvedRelationResolver resolver = SparkSqlUnresolvedRelationResolver.builder().plan(sourceName, s.logicalPlan()).build();
            return resolver.resolveAsDataset(s.sparkSession(), sql);
        };
    }

    public static <S1, S2> DataBiTransformation<S1, S2, Row> sqlMerge(@Nonnull String sourceName1,
                                                                      @Nonnull String sourceName2,
                                                                      @Nonnull String sql) {
        return (s1, s2) -> {
            SparkSqlUnresolvedRelationResolver resolver = SparkSqlUnresolvedRelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .build();
            return resolver.resolveAsDataset(SparkSession.active(), sql);
        };
    }

    public static <S1, S2, S3> DataPipe.Data3Transformation<S1, S2, S3, Row> sqlMerge(@Nonnull String sourceName1,
                                                                                      @Nonnull String sourceName2,
                                                                                      @Nonnull String sourceName3,
                                                                                      @Nonnull String sql) {
        return (s1, s2, s3) -> {
            SparkSqlUnresolvedRelationResolver resolver = SparkSqlUnresolvedRelationResolver.builder()
                    .plan(sourceName1, s1.logicalPlan())
                    .plan(sourceName2, s2.logicalPlan())
                    .plan(sourceName3, s3.logicalPlan())
                    .build();
            return resolver.resolveAsDataset(SparkSession.active(), sql);
        };
    }

}
