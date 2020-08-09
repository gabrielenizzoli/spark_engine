package dataengine.spark.sql;

import dataengine.spark.sql.relation.RelationResolver;
import dataengine.spark.sql.udf.FunctionResolver;
import dataengine.spark.sql.udf.Udf;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;


public class SparkSqlPlanMapperTest extends SparkSessionBase {

    @Test
    public void testSqlUdfsResolver() throws PlanMapperException {

        // given
        SparkSqlPlanMapper resolver = SparkSqlPlanMapper.builder()
                .planMapper(FunctionResolver.builder()
                        .udf(new Udf() {
                            @Nonnull
                            public String getName() {
                                return "plusOne";
                            }

                            @Nonnull
                            public DataType getReturnType() {
                                return DataTypes.IntegerType;
                            }

                            public UDF1<Integer, Integer> getUdf1() {
                                return (Integer i) -> i + 1;
                            }
                        })
                        .build())
                .build();

        // when
        Dataset<Row> datasetResolved = resolver.mapAsDataset(sparkSession, "select * from (select plusOne(plusOne(-2)) as value, (select plusOne(9)) as ten)");

        // then
        Assertions.assertEquals(Collections.singletonList(0), datasetResolved.select("value").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(10), datasetResolved.select("ten").as(Encoders.INT()).collectAsList());

    }

    @Test
    public void testSqlResolver() throws ParseException, PlanMapperException {
        // given
        LogicalPlan logicalPlanWithData = sparkSession.sql("select 100 as value").logicalPlan();
        SparkSqlPlanMapper resolver = SparkSqlPlanMapper.builder()
                .planMapper(RelationResolver.builder().plan("table", logicalPlanWithData).build())
                .build();

        // when
        Dataset<Row> datasetResolved = resolver
                .mapAsDataset(sparkSession, "select * from (select value+1 as valueWithOperation, (select value from table) as valueSubquery from table)");

        // then
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(100), datasetResolved.select("valueSubquery").as(Encoders.INT()).collectAsList());
    }

    @Test
    public void testAllResolvers() throws PlanMapperException {
        // given
        LogicalPlan logicalPlanWithData = sparkSession.sql("select 100 as value").logicalPlan();
        SparkSqlPlanMapper resolver = SparkSqlPlanMapper.builder()
                .planMapper(RelationResolver.builder().plan("table", logicalPlanWithData).build())
                .planMapper(FunctionResolver.builder()
                        .udf(new Udf() {
                            @Nonnull
                            public String getName() {
                                return "plusOne";
                            }

                            @Nonnull
                            public DataType getReturnType() {
                                return DataTypes.IntegerType;
                            }

                            public UDF1<Integer, Integer> getUdf1() {
                                return (Integer i) -> i + 1;
                            }
                        })
                        .build())
                .build();

        // when
        Dataset<Row> datasetResolved = resolver
                .mapAsDataset(sparkSession, "select * from (select value+1 as valueWithOperation, (select plusOne(value) from table) as valueSubquery from table)");

        // then
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueSubquery").as(Encoders.INT()).collectAsList());
    }

}
