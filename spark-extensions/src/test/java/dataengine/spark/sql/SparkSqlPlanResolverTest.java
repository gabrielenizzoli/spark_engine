package dataengine.spark.sql;

import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;


public class SparkSqlPlanResolverTest extends SparkSessionBase {

    public static class TestUdf implements UDF1<Integer, Integer>, Serializable {
        @Override
        public Integer call(Integer integer) {
            return integer + 1;
        }
    }

    @Test
    public void testSqlUdfsResolver() throws ParseException {

        // given
        SparkSqlPlanResolver resolver = SparkSqlPlanResolver.builder()
                .planMapper(FunctionResolver.builder()
                        .functionFactory(
                                "plusOne",
                                UdfFactory.builder()
                                        .userDefinedFunction((Integer in) -> in+1)
                                        .returnType(DataTypes.IntegerType)
                                        .build())
                        .build())
                .build();

        // when
        Dataset<Row> datasetResolved = resolver.resolveAsDataset(sparkSession, "select plusOne(plusOne(-2)) as value, 1 as one");

        // then
        List<Integer> list = datasetResolved.select("value").as(Encoders.INT()).collectAsList();
        Assertions.assertEquals(Collections.singletonList(0), list);
    }

    @Test
    public void testSqlResolver() throws ParseException {
        // given
        LogicalPlan logicalPlanWithData = sparkSession.sessionState().sqlParser().parsePlan("select 100 as value");
        SparkSqlPlanResolver resolver = SparkSqlPlanResolver.builder()
                .planMapper(RelationResolver.builder().plan("table", logicalPlanWithData).build())
                .build();

        // when
        Dataset<Row> datasetResolved = resolver.resolveAsDataset(sparkSession, "select value+1 as valueWithOperation from table");

        // then
        List<Integer> list = datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList();
        Assertions.assertEquals(Collections.singletonList(101), list);
    }

}
