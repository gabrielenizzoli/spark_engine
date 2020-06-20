package dataengine.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;


public class SparkSqlUnresolvedRelationResolverTest {

    public static final String SQL1_SOURCE = "select 100 as value";
    public static final String SQL1_UNRESOLVED = "select value+1 as valueWithOperation from table";
    public static final String SQL1_TABLE = "table";

    static SparkSession sparkSession;

    @BeforeAll
    static void init() {
        sparkSession = SparkSession.builder().master("local").getOrCreate();
    }

    @AfterAll
    static void close() {
        sparkSession.close();
    }

    @Test
    public void testSqlResolver() throws ParseException {
        // given
        LogicalPlan logicalPlanWithData = sparkSession.sessionState().sqlParser().parsePlan(SQL1_SOURCE);
        SparkSqlUnresolvedRelationResolver resolver = SparkSqlUnresolvedRelationResolver.builder().plan(SQL1_TABLE, logicalPlanWithData).build();

        // when
        Dataset<Row> datasetResolved = resolver.resolveAsDataset(sparkSession, SQL1_UNRESOLVED);

        // then
        List<Integer> list = datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList();
        Assertions.assertEquals(Collections.singletonList(101), list);
    }

    @Test
    public void testSqlResolverWithExplosion() throws ParseException {
        // given
        LogicalPlan logicalPlanWithData = sparkSession.sessionState().sqlParser().parsePlan(SQL1_SOURCE);
        SparkSqlUnresolvedRelationResolver resolver = SparkSqlUnresolvedRelationResolver.builder().plan(SQL1_TABLE, logicalPlanWithData).build();

        // when
        Dataset<Row> datasetResolved = resolver.resolveAsDataset(sparkSession, SQL1_UNRESOLVED);

        // then
        List<Integer> list = datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList();
        Assertions.assertEquals(Collections.singletonList(101), list);
    }

}
