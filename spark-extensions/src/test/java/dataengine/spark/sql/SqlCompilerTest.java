package dataengine.spark.sql;

import dataengine.spark.sql.logicalplan.PlanMapperException;
import dataengine.spark.sql.logicalplan.SqlCompiler;
import dataengine.spark.sql.logicalplan.tableresolver.Table;
import dataengine.spark.test.SparkSessionBase;
import dataengine.spark.utils.UdafIntegerSummer;
import dataengine.spark.utils.UdfPlusOne;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class SqlCompilerTest extends SparkSessionBase {

    @Test
    public void testSqlUdfsResolver() throws PlanMapperException {

        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .functionResolver(new UdfPlusOne())
                .build();

        // when
        var datasetResolved = sqlCompiler.sql("select * from (select plusOne(plusOne(-2)) as value, (select plusOne(9)) as ten)");

        // then
        Assertions.assertEquals(Collections.singletonList(0), datasetResolved.select("value").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(10), datasetResolved.select("ten").as(Encoders.INT()).collectAsList());

    }

    @Test
    public void testTableResolver() throws ParseException, PlanMapperException {
        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(Table.ofDataset("table", sparkSession.sql("select 100 as value")))
                .build();

        // when
        var datasetResolved = sqlCompiler
                .sql("select * from (select value+1 as valueWithOperation, (select value from table) as valueSubquery from table)");

        // then
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(100), datasetResolved.select("valueSubquery").as(Encoders.INT()).collectAsList());
    }

    @Test
    public void testTableResolverWithMultipleTables() throws ParseException, PlanMapperException {
        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(Table.ofDataset("table", sparkSession.sql("select 100 as value")), Table.ofDataset("table2", sparkSession.sql("select 200 as value")))
                .build();

        // when
        Dataset<Row> datasetResolved = sqlCompiler
                .sql("select * from (select value+1 as valueWithOperation, (select value-1 from table2) as valueSubqueryFromTable2 from table)");

        // then
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(199), datasetResolved.select("valueSubqueryFromTable2").as(Encoders.INT()).collectAsList());
    }

    @Test
    public void testAllResolversWithUdf() throws PlanMapperException {
        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(Table.ofDataset("table", sparkSession.sql("select 100 as value")))
                .functionResolver(new UdfPlusOne())
                .build();

        // when
        var datasetResolved = sqlCompiler
                .sql("select * from (select value+1 as valueWithOperation, (select plusOne(value) from table) as valueSubquery from table)");

        // then
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueWithOperation").as(Encoders.INT()).collectAsList());
        Assertions.assertEquals(Collections.singletonList(101), datasetResolved.select("valueSubquery").as(Encoders.INT()).collectAsList());
    }

    @Test
    public void testAllResolversWithUdaf() throws PlanMapperException {

        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(Table.ofDataset("table", sparkSession.createDataset(List.of(1, 2, 3, 4), Encoders.INT())))
                .functionResolver(new UdafIntegerSummer())
                .build();

        // when
        var datasetResolved = sqlCompiler.sql("select summer(value) as value from table");

        // then
        Assertions.assertEquals(Collections.singletonList(10), datasetResolved.select("value").as(Encoders.INT()).collectAsList());

    }

    @Test
    void testSqlWithJoin() throws PlanMapperException {

        // given
        var sqlCompiler = SqlCompiler.builder()
                .sparkSession(sparkSession)
                .tableResolver(
                        Table.ofDataset("source1", sparkSession.createDataset(Arrays.asList(1, 2, 3), Encoders.INT())),
                        Table.ofDataset("source2", sparkSession.createDataset(Arrays.asList(2, 3, 4), Encoders.INT())))
                .functionResolver(new UdafIntegerSummer())
                .build();

        // when
        var datasetResolved = sqlCompiler.sql("select source1.value as value from source1 join source2 on source1.value = source2.value");

        // then
        Assertions.assertEquals(List.of(2, 3), datasetResolved.select("value").as(Encoders.INT()).collectAsList());

    }

}
