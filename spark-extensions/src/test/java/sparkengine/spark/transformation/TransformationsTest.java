package sparkengine.spark.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import sparkengine.spark.sql.logicalplan.functionresolver.Function;
import sparkengine.spark.sql.logicalplan.functionresolver.FunctionResolverException;
import sparkengine.spark.sql.logicalplan.tableresolver.TableResolverException;
import sparkengine.spark.sql.udf.UdfDefinition;
import sparkengine.spark.test.SparkSessionManager;
import sparkengine.spark.utils.UdafIntegerSummer;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TransformationsTest extends SparkSessionManager {

    @Test
    void sql() {

        // given
        DataTransformation<Row, Row> sqlTranformation = Transformations.sql(
                "table",
                "select testFunction(value) from table limit 1",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when
        Dataset<Row> outputDs = sqlTranformation.apply(dsInput);

        // then
        List<Integer> data = outputDs.as(Encoders.INT()).collectAsList();
        assertEquals(1, data.size());
        assertEquals(2, data.get(0));
    }

    @Test
    void sqlMultipleTransformations() {

        // given
        DataTransformation<Row, Row> sqlTranformation1 = Transformations.sql(
                "table",
                "select testFunction(value) as value from table",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = Transformations.sql(
                "table",
                "select testFunction(value) from table",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i * 10)));

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when
        Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);

        // then
        List<Integer> data = outputDs.as(Encoders.INT()).collectAsList();
        assertEquals(1, data.size());
        assertEquals(20, data.get(0));

    }

    @Test
    public void sqlWithUdaf() throws Throwable {

        // given
        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Arrays.asList(RowFactory.create("a", 1), RowFactory.create("a", 2), RowFactory.create("b", 1)),
                DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("key", DataTypes.StringType, true),
                        DataTypes.createStructField("value", DataTypes.IntegerType, true)
                )));

        DataTransformation<Row, Row> sqlTranformation = Transformations.sql(
                "table",
                "select key, addOne(summer(addOne(value))) from table group by key",
                Function.ofSqlFunctions(
                        UdfDefinition.<Integer, Integer>wrapUdf1("addOne", DataTypes.IntegerType, i -> i + 1),
                        new UdafIntegerSummer())
        );

        // when
        Dataset<Row> outputDs = sqlTranformation.apply(dsInput);

        // them
        Map<String, Integer> aggregatedData = outputDs.as(Encoders.tuple(Encoders.STRING(), Encoders.INT()))
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        Map<String, Integer> expectedMap = new HashMap<>();
        expectedMap.put("a", 6);
        expectedMap.put("b", 3);

        assertEquals(expectedMap, aggregatedData);

    }

    @Test
    void sqlMultipleTransformationsWithFailedFunctionResolution() {

        // given
        DataTransformation<Row, Row> sqlTranformation1 = Transformations.sql(
                "table",
                "select testFunction(value) as value from table",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = Transformations.sql(
                "table",
                "select testFunction(value) from table");

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when - then
        TransformationException e = assertThrows(TransformationException.class, () -> {
            Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);
            outputDs.count();
        });

        assertEquals(FunctionResolverException.class, e.getCause().getClass());
    }

    @Test
    void sqlMultipleTransformationsWithFailedTableResolution() {

        // given
        DataTransformation<Row, Row> sqlTranformation1 = Transformations.sql(
                "table",
                "select testFunction(value) as value from table",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = Transformations.sql(
                "table2",
                "select testFunction(value) from table",
                Function.ofSqlFunctions(UdfDefinition.<Integer, Integer>wrapUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when - then
        TransformationException e = assertThrows(TransformationException.class, () -> {
            Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);
            outputDs.count();
        });

        assertEquals(TableResolverException.class, e.getCause().getClass());
    }

    @Test
    void sqlWithJoin() {

        // given
        var ds1 = sparkSession.createDataset(Arrays.asList(1, 2, 3), Encoders.INT());
        var ds2 = sparkSession.createDataset(Arrays.asList(2, 3, 4), Encoders.INT());

        // when
        DataTransformation2<Integer, Integer, Row> join = Transformations
                .sql("source1", "source2", "select source1.value from source1 join source2 on source1.value = source2.value");

        // then
        assertEquals(List.of(2, 3), join.apply(ds1, ds2).select("value").as(Encoders.INT()).collectAsList());

    }

    @Test
    void testSchema() {

        // given
        var schema = DataTypes.createStructType(List.of(
                DataTypes.createStructField("key", DataTypes.StringType, true),
                DataTypes.createStructField("value", DataTypes.IntegerType, true)));
        var src = sparkSession.createDataFrame(List.of(), schema);
        var tx = Transformations.<Row>verifySchemaWith(schema);

        System.out.println(schema.toDDL());

        // when
        var dst = tx.apply(src);

        // then
        assertEquals(schema, dst.schema());
    }

    @Test
    void testBadSchema() {

        // given
        var schemaSrc = DataTypes.createStructType(List.of(
                DataTypes.createStructField("n1", DataTypes.StringType, true),
                DataTypes.createStructField("n2", DataTypes.IntegerType, true)));
        var src = sparkSession.createDataFrame(List.of(), schemaSrc);

        var schemaTest = DataTypes.createStructType(List.of(
                DataTypes.createStructField("n2", DataTypes.IntegerType, true),
                DataTypes.createStructField("n1", DataTypes.StringType, true)));
        var tx = Transformations.<Row>verifySchemaWith(schemaTest);

        // when
        assertThrows(TransformationException.InvalidSchema.class, () -> tx.apply(src));

    }

}