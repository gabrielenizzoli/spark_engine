package dataengine.spark.transformation;

import dataengine.spark.sql.relation.RelationResolverException;
import dataengine.spark.sql.udf.FunctionResolverException;
import dataengine.spark.sql.udf.SqlFunctionCollection;
import dataengine.spark.sql.udf.Udaf;
import dataengine.spark.sql.udf.Udf;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTransformationsTest extends SparkSessionBase {

    @Test
    void sql() {

        // given
        DataTransformation<Row, Row> sqlTranformation = SqlTransformations.sql(
                "table",
                "select testFunction(value) from table limit 1",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

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
        DataTransformation<Row, Row> sqlTranformation1 = SqlTransformations.sql(
                "table",
                "select testFunction(value) as value from table",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
                "table",
                "select testFunction(value) from table",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i * 10)));

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

    public static class IntegerSummer extends Aggregator<Integer, Integer, Integer> {

        @Override
        public Integer zero() {
            return 0;
        }

        @Override
        public Integer reduce(Integer b, Integer a) {
            return a + b;
        }

        @Override
        public Integer merge(Integer b1, Integer b2) {
            return b1 + b2;
        }

        @Override
        public Integer finish(Integer reduction) {
            return reduction;
        }

        @Override
        public Encoder<Integer> bufferEncoder() {
            return Encoders.INT();
        }

        @Override
        public Encoder<Integer> outputEncoder() {
            return Encoders.INT();
        }

    }

    public static class SummerUdaf implements Udaf<Integer, Integer, Integer> {

        @Nonnull
        @Override
        public Encoder<Integer> getInputEncoder() {
            return Encoders.INT();
        }

        @Nonnull
        @Override
        public Aggregator<Integer, Integer, Integer> getAggregator() {
            return new IntegerSummer();
        }

        @Nonnull
        @Override
        public String getName() {
            return "summer";
        }
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

        DataTransformation<Row, Row> sqlTranformation = SqlTransformations.sql(
                "table",
                "select key, addOne(summer(addOne(value))) from table group by key",
                SqlFunctionCollection.of(
                        Udf.<Integer, Integer>ofUdf1("addOne", DataTypes.IntegerType, i -> i + 1),
                        new SummerUdaf())
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
        DataTransformation<Row, Row> sqlTranformation1 = SqlTransformations.sql(
                "table",
                "select testFunction(value) as value from table",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
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
        DataTransformation<Row, Row> sqlTranformation1 = SqlTransformations.sql(
                "table",
                "select testFunction(value) as value from table",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
                "table2",
                "select testFunction(value) from table",
                SqlFunctionCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i + 1)));

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when - then
        TransformationException e = assertThrows(TransformationException.class, () -> {
            Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);
            outputDs.count();
        });

        assertEquals(RelationResolverException.class, e.getCause().getClass());
    }

}