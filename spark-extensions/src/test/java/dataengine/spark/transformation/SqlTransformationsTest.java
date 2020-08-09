package dataengine.spark.transformation;

import dataengine.spark.sql.PlanMapperException;
import dataengine.spark.sql.relation.RelationResolverException;
import dataengine.spark.sql.udf.FunctionResolverException;
import dataengine.spark.sql.udf.Udf;
import dataengine.spark.sql.udf.UdfCollection;
import dataengine.spark.test.SparkSessionBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTransformationsTest extends SparkSessionBase {

    @Test
    void sql() {

        // given
        DataTransformation<Row, Row> sqlTranformation = SqlTransformations.sql(
                "table",
                "select testFunction(value) from table limit 1",
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i+1)));

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
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i+1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
                "table",
                "select testFunction(value) from table",
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i*10)));

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
    void sqlMultipleTransformationsWithFailedFunctionResolution() {

        // given
        DataTransformation<Row, Row> sqlTranformation1 = SqlTransformations.sql(
                "table",
                "select testFunction(value) as value from table",
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i+1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
                "table",
                "select testFunction(value) from table");

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when - then
        assertThrows(FunctionResolverException.class, () -> {
            Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);
            outputDs.count();
        });
    }

    @Test
    void sqlMultipleTransformationsWithFailedTableResolution() {

        // given
        DataTransformation<Row, Row> sqlTranformation1 = SqlTransformations.sql(
                "table",
                "select testFunction(value) as value from table",
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i+1)));

        DataTransformation<Row, Row> sqlTranformation2 = SqlTransformations.sql(
                "table2",
                "select testFunction(value) from table",
                UdfCollection.of(Udf.<Integer, Integer>ofUdf1("testFunction", DataTypes.IntegerType, i -> i+1)));

        Dataset<Row> dsInput = sparkSession.createDataFrame(
                Collections.singletonList(RowFactory.create(1)),
                DataTypes.createStructType(Collections.singletonList(DataTypes.createStructField("value", DataTypes.IntegerType, true))));

        // when - then
        assertThrows(RelationResolverException.class, () -> {
            Dataset<Row> outputDs = sqlTranformation1.andThen(sqlTranformation2).apply(dsInput);
            outputDs.count();
        });
    }

}