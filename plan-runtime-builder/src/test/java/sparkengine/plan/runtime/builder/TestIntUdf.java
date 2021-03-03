package sparkengine.plan.runtime.builder;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sparkengine.spark.sql.udf.Udf;

import javax.annotation.Nonnull;

public class TestIntUdf implements Udf {

    @Nonnull
    @Override
    public String getName() {
        return "testIntUdf";
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return DataTypes.IntegerType;
    }

    @Override
    public UDF2<Integer, Integer, Integer> getUdf2() {
        return Integer::sum;
    }
}
