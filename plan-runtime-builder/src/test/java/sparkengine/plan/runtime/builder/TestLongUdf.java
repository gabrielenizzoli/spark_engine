package sparkengine.plan.runtime.builder;

import sparkengine.spark.sql.udf.Udf;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import javax.annotation.Nonnull;

public class TestLongUdf implements Udf {

    @Nonnull
    @Override
    public String getName() {
        return "testLongUdf";
    }

    @Nonnull
    @Override
    public DataType getReturnType() {
        return DataTypes.LongType;
    }

    @Override
    public UDF2<Long, Integer, Long> getUdf2() {
        return (l, i) -> l + i;
    }
}
