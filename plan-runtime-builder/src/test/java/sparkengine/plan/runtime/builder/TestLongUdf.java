package sparkengine.plan.runtime.builder;

import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sparkengine.spark.sql.udf.UdfDefinition;

import javax.annotation.Nonnull;

public class TestLongUdf implements UdfDefinition {

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
    public UDF2<Long, Integer, Long> asUdf2() {
        return (l, i) -> l + i;
    }
}
