package sparkengine.spark.utils;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import sparkengine.spark.sql.udf.UdfDefinition;

import javax.annotation.Nonnull;

public class UdfPlusOne implements UdfDefinition {
    @Nonnull
    public String getName() {
        return "plusOne";
    }

    @Nonnull
    public DataType getReturnType() {
        return DataTypes.IntegerType;
    }

    public UDF1<Integer, Integer> asUdf1() {
        return i -> i + 1;
    }
}
